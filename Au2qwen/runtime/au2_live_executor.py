#!/usr/bin/env python3
"""AU2 LIVE EXECUTOR v4 — Parity-aligned, single decision path.

Entry decision flow per tick (flat only)
=========================================
  1. Risk gate       — RiskEngine.can_trade() + RED state check
  2. Feature extract — cvd, trend, vol, spread, trend30, range30 from state
  3. Dynamic thr     — SelectivityEngine.compute_dynamic_multiplier()
  4. Score           — SignalProcessor.score()  [called ONCE — V3 state update]
  5. Signal counter  — update from score direction
  6. Cluster check   — SelectivityEngine.is_clustered()
  7. Decision        — build_trade_decision()   [single authoritative call]
  8. State update    — last_score = dlog.score  [always, win or lose]
  9. Entry or log    — open position if dlog.approved

All entry quality logic lives in build_trade_decision() (au2_decision.py).
This class is a stateful dispatch shell; it does not contain quality checks.
"""
from __future__ import annotations
import sys, pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

import asyncio
import logging
import time
from collections import defaultdict
from typing import Optional

from au2_core import (
    CoreConfig, RiskEngine, PositionManager, SignalProcessor, PositionFill,
    PositionEvent, SizingEngine, Regime, SelectivityEngine, TradeGate,
    RiskState, PositionState, REGIME_PROFILES,
)
from au2_risk_overlay import RiskOverlay, RiskOverlayConfig
from au2_state_manager import StatePersistence
from au2_decision import build_trade_decision, TradeDecisionLog
from au2_decision_logger import DecisionLogger

log = logging.getLogger("au2_live")

# Guard: count LiveExecutor instances alive in this process.
# More than one shares the module-level _signal_v3 in au2_core — see ARCH note there.
_live_executor_instance_count: int = 0


class LiveExecutor:
    def __init__(self, cfg: CoreConfig, start_equity: float = 10_000.0,
                 live_mode: bool = False,
                 overlay: Optional[RiskOverlay] = None,
                 decision_log_path: Optional[str] = str(
                     pathlib.Path(__file__).resolve().parent.parent / "data" / "live" / "au2_decisions.jsonl"
                 )):
        """
        Parameters
        ----------
        decision_log_path : Path for structured JSONL decision log.
                            Pass None to disable file logging.
        """
        import warnings as _warnings
        global _live_executor_instance_count
        _live_executor_instance_count += 1
        if _live_executor_instance_count > 1:
            _warnings.warn(
                f"LiveExecutor instance #{_live_executor_instance_count} created in the same "
                "Python process.  _signal_v3 (RegimeAdaptiveSignal) in au2_core is a "
                "module-level singleton — all instances share its price buffer and will "
                "corrupt each other's macro-trend state.  "
                "Run one bot per Python process.",
                RuntimeWarning,
                stacklevel=2,
            )
        self.cfg       = cfg
        self.live_mode = live_mode
        self.overlay   = overlay or RiskOverlay(start_equity, RiskOverlayConfig(daily_profit_cap_pct=5.0))

        # ── Core evaluation components (shared, stateless) ────────────────────
        self.risk  = RiskEngine(cfg, start_equity)
        self.pm    = PositionManager(cfg)
        self.sel   = SelectivityEngine(cfg)
        self._gate = TradeGate(cfg)

        # ── Per-session state ─────────────────────────────────────────────────
        self.builder:        Optional[object] = None
        self.last_trade_ts:  float = 0.0
        self.signal_side:    str   = ""
        self.signal_count:   int   = 0
        self.signal_ts:      float = 0.0
        self._last_score:    float = 0.0   # score of the previous evaluated tick

        # ── Async plumbing ────────────────────────────────────────────────────
        self._pending_state: Optional[dict] = None
        self._ready = asyncio.Event()

        # ── Structured decision log ───────────────────────────────────────────
        self.dlog: Optional[DecisionLogger] = (
            DecisionLogger(decision_log_path) if decision_log_path else None
        )

        # ── Persistence ───────────────────────────────────────────────────────
        self.state_mgr = StatePersistence()
        self._load_or_init(start_equity)

        # ── Diagnostic counters (read-only from outside) ──────────────────────
        self._diag: dict = {
            "ticks_total":       0,
            "ticks_in_pos":      0,
            "ticks_risk_block":  0,
            "signals_nonflat":   0,
            "signals_approved":  0,
            "near_misses":       0,
            "rejection_counts":  defaultdict(int),
            "score_count":       0,
            "score_abs_sum":     0.0,
            "score_abs_min":     float("inf"),
            "score_abs_max":     0.0,
            "score_below_thr":   0,
            "last_mkt_log_ts":   0.0,
            "last_mkt_snapshot": {},
        }

    # ── Persistence ──────────────────────────────────────────────────────────

    def _load_or_init(self, equity: float) -> None:
        saved = self.state_mgr.load()
        if saved and saved.get("ts", 0) > time.time() - 86400:
            log.info("State restored from %s",
                     time.strftime("%H:%M", time.gmtime(saved["ts"])))
            self.risk.current_equity     = saved["equity"]
            self.last_trade_ts           = saved["last_trade_ts"]
            self.risk.consecutive_losses = saved["loss_streak"]
            self.risk.day_start_equity   = saved["equity"]
            self.overlay.current_equity  = saved["equity"]
            self.overlay.day_start_equity = saved["equity"]
            self._restore_position_from_checkpoint(saved)
        else:
            log.info("Fresh start or expired state.")

    def _restore_position_from_checkpoint(self, saved: dict) -> None:
        """Restore PositionState from checkpoint open_position dict (paper mode).

        BinanceExecutor overrides this with reconcile_on_start() (async, Binance API).
        """
        p = saved.get("open_position")
        if not p or not p.get("sl_price"):
            return
        try:
            regime = Regime(p.get("regime", Regime.TREND.value))
        except ValueError:
            regime = Regime.TREND
        profile = REGIME_PROFILES[regime]
        self.pm.pos = PositionState(
            side=p["side"], exec_price=float(p["exec_price"]),
            entry_ts=float(p.get("entry_ts", time.time())),
            initial_qty=float(p["initial_qty"]),
            remaining_qty=float(p.get("remaining_qty", p["initial_qty"])),
            score=float(p.get("score", self.cfg.threshold)),
            sl_price=float(p["sl_price"]), tp1_price=float(p["tp1_price"]),
            tp2_price=float(p["tp2_price"]),
            peak_price=float(p.get("peak_price", p["exec_price"])),
            trough_price=float(p.get("trough_price", p["exec_price"])),
            regime=regime.value, confidence=float(p.get("confidence", 0.90)),
            profile=profile, trail_price=float(p.get("trail_price", p["sl_price"])),
            tp1_done=bool(p.get("tp1_done", False)),
            tp2_done=bool(p.get("tp2_done", False)),
        )
        log.info("[H3] Position restored from checkpoint: %s %.4f BTC @ %.1f",
                 p["side"], self.pm.pos.remaining_qty, self.pm.pos.exec_price)

    # ── Async plumbing ────────────────────────────────────────────────────────

    def push_state(self, state: dict) -> None:
        self._pending_state = state
        self._ready.set()

    async def _safe_exec(self, coro, timeout: float = 5.0, retries: int = 3):
        for i in range(retries):
            try:
                return await asyncio.wait_for(coro(), timeout=timeout)
            except asyncio.TimeoutError:
                log.warning("Exec timeout #%d", i + 1)
            except Exception as exc:
                log.error("Exec error: %s", exc)
            await asyncio.sleep(min(2 ** i * 1.5, 5.0))
        return None

    # ── Main tick handler ─────────────────────────────────────────────────────

    async def process_tick(self) -> Optional[PositionFill]:
        await self._ready.wait()
        self._ready.clear()
        if not self._pending_state:
            return None

        s     = self._pending_state
        ts    = float(s.get("ts",  time.time()) or time.time())
        price = float(s.get("mid", 0.0)         or 0.0)
        if not price:
            return None

        self._diag["ticks_total"] += 1

        # ── Daily equity reset ────────────────────────────────────────────────
        day = time.strftime("%Y-%m-%d", time.gmtime(ts))
        if day != self.risk.last_day:
            self.risk.reset_day(day)
            self.overlay.reset_day()
            log.info("DAY RESET | day=%s equity=$%.2f", day, self.risk.current_equity)

        # ── Position management — runs independently of entry eligibility ─────
        if self.pm.pos:
            self._diag["ticks_in_pos"] += 1
            spread = float(s.get("spread_bps", 0.0) or 0.0)
            hits = self.pm.update(ts, price, spread)
            for h in hits:
                if h.qty > 0:
                    self.risk.update_equity(h.pnl_usd)
                    self.risk.record_result(h.pnl_usd)
                    self.overlay.update_equity(h.pnl_usd, ts)
                    if self.builder:
                        self.builder.add_fill(h)
                    if self.pm.pos:  # partial fill — position still open
                        self.state_mgr.save(self.state_mgr.build_checkpoint(
                            self.risk.current_equity, ts, self.risk.consecutive_losses,
                            self._checkpoint_pos(), self._checkpoint_builder(),
                        ))
                    asyncio.create_task(
                        self._safe_exec(lambda fill=h: self._route_fill(fill))
                    )
            if not self.pm.pos:
                exit_reason = hits[-1].event.name if hits else "?"
                result = self.builder.build(ts, exit_reason) if self.builder else None
                pnl_str = f"${result.pnl_usd:.2f}" if result else "n/a"
                log.info("TRADE CLOSED | PnL=%s | Reason=%s", pnl_str, exit_reason)
                self.state_mgr.save(
                    self.state_mgr.build_checkpoint(
                        self.risk.current_equity, ts,
                        self.risk.consecutive_losses, None, None,
                    )
                )
                self.last_trade_ts = ts
                self.builder = None
                if self.dlog:
                    if result is not None:
                        self.dlog.log_result(result)
                    self.dlog.log_summary(log)
            return hits[-1] if hits else None

        # ── Entry gate — only reached when flat ───────────────────────────────

        # Step 1a — Daily profit cap -------------------------------------------
        cap_blocked, cap_reason = self.overlay.should_block(ts)
        if cap_blocked:
            self._diag["ticks_risk_block"]                += 1
            self._diag["rejection_counts"][cap_reason]    += 1
            return None

        # Step 1b — Risk check -------------------------------------------------
        r_state, r_mult, _ = self.risk.evaluate(ts)
        if not self.risk.can_trade(ts) or r_state == RiskState.RED:
            self._diag["ticks_risk_block"]            += 1
            self._diag["rejection_counts"]["risk_block"] += 1
            return None

        # Step 2 — Feature extraction -----------------------------------------
        regime_str = s.get("regime", "CHOP")
        regime = (Regime[regime_str]
                  if regime_str in Regime.__members__ else Regime.CHOP)
        cvd     = float(s.get("cvd_delta_5s",    0.0) or 0.0)
        trend   = float(s.get("trend_bps",        0.0) or 0.0)
        vol     = float(s.get("realized_vol_bps", 0.0) or 0.0)
        spread  = float(s.get("spread_bps",       0.0) or 0.0)
        trend30 = float(s.get("trend_30s_bps",    0.0) or 0.0)
        range30 = float(s.get("range_30s_bps",    0.0) or 0.0)

        # Step 3 — Dynamic threshold (mirrors SelectivityEngine in backtest) ---
        day_dd_pct = max(
            (self.risk.day_start_equity - self.risk.current_equity)
            / max(self.risk.day_start_equity, 1.0) * 100.0,
            0.0,
        )
        dyn_t, dyn_r = self.sel.compute_dynamic_multiplier(
            self.risk.current_equity, self.risk.day_start_equity,
            self.risk.recent_wr(), day_dd_pct,
        )
        eff_thr = self.cfg.threshold * dyn_t

        # Step 4 — Score (V3 → V2 → V1 → linear) — called ONCE per tick ------
        # RegimeAdaptiveSignal (V3) mutates internal price history on every
        # call via on_tick().  Do NOT call SignalProcessor.score() again after
        # this point for the same tick.
        _v2_cache: dict = {}
        score = SignalProcessor.score(
            cvd, trend, vol, regime, self.cfg,
            _v2_cache=_v2_cache,
            trend30_bps=trend30,
            range30_bps=range30,
            ts=ts,
            price=price,
        )

        # Step 5 — Signal confirmation counter --------------------------------
        # Use score + threshold to determine direction without re-scoring.
        raw_dir = SignalProcessor.determine_signal(score, eff_thr)
        if raw_dir != "FLAT":
            if raw_dir == self.signal_side and (ts - self.signal_ts) <= 3.0:
                self.signal_count += 1
            else:
                self.signal_side  = raw_dir
                self.signal_count = 1
                self.signal_ts    = ts
        else:
            self.signal_side  = ""
            self.signal_count = 0

        # Step 6 — Cluster check ----------------------------------------------
        clustered = (self.sel.is_clustered(ts, self.signal_side)
                     if self.signal_side else False)

        # Step 7 — Full decision (single call) ─────────────────────────────────
        dlog = build_trade_decision(
            score=score,
            ts=ts,
            price=price,
            cvd=cvd,
            trend=trend,
            vol=vol,
            spread=spread,
            regime=regime,
            eff_thr=eff_thr,
            r_mult=r_mult,
            signal_count=self.signal_count,
            clustered=clustered,
            last_trade_ts=self.last_trade_ts,
            last_score=self._last_score,
            cfg=self.cfg,
            gate=self._gate,
            v2_result=_v2_cache,
        )

        # Step 8 — Update last_score (always, regardless of outcome) ----------
        self._last_score = dlog.score

        # ── Structured decision log (approved + near-misses by default) ───────
        if self.dlog:
            self.dlog.log(dlog)

        # ── Score diagnostics ─────────────────────────────────────────────────
        abs_s = abs(dlog.score)
        self._diag["score_count"]   += 1
        self._diag["score_abs_sum"] += abs_s
        if abs_s < self._diag["score_abs_min"]: self._diag["score_abs_min"] = abs_s
        if abs_s > self._diag["score_abs_max"]: self._diag["score_abs_max"] = abs_s
        if abs_s < eff_thr:                     self._diag["score_below_thr"] += 1
        if dlog.near_miss:                      self._diag["near_misses"] += 1

        # ── Periodic market snapshot (every 60 s) ─────────────────────────────
        if ts - self._diag["last_mkt_log_ts"] >= 60.0:
            self._diag["last_mkt_log_ts"] = ts
            snap = {
                "cvd": round(cvd, 4), "trend": round(trend, 4),
                "vol": round(vol, 4), "spread": round(spread, 4),
                "regime": regime_str,
                "score": round(dlog.score, 4), "thr": round(eff_thr, 4),
                "price": round(price, 2), "trend30": round(trend30, 4),
            }
            self._diag["last_mkt_snapshot"] = snap
            log.info(
                "MARKET_SNAP | cvd=%.3f trend=%.3f vol=%.3f "
                "regime=%s score=%.3f thr=%.3f price=%.2f t30=%.3f",
                cvd, trend, vol, regime_str,
                dlog.score, eff_thr, price, trend30,
            )

        # ── Decision logging ──────────────────────────────────────────────────
        if dlog.signal != "FLAT":
            self._diag["signals_nonflat"] += 1
            log.debug(
                "SIGNAL | %s score=%.3f conf=%.2f rq=%.2f "
                "adv=%.2f acc=%s coh=%s regime=%s count=%d",
                dlog.signal, dlog.score, dlog.confidence, dlog.regime_quality,
                dlog.adv_final, dlog.acc_ok, dlog.coh_ok,
                regime_str, dlog.signal_count,
            )

        if not dlog.approved:
            reason = dlog.rejection_reason or "flat_signal"
            self._diag["rejection_counts"][reason] += 1
            if dlog.signal != "FLAT":
                log.debug("GATE | rejected=%s score=%.3f adv=%.2f",
                          reason, dlog.score, dlog.adv_final)
            return None

        # Step 9 — Open position ──────────────────────────────────────────────
        sl_dist   = self.cfg.stop_loss_pct / 100.0
        entry_fee = (self.cfg.maker_fee_bps
                     if self.cfg.entry_fee_mode == "maker"
                     else self.cfg.taker_fee_bps)
        fee_rt    = (entry_fee + self.cfg.taker_fee_bps) / 10_000.0

        risk_usd = SizingEngine.compute_risk_usd(
            self.risk.current_equity, self.cfg,
            dlog.confidence, dlog.regime_quality,
            r_mult * dyn_r, self.risk.consecutive_losses,
        )
        qty = risk_usd / max(price * (sl_dist + fee_rt), 1e-9)

        _, exec_px = self.pm.open(
            ts, price, dlog.score, qty,
            dlog.signal, spread, regime, dlog.confidence,
        )
        self.builder = __import__(
            "au2_core", fromlist=["TradeBuilder"]
        ).TradeBuilder(
            dlog.signal, exec_px, ts, dlog.score,
            regime, qty, dlog.confidence,
        )
        self.sel.record_entry(ts, dlog.signal)
        self.risk.record_trade(ts)
        self.signal_side  = ""
        self.signal_count = 0
        self.last_trade_ts = ts
        self._diag["signals_approved"] += 1
        self.state_mgr.save(self.state_mgr.build_checkpoint(
            self.risk.current_equity, ts, self.risk.consecutive_losses,
            self._checkpoint_pos(), self._checkpoint_builder(),
        ))

        asyncio.create_task(
            self._safe_exec(
                lambda ep=exec_px, q=qty, sig=dlog.signal:
                    self.execute_entry(ep, q, sig),
                timeout=10.0,
                retries=1,
            )
        )
        log.info(
            "[ENTRY] %s | conf=%.2f | risk=$%.2f | adv=%.2f"
            " | regime=%s | score=%.3f | qty=%.6f",
            dlog.signal, dlog.confidence, risk_usd, dlog.adv_final,
            regime_str, dlog.score, qty,
        )
        return None

    # ── Checkpoint helpers ───────────────────────────────────────────────────

    def _checkpoint_pos(self) -> Optional[dict]:
        p = self.pm.pos
        if p is None:
            return None
        return {
            "side": p.side, "exec_price": p.exec_price, "entry_ts": p.entry_ts,
            "initial_qty": p.initial_qty, "remaining_qty": p.remaining_qty,
            "score": p.score, "sl_price": p.sl_price,
            "tp1_price": p.tp1_price, "tp2_price": p.tp2_price,
            "peak_price": p.peak_price, "trough_price": p.trough_price,
            "regime": p.regime, "confidence": p.confidence,
            "trail_price": p.trail_price,
            "tp1_done": p.tp1_done, "tp2_done": p.tp2_done,
            "runner_active": p.runner_active,
        }

    def _checkpoint_builder(self) -> Optional[dict]:
        b = self.builder
        if b is None:
            return None
        return {
            "signal": b.signal, "entry_price": b.entry_price,
            "entry_ts": b.entry_ts, "score": b.score, "regime": b.regime,
            "qty": b.qty, "confidence": b.confidence,
            "regime_quality": b.regime_quality, "pnl_so_far": b._pnl,
        }

    # ── Order routing (override in subclasses) ────────────────────────────────

    async def _route_fill(self, fill: PositionFill) -> None:
        if fill.event == PositionEvent.OPEN:
            await self.execute_entry(fill.price, fill.qty, fill.side)
        elif fill.qty > 0:
            await self.execute_partial(fill)

    async def execute_entry(self, price: float, qty: float, side: str) -> None:
        """Override to place a real or paper entry order."""

    async def execute_partial(self, fill: PositionFill) -> None:
        """Override to place a real or paper partial-exit order."""

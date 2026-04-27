#!/usr/bin/env python3
"""
bt_engine — reusable backtest engine for grid search and comparison.

Exposes:
  - load_signals(db_path) -> list of rows
  - run_backtest(cfg, overlay_cfg, prop, start_equity, rows, flip_signal=False) -> RunMetrics

Uses the DB's pre-computed `score` column (v22_5 formula, range ≈ ±8) so
parameters are only differentiated by threshold / filters / position mgmt.

The `flip_signal` flag inverts the direction mapping:
  default:  score >= thr → SHORT  (mean-revert: "overbought" → short)
  flipped:  score >= thr → LONG   (momentum: "buying pressure" → long)
"""
from __future__ import annotations
import datetime, collections, sqlite3
from dataclasses import dataclass, field
from typing import List, Dict, Optional

from au2_core import (
    CoreConfig, RiskEngine, PositionManager, SizingEngine,
    SignalProcessor, PropProfile, RiskState, Regime,
    TradeResult, PositionFill, SelectivityEngine, SignalQuality,
)
from au2_risk_overlay import RiskOverlay, RiskOverlayConfig

# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class RunMetrics:
    trades:       List[TradeResult] = field(default_factory=list)
    rejections:   Dict[str, int]    = field(default_factory=lambda: collections.defaultdict(int))
    start_equity: float = 10000.0

# ─────────────────────────────────────────────────────────────────────────────
def load_signals(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT ts, price, score, cvd_delta_5s, trend_bps, realized_vol_bps, "
            "COALESCE(spread_bps, 0.0) as spread_bps FROM signals ORDER BY ts ASC"
        ).fetchall()
    except sqlite3.OperationalError:
        rows = conn.execute(
            "SELECT ts, price, score, cvd_delta_5s, trend_bps, realized_vol_bps, "
            "0.0 as spread_bps FROM signals ORDER BY ts ASC"
        ).fetchall()
    conn.close()
    return rows

# ─────────────────────────────────────────────────────────────────────────────
def run_backtest(
    cfg: CoreConfig,
    overlay_cfg: RiskOverlayConfig,
    prop: Optional[PropProfile],
    start_equity: float,
    rows: list,
    flip_signal: bool = False,
) -> RunMetrics:
    risk    = RiskEngine(cfg, start_equity)
    pm      = PositionManager(cfg)
    overlay = RiskOverlay(start_equity=start_equity, cfg=overlay_cfg)
    sel     = SelectivityEngine(cfg)

    result  = RunMetrics(start_equity=start_equity)
    rej     = result.rejections

    last_trade_ts = 0.0
    signal_side   = ""
    signal_count  = 0
    signal_ts     = 0.0
    last_score    = 0.0
    cur_side  = ""
    cur_entry = 0.0
    cur_score = 0.0
    cur_qty   = 0.0
    cur_regime = ""
    cur_ts    = 0.0
    cur_conf  = 0.0
    partial_fills: List[PositionFill] = []

    for row in rows:
        ts    = float(row["ts"])
        price = float(row["price"])
        if not price or price <= 0:
            continue

        # Daily reset
        day = datetime.datetime.fromtimestamp(ts, datetime.UTC).strftime("%Y-%m-%d")
        if day != risk.last_day:
            risk.reset_day(day)
            if hasattr(overlay, "reset_day"):
                overlay.reset_day()

        r_state, r_mult, _ = risk.evaluate(ts)
        can_trade = risk.can_trade(ts)
        ov_blocked, ov_reason = overlay.should_block(ts)

        # Position management (always runs)
        if pm.pos:
            spread = float(row["spread_bps"] or 0) or cfg.assume_spread_bps
            hits = pm.update(ts, price, spread)
            for h in hits:
                partial_fills.append(h)
                if h.qty > 0:
                    risk.update_equity(h.pnl_usd)
            if not pm.pos and partial_fills:
                total_pnl   = sum(f.pnl_usd for f in partial_fills if f.qty > 0)
                exit_reason = partial_fills[-1].event.name
                exit_fill   = partial_fills[-1]
                hold_s      = (exit_fill.ts - cur_ts) if cur_ts else 0.0
                result.trades.append(TradeResult(
                    signal=cur_side, entry_price=cur_entry, entry_ts=cur_ts,
                    exit_price=exit_fill.price, exit_ts=exit_fill.ts,
                    pnl_usd=total_pnl, hold_seconds=hold_s,
                    exit_reason=exit_reason, entry_score=cur_score,
                    confidence=cur_conf, regime=cur_regime, qty=cur_qty,
                ))
                risk.record_result(total_pnl, ts)
                risk.trigger_loss_pause(ts)
                overlay.update_equity(total_pnl, ts)
                last_trade_ts = ts
                partial_fills = []
            continue

        if not can_trade:
            rej["blocked_by_core_trade_limits"] += 1; continue
        if r_state == RiskState.RED:
            rej["blocked_by_core_risk"] += 1; continue
        if ov_blocked:
            rej[ov_reason] += 1; continue

        db_score = float(row["score"] or 0)
        cvd   = float(row["cvd_delta_5s"] or 0)
        trend = float(row["trend_bps"] or 0)
        vol   = float(row["realized_vol_bps"] or 0)
        spread = float(row["spread_bps"] or 0)
        regime_str = SignalProcessor.classify_regime(vol, trend, cvd)
        regime = Regime[regime_str] if regime_str in Regime.__members__ else Regime.CHOP

        day_dd_pct = max(
            (risk.day_start_equity - risk.current_equity) / max(risk.day_start_equity, 1) * 100.0, 0.0
        )
        dyn_t, dyn_r = sel.compute_dynamic_multiplier(
            risk.current_equity, risk.day_start_equity, risk.recent_wr(), day_dd_pct
        )
        eff_thr = cfg.threshold * dyn_t

        abs_score = abs(db_score)
        if db_score >= eff_thr:
            signal = "LONG" if flip_signal else "SHORT"
        elif db_score <= -eff_thr:
            signal = "SHORT" if flip_signal else "LONG"
        else:
            signal_side = ""; signal_count = 0
            continue

        conf = min(abs_score / max(eff_thr, 1e-9), 1.5)
        rq   = cfg.regime_quality_multiplier.get(regime, 1.0)
        eff  = abs_score * conf * rq

        trade_ok = (
            eff >= cfg.min_setup_quality
            and conf >= cfg.min_confidence_threshold
            and rq >= 0.75
        )
        if not trade_ok:
            reason = (
                "conf_low"   if conf < cfg.min_confidence_threshold else
                "regime_weak" if rq < 0.75 else
                "setup_weak"
            )
            rej[reason] += 1; continue

        if vol < cfg.min_vol_bps:   rej["vol_too_low"] += 1;      continue
        if spread > cfg.max_spread_bps: rej["spread_too_wide"] += 1; continue
        if ts - last_trade_ts < cfg.cooldown_seconds:
            rej["cooldown"] += 1; continue
        if sel.is_clustered(ts, signal):
            rej["cluster"] += 1; continue

        if signal == signal_side and (ts - signal_ts) <= 3.0:
            signal_count += 1
        else:
            signal_side, signal_count, signal_ts = signal, 1, ts

        if signal_count < cfg.confirmation_cycles:
            rej["confirmation_pending"] += 1; continue

        if prop:
            if risk._daily_trades >= prop.daily_trade_cap:
                rej["blocked_by_prop_trade_cap"] += 1; continue
            prop_day_dd = max(
                (risk.day_start_equity - risk.current_equity) / max(risk.day_start_equity, 1) * 100, 0.0
            )
            if prop_day_dd >= prop.max_daily_dd_pct:
                rej["blocked_by_prop_daily_dd"] += 1; continue

        risk_usd = SizingEngine.compute_risk_usd(
            risk.current_equity, cfg, conf, rq, r_mult * dyn_r, risk.consecutive_losses
        )
        sl_dist = cfg.stop_loss_pct / 100.0
        fee_rt  = 2.0 * cfg.taker_fee_bps / 10_000.0
        qty = risk_usd / max(price * (sl_dist + fee_rt), 1e-9)

        pos, exec_px = pm.open(ts, price, db_score, qty, signal, spread, regime, conf)
        partial_fills = []
        cur_side, cur_entry, cur_ts = signal, exec_px, ts
        cur_score, cur_qty, cur_regime, cur_conf = db_score, qty, regime_str, conf
        last_score = db_score
        sel.record_entry(ts, signal)
        risk.record_trade(ts)
        signal_side, signal_count = "", 0

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Analytics helpers
# ─────────────────────────────────────────────────────────────────────────────
def active_days(trades: List[TradeResult]) -> int:
    days = {datetime.datetime.fromtimestamp(t.entry_ts, datetime.UTC).strftime("%Y-%m-%d") for t in trades}
    return max(len(days), 1)

def profit_factor(trades: List[TradeResult]) -> float:
    gw = sum(t.pnl_usd for t in trades if t.pnl_usd > 0)
    gl = sum(-t.pnl_usd for t in trades if t.pnl_usd < 0)
    return gw / max(gl, 1e-9)

def max_drawdown_pct(trades: List[TradeResult], start_eq: float) -> float:
    eq = start_eq; peak = eq; mdd = 0.0
    for t in sorted(trades, key=lambda x: x.exit_ts):
        eq += t.pnl_usd
        if eq > peak: peak = eq
        dd = (peak - eq) / max(peak, 1e-9) * 100.0
        if dd > mdd: mdd = dd
    return mdd

def summarize(rm: RunMetrics) -> dict:
    trades = rm.trades
    n = len(trades)
    wins = sum(1 for t in trades if t.pnl_usd > 0)
    losses = n - wins
    total_pnl = sum(t.pnl_usd for t in trades)
    pf = profit_factor(trades)
    exp = total_pnl / n if n else 0.0
    wr = wins / n if n else 0.0
    dd = max_drawdown_pct(trades, rm.start_equity)
    ndays = active_days(trades) if trades else 1
    # Exit reason breakdown
    exit_c = collections.Counter(t.exit_reason for t in trades)
    return dict(
        n=n, wins=wins, losses=losses, wr=wr,
        total_pnl=total_pnl, pf=pf, exp=exp, dd=dd,
        ndays=ndays, trades_per_day=n/ndays,
        pnl_per_day=total_pnl/ndays,
        exit_time_pct=(exit_c.get("EXIT_TIME", 0) / n * 100) if n else 0.0,
        exit_sl_pct  =(exit_c.get("EXIT_SL",   0) / n * 100) if n else 0.0,
        exit_tp1_pct =(exit_c.get("TP1_HIT",   0) / n * 100) if n else 0.0,
        exit_tp2_pct =(exit_c.get("TP2_HIT",   0) / n * 100) if n else 0.0,
    )

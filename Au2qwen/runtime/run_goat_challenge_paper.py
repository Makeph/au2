#!/usr/bin/env python3
"""GOAT CHALLENGE V3 — Paper live runner.

Usage
-----
  python runtime/run_goat_challenge_paper.py          # BTCUSDT, $10 000
  START_EQUITY=25000 python runtime/run_goat_challenge_paper.py
  LOG_LEVEL=DEBUG python runtime/run_goat_challenge_paper.py

What this does
--------------
Same pipeline as au2_bot_live.py (GOAT), but with GOAT_CHALLENGE_V3_CFG:
  - risk_per_trade_pct = 0.50  (GOAT: 1.0)
  - max_risk_usd       = 50.0  (GOAT: 350.0)
  - everything else   = identical to GOAT validated

No real orders are placed.  execute_entry / execute_partial log only.

Outputs
-------
  data/live/au2_challenge_v3_decisions.jsonl  — JSONL decision log (every tick)
  data/live/au2_challenge_v3_state.json       — crash-recovery checkpoint (atomic)
  console                                     — STATUS every 60 s

Parity check (after a session)
-------------------------------
  python diagnostics/diag_parity_live.py

Safety
------
- Separate state file from GOAT bot (no conflict on same machine).
- V3 config verified via dataclasses diff at startup (2 fields only).

ARCH CONSTRAINT — ONE BOT PER PROCESS
  _signal_v3 (RegimeAdaptiveSignal) in au2_core is a module-level singleton.
  Its internal price buffer is mutated by every SignalProcessor.score() call.
  Running this script and au2_bot_live.py inside the same Python process would
  share that buffer and corrupt both bots' macro-trend state.
  Supported and tested deployment: one `python` invocation per bot.
  LiveExecutor.__init__ emits a RuntimeWarning if this constraint is violated.
"""
from __future__ import annotations
import sys, pathlib, dataclasses, time
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime", _ROOT / "presets"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

import asyncio
import logging
import os
import signal

from dotenv import load_dotenv

from au2_config import GOAT_VALIDATED_CFG
from au2_goat_challenge_v3 import GOAT_CHALLENGE_V3_CFG, GOAT_CHALLENGE_V3_OVERLAY
from au2_bot_live import PaperExecutor, MarketState, Au2QwenBot
from au2_state_manager import StatePersistence
from au2_core import REGIME_PROFILES, Regime

load_dotenv()
log = logging.getLogger("au2_challenge_v3")

_DECISIONS_PATH = str(_ROOT / "data" / "live" / "au2_challenge_v3_decisions.jsonl")
_STATE_PATH     = str(_ROOT / "data" / "live" / "au2_challenge_v3_state.json")


# ---------------------------------------------------------------------------
# Phase 1 — Boot diff : guarantee exactly 2 fields differ from GOAT validated
# ---------------------------------------------------------------------------

def _verify_and_print_diff() -> None:
    """Print diff between GOAT_VALIDATED_CFG and GOAT_CHALLENGE_V3_CFG.
    Aborts if anything other than the two expected fields differs.
    """
    fields = dataclasses.fields(GOAT_VALIDATED_CFG)
    diffs = {
        f.name: (getattr(GOAT_VALIDATED_CFG, f.name),
                 getattr(GOAT_CHALLENGE_V3_CFG, f.name))
        for f in fields
        if getattr(GOAT_VALIDATED_CFG, f.name) != getattr(GOAT_CHALLENGE_V3_CFG, f.name)
    }
    expected = {"risk_per_trade_pct", "max_risk_usd"}
    unexpected = set(diffs) - expected
    if unexpected:
        # Hard abort — do not start if config integrity is broken.
        raise RuntimeError(
            f"[ABORT] GOAT_CHALLENGE_V3 differs from GOAT_VALIDATED on unexpected fields: "
            f"{unexpected}.  Fix au2_goat_challenge_v3.py before running."
        )
    log.info(
        "CONFIG DIFF (V3 vs GOAT validated) — %d field(s) changed:", len(diffs)
    )
    for name, (old, new) in sorted(diffs.items()):
        log.info("  %-24s %s  →  %s", name, old, new)
    log.info(
        "CONFIG DIFF — overlay: daily_profit_cap_pct=%.1f%% (GOAT: 5.0%%)",
        GOAT_CHALLENGE_V3_OVERLAY.daily_profit_cap_pct,
    )


# ---------------------------------------------------------------------------
# Phase 2 — Executor with isolated state file
# ---------------------------------------------------------------------------

class ChallengeV3Executor(PaperExecutor):
    """PaperExecutor wired to GOAT_CHALLENGE_V3_CFG.

    Overrides _load_or_init to use a dedicated state file so this bot and
    the GOAT bot can run on the same machine without state conflicts.
    """

    def _load_or_init(self, equity: float) -> None:
        # Replace the default state path BEFORE the parent reads from it.
        self.state_mgr = StatePersistence(_STATE_PATH)
        saved = self.state_mgr.load()
        if saved and saved.get("ts", 0) > time.time() - 86_400:
            log.info(
                "V3 state restored from %s  equity=$%.2f  streak=%d",
                time.strftime("%H:%M:%S", time.gmtime(saved["ts"])),
                saved["equity"],
                saved.get("loss_streak", 0),
            )
            self.risk.current_equity     = saved["equity"]
            self.last_trade_ts           = saved["last_trade_ts"]
            self.risk.consecutive_losses = saved.get("loss_streak", 0)
            self.risk.day_start_equity   = saved["equity"]
        else:
            log.info("V3 fresh start (no recent checkpoint).")


# ---------------------------------------------------------------------------
# Phase 2 — Bot runtime
# ---------------------------------------------------------------------------

class ChallengeV3Bot(Au2QwenBot):
    """Au2QwenBot subclass that uses GOAT_CHALLENGE_V3_CFG.

    Adds a status_loop task that prints equity / PnL / DD every 60 s.
    """

    def __init__(self) -> None:
        # Intentionally bypass Au2QwenBot.__init__ to inject our config.
        self.symbol     = os.getenv("BOT_SYMBOL", "BTCUSDT").lower()
        self.ws_base    = os.getenv("BINANCE_FAPI_WS",
                                    "wss://fstream.binance.com/stream")
        start_equity    = float(os.getenv("START_EQUITY", "10000"))

        self.executor   = ChallengeV3Executor(
            GOAT_CHALLENGE_V3_CFG,
            start_equity=start_equity,
            live_mode=False,
            decision_log_path=_DECISIONS_PATH,
        )
        self.market     = MarketState(
            window_s=5.0,
            assume_spread_bps=GOAT_CHALLENGE_V3_CFG.assume_spread_bps,
        )
        self.stop_event    = asyncio.Event()
        self._start_ts     = time.time()
        self._start_equity = start_equity
        self._error_count  = 0

    # ── Status loop — printed to console every 60 s ──────────────────────────

    async def status_loop(self) -> None:
        while not self.stop_event.is_set():
            await asyncio.sleep(60)
            eq        = self.executor.risk.current_equity
            pnl       = eq - self._start_equity
            pnl_pct   = pnl / max(self._start_equity, 1) * 100
            day_start = self.executor.risk.day_start_equity
            day_dd    = max((day_start - eq) / max(day_start, 1) * 100, 0.0)
            n_trades  = self.executor._diag["signals_approved"]
            n_nonflat = self.executor._diag["signals_nonflat"]
            n_near    = self.executor._diag["near_misses"]
            rej       = self.executor._diag["rejection_counts"]

            pos_str = "FLAT"
            if self.executor.pm.pos:
                p = self.executor.pm.pos
                held_s = time.time() - p.entry_ts
                pos_str = (f"{p.side}  entry={p.exec_price:.2f}"
                           f"  qty={p.remaining_qty:.4f}  held={held_s:.0f}s")

            uptime_h = (time.time() - self._start_ts) / 3600

            log.info(
                "STATUS | uptime=%.1fh  equity=$%.2f  pnl=$%.2f (%+.2f%%)"
                "  day_dd=%.2f%%  trades=%d  pos=%s",
                uptime_h, eq, pnl, pnl_pct, day_dd, n_trades, pos_str,
            )
            if n_nonflat > 0:
                approval_rate = n_trades / n_nonflat * 100
                log.info(
                    "FUNNEL | non_flat=%d  approved=%d (%.1f%%)  near_miss=%d",
                    n_nonflat, n_trades, approval_rate, n_near,
                )
            if rej:
                top3 = sorted(rej.items(), key=lambda x: -x[1])[:3]
                log.info("TOP REJECTIONS | %s",
                         "  |  ".join(f"{r}: {c}" for r, c in top3))
            # Task 3/4 — FLOW exit metrics + safety warning
            flow_total = (
                self.executor._diag["flow_exit_time"] +
                self.executor._diag["flow_exit_be_fallback"] +
                self.executor._diag["flow_exit_sl"]
            )
            if flow_total > 0:
                flow_sl_rate   = self.executor._diag["flow_exit_sl"]          / flow_total
                flow_be_rate   = self.executor._diag["flow_exit_be_fallback"] / flow_total
                flow_time_rate = self.executor._diag["flow_exit_time"]        / flow_total
                log.info(
                    "FLOW | n=%d  time=%.0f%%  be=%.0f%%  sl=%.0f%%",
                    flow_total, flow_time_rate * 100, flow_be_rate * 100, flow_sl_rate * 100,
                )
                if flow_sl_rate > 0.10:
                    log.warning(
                        "FLOW SL rate elevated (%.0f%% > 10%%) -- possible market regime issue",
                        flow_sl_rate * 100,
                    )

    # ── Override run() to inject status_loop task ────────────────────────────

    async def run(self) -> None:
        logging.basicConfig(
            level=getattr(logging,
                          os.getenv("LOG_LEVEL", "INFO").upper(),
                          logging.INFO),
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )
        _verify_and_print_diff()
        # Task 2 — startup verification of be_trigger_bps values
        log.info("[CONFIG] FLOW be_trigger_bps = %.1f",
                 REGIME_PROFILES[Regime.FLOW].be_trigger_bps)
        log.info("[CONFIG] CHOP be_trigger_bps = %.1f",
                 REGIME_PROFILES[Regime.CHOP].be_trigger_bps)
        log.info(
            "GOAT-CHALLENGE-V3 paper bot  |  symbol=%s  equity=$%.0f"
            "  decisions=%s",
            self.symbol.upper(), self._start_equity, _DECISIONS_PATH,
        )

        _task_coros = {
            "market":    self.run_market_stream,
            "strategy":  self.strategy_loop,
            "heartbeat": self.heartbeat_loop,
            "status":    self.status_loop,
        }
        tasks = {
            name: asyncio.create_task(coro(), name=name)
            for name, coro in _task_coros.items()
        }
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=10.0)
                break
            except asyncio.TimeoutError:
                pass
            for name, task in list(tasks.items()):
                if task.done():
                    exc = task.exception() if not task.cancelled() else None
                    if exc:
                        self._error_count += 1
                        log.error("task %s crashed (#%d): %s — restarting",
                                  name, self._error_count, exc, exc_info=exc)
                    else:
                        log.warning("task %s exited — restarting", name)
                    tasks[name] = asyncio.create_task(
                        _task_coros[name](), name=name
                    )
        for t in tasks.values():
            t.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        # Final session summary
        if self.executor.dlog:
            self.executor.dlog.log_summary(log)
        log.info("GOAT-CHALLENGE-V3 shutdown complete.")


# ---------------------------------------------------------------------------
# Immortal entrypoint
# ---------------------------------------------------------------------------

async def amain() -> None:
    loop          = asyncio.get_running_loop()
    restart_count = 0
    _sigterm_recv = False
    bot           = ChallengeV3Bot()

    def _on_signal() -> None:
        nonlocal _sigterm_recv
        _sigterm_recv = True
        bot.stop_event.set()

    while True:
        _sigterm_recv = False
        bot.stop_event.clear()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _on_signal)
            except NotImplementedError:
                pass
        try:
            await bot.run()
        except RuntimeError as exc:
            # Config integrity check failed — do not restart.
            log.critical("%s", exc)
            return
        except KeyboardInterrupt:
            log.info("KeyboardInterrupt.")
        except BaseException:
            log.error("CRASH in bot.run()", exc_info=True)
        if _sigterm_recv:
            log.info("SIGTERM — exiting.")
            return
        restart_count += 1
        log.warning("bot.run() ended (#%d) — restarting in 5 s.", restart_count)
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(amain())

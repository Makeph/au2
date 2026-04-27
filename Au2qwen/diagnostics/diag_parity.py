#!/usr/bin/env python3
"""AU2QWEN — Backtest / Live decision parity diagnostic.

Usage
-----
  python diag_parity.py [db_path]
  python diag_parity.py au2_spot_24h.db

What it does
------------
1. Runs the full backtest (ground truth — same config, same overlay).
2. Replays every DB row through the live entry-decision path:
     SignalProcessor.score() → build_trade_decision()
   using the same SelectivityEngine, TradeGate, and _last_score tracking
   as the refactored LiveExecutor.
3. Compares entry counts, rejection breakdowns, score distributions.
4. Explains expected divergences (overlay profit cap, always-flat sim).

Known / expected divergences
-----------------------------
- Backtest uses RiskOverlay (daily profit cap 5 %).  When the session is
  very profitable the overlay blocks entries that the live path would take.
  This explains the ~10 % gap on short profitable sessions.
- Backtest has position management (rows consumed while in a trade are
  skipped for new entries).  The simulator stays always flat, so it sees
  more candidate rows.
- These divergences are structural, not pipeline bugs.  The pipeline itself
  (score → quality → adv → gate) is identical.
"""
from __future__ import annotations

import sqlite3
import statistics
import sys
import pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

from collections import Counter
from datetime import datetime, timezone
from typing import Optional

from au2_core import (
    Au2Backtest, RiskEngine, SelectivityEngine, TradeGate, SignalProcessor,
    Regime, RiskState,
)
from au2_config import GOAT_VALIDATED_CFG, GOAT_OVERLAY_CFG
from au2_decision import build_trade_decision, TradeDecisionLog
from au2_risk_overlay import RiskOverlay

CFG = GOAT_VALIDATED_CFG


# ---------------------------------------------------------------------------
# Live path simulator
# ---------------------------------------------------------------------------

class LivePathSimulator:
    """Synchronous replay of the live entry-decision pipeline over a DB.

    Mirrors LiveExecutor.process_tick() exactly:
      - Score computed once per row
      - _last_score updated AFTER build_trade_decision (correct acc-check)
      - Cooldown handled inside build_trade_decision (not as early return)
      - Signal counter uses SignalProcessor.determine_signal (same as live)
      - No position management — stays always flat
    """

    def __init__(self) -> None:
        self.risk   = RiskEngine(CFG, start_equity=10_000.0)
        self.sel    = SelectivityEngine(CFG)
        self._gate  = TradeGate(CFG)

        self._last_score:   float = 0.0
        self.last_trade_ts: float = 0.0
        self.signal_side:   str   = ""
        self.signal_count:  int   = 0
        self.signal_ts:     float = 0.0

        # Counters
        self.ticks_total:    int     = 0
        self.risk_blocked:   int     = 0
        self.signals_nonflat: int    = 0
        self.signals_approved: int   = 0
        self.near_misses:    int     = 0
        self.rejections:     Counter = Counter()
        self.by_regime:      Counter = Counter()
        self.score_abs:      list    = []

    def process_row(self, row: dict) -> Optional[TradeDecisionLog]:
        ts    = float(row.get("ts",    0) or 0)
        price = float(row.get("price", 0) or 0)
        if not price or price <= 0:
            return None

        self.ticks_total += 1

        # Daily reset
        day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        if day != self.risk.last_day:
            self.risk.reset_day(day)

        # Risk gate
        r_state, r_mult, _ = self.risk.evaluate(ts)
        if not self.risk.can_trade(ts) or r_state == RiskState.RED:
            self.risk_blocked += 1
            return None

        # Dynamic threshold
        day_dd_pct = max(
            (self.risk.day_start_equity - self.risk.current_equity)
            / max(self.risk.day_start_equity, 1.0) * 100.0, 0.0,
        )
        dyn_t, _ = self.sel.compute_dynamic_multiplier(
            self.risk.current_equity, self.risk.day_start_equity,
            self.risk.recent_wr(), day_dd_pct,
        )
        eff_thr = CFG.threshold * dyn_t

        # Features
        cvd    = float(row.get("cvd_delta_5s",    0) or 0)
        trend  = float(row.get("trend_bps",        0) or 0)
        vol    = float(row.get("realized_vol_bps", 0) or 0)
        spread = float(row.get("spread_bps",       CFG.assume_spread_bps)
                       or CFG.assume_spread_bps)
        trend30 = float(row.get("trend_30s_bps",   0) or 0)
        range30 = float(row.get("range_30s_bps",   0) or 0)
        regime  = SignalProcessor.classify_regime(vol, trend, cvd)

        # Score — ONE call (V3 state update happens here)
        _v2_cache: dict = {}
        score = SignalProcessor.score(
            cvd, trend, vol, regime, CFG,
            _v2_cache=_v2_cache,
            trend30_bps=trend30, range30_bps=range30, ts=ts, price=price,
        )
        self.score_abs.append(abs(score))

        # Signal counter — uses determine_signal, no second score() call
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

        clustered = (self.sel.is_clustered(ts, self.signal_side)
                     if self.signal_side else False)

        # Full decision — last_score is from PREVIOUS tick (acc check correct)
        dlog = build_trade_decision(
            score=score,
            ts=ts, price=price,
            cvd=cvd, trend=trend, vol=vol, spread=spread,
            regime=regime, eff_thr=eff_thr, r_mult=r_mult,
            signal_count=self.signal_count, clustered=clustered,
            last_trade_ts=self.last_trade_ts,
            last_score=self._last_score,   # previous tick's score
            cfg=CFG, gate=self._gate,
            v2_result=_v2_cache,
        )

        # Update last_score AFTER the decision (mirrors live executor step 8)
        self._last_score = dlog.score

        # Stats
        if dlog.signal != "FLAT":
            self.signals_nonflat += 1
        if dlog.near_miss:
            self.near_misses += 1

        if dlog.approved:
            self.signals_approved += 1
            self.by_regime[dlog.regime] += 1
            self.sel.record_entry(ts, dlog.signal)
            self.last_trade_ts = ts
            self.signal_side   = ""
            self.signal_count  = 0
            self.risk.record_trade(ts)
        else:
            self.rejections[dlog.rejection_reason or "flat_signal"] += 1

        return dlog


# ---------------------------------------------------------------------------
# Report helpers
# ---------------------------------------------------------------------------

def _pct(n: int, d: int) -> str:
    return f"{n/max(d,1)*100:.1f}%"


def _col(label: str, bt, live) -> str:
    delta = live - bt if isinstance(bt, (int, float)) and isinstance(live, (int, float)) else ""
    delta_str = f"{delta:+}" if isinstance(delta, (int, float)) else ""
    return f"  {label:<36} {str(bt):>10} {str(live):>10} {delta_str:>8}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_parity_check(db_path: str) -> None:
    print(f"\n{'='*68}")
    print(f"  AU2QWEN Parity Check | {db_path}")
    print("=" * 68 + "\n")

    # 1. Backtest (ground truth) -------------------------------------------
    print("Running backtest ...")
    overlay = RiskOverlay(cfg=GOAT_OVERLAY_CFG)
    bt = Au2Backtest(CFG, overlay=overlay)
    trades, _, metrics = bt.run(db_path)
    print(f"  BT : {metrics.total_trades:3d} trades | "
          f"WR {metrics.win_rate:.1%} | "
          f"PnL ${metrics.total_pnl:.2f} | "
          f"PF {metrics.profit_factor:.2f} | "
          f"DD {metrics.max_dd_pct:.2f}%\n")

    # 2. Live path simulation (always flat) --------------------------------
    print("Running live path simulation ...")
    sim = LivePathSimulator()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM signals ORDER BY ts ASC").fetchall()
    conn.close()
    for row in rows:
        sim.process_row(dict(row))
    print(f"  SIM: {sim.signals_approved:3d} approvals | "
          f"{sim.signals_nonflat} non-flat | "
          f"{sim.near_misses} near-misses\n")

    # 3. Comparison table --------------------------------------------------
    bt_n   = metrics.total_trades
    live_n = sim.signals_approved
    pct_diff = abs(live_n - bt_n) / max(bt_n, 1) * 100

    print(f"  {'Metric':<36} {'Backtest':>10} {'Live sim':>10} {'Delta':>8}")
    print("  " + "-" * 66)
    print(_col("Entries approved", bt_n, live_n))
    bt_eval = metrics.total_evaluated or "?"
    print(_col("Non-flat signals evaluated", bt_eval, sim.signals_nonflat))
    print(_col("Near-misses", "?", sim.near_misses))
    print(_col("Ticks risk-blocked", "?", sim.risk_blocked))

    # 4. Score distribution ------------------------------------------------
    if sim.score_abs:
        sa = sorted(sim.score_abs)
        n  = len(sa)
        print(f"\n  Score distribution (live sim, {n} scored ticks):")
        print(f"    mean={statistics.mean(sa):.3f}  "
              f"median={statistics.median(sa):.3f}  "
              f"p90={sa[int(0.9*n)]:.3f}  "
              f"max={sa[-1]:.3f}")
        below = sum(1 for s in sa if s < CFG.threshold)
        print(f"    threshold={CFG.threshold:.1f}  "
              f"below_thr={below}/{n} ({_pct(below,n)})")

    # 5. Rejection breakdown side-by-side ----------------------------------
    bt_rej  = dict(metrics.rejection_counts)
    sim_rej = dict(sim.rejections)
    all_reasons = sorted(
        set(bt_rej) | set(sim_rej),
        key=lambda r: -(bt_rej.get(r, 0) + sim_rej.get(r, 0)),
    )
    if all_reasons:
        print(f"\n  {'Rejection reason':<36} {'BT':>10} {'Live':>10}")
        print("  " + "-" * 58)
        for r in all_reasons:
            print(f"  {r:<36} {bt_rej.get(r, 0):>10} {sim_rej.get(r, 0):>10}")

    # 6. Approvals by regime -----------------------------------------------
    if sim.by_regime:
        print(f"\n  Approvals by regime (live sim):")
        for regime, cnt in sim.by_regime.most_common():
            print(f"    {regime:<16} {cnt:>4}")

    # 7. Divergence explanation and verdict --------------------------------
    print("\n  " + "-" * 66)

    # Overlay contribution
    overlay_block = bt_rej.get("blocked_by_daily_profit_cap", 0)
    if overlay_block > 0:
        print(f"  Note: BT overlay blocked {overlay_block:,} rows (profit cap 5%).")
        print(f"        Live sim has no overlay -> may show more approvals.")

    # Position rows contribution
    in_pos_rows = bt_rej.get("blocked_by_core_trade_limits", 0)
    if in_pos_rows > 0:
        print(f"  Note: BT skipped ~{in_pos_rows:,} rows while in position.")
        print(f"        Live sim stays flat -> scores every row.")

    if pct_diff <= 10:
        verdict = "[OK]   Entry counts within 10%"
    elif pct_diff <= 25:
        verdict = "[WARN] Entry counts differ by"
    else:
        verdict = "[FAIL] Entry counts differ by"
    print(f"\n  {verdict} {pct_diff:.1f}% ({live_n} vs {bt_n})\n")


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else str(_ROOT / "data" / "validated" / "au2_spot_24h.db")
    run_parity_check(db)

#!/usr/bin/env python3
"""AU2QWEN — Live vs Backtest parity check for GOAT CHALLENGE V3.

Usage
-----
  python diagnostics/diag_parity_live.py                          # defaults
  python diagnostics/diag_parity_live.py [db_path] [jsonl_path]
  python diagnostics/diag_parity_live.py --demo                   # synthetic sample

Two-level verdict
-----------------
  PIPELINE STATUS   : immediate health check — independent of sample size.
                      Based on field coherence, presence of decisions, gate logic.
                      OK / WARN / FAIL.

  STATISTICAL STATUS: meaningfulness of WR / PF / approval-rate comparison.
                      INSUFFICIENT SAMPLE (<5 trades)
                      LOW SAMPLE         (5-19 trades — pipeline only reliable)
                      OK / WARN / FAIL   (>=20 trades)

Expected structural divergences (not bugs)
------------------------------------------
- Live stays flat between trades; backtest skips in-position rows.
- Backtest overlay (daily profit cap 5%) blocks rows on profitable days.
- Time windows differ — count comparison is always approximate.
"""
from __future__ import annotations

import os
import statistics
import sys
import pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime", _ROOT / "presets"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

from collections import Counter
from dataclasses import dataclass, field
from typing import List, Optional

from au2_core import Au2Backtest
from au2_risk_overlay import RiskOverlay
from au2_goat_challenge_v3 import GOAT_CHALLENGE_V3_CFG, GOAT_CHALLENGE_V3_OVERLAY
from au2_decision_logger import DecisionLogger
from au2_decision import TradeDecisionLog

_DEFAULT_DB    = str(_ROOT / "data" / "validated" / "au2_spot_24h.db")
_DEFAULT_JSONL = str(_ROOT / "data" / "live" / "au2_challenge_v3_decisions.jsonl")

# Sample-size thresholds
_STAT_MIN_TRADES    = 20   # below this: statistical comparison not reliable
_STAT_LOW_TRADES    =  5   # below this: even trend direction is noise
_WR_WARN_DELTA      = 0.10 # abs(live_wr - bt_wr) > 10pp → WARN
_WR_FAIL_DELTA      = 0.20 # abs(live_wr - bt_wr) > 20pp → FAIL
_PF_WARN_RATIO      = 0.25 # |live_pf - bt_pf| / bt_pf > 25% → WARN
_PF_FAIL_RATIO      = 0.50 # |live_pf - bt_pf| / bt_pf > 50% → FAIL
_RATE_WARN_RATIO    = 0.30 # approval-rate delta > 30% relative → WARN


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class LiveStats:
    """Aggregated stats read from the JSONL decision log."""
    total_entries:    int = 0          # all lines in JSONL
    near_miss_flat:   int = 0          # signal=FLAT and near_miss=True
    nonflat:          int = 0          # signal != FLAT
    approved:         int = 0          # approved=True
    rejected_nonflat: int = 0          # signal != FLAT and not approved
    rejections:       Counter = field(default_factory=Counter)
    regimes:          Counter = field(default_factory=Counter)
    scores_approved:  List[float] = field(default_factory=list)
    scores_nonflat:   List[float] = field(default_factory=list)
    scores_near_miss: List[float] = field(default_factory=list)
    ts_first:         float = 0.0
    ts_last:          float = 0.0
    field_errors:     List[str] = field(default_factory=list)


@dataclass
class ExitStats:
    """Aggregated trade-result stats from _type=trade_result records."""
    n_trades:      int   = 0
    n_wins:        int   = 0
    gross_profit:  float = 0.0
    gross_loss:    float = 0.0      # stored positive
    hold_seconds:  List[float] = field(default_factory=list)
    exit_reasons:  Counter = field(default_factory=Counter)
    pnls:          List[float] = field(default_factory=list)

    @property
    def win_rate(self) -> Optional[float]:
        return self.n_wins / self.n_trades if self.n_trades else None

    @property
    def profit_factor(self) -> Optional[float]:
        return self.gross_profit / self.gross_loss if self.gross_loss > 0 else None

    @property
    def avg_hold_seconds(self) -> Optional[float]:
        return statistics.mean(self.hold_seconds) if self.hold_seconds else None

    @property
    def total_pnl(self) -> float:
        return self.gross_profit - self.gross_loss

    @property
    def session_seconds(self) -> float:
        return max(self.ts_last - self.ts_first, 0.0)

    @property
    def approval_rate(self) -> float:
        return self.approved / max(self.nonflat, 1)

    @property
    def near_miss_rate(self) -> float:
        """Near-misses as % of all scored signals (flat near-miss + nonflat)."""
        total = self.near_miss_flat + self.nonflat
        nm_total = self.near_miss_flat + sum(
            1 for _ in self.scores_near_miss  # nonflat near-misses (rare)
        )
        return nm_total / max(total, 1)


# ---------------------------------------------------------------------------
# JSONL reader — handles FLAT near-misses correctly
# ---------------------------------------------------------------------------

def _read_jsonl(path: str) -> LiveStats:
    """Read a JSONL decision log and return aggregated LiveStats.

    Key fix vs previous version: near-misses have signal=FLAT (they didn't
    cross the threshold, so no direction is assigned).  They were previously
    skipped by the ``if dlog.signal == 'FLAT': continue`` guard.  Now they
    are counted in their own bucket so the near-miss rate is accurate.
    """
    stats = LiveStats()
    if not os.path.exists(path):
        return stats

    for dlog in DecisionLogger.iter_file(path):
        stats.total_entries += 1
        ts = getattr(dlog, "ts", 0.0) or 0.0
        if ts > 0:
            if stats.ts_first == 0.0:
                stats.ts_first = ts
            stats.ts_last = ts

        # Field coherence check
        if dlog.approved and dlog.rejection_reason:
            stats.field_errors.append(
                f"ts={ts:.0f}: approved=True but rejection_reason={dlog.rejection_reason!r}"
            )
        if dlog.approved and dlog.signal == "FLAT":
            stats.field_errors.append(
                f"ts={ts:.0f}: approved=True but signal=FLAT"
            )
        if not dlog.approved and dlog.signal != "FLAT" and not dlog.rejection_reason:
            stats.field_errors.append(
                f"ts={ts:.0f}: rejected non-flat but rejection_reason is empty"
            )

        # Flat near-miss bucket (below threshold but worth tracking)
        if dlog.signal == "FLAT":
            if dlog.near_miss:
                stats.near_miss_flat += 1
                stats.scores_near_miss.append(abs(dlog.score))
            continue

        # Non-flat from here
        stats.nonflat += 1
        stats.scores_nonflat.append(abs(dlog.score))

        if dlog.approved:
            stats.approved += 1
            stats.regimes[dlog.regime] += 1
            stats.scores_approved.append(abs(dlog.score))
        else:
            stats.rejected_nonflat += 1
            stats.rejections[dlog.rejection_reason or "unknown"] += 1

    return stats


# ---------------------------------------------------------------------------
# Exit-stats reader
# ---------------------------------------------------------------------------

def _read_exit_stats(path: str) -> ExitStats:
    """Read trade_result records from the JSONL file."""
    stats = ExitStats()
    if not os.path.exists(path):
        return stats
    for r in DecisionLogger.iter_results(path):
        pnl = float(r.get("pnl_usd", 0.0))
        stats.n_trades += 1
        stats.pnls.append(pnl)
        if pnl > 0:
            stats.n_wins      += 1
            stats.gross_profit += pnl
        else:
            stats.gross_loss   += abs(pnl)
        hold = float(r.get("hold_seconds", 0.0))
        if hold > 0:
            stats.hold_seconds.append(hold)
        reason = r.get("exit_reason", "")
        stats.exit_reasons[reason] += 1
    return stats


# ---------------------------------------------------------------------------
# Pipeline verdict
# ---------------------------------------------------------------------------

def _pipeline_verdict(stats: LiveStats, jsonl_path: str):
    """Return (status, reasons) where status is 'OK'/'WARN'/'FAIL'."""
    if stats.total_entries == 0:
        if not os.path.exists(jsonl_path):
            return "FAIL", ["JSONL file not found — bot has not run yet"]
        return "FAIL", ["JSONL exists but contains 0 entries — bot may not have connected"]

    issues  = []
    warns   = []

    # Field coherence
    if stats.field_errors:
        for e in stats.field_errors[:3]:
            issues.append(f"Field error: {e}")

    # At least some signal activity
    total_signals = stats.near_miss_flat + stats.nonflat
    if total_signals == 0:
        issues.append("No scored signals found — all ticks were risk-blocked or errored")
    elif stats.nonflat == 0 and stats.near_miss_flat > 0:
        warns.append(
            f"Only near-misses observed ({stats.near_miss_flat}) — "
            "market may be in low-vol CHOP; threshold not crossed yet"
        )

    # Approved trade sanity
    if stats.approved > 0:
        bad_scores = [s for s in stats.scores_approved if s < GOAT_CHALLENGE_V3_CFG.threshold]
        if bad_scores:
            issues.append(
                f"{len(bad_scores)} approved trade(s) have abs(score) < threshold "
                f"({GOAT_CHALLENGE_V3_CFG.threshold}) — gate logic anomaly"
            )

    if issues:
        return "FAIL", issues
    if warns:
        return "WARN", warns
    return "OK", ["Decisions logged, field coherence verified, pipeline running normally"]


# ---------------------------------------------------------------------------
# Statistical verdict
# ---------------------------------------------------------------------------

def _statistical_verdict(stats: LiveStats, bt_wr: float, bt_pf: float,
                          bt_approval_rate: float,
                          exits: Optional["ExitStats"] = None):
    """Return (status, reasons, live_wr, live_pf).

    When exit data is present (exits.n_trades > 0) and the sample is
    large enough, compares WR and PF against backtest reference.
    """
    n = stats.approved
    n_exits = exits.n_trades if exits else 0

    if n < _STAT_LOW_TRADES:
        return (
            "INSUFFICIENT SAMPLE",
            [f"{n} trade(s) logged — need >= {_STAT_LOW_TRADES} for any trend, "
             f">= {_STAT_MIN_TRADES} for WR/PF comparison"],
            None, None,
        )

    if n < _STAT_MIN_TRADES:
        msg = (f"{n} trades logged — pipeline check valid; "
               f"WR/PF comparison requires >= {_STAT_MIN_TRADES} trades")
        if n_exits > 0:
            msg += f" ({n_exits} exits recorded)"
        return "LOW SAMPLE", [msg], None, None

    # Enough entry trades. Check if we also have exit data for WR/PF comparison.
    if not exits or n_exits < _STAT_LOW_TRADES:
        return "OK", [
            f"{n} trades logged — sample adequate for rejection-breakdown comparison; "
            f"WR/PF pending ({n_exits} exits so far, need >= {_STAT_LOW_TRADES})"
        ], None, None

    live_wr = exits.win_rate
    live_pf = exits.profit_factor

    reasons = []
    status  = "OK"

    if live_wr is not None:
        delta_wr = abs(live_wr - bt_wr)
        if delta_wr > _WR_FAIL_DELTA:
            status = "FAIL"
            reasons.append(f"WR delta={delta_wr:.1%} (live={live_wr:.1%} vs bt={bt_wr:.1%}) "
                           f"exceeds FAIL threshold ({_WR_FAIL_DELTA:.0%})")
        elif delta_wr > _WR_WARN_DELTA:
            if status == "OK": status = "WARN"
            reasons.append(f"WR delta={delta_wr:.1%} (live={live_wr:.1%} vs bt={bt_wr:.1%}) "
                           f"exceeds WARN threshold ({_WR_WARN_DELTA:.0%})")

    if live_pf is not None and bt_pf > 0:
        pf_ratio = abs(live_pf - bt_pf) / bt_pf
        if pf_ratio > _PF_FAIL_RATIO:
            status = "FAIL"
            reasons.append(f"PF delta={pf_ratio:.0%} (live={live_pf:.2f} vs bt={bt_pf:.2f}) "
                           f"exceeds FAIL threshold ({_PF_FAIL_RATIO:.0%})")
        elif pf_ratio > _PF_WARN_RATIO:
            if status == "OK": status = "WARN"
            reasons.append(f"PF delta={pf_ratio:.0%} (live={live_pf:.2f} vs bt={bt_pf:.2f}) "
                           f"exceeds WARN threshold ({_PF_WARN_RATIO:.0%})")

    if not reasons:
        reasons.append(
            f"{n_exits} exits — WR={live_wr:.1%} / PF={live_pf:.2f if live_pf else 'n/a'} "
            f"within expected range of BT (WR={bt_wr:.1%} / PF={bt_pf:.2f})"
        )

    return status, reasons, live_wr, live_pf


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

def _sep(char: str = "-", width: int = 68) -> str:
    return "  " + char * width


def _col(label: str, bt, live, width: int = 38) -> str:
    try:
        delta     = live - bt
        delta_str = f"{delta:+.3g}"
    except TypeError:
        delta_str = ""
    return f"  {label:<{width}} {str(bt):>10} {str(live):>10} {delta_str:>8}"


def _pct(n: int, d: int) -> str:
    return f"{n / max(d, 1) * 100:.1f}%"


def _score_line(label: str, scores: List[float], thr: float) -> None:
    if not scores:
        print(f"  {label}: (none)")
        return
    sa = sorted(scores)
    n  = len(sa)
    p90_idx = min(int(0.9 * n), n - 1)
    above   = sum(1 for s in sa if s >= thr)
    print(f"  {label} (n={n}): "
          f"min={sa[0]:.2f}  avg={statistics.mean(sa):.2f}  "
          f"p90={sa[p90_idx]:.2f}  max={sa[-1]:.2f}  "
          f"above_thr={above}/{n} ({_pct(above, n)})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_live_parity(db_path: str, jsonl_path: str) -> None:
    print(f"\n{'='*70}")
    print("  AU2QWEN Live Parity  |  GOAT CHALLENGE V3")
    print(f"  DB   : {db_path}")
    print(f"  LOG  : {jsonl_path}")
    print("=" * 70)

    # ── 1. Backtest ground truth ───────────────────────────────────────────────
    print("\n[1/4] Running backtest (GOAT_CHALLENGE_V3_CFG) ...")
    overlay = RiskOverlay(cfg=GOAT_CHALLENGE_V3_OVERLAY)
    bt      = Au2Backtest(GOAT_CHALLENGE_V3_CFG, overlay=overlay)
    trades, _, metrics = bt.run(db_path)
    bt_rej  = dict(metrics.rejection_counts) if hasattr(metrics, "rejection_counts") else {}
    bt_total_eval = metrics.total_evaluated or 0
    bt_approval   = metrics.total_trades / max(bt_total_eval, 1)
    print(
        f"  BT : {metrics.total_trades} trades | "
        f"WR {metrics.win_rate:.1%} | "
        f"PnL ${metrics.total_pnl:.2f} | "
        f"PF {metrics.profit_factor:.2f} | "
        f"DD {metrics.max_dd_pct:.2f}%"
    )

    # ── 2. Read JSONL ──────────────────────────────────────────────────────────
    print("\n[2/4] Reading live JSONL ...")
    stats = _read_jsonl(jsonl_path)
    exits = _read_exit_stats(jsonl_path)
    session_min = stats.session_seconds / 60
    if stats.total_entries:
        print(
            f"  Entries : {stats.total_entries} lines  |  "
            f"session ~{session_min:.1f} min  |  "
            f"trades={stats.approved}  non-flat={stats.nonflat}  "
            f"near-miss(flat)={stats.near_miss_flat}"
        )
        if exits.n_trades:
            wr_str = f"{exits.win_rate:.1%}" if exits.win_rate is not None else "n/a"
            pf_str = f"{exits.profit_factor:.2f}" if exits.profit_factor is not None else "n/a"
            print(f"  Exits   : {exits.n_trades} closed  |  WR={wr_str}  PF={pf_str}"
                  f"  PnL=${exits.total_pnl:.2f}")
        else:
            print("  Exits   : 0 (no trade_result records yet)")
    else:
        print(f"  (no data)")

    # ── 3. Pipeline verdict ────────────────────────────────────────────────────
    print("\n[3/4] Pipeline status ...")
    pipe_status, pipe_reasons = _pipeline_verdict(stats, jsonl_path)
    _pipe_icon = {"OK": "[OK]  ", "WARN": "[WARN]", "FAIL": "[FAIL]"}
    print(f"  {_pipe_icon.get(pipe_status, '     ')} PIPELINE : {pipe_status}")
    for r in pipe_reasons:
        print(f"         - {r}")

    # ── 4. Statistical verdict ─────────────────────────────────────────────────
    print("\n[4/4] Statistical status ...")
    stat_status, stat_reasons, live_wr, live_pf = _statistical_verdict(
        stats, metrics.win_rate, metrics.profit_factor, bt_approval, exits
    )
    _stat_icon = {
        "INSUFFICIENT SAMPLE": "[----]",
        "LOW SAMPLE":          "[LOW] ",
        "OK":                  "[OK]  ",
        "WARN":                "[WARN]",
        "FAIL":                "[FAIL]",
    }
    print(f"  {_stat_icon.get(stat_status, '     ')} STATISTICAL : {stat_status}")
    for r in stat_reasons:
        print(f"         - {r}")

    # ── Detailed metrics (always shown when we have data) ─────────────────────
    if stats.total_entries == 0:
        print("\n  No data to display.\n")
        return

    thr = GOAT_CHALLENGE_V3_CFG.threshold

    print(f"\n{_sep()}")
    print("  FUNNEL")
    print(_sep("."))
    total_sig = stats.near_miss_flat + stats.nonflat
    print(f"  {'Total scored signals':<36} {total_sig:>6}")
    print(f"  {'  of which near-miss (FLAT, score in [0.7*thr,thr))':<36} {stats.near_miss_flat:>6}"
          f"   ({_pct(stats.near_miss_flat, total_sig)})")
    print(f"  {'  of which non-flat (abs(score) >= thr)':<36} {stats.nonflat:>6}"
          f"   ({_pct(stats.nonflat, total_sig)})")
    print(f"  {'    -> approved':<36} {stats.approved:>6}"
          f"   ({_pct(stats.approved, stats.nonflat)} of non-flat)")
    print(f"  {'    -> rejected':<36} {stats.rejected_nonflat:>6}"
          f"   ({_pct(stats.rejected_nonflat, stats.nonflat)} of non-flat)")
    if session_min >= 5:
        rate_per_h = stats.approved / (session_min / 60)
        print(f"  {'Approvals / hour (extrapolated)':<36} {rate_per_h:>6.1f}")
    else:
        print(f"  {'Approvals / hour (extrapolated)':<36} {'n/a (<5 min session)':>6}")

    # Score distributions
    print(f"\n{_sep()}")
    print(f"  SCORE DISTRIBUTION  (threshold={thr:.1f})")
    print(_sep("."))
    _score_line("Approved   ", stats.scores_approved,  thr)
    _score_line("Non-flat   ", stats.scores_nonflat,   thr)
    _score_line("Near-miss  ", stats.scores_near_miss, thr)

    # Top rejection reasons
    if stats.rejections:
        print(f"\n{_sep()}")
        print("  REJECTION BREAKDOWN")
        print(_sep("."))
        print(f"  {'Reason':<36} {'Live':>8}   {'BT':>8}")
        print(_sep("."))
        for reason, cnt in stats.rejections.most_common(5):
            bt_cnt = bt_rej.get(reason, 0)
            print(f"  {reason:<36} {cnt:>8}   {bt_cnt:>8}")

    # Regime distribution of approved
    if stats.regimes:
        print(f"\n{_sep()}")
        print("  APPROVED TRADES BY REGIME")
        print(_sep("."))
        for regime, cnt in stats.regimes.most_common():
            bar = "#" * cnt
            print(f"  {regime:<20} {cnt:>4}  {_pct(cnt, stats.approved):>6}  {bar}")

    # Exit-side metrics (shown when trade_result records are present)
    if exits.n_trades > 0:
        print(f"\n{_sep()}")
        print("  EXIT-SIDE METRICS  (from trade_result records in JSONL)")
        print(_sep("."))
        wr_str  = f"{exits.win_rate:.1%}"  if exits.win_rate  is not None else "n/a"
        pf_str  = f"{exits.profit_factor:.2f}" if exits.profit_factor is not None else "n/a"
        hold_str = (f"{exits.avg_hold_seconds:.0f}s"
                    if exits.avg_hold_seconds is not None else "n/a")
        print(f"  {'Closed trades':<30} {exits.n_trades:>8}")
        print(f"  {'Win rate':<30} {wr_str:>8}")
        print(f"  {'Profit factor':<30} {pf_str:>8}")
        print(f"  {'Gross profit':<30} ${exits.gross_profit:>7.2f}")
        print(f"  {'Gross loss':<30} ${exits.gross_loss:>7.2f}")
        print(f"  {'Net PnL':<30} ${exits.total_pnl:>7.2f}")
        print(f"  {'Avg hold time':<30} {hold_str:>8}")
        if exits.exit_reasons:
            print(_sep("."))
            print(f"  {'Exit reason':<30} {'count':>8}   {'%':>6}")
            print(_sep("."))
            for reason, cnt in exits.exit_reasons.most_common():
                pct = cnt / exits.n_trades * 100
                print(f"  {reason:<30} {cnt:>8}   {pct:>5.1f}%")

    # BT comparison table (time-adjusted reminder)
    print(f"\n{_sep()}")
    print("  BACKTEST REFERENCE  (full 24h -- time window differs from live)")
    print(_sep("."))
    live_wr_str = f"{live_wr:.1%}"  if live_wr  is not None else ("n/a" if exits.n_trades == 0 else f"{exits.win_rate:.1%}")
    live_pf_str = f"{live_pf:.2f}" if live_pf  is not None else ("n/a" if exits.n_trades == 0 else f"{exits.profit_factor:.2f}" if exits.profit_factor else "n/a")
    print(f"  {'Metric':<36} {'BT 24h':>10} {'Live':>10}")
    print(_sep("."))
    print(f"  {'Trades':<36} {metrics.total_trades:>10} {stats.approved:>10}")
    print(f"  {'WR':<36} {metrics.win_rate:>10.1%} {live_wr_str:>10}")
    print(f"  {'PF':<36} {metrics.profit_factor:>10.2f} {live_pf_str:>10}")
    print(f"  {'Approval rate (non-flat->approved)':<36} {bt_approval:>10.1%} {stats.approval_rate:>10.1%}")
    print(f"  {'Max DD':<36} {metrics.max_dd_pct:>10.2f}% {'n/a':>10}")

    # ── Final summary ──────────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("  VERDICT SUMMARY")
    print(f"  Pipeline    : {pipe_status}")
    print(f"  Statistical : {stat_status}")
    print()

    if pipe_status == "FAIL":
        print("  ACTION REQUIRED -- pipeline issue detected. Check reasons above.")
    elif pipe_status == "WARN":
        print("  Pipeline warning -- review reasons above before interpreting stats.")
    elif stat_status == "INSUFFICIENT SAMPLE":
        print(
            f"  Pipeline is healthy. Let the bot run longer to collect "
            f">= {_STAT_MIN_TRADES} trades for statistical comparison."
        )
        if session_min >= 5 and stats.approved > 0:
            trades_per_h = stats.approved / (session_min / 60)
            hours_needed = _STAT_MIN_TRADES / max(trades_per_h, 0.01)
            if hours_needed < 1.0:
                print(f"  At current rate ({trades_per_h:.1f} trades/h), "
                      f"~{hours_needed * 60:.0f} min to reach {_STAT_MIN_TRADES} trades.")
            else:
                print(f"  At current rate ({trades_per_h:.1f} trades/h), "
                      f"~{hours_needed:.1f} h to reach {_STAT_MIN_TRADES} trades.")
    elif stat_status in ("OK", "LOW SAMPLE"):
        print("  Pipeline healthy. Collecting more data for full statistical validation.")
    else:
        print("  Statistical divergence detected -- review rejection breakdown and "
              "score distribution for root cause.")

    if exits.n_trades == 0:
        print(
            "\n  Note: WR/PF will appear here once the bot closes its first trade."
            "\n  Exit data is written automatically to the JSONL as trade_result records."
        )
    print(f"{'='*70}\n")


# ---------------------------------------------------------------------------
# Demo mode — synthetic data showing both verdict levels
# ---------------------------------------------------------------------------

def _run_demo() -> None:
    """Generate two synthetic JSONL files and show both verdict scenarios."""
    import tempfile, json, time, dataclasses

    thr = GOAT_CHALLENGE_V3_CFG.threshold
    now = time.time()

    def _make_entry(i: int, approved: bool, score: float,
                    regime: str = "CHOP") -> dict:
        from au2_decision import TradeDecisionLog
        near = (abs(score) >= 0.7 * thr) and (abs(score) < thr)
        sig  = "LONG" if approved else ("FLAT" if near else "LONG")
        rej  = "" if approved else ("flat_signal" if near else "vol_too_low")
        d    = TradeDecisionLog(
            ts=now + i, price=76000.0, regime=regime,
            score=score, eff_threshold=thr,
            signal=sig, confidence=1.1 if approved else 0.0,
            regime_quality=1.0, approved=approved,
            rejection_reason=rej, near_miss=near,
        )
        rec = dataclasses.asdict(d)
        rec["_logged_at"] = "2026-04-18T15:00:00Z"
        return rec

    # Scenario A: 2 trades — INSUFFICIENT SAMPLE
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False,
                                     encoding="utf-8") as f:
        path_small = f.name
        for i in range(40):   # 40 flat near-misses
            json.dump(_make_entry(i, False, thr * 0.75), f)
            f.write("\n")
        for i in range(2):    # 2 approved trades
            json.dump(_make_entry(40 + i, True, thr * 1.2), f)
            f.write("\n")

    # Scenario B: 25 trades — LOW SAMPLE (enough for rate comparison)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False,
                                     encoding="utf-8") as f:
        path_large = f.name
        for i in range(200):
            json.dump(_make_entry(i * 15, False, thr * 0.78), f)
            f.write("\n")
        for i in range(25):
            json.dump(_make_entry(200 * 15 + i * 30, True,
                                  thr * 1.15, "FLOW"), f)
            f.write("\n")
        for i in range(5):
            json.dump(_make_entry(200 * 15 + 25 * 30 + i * 10, False,
                                  thr * 1.05), f)
            f.write("\n")

    print("\n" + "#" * 70)
    print("#  DEMO MODE — scenario A: INSUFFICIENT SAMPLE (2 trades)")
    print("#" * 70)
    stats_a = _read_jsonl(path_small)
    p_a, pr_a = _pipeline_verdict(stats_a, path_small)
    s_a, sr_a, _, _ = _statistical_verdict(stats_a, 0.71, 1.63, 0.006)
    print(f"  Pipeline    : {p_a}  — {pr_a[0]}")
    print(f"  Statistical : {s_a}  — {sr_a[0]}")
    print(f"  Funnel      : {stats_a.near_miss_flat} near-misses | "
          f"{stats_a.nonflat} non-flat | {stats_a.approved} approved")

    print("\n" + "#" * 70)
    print("#  DEMO MODE — scenario B: LOW SAMPLE (25 trades, rate-comparable)")
    print("#" * 70)
    stats_b = _read_jsonl(path_large)
    p_b, pr_b = _pipeline_verdict(stats_b, path_large)
    s_b, sr_b, _, _ = _statistical_verdict(stats_b, 0.71, 1.63, 0.006)
    print(f"  Pipeline    : {p_b}  — {pr_b[0]}")
    print(f"  Statistical : {s_b}  — {sr_b[0]}")
    print(f"  Funnel      : {stats_b.near_miss_flat} near-misses | "
          f"{stats_b.nonflat} non-flat | {stats_b.approved} approved")
    print(f"  Approval rate : {stats_b.approval_rate:.1%}"
          f"  (BT ref: {bt_approval_demo:.1%})"
          if False else "")

    os.unlink(path_small)
    os.unlink(path_large)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = sys.argv[1:]
    if "--demo" in args:
        _run_demo()
    else:
        db   = args[0] if len(args) > 0 and not args[0].startswith("--") else _DEFAULT_DB
        log_ = args[1] if len(args) > 1 and not args[1].startswith("--") else _DEFAULT_JSONL
        run_live_parity(db, log_)

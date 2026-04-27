#!/usr/bin/env python3
"""GOAT CHALLENGE V3 -- Session Report Generator.

Reads the decision JSONL and state checkpoint, produces a human-readable
session report, and saves it to intelligence/daily_reports/.

Usage
-----
  python diagnostics/session_report_live.py
  python diagnostics/session_report_live.py data/live/au2_challenge_v3_decisions.jsonl
  python diagnostics/session_report_live.py --no-save    # console only

What is computed and what is NOT
---------------------------------
  AVAILABLE (from JSONL):
    - Session duration, entry count
    - Approved trades, near-miss count
    - Direction split (LONG / SHORT)
    - Regime distribution
    - Score statistics (near-miss bucket, approved bucket)
    - ADV reason breakdown (why adv_final was low)
    - Rejection reason breakdown

  AVAILABLE (from state checkpoint):
    - Current equity and delta from start equity
    - Loss streak, open position, risk state

  AVAILABLE (from trade_result records in JSONL, written on position close):
    - Win rate, profit factor, net PnL
    - Avg / best / worst trade PnL
    - Avg hold time
    - Exit reason distribution (SL / TP1 / TP2 / TIME_EXIT / BE)
    Section 4 shows "NO EXITS YET" until the first position closes.

  NOT AVAILABLE:
    - MFE / MAE per trade (require intra-trade price extremes)
    - Realised fees per trade (require qty * price * fee_rate at exit)
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import statistics
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Path setup (works whether run from repo root or diagnostics/)
# ---------------------------------------------------------------------------
_ROOT = pathlib.Path(__file__).resolve().parent.parent
_DEFAULT_JSONL  = str(_ROOT / "data" / "live" / "au2_challenge_v3_decisions.jsonl")
_DEFAULT_STATE  = str(_ROOT / "data" / "live" / "au2_challenge_v3_state.json")
_REPORTS_DIR    = _ROOT / "intelligence" / "daily_reports"

_START_EQUITY_DEFAULT = 10_000.0   # fallback -- matches run_goat_challenge_paper.py default
_BOT_LOG_PATH         = str(_ROOT / "data" / "live" / "bot_run.log")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class DecisionEntry:
    ts: float
    price: float
    signal: str
    score: float
    eff_threshold: float
    regime: str
    approved: bool
    near_miss: bool
    rejection_reason: str
    adv_reason: str
    adv_final: float
    confidence: float


@dataclass
class SessionData:
    # Raw entries
    entries: List[DecisionEntry] = field(default_factory=list)
    field_errors: List[str]     = field(default_factory=list)

    # Derived -- filled by analyse()
    approved:     List[DecisionEntry] = field(default_factory=list)
    near_misses:  List[DecisionEntry] = field(default_factory=list)
    nonflat:      List[DecisionEntry] = field(default_factory=list)

    ts_first: float = 0.0
    ts_last:  float = 0.0

    regimes_all:      Counter = field(default_factory=Counter)
    regimes_approved: Counter = field(default_factory=Counter)
    rejections:       Counter = field(default_factory=Counter)
    adv_reasons:      Counter = field(default_factory=Counter)
    directions:       Counter = field(default_factory=Counter)

    scores_near_miss: List[float] = field(default_factory=list)
    scores_approved:  List[float] = field(default_factory=list)
    eff_threshold: float = 8.0

    def analyse(self) -> None:
        if not self.entries:
            return
        self.ts_first = min(e.ts for e in self.entries)
        self.ts_last  = max(e.ts for e in self.entries)

        for e in self.entries:
            self.regimes_all[e.regime] += 1
            if e.adv_reason:
                self.adv_reasons[e.adv_reason] += 1

            if e.signal == "FLAT":
                if e.near_miss:
                    self.near_misses.append(e)
                    self.scores_near_miss.append(abs(e.score))
                continue

            # Non-flat signal
            self.nonflat.append(e)
            if e.approved:
                self.approved.append(e)
                self.regimes_approved[e.regime] += 1
                self.scores_approved.append(abs(e.score))
                self.directions[e.signal] += 1
            else:
                reason = e.rejection_reason or "unknown"
                self.rejections[reason] += 1

        if self.entries:
            self.eff_threshold = self.entries[-1].eff_threshold


# ---------------------------------------------------------------------------
# JSONL reader
# ---------------------------------------------------------------------------

def _read_jsonl(path: str, since: Optional[str] = None) -> SessionData:
    """Read decision entries from JSONL.

    Parameters
    ----------
    since : ISO logged_at string (e.g. '2026-04-22T01:00:00Z').
            When provided, only entries with _logged_at >= since are included.
    """
    data = SessionData()
    if not os.path.exists(path):
        return data
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                d = json.loads(raw)
            except json.JSONDecodeError as exc:
                data.field_errors.append(f"line {lineno}: JSON parse error: {exc}")
                continue
            # Skip trade_result records — handled separately by _read_exit_stats()
            if d.get("_type") == "trade_result":
                continue
            # Apply since filter
            if since and d.get("_logged_at", "") < since:
                continue
            try:
                entry = DecisionEntry(
                    ts              = float(d["ts"]),
                    price           = float(d.get("price", 0.0)),
                    signal          = str(d.get("signal", "FLAT")),
                    score           = float(d.get("score", 0.0)),
                    eff_threshold   = float(d.get("eff_threshold", 8.0)),
                    regime          = str(d.get("regime", "UNKNOWN")),
                    approved        = bool(d.get("approved", False)),
                    near_miss       = bool(d.get("near_miss", False)),
                    rejection_reason= str(d.get("rejection_reason", "")),
                    adv_reason      = str(d.get("adv_reason", "")),
                    adv_final       = float(d.get("adv_final", 0.0)),
                    confidence      = float(d.get("confidence", 0.0)),
                )
                data.entries.append(entry)
            except (KeyError, TypeError, ValueError) as exc:
                data.field_errors.append(f"line {lineno}: field error: {exc}")
    data.analyse()
    return data


# ---------------------------------------------------------------------------
# State file reader
# ---------------------------------------------------------------------------

def _read_state(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Restart detection
# ---------------------------------------------------------------------------

def _last_restart_logged_at(log_path: str = _BOT_LOG_PATH) -> Optional[str]:
    """Return ISO-like logged_at string of the most recent bot startup.

    Scans bot_run.log for the FIRST line containing 'GOAT-CHALLENGE' (the
    startup banner, written once per process launch) and converts its local
    timestamp to a UTC-aligned string by computing the offset between the
    local log clock and datetime.now(utc).

    The offset is estimated from the FIRST log line in the file:
      utc_offset = datetime.now(utc) - datetime.now(local)
    This keeps the function self-contained without requiring platform TZ APIs.

    Returns None if the log is absent or no startup marker is found.
    """
    if not os.path.exists(log_path):
        return None

    # Compute local-to-UTC offset from current wall clock
    import datetime as _dt
    now_utc   = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
    now_local = _dt.datetime.now()
    utc_offset = now_utc - now_local   # timedelta: positive if local is behind UTC

    marker = "GOAT-CHALLENGE"
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if marker in line:
                    # Line format: "2026-04-24 11:47:23,937 | INFO | ..."
                    parts = line.split("|")
                    if parts:
                        raw = parts[0].strip().split(",")[0]
                        try:
                            dt_local = _dt.datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
                            dt_utc   = dt_local + utc_offset
                            return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                        except ValueError:
                            pass
    except OSError:
        pass
    return None


# ---------------------------------------------------------------------------
# Exit stats reader
# ---------------------------------------------------------------------------

@dataclass
class ExitData:
    n_trades:     int   = 0
    n_wins:       int   = 0
    gross_profit: float = 0.0
    gross_loss:   float = 0.0   # stored positive
    pnls:         List[float] = field(default_factory=list)
    hold_seconds: List[float] = field(default_factory=list)
    exit_reasons: Counter = field(default_factory=Counter)

    @property
    def win_rate(self) -> Optional[float]:
        return self.n_wins / self.n_trades if self.n_trades else None

    @property
    def profit_factor(self) -> Optional[float]:
        return self.gross_profit / self.gross_loss if self.gross_loss > 0 else None

    @property
    def total_pnl(self) -> float:
        return self.gross_profit - self.gross_loss

    @property
    def avg_hold_seconds(self) -> Optional[float]:
        import statistics as _s
        return _s.mean(self.hold_seconds) if self.hold_seconds else None


def _read_exit_stats(path: str, since: Optional[str] = None) -> ExitData:
    """Read trade_result records from JSONL.

    Parameters
    ----------
    since : ISO logged_at cutoff — only records with _logged_at >= since included.
    """
    data = ExitData()
    if not os.path.exists(path):
        return data
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                d = json.loads(raw)
                if d.get("_type") != "trade_result":
                    continue
                if since and d.get("_logged_at", "") < since:
                    continue
                pnl = float(d.get("pnl_usd", 0.0))
                data.n_trades += 1
                data.pnls.append(pnl)
                if pnl > 0:
                    data.n_wins       += 1
                    data.gross_profit += pnl
                else:
                    data.gross_loss   += abs(pnl)
                hold = float(d.get("hold_seconds", 0.0))
                if hold > 0:
                    data.hold_seconds.append(hold)
                data.exit_reasons[str(d.get("exit_reason", ""))] += 1
            except Exception:
                continue
    return data


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_dur(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        return f"{seconds/60:.1f} min"
    return f"{seconds/3600:.2f} h"


def _score_stats(scores: List[float]) -> str:
    if not scores:
        return "n/a"
    if len(scores) == 1:
        return f"{scores[0]:.3f}"
    return (f"min={min(scores):.3f}  max={max(scores):.3f}"
            f"  mean={statistics.mean(scores):.3f}"
            f"  median={statistics.median(scores):.3f}")


def _bar(count: int, total: int, width: int = 20) -> str:
    if total == 0:
        return " " * width
    filled = round(count / total * width)
    return "#" * filled + "." * (width - filled)


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def build_report(
    jsonl_path: str,
    state_path: str,
    start_equity: float,
    since: Optional[str] = None,
    show_trade_rate: bool = False,
) -> str:
    data  = _read_jsonl(jsonl_path, since=since)
    exits = _read_exit_stats(jsonl_path, since=since)
    state = _read_state(state_path)
    now   = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines: List[str] = []

    def h(title: str = "") -> None:
        lines.append("=" * 72)
        if title:
            lines.append(f"  {title}")
            lines.append("=" * 72)

    def s(text: str = "") -> None:
        lines.append(text)

    def kv(key: str, value: str) -> None:
        lines.append(f"  {key:<32} {value}")

    # ── Header ───────────────────────────────────────────────────────────────
    h("GOAT CHALLENGE V3  --  SESSION REPORT")
    s(f"  Generated : {now}")
    s(f"  JSONL     : {jsonl_path}")
    s(f"  State     : {state_path}")
    if since:
        s(f"  Since     : {since}  (--since-restart filter active)")
    s()

    # ── Section 1: Session Overview ───────────────────────────────────────────
    h("SECTION 1 -- SESSION OVERVIEW")
    n_entries = len(data.entries)
    if n_entries == 0:
        s("  [EMPTY] No entries found in JSONL.")
        s()
    else:
        duration_s = data.ts_last - data.ts_first
        kv("Period (UTC):",
           f"{_fmt_ts(data.ts_first)}  ->  {_fmt_ts(data.ts_last)}")
        kv("Session duration:",  _fmt_dur(duration_s))
        kv("Total JSONL entries:", str(n_entries))
        kv("  of which approved:", str(len(data.approved)))
        kv("  of which near-miss:", str(len(data.near_misses)))
        # "other FLAT" = entries that are FLAT but NOT near-miss and NOT non-flat
        other_flat = n_entries - len(data.near_misses) - len(data.nonflat)
        kv("  of which other FLAT:", str(other_flat))
        kv("Non-flat signals:", str(len(data.nonflat)))
        if data.entries:
            kv("Price range (BTC):",
               f"${min(e.price for e in data.entries):,.2f}  to  "
               f"${max(e.price for e in data.entries):,.2f}")
        s()

    # ── Section 2: Entry-Side Metrics ────────────────────────────────────────
    h("SECTION 2 -- ENTRY-SIDE METRICS  [from JSONL]")
    if n_entries == 0:
        s("  No data.")
        s()
    else:
        n_appr   = len(data.approved)
        n_nm     = len(data.near_misses)
        n_nf     = len(data.nonflat)
        thr      = data.eff_threshold

        # Approval funnel
        s()
        s("  --- Approval funnel ---")
        if n_nf > 0:
            kv("Non-flat signals:", f"{n_nf}")
            kv("  -> approved:",
               f"{n_appr}  ({n_appr/n_nf*100:.1f}%)")
            kv("  -> rejected:",
               f"{n_nf - n_appr}  ({(n_nf-n_appr)/n_nf*100:.1f}%)")
        else:
            kv("Non-flat signals:", "0  (no signal crossed threshold)")
        kv("Near-misses (FLAT):", f"{n_nm}")
        s()

        # Direction split
        s("  --- Approved trade directions ---")
        if n_appr == 0:
            s("    No approved trades.")
        else:
            for side in ("LONG", "SHORT"):
                c = data.directions.get(side, 0)
                s(f"    {side:<6}  {c:>3}  [{_bar(c, n_appr)}]  {c/n_appr*100:.1f}%")
        s()

        # Regime distribution (all entries)
        s("  --- Regime distribution (all entries) ---")
        for regime, cnt in sorted(data.regimes_all.items(), key=lambda x: -x[1]):
            pct = cnt / n_entries * 100
            s(f"    {regime:<12}  {cnt:>4}  [{_bar(cnt, n_entries)}]  {pct:.1f}%")
        s()

        if data.regimes_approved:
            s("  --- Regime distribution (approved only) ---")
            for regime, cnt in sorted(data.regimes_approved.items(), key=lambda x: -x[1]):
                pct = cnt / max(n_appr, 1) * 100
                s(f"    {regime:<12}  {cnt:>4}  [{_bar(cnt, n_appr)}]  {pct:.1f}%")
            s()

        # Score distributions
        s("  --- Score distributions ---")
        kv("Effective threshold:", f"{thr:.1f}")
        if data.scores_near_miss:
            kv("Near-miss scores:", _score_stats(data.scores_near_miss))
            gap_to_thr = thr - statistics.mean(data.scores_near_miss)
            kv("  avg gap to threshold:", f"{gap_to_thr:.3f}  ({gap_to_thr/thr*100:.1f}% of threshold)")
        else:
            kv("Near-miss scores:", "none")
        if data.scores_approved:
            kv("Approved scores:", _score_stats(data.scores_approved))
        else:
            kv("Approved scores:", "none")
        s()

        # ADV reason breakdown
        if data.adv_reasons:
            s("  --- ADV reason breakdown (signal dampening) ---")
            total_adv = sum(data.adv_reasons.values())
            for reason, cnt in sorted(data.adv_reasons.items(), key=lambda x: -x[1]):
                pct = cnt / total_adv * 100
                s(f"    {reason:<24}  {cnt:>4}  {pct:.1f}%")
            s()

        # Rejection reason breakdown (non-flat rejected)
        if data.rejections:
            s("  --- Gate rejection reasons (non-flat signals only) ---")
            total_rej = sum(data.rejections.values())
            for reason, cnt in sorted(data.rejections.items(), key=lambda x: -x[1]):
                pct = cnt / total_rej * 100
                s(f"    {reason:<28}  {cnt:>4}  {pct:.1f}%")
            s()

    # ── Section 3: Account State ──────────────────────────────────────────────
    h("SECTION 3 -- ACCOUNT STATE  [from state checkpoint]")
    if state is None:
        s("  [MISSING] State file not found.")
        s()
    else:
        ckpt_ts    = float(state.get("ts", 0.0))
        equity     = float(state.get("equity", start_equity))
        delta      = equity - start_equity
        delta_pct  = delta / max(start_equity, 1) * 100
        streak     = int(state.get("loss_streak", 0))
        open_pos   = state.get("open_position")
        risk_state = state.get("risk_state", "?")
        age_s      = time.time() - ckpt_ts

        kv("Checkpoint time:", _fmt_ts(ckpt_ts))
        kv("Checkpoint age:", _fmt_dur(age_s))
        kv("Start equity:",   f"${start_equity:,.2f}")
        kv("Current equity:", f"${equity:,.2f}")
        sign = "+" if delta >= 0 else ""
        kv("Session delta:",  f"{sign}${delta:,.2f}  ({sign}{delta_pct:.3f}%)")
        kv("Loss streak:",    str(streak))
        kv("Open position:",  "NONE" if not open_pos else str(open_pos))
        kv("Risk state:",     risk_state)
        s()

        if ckpt_ts > 0 and n_entries > 0:
            # Cross-reference: equity delta vs approved count
            if len(data.approved) >= 1 and delta != 0:
                avg_per_trade = delta / len(data.approved)
                kv("[est] Avg PnL / approved trade:",
                   f"${avg_per_trade:+,.2f}  (equity delta / approved count, NOT per-trade PnL)")
                s("  NOTE: this is a session-level estimate, not per-trade accounting.")
                s()

    # ── Section 4: Exit-Side Metrics ─────────────────────────────────────────
    if exits.n_trades == 0:
        h("SECTION 4 -- EXIT-SIDE METRICS  [NO EXITS YET]")
        s()
        s("  No trade_result records found in the JSONL.")
        s("  WR / PF / hold time will appear here after the first position closes.")
        s()
    else:
        h("SECTION 4 -- EXIT-SIDE METRICS  [from trade_result records]")
        s()
        wr_str   = f"{exits.win_rate:.1%}"      if exits.win_rate   is not None else "n/a"
        pf_str   = f"{exits.profit_factor:.3f}" if exits.profit_factor is not None else "inf (no losses)"
        hold_str = (_fmt_dur(exits.avg_hold_seconds)
                    if exits.avg_hold_seconds is not None else "n/a")
        kv("Closed trades:", str(exits.n_trades))
        kv("Win rate:", f"{wr_str}  ({exits.n_wins}W / {exits.n_trades - exits.n_wins}L)")
        kv("Profit factor:", pf_str)
        kv("Gross profit:", f"${exits.gross_profit:.4f}")
        kv("Gross loss:", f"${exits.gross_loss:.4f}")
        sign = "+" if exits.total_pnl >= 0 else ""
        kv("Net PnL:", f"{sign}${exits.total_pnl:.4f}")
        kv("Avg hold time:", hold_str)
        if len(exits.pnls) > 1:
            sorted_pnl = sorted(exits.pnls)
            kv("Best trade:", f"${max(exits.pnls):.4f}")
            kv("Worst trade:", f"${min(exits.pnls):.4f}")
        s()
        if exits.exit_reasons:
            s("  --- Exit reason breakdown ---")
            for reason, cnt in exits.exit_reasons.most_common():
                pct = cnt / exits.n_trades * 100
                s(f"    {reason:<22}  {cnt:>4}  {pct:.1f}%")
            s()
        s("  Note: MFE/MAE and per-fee accounting not available (require intra-trade data).")
        s()

    # ── Section 5: Sample Sufficiency / Trade Rate ───────────────────────────
    section5_title = ("SECTION 5 -- TRADE RATE & SAMPLE SUFFICIENCY"
                      if show_trade_rate else "SECTION 5 -- SAMPLE SUFFICIENCY")
    h(section5_title)
    s()
    n_appr = len(data.approved)
    if n_appr == 0:
        s("  No approved trades yet.  Cannot evaluate statistical adequacy.")
    else:
        duration_s  = max(data.ts_last - data.ts_first, 1.0)
        session_min = duration_s / 60.0
        rate_per_h  = n_appr / max(duration_s / 3600, 1e-9)
        need_for_20  = max(0.0, (20  - n_appr) / max(rate_per_h, 1e-9))
        need_for_100 = max(0.0, (100 - n_appr) / max(rate_per_h, 1e-9))

        kv("Approved trades so far:", str(n_appr))
        kv("Session duration:", _fmt_dur(duration_s))

        if session_min >= 5.0:
            kv("Approval rate:", f"{rate_per_h:.1f} trades/h")
            if show_trade_rate:
                # Detailed ticks-to-trade funnel
                n_total  = len(data.entries)
                n_nf     = len(data.nonflat)
                n_nm     = len(data.near_misses)
                tick_rate = n_total / max(duration_s / 3600, 1e-9)
                s()
                s("  --- Signal funnel (per hour at current rate) ---")
                kv("  Ticks evaluated / h:",   f"{tick_rate:.0f}")
                kv("  Non-flat signals / h:",  f"{n_nf / max(duration_s/3600,1e-9):.1f}")
                kv("  Near-misses / h:",       f"{n_nm / max(duration_s/3600,1e-9):.1f}")
                kv("  Approved trades / h:",   f"{rate_per_h:.1f}")
                if n_nf > 0:
                    kv("  Gate pass rate:",    f"{n_appr/n_nf*100:.1f}%  of non-flat signals")
                s()
                s("  --- Projection (at current approval rate) ---")
        else:
            kv("Approval rate:", f"{rate_per_h:.1f} trades/h  (session < 5 min -- unreliable)")

        s()
        if n_appr < 5:
            s("  STATUS: INSUFFICIENT  (< 5 trades -- no statistical basis)")
        elif n_appr < 20:
            s("  STATUS: LOW SAMPLE  (5-19 trades -- patterns visible, not conclusive)")
        else:
            s("  STATUS: ADEQUATE  (>= 20 trades -- statistical comparison possible)")
        s()
        if session_min >= 5.0:
            if need_for_20 > 0:
                unit = "min" if need_for_20 < 1.0 else "h"
                val  = need_for_20 * 60 if need_for_20 < 1.0 else need_for_20
                kv("  Est. time to 20 trades:", f"{val:.1f} {unit}  (at current rate)")
            if need_for_100 > 0:
                unit = "min" if need_for_100 < 1.0 else "h"
                val  = need_for_100 * 60 if need_for_100 < 1.0 else need_for_100
                kv("  Est. time to 100 trades:", f"{val:.1f} {unit}")
        else:
            s("  Run for >= 5 min before extrapolating sample-size targets.")
        s()
        s("  Backtest reference (GOAT V3 + be_trigger patch, spot_24h):")
        s("    WR=73.0%  PF=1.87  100 trades  DD=1.59%")
        s("  Statistical parity check: python diagnostics/diag_parity_live.py")
    s()

    # ── Footer ────────────────────────────────────────────────────────────────
    h()
    if data.field_errors:
        s(f"  PARSE WARNINGS ({len(data.field_errors)} lines had errors):")
        for e in data.field_errors[:5]:
            s(f"    {e}")
        if len(data.field_errors) > 5:
            s(f"    ... and {len(data.field_errors)-5} more")
        s()

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Save report
# ---------------------------------------------------------------------------

def _save_report(report: str, jsonl_path: str) -> str:
    """Save to intelligence/daily_reports/ -- returns path written."""
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Derive filename from JSONL name + current time
    base = pathlib.Path(jsonl_path).stem  # e.g. au2_challenge_v3_decisions
    ts_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M")
    out_path = _REPORTS_DIR / f"session_report_{ts_str}_{base}.txt"

    out_path.write_text(report, encoding="utf-8")
    return str(out_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate a session report for the GOAT Challenge V3 paper bot.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "jsonl",
        nargs="?",
        default=_DEFAULT_JSONL,
        help=f"Path to decision JSONL (default: {_DEFAULT_JSONL})",
    )
    p.add_argument(
        "--state",
        default=_DEFAULT_STATE,
        help=f"Path to state JSON checkpoint (default: {_DEFAULT_STATE})",
    )
    p.add_argument(
        "--start-equity",
        type=float,
        default=_START_EQUITY_DEFAULT,
        help=f"Starting equity used for delta calculation (default: {_START_EQUITY_DEFAULT})",
    )
    p.add_argument(
        "--no-save",
        action="store_true",
        help="Print to console only, do not write to intelligence/daily_reports/",
    )
    p.add_argument(
        "--since-restart",
        action="store_true",
        help="Filter JSONL to entries since the last bot restart (reads data/live/bot_run.log)",
    )
    p.add_argument(
        "--since",
        default=None,
        metavar="ISO_TS",
        help="Filter JSONL to entries with _logged_at >= ISO_TS (e.g. 2026-04-22T01:00:00Z)",
    )
    p.add_argument(
        "--trade-rate",
        action="store_true",
        help="Show detailed signal funnel and trade-rate projection in Section 5",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    since: Optional[str] = None
    if args.since_restart:
        since = _last_restart_logged_at()
        if since is None:
            print("[WARN] --since-restart: could not detect last restart from bot_run.log; "
                  "showing full JSONL.", file=sys.stderr)
        else:
            print(f"[INFO] --since-restart: showing entries since {since}", file=sys.stderr)
    elif args.since:
        since = args.since

    report = build_report(
        args.jsonl, args.state, args.start_equity,
        since=since,
        show_trade_rate=args.trade_rate,
    )

    print(report)

    if not args.no_save:
        try:
            out_path = _save_report(report, args.jsonl)
            print(f"\n[Saved] {out_path}")
        except Exception as exc:
            print(f"\n[WARN] Could not save report: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()

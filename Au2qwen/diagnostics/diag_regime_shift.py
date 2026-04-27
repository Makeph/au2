#!/usr/bin/env python3
"""Regime shift diagnostic — compare BT periods + recent DB.

Answers the question: is live underperformance due to
  1. sample noise
  2. market regime shift
  3. CHOP low-vol failure
  4. live/backtest parity issue

Usage
-----
  python diagnostics/diag_regime_shift.py
  python diagnostics/diag_regime_shift.py --recent data/validated/au2_apr20_26.db
"""
from __future__ import annotations
import sys, pathlib, argparse, collections, sqlite3
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "presets"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

from au2_goat_challenge_v3 import GOAT_CHALLENGE_V3_CFG, GOAT_CHALLENGE_V3_OVERLAY
from au2_risk_overlay import RiskOverlay
from au2_core import Au2Backtest, Regime

SEP  = "-" * 78
SEP2 = "=" * 78

BT_DBS = {
    "apr15-16": "data/validated/au2_real_24h.db",
    "apr16-17": "data/validated/au2_spot_24h.db",
    "apr17-18": "data/validated/au2_fresh_24h.db",
}

LIVE_SNAPSHOT = {
    "label"        : "live (since restart)",
    "trades"       : 26,
    "wr"           : 0.385,
    "pf"           : 0.434,
    "net_pnl"      : -46.0,
    "chop_pct"     : 0.96,
    "flow_pct"     : 0.04,
    "exit_time_pct": 0.808,
    "be_fall_pct"  : 0.115,
    "sl_pct"       : 0.077,
}


# ── helpers ─────────────────────────────────────────────────────────────────

def _regime_pf(trades, regime_name):
    t = [x for x in trades if x.regime == regime_name]
    if not t:
        return 0, 0.0, float("nan"), 0.0
    wins = [x for x in t if x.pnl_usd > 0]
    gp   = sum(x.pnl_usd for x in wins)
    gl   = abs(sum(x.pnl_usd for x in t if x.pnl_usd <= 0))
    pf   = gp / gl if gl > 0 else float("inf")
    return len(t), len(wins) / len(t), pf, sum(x.pnl_usd for x in t)


def _exit_dist(trades):
    if not trades:
        return {}
    ec = collections.Counter(t.exit_reason for t in trades)
    total = len(trades)
    return {k: v / total for k, v in sorted(ec.items(), key=lambda x: -x[1])}


def _chop_vol_buckets(trades, db_path):
    """Break CHOP trades into vol buckets using the realized_vol_bps from the DB."""
    chop_trades = [t for t in trades if t.regime == "CHOP"]
    if not chop_trades:
        return {}

    # Build ts->vol lookup from DB
    conn = sqlite3.connect(db_path)
    ts_set = tuple(t.entry_ts for t in chop_trades)
    placeholders = ",".join("?" * len(ts_set))
    rows = conn.execute(
        f"SELECT ts, realized_vol_bps FROM signals WHERE ts IN ({placeholders})",
        ts_set,
    ).fetchall()
    conn.close()

    vol_map = {r[0]: r[1] for r in rows}

    buckets = collections.defaultdict(list)
    unmatched = 0
    for t in chop_trades:
        vol = vol_map.get(t.entry_ts)
        if vol is None:
            # Try nearest timestamp (within 1s)
            unmatched += 1
            buckets["unknown"].append(t)
            continue
        if vol < 2.0:
            buckets["<2 (auto-CHOP)"].append(t)
        elif vol < 3.0:
            buckets["2-3"].append(t)
        elif vol < 4.0:
            buckets["3-4"].append(t)
        elif vol < 6.0:
            buckets["4-6"].append(t)
        else:
            buckets["6+"].append(t)

    return dict(buckets)


def _bucket_stats(t_list):
    if not t_list:
        return 0, 0.0, float("nan"), 0.0
    wins = [t for t in t_list if t.pnl_usd > 0]
    gp   = sum(t.pnl_usd for t in wins)
    gl   = abs(sum(t.pnl_usd for t in t_list if t.pnl_usd <= 0))
    pf   = gp / gl if gl > 0 else float("inf")
    return len(t_list), len(wins) / len(t_list), pf, sum(t.pnl_usd for t in t_list)


def _run_bt(db_path):
    overlay = RiskOverlay(cfg=GOAT_CHALLENGE_V3_OVERLAY)
    bt = Au2Backtest(GOAT_CHALLENGE_V3_CFG, overlay=overlay)
    trades, _, metrics = bt.run(db_path)
    return trades, metrics


def _signal_regime_dist(db_path):
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT regime, COUNT(*) FROM signals GROUP BY regime").fetchall()
    total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    vol_rows = conn.execute("""
        SELECT AVG(realized_vol_bps),
               SUM(CASE WHEN realized_vol_bps < 2.0 THEN 1 ELSE 0 END)*1.0/COUNT(*),
               SUM(CASE WHEN realized_vol_bps < 3.0 THEN 1 ELSE 0 END)*1.0/COUNT(*)
        FROM signals
    """).fetchone()
    conn.close()
    d = {r[0]: r[1] / total for r in rows}
    return d, total, vol_rows


# ── sections ─────────────────────────────────────────────────────────────────

def section_overview(all_results):
    print(SEP2)
    print("SECTION 1 — BT OVERVIEW (GOAT CHALLENGE V3)")
    print(SEP2)
    hdr = f"{'PERIOD':<18}  {'N':>5}  {'WR':>6}  {'PF':>5}  {'PnL':>8}  {'DD%':>6}  {'CHOP':>6}  {'FLOW':>6}  {'TIME%':>6}  {'BE%':>6}  {'SL%':>5}"
    print(hdr)
    print("-" * len(hdr))

    for label, (trades, metrics, db_path) in all_results.items():
        total = len(trades)
        rc    = collections.Counter(t.regime for t in trades)
        ec    = collections.Counter(t.exit_reason for t in trades)
        chop_pct = rc.get("CHOP", 0) / max(total, 1)
        flow_pct = rc.get("FLOW", 0) / max(total, 1)
        time_pct = ec.get("EXIT_TIME", 0) / max(total, 1)
        be_pct   = ec.get("EXIT_BE_FALLBACK", 0) / max(total, 1)
        sl_pct   = ec.get("EXIT_SL", 0) / max(total, 1)
        print(f"{label:<18}  {total:>5}  {metrics.win_rate:>5.1%}  {metrics.profit_factor:>5.2f}  {metrics.total_pnl:>7.2f}  {metrics.max_dd_pct:>5.2f}%  {chop_pct:>5.1%}  {flow_pct:>5.1%}  {time_pct:>5.1%}  {be_pct:>5.1%}  {sl_pct:>4.1%}")

    # Live row
    l = LIVE_SNAPSHOT
    print(f"{'[LIVE]':<18}  {l['trades']:>5}  {l['wr']:>5.1%}  {l['pf']:>5.2f}  {l['net_pnl']:>7.2f}  {'?':>6}  {l['chop_pct']:>5.1%}  {l['flow_pct']:>5.1%}  {l['exit_time_pct']:>5.1%}  {l['be_fall_pct']:>5.1%}  {l['sl_pct']:>4.1%}")
    print()


def section_signal_regimes(all_results, dbs_with_paths):
    print(SEP)
    print("SECTION 2 — SIGNAL BAR REGIME DISTRIBUTION (all ticks)")
    print(SEP)
    hdr = f"{'PERIOD':<18}  {'TOTAL':>8}  {'CHOP':>6}  {'FLOW':>6}  {'TREND':>6}  {'MR':>6}  {'LIQ':>6}  {'avg_vol':>8}  {'<2bps':>6}  {'<3bps':>6}"
    print(hdr)
    print("-" * len(hdr))

    for label, db_path in dbs_with_paths.items():
        dist, total, vol_row = _signal_regime_dist(db_path)
        chop  = dist.get("CHOP", 0)
        flow  = dist.get("FLOW", 0)
        trend = dist.get("TREND", 0)
        mr    = dist.get("MEAN_REVERT", 0)
        liq   = dist.get("LIQUIDATION", 0)
        print(f"{label:<18}  {total:>8,}  {chop:>5.1%}  {flow:>5.1%}  {trend:>5.1%}  {mr:>5.1%}  {liq:>5.1%}  {vol_row[0]:>8.2f}  {vol_row[1]:>5.1%}  {vol_row[2]:>5.1%}")
    print(f"{'[LIVE ~today]':<18}  {'?':>8}  {'96%':>6}  {'4%':>6}  {'~0%':>6}  {'~0%':>6}  {'~0%':>6}  {'0.6?':>8}  {'~93%':>6}  {'~85%':>6}")
    print()


def section_regime_pf(all_results):
    print(SEP)
    print("SECTION 3 — PER-REGIME PERFORMANCE")
    print(SEP)
    for label, (trades, metrics, db_path) in all_results.items():
        print(f"\n  {label}  (total={len(trades)}, PF={metrics.profit_factor:.2f})")
        print(f"  {'REGIME':<14}  {'N':>5}  {'WR':>6}  {'PF':>6}  {'NetPnL':>8}")
        for r in ["CHOP", "FLOW", "TREND", "MEAN_REVERT", "LIQUIDATION"]:
            n, wr, pf, net = _regime_pf(trades, r)
            if n == 0:
                continue
            pf_str = f"{pf:.2f}" if pf != float("inf") else "inf"
            print(f"  {r:<14}  {n:>5}  {wr:>5.1%}  {pf_str:>6}  {net:>8.2f}")
    print()


def section_exits(all_results):
    print(SEP)
    print("SECTION 4 — EXIT DISTRIBUTION BY REGIME")
    print(SEP)
    for label, (trades, metrics, db_path) in all_results.items():
        print(f"\n  {label}")
        for regime in ["CHOP", "FLOW"]:
            t = [x for x in trades if x.regime == regime]
            if not t:
                continue
            dist = _exit_dist(t)
            parts = "  ".join(f"{k}={v:.0%}" for k, v in list(dist.items())[:4])
            avg_hold = sum(x.hold_seconds for x in t) / len(t)
            print(f"    {regime:<6}  n={len(t)}  hold={avg_hold:.0f}s  {parts}")
    print()


def section_chop_vol_buckets(all_results):
    print(SEP)
    print("SECTION 5 — CHOP PERFORMANCE BY VOL BUCKET")
    print(SEP)
    hdr = f"  {'PERIOD':<18}  {'BUCKET':<18}  {'N':>5}  {'WR':>6}  {'PF':>6}  {'NetPnL':>8}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    for label, (trades, metrics, db_path) in all_results.items():
        buckets = _chop_vol_buckets(trades, db_path)
        if not buckets:
            print(f"  {label:<18}  no CHOP trades")
            continue
        for bucket_label in ["<2 (auto-CHOP)", "2-3", "3-4", "4-6", "6+", "unknown"]:
            t_list = buckets.get(bucket_label, [])
            if not t_list:
                continue
            n, wr, pf, net = _bucket_stats(t_list)
            pf_str = f"{pf:.2f}" if pf != float("inf") and pf == pf else ("inf" if pf == float("inf") else "nan")
            print(f"  {label:<18}  {bucket_label:<18}  {n:>5}  {wr:>5.1%}  {pf_str:>6}  {net:>8.2f}")
    print()


def section_diagnosis(all_results, recent_label):
    print(SEP2)
    print("SECTION 6 — DIAGNOSIS")
    print(SEP2)

    labels = list(all_results.keys())
    recent = all_results.get(recent_label)
    old_periods = [v for k, v in all_results.items() if k != recent_label]

    # 1. Sample noise check
    live_n = LIVE_SNAPSHOT["trades"]
    print(f"\n[1] SAMPLE NOISE")
    print(f"    Live trades: {live_n}  (stat-sig threshold ~50)")
    if live_n < 50:
        print(f"    VERDICT: INSUFFICIENT SAMPLE — {50 - live_n} more trades needed before conclusions")
    else:
        print(f"    VERDICT: sample adequate for first-order analysis")

    # 2. Regime shift check
    print(f"\n[2] MARKET REGIME SHIFT")
    if recent:
        rt, rm, rdb = recent
        rc = collections.Counter(t.regime for t in rt)
        r_chop = rc.get("CHOP", 0) / max(len(rt), 1)
        r_flow = rc.get("FLOW", 0) / max(len(rt), 1)
        print(f"    Recent BT ({recent_label}): CHOP={r_chop:.1%}  FLOW={r_flow:.1%}  n_trades={len(rt)}")

        old_chops = [collections.Counter(t.regime for t in ts).get("CHOP", 0) / max(len(ts), 1)
                     for ts, ms, db in old_periods]
        avg_old_chop = sum(old_chops) / len(old_chops) if old_chops else 0
        print(f"    Old BT avg CHOP: {avg_old_chop:.1%}  |  Recent CHOP: {r_chop:.1%}  |  Live CHOP: 96%")

        if r_chop > avg_old_chop + 0.05:
            print(f"    VERDICT: REGIME SHIFT DETECTED — CHOP up +{r_chop - avg_old_chop:.1%} in recent vs old BT")
        else:
            print(f"    VERDICT: no significant regime shift in BT data (regime composition stable)")

    # 3. CHOP low-vol failure
    print(f"\n[3] CHOP LOW-VOL FAILURE")
    for label, (trades, metrics, db_path) in all_results.items():
        buckets = _chop_vol_buckets(trades, db_path)
        lo = buckets.get("<2 (auto-CHOP)", [])
        if lo:
            n, wr, pf, net = _bucket_stats(lo)
            pf_str = f"{pf:.2f}" if pf != float("inf") else "inf"
            flag = "LOSING" if pf < 1.0 else "OK"
            print(f"    {label}: <2bps CHOP  n={n}  WR={wr:.0%}  PF={pf_str}  [{flag}]")

    # 4. Live/BT parity
    print(f"\n[4] LIVE/BT PARITY")
    print(f"    Live EXIT_TIME: {LIVE_SNAPSHOT['exit_time_pct']:.1%}  vs BT range: see Section 4")
    print(f"    Live BE_FALLBACK: {LIVE_SNAPSHOT['be_fall_pct']:.1%}")
    print(f"    Note: be_trigger_bps was patched CHOP 3.0->2.5, FLOW 4.0->3.0 on 2026-04-22")
    print(f"    Patch was applied to LIVE code but BT uses same patched REGIME_PROFILES.")

    print()
    print(SEP2)
    print("SUMMARY — actionable conclusions follow from Section 6")
    print(SEP2)
    print()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--recent", default="data/validated/au2_apr20_26.db",
                   help="Path to recent signals DB (Apr 20-26)")
    args = p.parse_args()

    all_dbs = dict(BT_DBS)
    if pathlib.Path(args.recent).exists():
        all_dbs["apr20-26 (recent)"] = args.recent
    else:
        print(f"Warning: recent DB not found at {args.recent} — running without it")

    print(SEP2)
    print("AU2 GOAT CHALLENGE V3 — REGIME SHIFT DIAGNOSTIC")
    print(SEP2)
    print()

    print("Running backtests...")
    all_results = {}
    for label, db_path in all_dbs.items():
        if not pathlib.Path(db_path).exists():
            print(f"  {label}: DB missing ({db_path}) — skipping")
            continue
        print(f"  {label}...", end="", flush=True)
        trades, metrics = _run_bt(db_path)
        all_results[label] = (trades, metrics, db_path)
        print(f" {len(trades)} trades | PF={metrics.profit_factor:.2f}")

    print()

    recent_label = "apr20-26 (recent)" if "apr20-26 (recent)" in all_results else None

    section_overview(all_results)
    section_signal_regimes(all_results, all_dbs)
    section_regime_pf(all_results)
    section_exits(all_results)
    section_chop_vol_buckets(all_results)
    section_diagnosis(all_results, recent_label)


if __name__ == "__main__":
    main()

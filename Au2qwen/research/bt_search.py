#!/usr/bin/env python3
"""
Grid search over critical parameters to find a profitable configuration.

Hypotheses tested:
  1. Direction is wrong → flip signal (momentum instead of mean-revert)
  2. max_hold_seconds too short → positions timeout before TP
  3. tp1_pct too far → moves too small to hit in time
  4. threshold too strict/loose → signal noise vs opportunity
  5. stop_loss_pct too tight/wide → getting stopped out in noise
"""
import sys, time, itertools, dataclasses, logging
from dataclasses import replace

logging.basicConfig(level=logging.ERROR)  # silence overlay resets

from bt_engine import load_signals, run_backtest, summarize
from au2_goat_cash_preset import CFG as GOAT_BASE, OVERLAY_CFG as GOAT_OV, GOAT_CASH_START_EQUITY
from au2_risk_overlay import RiskOverlayConfig

DB = sys.argv[1] if len(sys.argv) > 1 else "../au2_v22_5_live_fresh.db"

# Disable overlay/profit-cap so search measures raw edge
SEARCH_OVERLAY = RiskOverlayConfig(
    daily_profit_cap_pct=999.0,
    post_loss_pause_trades=99,
    pause_duration_seconds=0.0,
    enable_post_loss_pause=False,
)

# Also relax base cfg to not block search via daily DD / loss streak
BASE = replace(
    GOAT_BASE,
    max_daily_trades=50,         # don't cap daily activity during search
    max_daily_dd_pct=20.0,       # relaxed
    daily_dd_amber_pct=10.0,
    daily_dd_red_pct=15.0,
    total_dd_amber_pct=15.0,
    total_dd_red_pct=18.0,
    max_total_dd_pct=25.0,
    max_consecutive_losses=99,   # no loss pause
    loss_pause_seconds=0,
    cooldown_seconds=30,         # keep reasonable cooldown
    confirmation_cycles=1,
    min_vol_bps=2.0,             # allow more signals in
    min_confidence_threshold=0.85,
    min_setup_quality=0.30,
)

# ── Search grid ─────────────────────────────────────────────────────────────
GRID = {
    "flip":        [False, True],
    "threshold":   [4.0, 5.5],
    "max_hold":    [120, 240, 480],
    "tp1":         [0.15, 0.30],
    "sl":          [0.15, 0.25],
}

def build_cfg(base, threshold, max_hold, tp1, sl):
    # Ensure tp2 > tp1 and runner coherent
    tp2 = max(tp1 * 2.2, tp1 + 0.15)
    return replace(
        base,
        threshold          = threshold,
        max_hold_seconds   = max_hold,
        tp1_pct            = tp1,
        tp2_pct            = tp2,
        stop_loss_pct      = sl,
    )

# ── Run grid ────────────────────────────────────────────────────────────────
print(f"Loading data from {DB}...")
t0 = time.time()
rows = load_signals(DB)
print(f"  {len(rows):,} rows loaded in {time.time()-t0:.1f}s")

combos = list(itertools.product(*GRID.values()))
print(f"\nRunning {len(combos)} configurations...\n")

results = []
for i, (flip, thr, mh, tp1, sl) in enumerate(combos, 1):
    cfg = build_cfg(BASE, thr, mh, tp1, sl)
    rm  = run_backtest(cfg, SEARCH_OVERLAY, None, GOAT_CASH_START_EQUITY, rows, flip_signal=flip)
    s   = summarize(rm)
    s["flip"] = flip; s["thr"] = thr; s["mh"] = mh; s["tp1"] = tp1; s["sl"] = sl
    results.append(s)
    tag = "MOM " if flip else "REV "
    print(f"  [{i:3d}/{len(combos)}] {tag} thr={thr:.1f} mh={mh:4d} tp1={tp1:.2f} sl={sl:.2f} "
          f"| n={s['n']:4d} wr={s['wr']*100:5.1f}% "
          f"pnl=${s['total_pnl']:+9.2f} pf={s['pf']:5.2f} exp=${s['exp']:+6.2f} "
          f"dd={s['dd']:5.2f}% tpH={s['exit_tp1_pct']:4.1f}%")

print("\n" + "═" * 95)

# ── Rank: profitable first, then by total PnL ───────────────────────────────
results.sort(key=lambda s: (-(s["exp"] > 0), -s["total_pnl"]))

print("\n── TOP 20 CONFIGURATIONS (sorted by expectancy>0 then total PnL) ──\n")
print(f"{'#':>3} {'dir':<5} {'thr':>4} {'mh':>4} {'tp1':>5} {'sl':>5}  "
      f"{'n':>4} {'wr':>6} {'pnl':>10} {'pf':>6} {'exp':>8} {'dd%':>6} {'tpH%':>6} {'tmH%':>6}")
print("-" * 95)
for i, s in enumerate(results[:20], 1):
    d = "MOM" if s["flip"] else "REV"
    print(f"{i:>3} {d:<5} {s['thr']:>4.1f} {s['mh']:>4d} {s['tp1']:>5.2f} {s['sl']:>5.2f}  "
          f"{s['n']:>4d} {s['wr']*100:>5.1f}% ${s['total_pnl']:>+8.2f} "
          f"{s['pf']:>5.2f}x ${s['exp']:>+6.2f} {s['dd']:>5.2f}% "
          f"{s['exit_tp1_pct']:>5.1f}% {s['exit_time_pct']:>5.1f}%")

# ── Summary verdict ──────────────────────────────────────────────────────────
profitable = [s for s in results if s["exp"] > 0 and s["n"] >= 10]
print("\n" + "═" * 95)
if profitable:
    best = profitable[0]
    d = "MOMENTUM" if best["flip"] else "MEAN-REVERT"
    print(f"\n✓ FOUND {len(profitable)} profitable configurations (expectancy > 0, n >= 10)")
    print(f"\nBest:")
    print(f"  Direction    : {d}")
    print(f"  Threshold    : {best['thr']}")
    print(f"  max_hold     : {best['mh']}s")
    print(f"  tp1_pct      : {best['tp1']}%")
    print(f"  stop_loss_pct: {best['sl']}%")
    print(f"  Trades       : {best['n']} over {best['ndays']} days ({best['trades_per_day']:.1f}/day)")
    print(f"  Win rate     : {best['wr']*100:.1f}%")
    print(f"  Total PnL    : ${best['total_pnl']:+.2f}")
    print(f"  Profit factor: {best['pf']:.2f}x")
    print(f"  Expectancy   : ${best['exp']:+.2f} per trade")
    print(f"  Max DD       : {best['dd']:.2f}%")
    print(f"  TP1 hit rate : {best['exit_tp1_pct']:.1f}%")
else:
    print("\n✗ NO PROFITABLE CONFIGURATION FOUND across the grid.")
    print("  Best expectancy:")
    top3 = sorted(results, key=lambda s: -s["exp"])[:3]
    for s in top3:
        d = "MOM" if s["flip"] else "REV"
        print(f"    {d} thr={s['thr']} mh={s['mh']} tp1={s['tp1']} sl={s['sl']}  "
              f"exp=${s['exp']:+.2f} pnl=${s['total_pnl']:+.2f} n={s['n']} pf={s['pf']:.2f}")
    print("\n  ==> v22_5 signal does NOT have exploitable alpha on this period.")
    print("      Do NOT deploy live. Review signal generation before tuning presets.")

# ── Key diagnostic aggregates ────────────────────────────────────────────────
print("\n── Direction test (average across grid) ──")
for flip in [False, True]:
    subset = [s for s in results if s["flip"] == flip]
    if subset:
        avg_pnl = sum(s["total_pnl"] for s in subset) / len(subset)
        avg_pf  = sum(s["pf"] for s in subset) / len(subset)
        avg_wr  = sum(s["wr"] for s in subset) / len(subset) * 100
        n_prof  = sum(1 for s in subset if s["exp"] > 0)
        label = "MOMENTUM (flipped)" if flip else "MEAN-REVERT (default)"
        print(f"  {label:<25}: avg_pnl=${avg_pnl:+8.2f}  avg_pf={avg_pf:.2f}x  avg_wr={avg_wr:.1f}%  profitable_configs={n_prof}/{len(subset)}")

print()

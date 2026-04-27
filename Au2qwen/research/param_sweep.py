#!/usr/bin/env python3
"""Parameter sweep to maximize PF on 24h data."""
import sys
from collections import defaultdict
from au2_core import Au2Backtest, CoreConfig, Regime
from au2_risk_overlay import RiskOverlay, RiskOverlayConfig

DB = sys.argv[1] if len(sys.argv) > 1 else "au2_spot_24h.db"

def run_cfg(threshold, sl_pct, be_bps, hold_s, tp1_pct):
    CFG = CoreConfig(
        threshold=threshold,
        regime_multiplier={Regime.TREND: 0.95, Regime.FLOW: 0.95, Regime.MEAN_REVERT: 1.05, Regime.LIQUIDATION: 0.9, Regime.CHOP: 1.4},
        risk_per_trade_pct=1.0, max_risk_usd=350.0,
        stop_loss_pct=sl_pct, tp1_pct=tp1_pct, tp2_pct=0.22, trailing_pct=0.10,
        breakeven_trigger_bps=be_bps,
        max_hold_seconds=hold_s, cooldown_seconds=15, confirmation_cycles=1, max_daily_trades=50,
        loss_pause_seconds=600, max_consecutive_losses=3, loss_penalty_risk_mult=0.5,
        daily_dd_amber_pct=3.5, daily_dd_red_pct=4.5,
        total_dd_amber_pct=7.0, total_dd_red_pct=8.5, max_total_dd_pct=10.0,
        entry_fee_mode="maker", taker_fee_bps=0.5,  # limit-stop blended: 95% maker + 5% taker
    )
    overlay = RiskOverlay(cfg=RiskOverlayConfig(daily_profit_cap_pct=5.0))
    bt = Au2Backtest(CFG, overlay=overlay)
    trades, _, m = bt.run(DB)

    # exit breakdown
    by_exit = defaultdict(list)
    for t in trades:
        by_exit[t.exit_reason].append(t.pnl_usd)
    sl_cnt = len(by_exit.get("EXIT_SL", []))
    sl_total = sum(by_exit.get("EXIT_SL", []))
    time_cnt = len(by_exit.get("EXIT_TIME", []))
    time_total = sum(by_exit.get("EXIT_TIME", []))
    be_cnt = len(by_exit.get("EXIT_BE_FALLBACK", []))

    return m, sl_cnt, sl_total, time_cnt, time_total, be_cnt


print(f"\nSweep on {DB}")
print(f"{'Thr':>5} {'SL%':>5} {'BE':>5} {'Hold':>5} {'TP1':>5} | {'Trades':>6} {'WR':>6} {'PF':>6} {'PnL':>8} {'DD':>6} | SL_n/tot | TIME_n/tot")
print("-" * 110)

# Threshold sweep (fixed other params)
for thr in [5.0, 6.0, 7.0, 8.0, 9.0, 10.0]:
    m, sl_n, sl_tot, t_n, t_tot, be_n = run_cfg(thr, 0.12, 12.0, 120, 0.15)
    print(f"  {thr:>3.1f}  0.12   12   120  0.15 | {m.total_trades:>6} {m.win_rate:>6.1%} {m.profit_factor:>6.2f} {m.total_pnl:>8.2f} {m.max_dd_pct:>5.1f}% | {sl_n:>3}/{sl_tot:>7.0f} | {t_n:>4}/{t_tot:>7.0f}")

print()
print("--- SL width sweep (thr=7.0) ---")
for sl in [0.06, 0.08, 0.10, 0.12, 0.15, 0.18]:
    be = sl * 100 * 0.8  # BE trigger = 80% of SL in bps
    m, sl_n, sl_tot, t_n, t_tot, be_n = run_cfg(7.0, sl, be, 120, sl * 1.3)
    print(f"  7.0  {sl:>4.2f}  {be:>4.1f}   120  {sl*1.3:>4.2f} | {m.total_trades:>6} {m.win_rate:>6.1%} {m.profit_factor:>6.2f} {m.total_pnl:>8.2f} {m.max_dd_pct:>5.1f}% | {sl_n:>3}/{sl_tot:>7.0f} | {t_n:>4}/{t_tot:>7.0f}")

print()
print("--- Hold time sweep (thr=7.0, sl=0.12) ---")
for hold in [30, 45, 60, 90, 120, 180]:
    m, sl_n, sl_tot, t_n, t_tot, be_n = run_cfg(7.0, 0.12, 12.0, hold, 0.15)
    print(f"  7.0  0.12   12  {hold:>4}  0.15 | {m.total_trades:>6} {m.win_rate:>6.1%} {m.profit_factor:>6.2f} {m.total_pnl:>8.2f} {m.max_dd_pct:>5.1f}% | {sl_n:>3}/{sl_tot:>7.0f} | {t_n:>4}/{t_tot:>7.0f}")

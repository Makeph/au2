#!/usr/bin/env python3
"""Sweep TREND_THRESHOLD_BPS in au2_signal_regime.py."""
import sys
import au2_signal_regime as reg_mod
from au2_core import Au2Backtest, CoreConfig, Regime
from au2_risk_overlay import RiskOverlay, RiskOverlayConfig

DB = sys.argv[1] if len(sys.argv) > 1 else "au2_spot_24h.db"

CFG = CoreConfig(
    threshold=7.0,
    regime_multiplier={Regime.TREND: 0.95, Regime.FLOW: 0.95, Regime.MEAN_REVERT: 1.05, Regime.LIQUIDATION: 0.9, Regime.CHOP: 1.4},
    risk_per_trade_pct=1.0, max_risk_usd=350.0,
    stop_loss_pct=0.12, tp1_pct=0.15, tp2_pct=0.22, trailing_pct=0.10,
    breakeven_trigger_bps=12.0,
    max_hold_seconds=120, cooldown_seconds=15, confirmation_cycles=1, max_daily_trades=50,
    loss_pause_seconds=600, max_consecutive_losses=3, loss_penalty_risk_mult=0.5,
    daily_dd_amber_pct=3.5, daily_dd_red_pct=4.5,
    total_dd_amber_pct=7.0, total_dd_red_pct=8.5, max_total_dd_pct=10.0,
    entry_fee_mode="maker", taker_fee_bps=0.5,
)

print(f"\nTREND_THRESHOLD_BPS sweep on {DB} (thr=7.0, sl=0.12, be=12.0)")
print(f"{'Trend_Thr':>10} | {'Trades':>6} {'WR':>6} {'PF':>6} {'PnL':>8} {'DD':>6}")
print("-" * 55)

for trend_thr in [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 10.0, 15.0]:
    # Monkey-patch the module constant and RegimeAdaptiveSignal default
    reg_mod.TREND_THRESHOLD_BPS = trend_thr

    overlay = RiskOverlay(cfg=RiskOverlayConfig(daily_profit_cap_pct=5.0))
    bt = Au2Backtest(CFG, overlay=overlay)
    _, _, m = bt.run(DB)
    print(f"  {trend_thr:>8.1f} | {m.total_trades:>6} {m.win_rate:>6.1%} {m.profit_factor:>6.2f} {m.total_pnl:>8.2f} {m.max_dd_pct:>5.1f}%")

# Reset
reg_mod.TREND_THRESHOLD_BPS = 5.0

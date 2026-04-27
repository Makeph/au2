#!/usr/bin/env python3
"""AU2 PTB MODE — Aggressive / Momentum Capture"""
from au2_core import Au2Backtest, CoreConfig, Regime
import sys

CFG = CoreConfig(
    threshold=3.50,
    regime_multiplier={Regime.TREND: 1.0, Regime.FLOW: 1.0, Regime.MEAN_REVERT: 0.95, Regime.LIQUIDATION: 1.05, Regime.CHOP: 1.2},
    risk_per_trade_pct=1.8, max_risk_usd=600.0,
    stop_loss_pct=0.28, tp1_pct=0.25, tp2_pct=0.60, trailing_pct=0.18,
    max_hold_seconds=180, cooldown_seconds=15, confirmation_cycles=1, max_daily_trades=12,
    daily_dd_amber_pct=4.0, daily_dd_red_pct=5.5,
    total_dd_amber_pct=6.5, total_dd_red_pct=8.0, max_total_dd_pct=10.0
)

if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv)>1 else "au2_v22_5_live_fresh.db"
    bt = Au2Backtest(CFG)
    trades, _, metrics = bt.run(db)
    print(f"🔴 PTB | Trades:{metrics.total_trades} | WR:{metrics.win_rate:.1%} | PnL:${metrics.total_pnl:.2f} | PF:{metrics.profit_factor:.2f} | DD:{metrics.max_dd_pct:.2f}%")
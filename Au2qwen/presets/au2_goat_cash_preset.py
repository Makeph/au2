#!/usr/bin/env python3
"""AU2 GOAT CASH PRESET — v6.1 | High Frequency / Cashflow Optimized"""
import sys
import logging
from au2_core import Au2Backtest, CoreConfig, Regime
from au2_risk_overlay import RiskOverlay, RiskOverlayConfig
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("au2_preset")

CFG = CoreConfig(
    threshold=4.80,
    regime_multiplier={Regime.TREND: 0.80, Regime.FLOW: 0.85, Regime.MEAN_REVERT: 1.05, Regime.LIQUIDATION: 0.75, Regime.CHOP: 1.60},
    regime_quality_multiplier={Regime.TREND: 1.0, Regime.FLOW: 1.0, Regime.MEAN_REVERT: 0.95, Regime.LIQUIDATION: 1.0, Regime.CHOP: 0.80},
    risk_per_trade_pct=0.65,
    max_risk_usd=175.0, min_risk_usd=10.0,
    stop_loss_pct=0.18,
    tp1_pct=0.35, tp2_pct=0.80,
    tp1_ratio=0.50, tp2_ratio=0.30, runner_ratio=0.20,
    trailing_pct=0.08,
    breakeven_trigger_bps=4.0, breakeven_buffer_bps=1.5,
    max_hold_seconds=150,
    cooldown_seconds=60, confirmation_cycles=1,
    max_daily_trades=6,
    min_vol_bps=3.0, max_spread_bps=4.0,
    min_confidence_threshold=0.88,
    min_setup_quality=0.50,
    min_score_acceleration=0.95,
    cluster_window_s=120, cluster_max_per_side=2,
    loss_pause_seconds=300, max_consecutive_losses=2, loss_penalty_risk_mult=0.50,
    taker_fee_bps=4.5, slippage_fixed_bps=0.5, slippage_spread_ratio=0.35,
    max_total_dd_pct=6.0,
    max_daily_dd_pct=2.5, daily_dd_amber_pct=1.5, daily_dd_red_pct=2.2,
    total_dd_amber_pct=3.0, total_dd_red_pct=4.5
)

OVERLAY_CFG = RiskOverlayConfig(
    daily_profit_cap_pct=2.0,
    post_loss_pause_trades=2,
    pause_duration_seconds=600.0,
    enable_post_loss_pause=False,
)
GOAT_CASH_START_EQUITY = 10000.0

def run_goat_cash(db_path: str):
    overlay = RiskOverlay(start_equity=GOAT_CASH_START_EQUITY, cfg=OVERLAY_CFG)
    bt = Au2Backtest(CFG, overlay=overlay, prop=None)
    return bt.run(db_path)

if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else "au2_v22_5_live_fresh.db"
    trades, _, metrics = run_goat_cash(db)
    log.info(f"🟢 GOAT CASH | T:{metrics.total_trades} | WR:{metrics.win_rate:.1%} | PF:{metrics.profit_factor:.2f} | DD:{metrics.max_dd_pct:.2f}% | PnL:${metrics.total_pnl:.2f}")
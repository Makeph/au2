#!/usr/bin/env python3
"""AU2 FTMO 10k PRESET — v6.1 Cleaned & Integrated"""
import sys
import logging
from au2_core import Au2Backtest, CoreConfig, Regime, PROP_FTMO_SAFE
from au2_risk_overlay import RiskOverlay, RiskOverlayConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("au2_preset")

CFG = CoreConfig(
    threshold=5.50, regime_multiplier={Regime.TREND: 0.85, Regime.FLOW: 0.90, Regime.MEAN_REVERT: 1.15, Regime.LIQUIDATION: 0.80, Regime.CHOP: 1.80},
    regime_quality_multiplier={Regime.TREND: 1.0, Regime.FLOW: 1.0, Regime.MEAN_REVERT: 0.95, Regime.LIQUIDATION: 1.05, Regime.CHOP: 0.75},
    risk_per_trade_pct=0.30, max_risk_usd=30.0, min_risk_usd=5.0,
    stop_loss_pct=0.18, tp1_pct=0.28, tp2_pct=0.45, tp1_ratio=0.55, tp2_ratio=0.25, runner_ratio=0.20,
    trailing_pct=0.08, breakeven_trigger_bps=4.0, breakeven_buffer_bps=1.5,
    max_hold_seconds=120, cooldown_seconds=120, confirmation_cycles=2, max_daily_trades=3,
    min_vol_bps=4.0, max_spread_bps=3.5, min_confidence_threshold=0.95, min_setup_quality=0.60,
    min_score_acceleration=1.05, cluster_window_s=180, cluster_max_per_side=2,
    loss_pause_seconds=300, max_consecutive_losses=2, loss_penalty_risk_mult=0.50,
    taker_fee_bps=4.5, slippage_fixed_bps=0.5, slippage_spread_ratio=0.35,
    max_total_dd_pct=10.0, max_daily_dd_pct=5.0, daily_dd_amber_pct=2.0, daily_dd_red_pct=4.0,
    total_dd_amber_pct=5.0, total_dd_red_pct=7.0, min_adv_score=2.5
)

OVERLAY_CFG = RiskOverlayConfig(daily_profit_cap_pct=0.80, post_loss_pause_trades=2, pause_duration_seconds=600.0, enable_post_loss_pause=False) # ✅ Patch 7

def run_ftmo_safe(db_path: str):
    overlay = RiskOverlay(cfg=OVERLAY_CFG)
    # ✅ Patch 4: Prop injected at construction
    bt = Au2Backtest(CFG, overlay=overlay, prop=PROP_FTMO_SAFE)
    return bt.run(db_path)

if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else "au2_v22_5_live_fresh.db"
    try:
        trades, _, metrics = run_ftmo_safe(db)
        log.info(f"🟢 FTMO 10k | T:{metrics.total_trades} | WR:{metrics.win_rate:.1%} | PF:{metrics.profit_factor:.2f} | DD:{metrics.max_dd_pct:.2f}% | PnL:${metrics.total_pnl:.2f}")
    except Exception as e:
        log.error(f"Backtest failed: {e}")
        sys.exit(1)
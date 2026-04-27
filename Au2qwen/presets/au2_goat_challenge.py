#!/usr/bin/env python3
"""AU2 GOAT CHALLENGE — Defensive risk profile for prop-firm challenge phase.

Based strictly on GOAT_VALIDATED_CFG (Apr 2026).
Signal, exits, and regime logic are unchanged.
Only risk sizing, daily limits, and selectivity filters are tightened.

Validated baseline (GOAT, for comparison):
  au2_spot_24h.db  24h  PF 1.86 | WR 74% | +$919 | DD 2.5%
  au2_fresh_24h.db 24h  PF 1.35 | WR 56% | +$261 | DD 3.7%

Challenge constraints assumed:
  - Daily DD hard limit  : 4.0%   (we cut at 2.25% red / 2.75% max)
  - Total DD hard limit  : 10.0%  (we cut at 5.5% red / 7.5% max)
  - Profit target        : ~8-10% over 30 days

Architecture notes (same as GOAT):
  - breakeven_trigger_bps and max_hold_seconds are NOT used by PositionManager;
    REGIME_PROFILES[regime] controls those at runtime.
  - Fee model: maker entry (0.2 bps) + taker SL (0.5 bps) = 0.7 bps round-trip.
"""
import sys, pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

from au2_core import CoreConfig, Regime
from au2_risk_overlay import RiskOverlay, RiskOverlayConfig


# ---------------------------------------------------------------------------
# Challenge config
# ---------------------------------------------------------------------------

def build_goat_challenge_config() -> CoreConfig:
    """Defensive variant of GOAT for challenge phase.  Signal is frozen."""
    return CoreConfig(
        # ── Signal — unchanged from GOAT validated ────────────────────────────
        threshold=8.0,
        regime_multiplier={
            Regime.TREND:       0.95,
            Regime.FLOW:        0.95,
            Regime.MEAN_REVERT: 1.05,
            Regime.LIQUIDATION: 0.90,
            Regime.CHOP:        1.40,
        },

        # ── Exits — unchanged from GOAT validated ────────────────────────────
        # NOTE: breakeven_trigger_bps / max_hold_seconds ignored by PositionManager.
        stop_loss_pct=0.18,
        tp1_pct=0.23,
        tp2_pct=0.35,
        trailing_pct=0.12,
        breakeven_trigger_bps=15.0,
        max_hold_seconds=120,

        # ── Risk sizing — reduced for challenge ──────────────────────────────
        risk_per_trade_pct=0.50,
        max_risk_usd=50.0,
        min_risk_usd=10.0,

        # ── Selectivity — tightened ──────────────────────────────────────────
        cooldown_seconds=60,
        confirmation_cycles=1,
        max_daily_trades=15,
        min_vol_bps=5.0,
        max_spread_bps=3.0,
        assume_spread_bps=0.5,

        # ── Quality gates — stricter than GOAT ───────────────────────────────
        min_confidence_threshold=0.95,
        min_setup_quality=0.60,
        min_score_acceleration=0.70,
        min_adv_score=2.8,

        # ── Anti-streak ───────────────────────────────────────────────────────
        loss_pause_seconds=1800,       # 30 min after streak (vs 10 min GOAT)
        max_consecutive_losses=2,      # cut after 2 (vs 3 GOAT)
        loss_penalty_risk_mult=0.5,

        # ── Fees ──────────────────────────────────────────────────────────────
        entry_fee_mode="maker",
        taker_fee_bps=0.5,
        maker_fee_bps=0.2,

        # ── Daily DD limits — cut well before challenge hard limits ───────────
        # Challenge hard limit assumed: 4% daily, 10% total.
        # We stop internally at 2.25% / 7.5% so margin is never breached.
        daily_dd_amber_pct=1.5,
        daily_dd_red_pct=2.25,
        max_daily_dd_pct=2.75,

        # ── Total DD limits ───────────────────────────────────────────────────
        total_dd_amber_pct=4.0,
        total_dd_red_pct=5.5,
        max_total_dd_pct=7.5,
    )


GOAT_CHALLENGE_CFG: CoreConfig = build_goat_challenge_config()

# Overlay locks in a green day early to protect the profit target.
GOAT_CHALLENGE_OVERLAY: RiskOverlayConfig = RiskOverlayConfig(
    daily_profit_cap_pct=2.5,
)


# ---------------------------------------------------------------------------
# Entry point — backtest
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from au2_core import Au2Backtest

    db = sys.argv[1] if len(sys.argv) > 1 else str(
        _ROOT / "data" / "validated" / "au2_spot_24h.db"
    )
    overlay = RiskOverlay(cfg=GOAT_CHALLENGE_OVERLAY)
    bt = Au2Backtest(GOAT_CHALLENGE_CFG, overlay=overlay)
    trades, _, metrics = bt.run(db)
    print(
        f"[GOAT-CHALLENGE] {db}\n"
        f"  Trades : {metrics.total_trades}\n"
        f"  WR     : {metrics.win_rate:.1%}\n"
        f"  PnL    : ${metrics.total_pnl:.2f}\n"
        f"  PF     : {metrics.profit_factor:.2f}\n"
        f"  DD     : {metrics.max_dd_pct:.2f}%"
    )

#!/usr/bin/env python3
"""AU2QWEN — Single source of truth for the validated GOAT configuration.

Import GOAT_CFG / GOAT_OVERLAY_CFG from here in ALL scripts:
backtest, live executor, paper bot, diagnostics, tests.
Never hard-code CoreConfig parameters elsewhere.

Validated results (3 independent periods — Apr 2026):
  au2_real.db      6h   Apr 16      PF 2.50 | WR 60% | +$579 | DD 1.0%
  au2_spot_24h.db  24h  Apr 16-17   PF 1.86 | WR 74% | +$919 | DD 2.5%
  au2_fresh_24h.db 24h  Apr 17-18   PF 1.35 | WR 56% | +$261 | DD 3.7%  (OOS)

Architecture notes:
  - TREND_THRESHOLD_BPS=5.0 in au2_signal_regime.py is the key regime filter.
  - CoreConfig.breakeven_trigger_bps and max_hold_seconds are NOT used by
    PositionManager — REGIME_PROFILES[regime] controls those at runtime.
  - Fee model: maker entry (0.2 bps) + taker SL (0.5 bps) = 0.7 bps round-trip.
  - SL=18 bps wide by design; trend filter keeps signal quality high enough that
    wide SL rarely fires on quality entries.
"""
from __future__ import annotations
from au2_core import CoreConfig, Regime
from au2_risk_overlay import RiskOverlayConfig
from au2_consistency_guard import ConsistencyGuardConfig


def build_goat_config() -> CoreConfig:
    """Build and return the validated GOAT CoreConfig. Use this everywhere."""
    return CoreConfig(
        threshold=8.0,
        regime_multiplier={
            Regime.TREND:       0.95,
            Regime.FLOW:        0.95,
            Regime.MEAN_REVERT: 1.05,
            Regime.LIQUIDATION: 0.90,
            Regime.CHOP:        1.40,
        },
        risk_per_trade_pct=1.0,
        max_risk_usd=350.0,
        stop_loss_pct=0.18,
        tp1_pct=0.23,
        tp2_pct=0.35,
        trailing_pct=0.12,
        # NOTE: breakeven_trigger_bps ignored by PositionManager —
        #       REGIME_PROFILES[regime].be_trigger_bps (3-6 bps) controls this.
        breakeven_trigger_bps=15.0,
        # NOTE: max_hold_seconds ignored by PositionManager —
        #       REGIME_PROFILES[regime].max_hold_seconds (80-150 s) controls this.
        max_hold_seconds=120,
        cooldown_seconds=15,
        confirmation_cycles=1,
        max_daily_trades=50,
        loss_pause_seconds=600,
        max_consecutive_losses=3,
        loss_penalty_risk_mult=0.5,
        daily_dd_amber_pct=3.5,
        daily_dd_red_pct=4.5,
        total_dd_amber_pct=7.0,
        total_dd_red_pct=8.5,
        max_total_dd_pct=10.0,
        entry_fee_mode="maker",
        taker_fee_bps=0.5,
    )


# Canonical frozen config — import this everywhere.
# The name "VALIDATED" signals it is the result of a controlled optimisation
# process (3 independent OOS periods) and must not be changed without a new
# validation cycle.  Aliases below keep existing scripts working during
# the transition period; they will be removed once all callers are updated.
GOAT_VALIDATED_CFG: CoreConfig = build_goat_config()
GOAT_OVERLAY_CFG: RiskOverlayConfig = RiskOverlayConfig(daily_profit_cap_pct=5.0)

# ── GOAT Pay Later profile ────────────────────────────────────────────────────
# For prop firm payout challenges: consistency guard active, stop after valid day.
# Risk params identical to GOAT_VALIDATED_CFG — only guard behavior differs.
GOAT_PAYLATER_CFG: CoreConfig = build_goat_config()   # same core params
GOAT_PAYLATER_OVERLAY_CFG: RiskOverlayConfig = RiskOverlayConfig(
    daily_profit_cap_pct=3.0,   # tighter cap — don't overshoot on any single day
)
GOAT_PAYLATER_CONSISTENCY_CFG: ConsistencyGuardConfig = ConsistencyGuardConfig(
    max_best_day_share       = 0.18,
    valid_day_min_profit_pct = 0.50,
    stop_after_valid_day     = True,
    min_valid_days           = 3,
    daily_target_pct         = 0.55,
    enabled                  = True,
)

# Backward-compat alias — do not use in new code
GOAT_CFG: CoreConfig = GOAT_VALIDATED_CFG

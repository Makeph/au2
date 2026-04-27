#!/usr/bin/env python3
"""AU2 GOAT CHALLENGE V2 — Same engine, defensive risk profile only.

Derived strictly from GOAT_VALIDATED_CFG (Apr 2026).
Every signal, gate, exit, and fee parameter is identical to the validated preset.
Only risk sizing, DD guards, and daily trade cap are tightened.

Rationale
---------
GOAT_CHALLENGE_V1 failed because it tightened quality gates simultaneously
with risk sizing, destroying the signal's statistical edge (WR 71% → 43%).
V2 does not touch any selection logic: same threshold, same gates, same cooldown.
The only levers are risk sizing and drawdown limits.

GOAT validated baseline (for comparison):
  au2_spot_24h.db  24h  PF 1.62 | WR 71.0% | +$715 | DD 3.42%
  au2_fresh_24h.db 24h  PF 1.23 | WR 54.4% | +$173 | DD 4.45%

Challenge constraints assumed:
  - Daily DD hard limit  : 4.0%   (we stop at 2.25% red / 2.75% max)
  - Total DD hard limit  : 10.0%  (we stop at 5.5% red / 7.5% max)

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
# Challenge V2 config — GOAT engine, defensive risk only
# ---------------------------------------------------------------------------

def build_goat_challenge_v2_config() -> CoreConfig:
    """GOAT signal + exits intact. Only risk sizing and DD limits tightened."""
    return CoreConfig(
        # ── Signal — IDENTICAL to GOAT validated ─────────────────────────────
        threshold=8.0,
        regime_multiplier={
            Regime.TREND:       0.95,
            Regime.FLOW:        0.95,
            Regime.MEAN_REVERT: 1.05,
            Regime.LIQUIDATION: 0.90,
            Regime.CHOP:        1.40,
        },

        # ── Exits — IDENTICAL to GOAT validated ──────────────────────────────
        # NOTE: breakeven_trigger_bps / max_hold_seconds ignored by PositionManager.
        stop_loss_pct=0.18,
        tp1_pct=0.23,
        tp2_pct=0.35,
        trailing_pct=0.12,
        breakeven_trigger_bps=15.0,
        max_hold_seconds=120,

        # ── Entry logic — IDENTICAL to GOAT validated ────────────────────────
        cooldown_seconds=15,
        confirmation_cycles=1,

        # ── Fees — IDENTICAL to GOAT validated ───────────────────────────────
        entry_fee_mode="maker",
        taker_fee_bps=0.5,

        # ── Anti-streak — IDENTICAL to GOAT validated ────────────────────────
        loss_pause_seconds=600,
        max_consecutive_losses=3,
        loss_penalty_risk_mult=0.5,

        # ── Risk sizing — REDUCED for challenge ──────────────────────────────
        risk_per_trade_pct=0.50,   # 1.0 → 0.50
        max_risk_usd=50.0,         # 350.0 → 50.0

        # ── Daily trade cap — light cap, does not restrict signal flow ────────
        max_daily_trades=20,       # 50 → 20

        # ── Daily DD limits — cut well before challenge hard limit (4%) ───────
        daily_dd_amber_pct=1.5,    # 3.5 → 1.5
        daily_dd_red_pct=2.25,     # 4.5 → 2.25
        max_daily_dd_pct=2.75,     # 5.0 → 2.75

        # ── Total DD limits — cut well before challenge hard limit (10%) ──────
        total_dd_amber_pct=4.0,    # 7.0 → 4.0
        total_dd_red_pct=5.5,      # 8.5 → 5.5
        max_total_dd_pct=7.5,      # 10.0 → 7.5
    )


GOAT_CHALLENGE_V2_CFG: CoreConfig = build_goat_challenge_v2_config()

# Overlay: lock in a green day at +2.5% without capping good trending days too early.
GOAT_CHALLENGE_V2_OVERLAY: RiskOverlayConfig = RiskOverlayConfig(
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
    overlay = RiskOverlay(cfg=GOAT_CHALLENGE_V2_OVERLAY)
    bt = Au2Backtest(GOAT_CHALLENGE_V2_CFG, overlay=overlay)
    trades, _, metrics = bt.run(db)
    print(
        f"[GOAT-CHALLENGE-V2] {db}\n"
        f"  Trades : {metrics.total_trades}\n"
        f"  WR     : {metrics.win_rate:.1%}\n"
        f"  PnL    : ${metrics.total_pnl:.2f}\n"
        f"  PF     : {metrics.profit_factor:.2f}\n"
        f"  DD     : {metrics.max_dd_pct:.2f}%"
    )

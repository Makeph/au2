#!/usr/bin/env python3
"""AU2 GOAT CHALLENGE V3 — Minimal risk sizing reduction only.

Derived from GOAT_VALIDATED_CFG via dataclasses.replace().
Guaranteed changes : risk_per_trade_pct 1.0 → 0.50, max_risk_usd 350 → 35.
Guaranteed unchanged : every other field, including overlay.

Why this is the only clean test
---------------------------------
V1 destroyed edge by tightening quality gates.
V2 destroyed edge by combining tight profit-cap + DD-stop (adverse selection).
V3 is the minimal hypothesis: same bot, half the size.

Expected behaviour (if risk sizing is the only lever):
  - Trades  : identical to GOAT (entry selection is unchanged)
  - WR      : identical to GOAT
  - PF      : identical to GOAT (ratio is size-independent)
  - PnL     : ≈ 50% of GOAT  (half risk per trade)
  - DD      : ≈ 50% of GOAT  (same structural behaviour, half amplitude)

Any deviation from the above signals an interaction effect worth investigating.
"""
import sys, pathlib, dataclasses
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

from au2_config import GOAT_VALIDATED_CFG, GOAT_OVERLAY_CFG
from au2_risk_overlay import RiskOverlay


# ---------------------------------------------------------------------------
# Challenge V3 — two fields changed, everything else frozen from GOAT_VALIDATED
# ---------------------------------------------------------------------------

GOAT_CHALLENGE_V3_CFG = dataclasses.replace(
    GOAT_VALIDATED_CFG,
    risk_per_trade_pct=0.50,   # 1.0  → 0.50
    max_risk_usd=35.0,         # 350  → 35.0  (was 50, reduced 2026-04-22: SL ~$35 vs ~$50, DD 1.59%->1.13% in BT, PF unchanged)
)

# Overlay reused exactly — daily_profit_cap_pct=5.0, unchanged.
GOAT_CHALLENGE_V3_OVERLAY = GOAT_OVERLAY_CFG


# ---------------------------------------------------------------------------
# Entry point — backtest
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from au2_core import Au2Backtest

    db = sys.argv[1] if len(sys.argv) > 1 else str(
        _ROOT / "data" / "validated" / "au2_spot_24h.db"
    )
    overlay = RiskOverlay(cfg=GOAT_CHALLENGE_V3_OVERLAY)
    bt = Au2Backtest(GOAT_CHALLENGE_V3_CFG, overlay=overlay)
    trades, _, metrics = bt.run(db)
    print(
        f"[GOAT-CHALLENGE-V3] {db}\n"
        f"  Trades : {metrics.total_trades}\n"
        f"  WR     : {metrics.win_rate:.1%}\n"
        f"  PnL    : ${metrics.total_pnl:.2f}\n"
        f"  PF     : {metrics.profit_factor:.2f}\n"
        f"  DD     : {metrics.max_dd_pct:.2f}%"
    )

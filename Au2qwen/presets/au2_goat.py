#!/usr/bin/env python3
"""AU2 GOAT MODE — Balanced / High Consistency

Validated config (Apr 2026 — 6h + 24h BTCUSDT spot):
  - 6h declining session: PF 2.47, WR 60%, +$569, DD 1.0%
  - 24h mixed session:    PF 1.85, WR 73.7%, +$901, DD 2.5%

Key design decisions:
  - TREND_THRESHOLD_BPS=5 (in au2_signal_regime.py):
      Classifies more periods as trending (5 bps 5-min return vs old 10 bps).
      Blocks counter-trend entries from V2/linear fallback in trend periods.
  - SL=0.18% (18 bps): Wider stop prevents premature exits on trend-aligned entries.
      Trend filtering makes signals directionally strong enough to survive noise.
  - TP1=0.23% (23 bps): Realistic target over 80-150s hold in trending conditions.
  - Maker-both: entry at 0.2 bps, TP/time exits at 0.2 bps, SL=limit-stop 0.5 bps.
"""
import sys, pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

from au2_core import Au2Backtest
from au2_risk_overlay import RiskOverlay
from au2_config import GOAT_CFG as CFG, GOAT_OVERLAY_CFG as OVERLAY_CFG

if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else "../data/validated/au2_spot_24h.db"
    overlay = RiskOverlay(cfg=OVERLAY_CFG)
    bt = Au2Backtest(CFG, overlay=overlay)
    trades, _, metrics = bt.run(db)
    print(f"[GOAT] Trades:{metrics.total_trades} | WR:{metrics.win_rate:.1%} | PnL:${metrics.total_pnl:.2f} | PF:{metrics.profit_factor:.2f} | DD:{metrics.max_dd_pct:.2f}%")

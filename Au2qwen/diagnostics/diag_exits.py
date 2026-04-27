#!/usr/bin/env python3
"""Diagnostic: exit breakdown for au2_goat config."""
import sys, pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

from collections import Counter, defaultdict
from au2_core import Au2Backtest
from au2_risk_overlay import RiskOverlay
from au2_config import GOAT_VALIDATED_CFG as CFG, GOAT_OVERLAY_CFG

db = sys.argv[1] if len(sys.argv) > 1 else str(_ROOT / "data" / "validated" / "au2_spot_24h.db")
overlay = RiskOverlay(cfg=GOAT_OVERLAY_CFG)
bt = Au2Backtest(CFG, overlay=overlay)
trades, _, metrics = bt.run(db)

# Exit breakdown
exits = Counter(t.exit_reason for t in trades)
pnl_by_exit = defaultdict(list)
for t in trades:
    pnl_by_exit[t.exit_reason].append(t.pnl_usd)

print(f"\n=== {db} ===")
print(f"Total: {metrics.total_trades} trades | WR:{metrics.win_rate:.1%} | PnL:${metrics.total_pnl:.2f} | PF:{metrics.profit_factor:.2f} | DD:{metrics.max_dd_pct:.2f}%\n")
print(f"{'Exit Reason':<22} {'Count':>5} {'Wins':>5} {'WR':>6} {'Avg$':>8} {'Total$':>10}")
print("-" * 62)
for reason, pnls in sorted(pnl_by_exit.items(), key=lambda x: sum(x[1])):
    wins = sum(1 for p in pnls if p > 0)
    total = len(pnls)
    avg = sum(pnls) / total
    print(f"  {reason:<20} {total:>5} {wins:>5} {wins/total:>6.0%} {avg:>8.2f} {sum(pnls):>10.2f}")

# Side breakdown
print("\n--- By Side ---")
by_side = defaultdict(list)
for t in trades:
    by_side[t.signal].append(t.pnl_usd)
for side, pnls in sorted(by_side.items()):
    wins = sum(1 for p in pnls if p > 0)
    print(f"  {side}: {len(pnls)} trades, {wins} wins ({wins/len(pnls):.0%}), avg ${sum(pnls)/len(pnls):.2f}, total ${sum(pnls):.2f}")

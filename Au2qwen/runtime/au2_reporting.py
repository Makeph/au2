#!/usr/bin/env python3
"""AU2 REPORTING — CSV Export, Metrics Breakdown, Matplotlib Visualization"""
from __future__ import annotations
import csv, os
from typing import List, Optional
from au2_core import TradeResult, PositionFill, BacktestMetrics

def export_to_csv(trades: List[TradeResult], events: List[PositionFill], out_dir: str = "./reports"):
    os.makedirs(out_dir, exist_ok=True)
    
    with open(os.path.join(out_dir, "trades.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TradeResult.__annotations__.keys())
        w.writeheader()
        for t in trades:
            w.writerow({k: getattr(t, k) for k in TradeResult.__annotations__})
            
    with open(os.path.join(out_dir, "events.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=PositionFill.__annotations__.keys())
        w.writeheader()
        for e in events:
            w.writerow({k: getattr(e, k) for k in PositionFill.__annotations__})
    print(f"📊 Exported to {out_dir}/")

def print_advanced_report(metrics: BacktestMetrics):
    print("\n" + "="*60)
    print("AU2 BACKTEST REPORT")
    print("="*60)
    print(f"Trades: {metrics.total_trades} | WR: {metrics.win_rate:.1%} | PF: {metrics.profit_factor:.2f}")
    print(f"Total PnL: ${metrics.total_pnl:.2f} | MaxDD: {metrics.max_dd_pct:.2f}% | Exp: ${metrics.expectancy:.2f}")
    print("\nEXIT BREAKDOWN:")
    for r, s in sorted(metrics.by_exit_reason.items(), key=lambda x: -x[1]["count"]):
        wr = s["wins"]/s["count"] if s["count"] else 0
        print(f"  {r:20s}: {s['count']:3d} | WR {wr:5.1%} | PnL ${s['pnl']:+8.2f}")

def plot_equity_curve(trades: List[TradeResult], save_path: str = "./reports/equity.png"):
    try:
        import matplotlib.pyplot as plt
        equity = 10000.0
        eq_curve = [equity]
        for t in trades:
            equity += t.pnl_usd
            eq_curve.append(equity)
            
        plt.figure(figsize=(10, 5))
        plt.plot(eq_curve, label="Equity", linewidth=2)
        plt.axhline(10000, color="gray", linestyle="--", alpha=0.5)
        plt.title("Cumulative Equity Curve")
        plt.xlabel("Trades")
        plt.ylabel("USD")
        plt.legend()
        plt.grid(True, alpha=0.3)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, dpi=150)
        plt.close()
        print(f"📈 Plot saved: {save_path}")
    except ImportError:
        print("⚠️  matplotlib not installed. Run: pip install matplotlib")
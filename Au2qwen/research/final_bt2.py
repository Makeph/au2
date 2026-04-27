import sys; sys.path.insert(0, '.')
from au2_core import Au2Backtest, CoreConfig, Regime
from au2_risk_overlay import RiskOverlay, RiskOverlayConfig

CFG = CoreConfig(
    threshold=4.25,
    regime_multiplier={Regime.TREND: 0.95, Regime.FLOW: 0.95, Regime.MEAN_REVERT: 1.05, Regime.LIQUIDATION: 0.9, Regime.CHOP: 1.4},
    risk_per_trade_pct=1.0, max_risk_usd=350.0,
    stop_loss_pct=0.05, tp1_pct=0.08, tp2_pct=0.12, trailing_pct=0.08,
    max_hold_seconds=120, cooldown_seconds=15, confirmation_cycles=1, max_daily_trades=50,
    loss_pause_seconds=0, max_consecutive_losses=99,
    daily_dd_amber_pct=3.5, daily_dd_red_pct=4.5,
    total_dd_amber_pct=7.0, total_dd_red_pct=8.5, max_total_dd_pct=10.0,
    entry_fee_mode="maker",
    taker_fee_bps=0.5,
)
overlay = RiskOverlay(cfg=RiskOverlayConfig(daily_profit_cap_pct=5.0))
bt = Au2Backtest(CFG, overlay=overlay)
trades, _, metrics = bt.run('au2_real.db')

print(f'=== TUNED BACKTEST (TP=8bps SL=5bps maker-both) ===')
print(f'Trades:        {metrics.total_trades}')
print(f'Win Rate:      {metrics.win_rate:.1%}')
print(f'PnL:           ${metrics.total_pnl:.2f}')
print(f'Profit Factor: {metrics.profit_factor:.2f}')
print(f'Max DD:        {metrics.max_dd_pct:.2f}%')
print(f'Evals/Takes:   {bt._eval_count}/{bt._take_count}')
print()
print('Rejections:')
for k,v in sorted(bt._rejections.items(), key=lambda x: -x[1])[:8]:
    print(f'  {k}: {v}')
print()
print('Trades by exit:')
from collections import Counter
exits = Counter(t.exit_reason for t in trades)
for k,v in exits.most_common():
    wins = sum(1 for t in trades if t.exit_reason==k and t.pnl_usd>0)
    pnl = sum(t.pnl_usd for t in trades if t.exit_reason==k)
    print(f'  {k}: n={v} wins={wins} pnl=${pnl:.2f}')
print()
print('Side breakdown:')
from collections import defaultdict
by_side = defaultdict(list)
for t in trades:
    side = getattr(t, 'side', getattr(t, 'signal', '?'))
    by_side[side].append(t.pnl_usd)
for side, pnls in by_side.items():
    wins = sum(1 for p in pnls if p>0)
    print(f'  {side}: n={len(pnls)} wr={wins/len(pnls):.1%} pnl=${sum(pnls):.2f}')

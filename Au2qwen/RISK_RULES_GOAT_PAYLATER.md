# GOAT Pay Later — Risk Rules Reference
_Single source of truth. Do not implement rule logic outside `core/au2_consistency_guard.py`._

---

## Prop firm consistency rule (external)
> "No single trading day may account for more than **20%** of total net profits
> over the payout period."

## Internal safety margin
| Parameter | External limit | Internal target |
|---|---|---|
| max_best_day_share | 20% | **18%** |
| min_valid_days | 3 | 3 |
| valid_day_min_profit | — | **0.5% of equity** |
| daily_target | — | **0.55% of equity** |

## Daily trading rules
1. After a **valid day** (profit ≥ 0.5%), **stop trading** unless manually overridden.
2. If continuing today would push today's share above 18% of period total → block entry.
3. Daily hard loss limit: **4.5%** (well below typical 5% prop breach threshold).
4. Max risk per trade: keep single-trade exposure ≤ 1% equity.

## Payout period tracking
- Period resets when `payout_period_start` is set (manual command via Telegram `/gpaylater_reset`).
- Each UTC day's PnL is accumulated in `period_day_profits[YYYY-MM-DD]`.
- A day qualifies as "valid" if `daily_pnl_pct >= valid_day_min_profit_pct` AND the day is closed.

## Block logic (consistency guard)
```
today_share = today_pnl / max(sum(positive_day_profits), ε)
if today_share >= max_best_day_share → BLOCK new entries
```
This pre-emptively blocks further profit accumulation on days that would exceed the share cap.

## Breach risks
- **Over-concentration**: one exceptional day while others are flat → easy 20%+ breach
- **Too few valid days**: requesting payout with < 3 valid days → prop rejection
- **Negative period**: if period net PnL is negative, consistency rule is moot but payout impossible

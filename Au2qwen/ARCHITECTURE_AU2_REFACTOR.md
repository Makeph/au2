# AU2QWEN — Architecture Audit & Refactor Map
_Generated 2026-04-29. Update after each structural change._

---

## Current file → target module mapping

| Current file | Role | Maps to (target) | Action |
|---|---|---|---|
| `core/au2_core.py` | RiskEngine, PositionManager, TradeBuilder, SignalProcessor, REGIME_PROFILES | `core/signal_engine.py` + `core/risk_engine.py` + `core/position_engine.py` | **Keep as-is** — well-tested, no prop logic inside |
| `core/au2_config.py` | CoreConfig singleton for GOAT V3 | `configs/goat_paylater_100k.py` + `configs/alpha_live.py` | **Extend** — add GOAT_PAYLATER profile, keep GOAT_CFG |
| `core/au2_decision.py` | build_trade_decision, TradeDecisionLog | `core/signal_engine.py` | **Keep as-is** — strategy-agnostic |
| `core/au2_risk_overlay.py` | Daily profit cap, post-loss pause | `guards/drawdown_guard.py` | **Keep as-is** — extend with consistency guard |
| `core/au2_feature_engine.py` | Feature extraction (CVD, vol, trend) | `core/market_data.py` | **Keep as-is** |
| `core/au2_signal_regime.py` | Regime classification | `core/signal_engine.py` | **Keep as-is** |
| `runtime/au2_live_executor.py` | Main tick dispatch, entry/exit orchestration | `core/execution_engine.py` | **Keep as-is** — wire consistency guard |
| `runtime/au2_bot_live.py` | WS feed, task supervisor, PaperExecutor | `runners/run_goat.py` | **Keep as-is** |
| `runtime/au2_state_manager.py` | JSON checkpoint persistence | `core/state_store.py` | **Extend** — add period_day_profits field |
| `runtime/au2_reporting.py` | Session reporting | `reports/session_report.py` | **Keep as-is** |
| `core/au2_telegram.py` | Fire-and-forget Telegram notifs | stays in `core/` | **Keep as-is** |
| `core/au2_goat_telegram_addon.py` | Telegram command bridge | stays in `core/` | **Keep as-is** |

---

## What genuinely doesn't exist yet (gaps)

| Target module | Status | Priority |
|---|---|---|
| `guards/consistency_guard.py` | ❌ Missing | **HIGH** — required for Pay Later payout |
| `reports/payout_readiness.py` | ❌ Missing | **HIGH** — monitoring prop eligibility |
| `configs/goat_paylater_100k.py` profile | ❌ Missing | **HIGH** |
| `strategies/alpha_mode.py` | ❌ Missing | LOW — no validated backtest yet |
| `guards/kill_switch.py` | Partial — lives in RiskEngine RED state | LOW |
| Tests for consistency guard | ❌ Missing | HIGH — after implementation |

---

## What is NOT needed

- **YAML configs** — Python dataclasses are type-safe, no parsing overhead, no new dependency
- **Full directory rename** — existing `core/` / `runtime/` structure is adequate
- **ALPHA mode** — premature until backtest validates edge
- **Renaming `au2_core.py`** — stable, tested, imported everywhere; rename = churn

---

## Architecture principles (current + target)

1. `au2_core.py` has **zero** prop-firm-specific logic — verified ✅
2. `au2_decision.py` has **zero** prop-firm-specific logic — verified ✅
3. All strategy-specific behavior lives in configs + guards (overlay, consistency guard)
4. The executor is a dispatch shell — it calls guards, not the other way around

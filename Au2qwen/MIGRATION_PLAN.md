# AU2QWEN Migration Plan
_Incremental. No big-bang rewrites. Bot stays live throughout._

---

## Phase 1 — Docs + consistency guard (this session) ✅
- [x] ARCHITECTURE_AU2_REFACTOR.md
- [x] MIGRATION_PLAN.md  
- [x] RISK_RULES_GOAT_PAYLATER.md
- [x] `core/au2_consistency_guard.py` — PropRulesGuard + PayoutReadinessReport
- [x] `core/au2_config.py` — add GOAT_PAYLATER_CFG profile
- [x] Wire consistency guard into `runtime/au2_live_executor.py`
- [x] Extend `runtime/au2_state_manager.py` — period_day_profits persistence
- [ ] Tests: `tests/test_consistency_guard.py`

## Phase 2 — Reporting (next session)
- [ ] `/gpaylater` Telegram command — live payout readiness report
- [ ] `diagnostics/payout_readiness_live.py` — standalone CLI report
- [ ] Daily report includes payout readiness section

## Phase 3 — ALPHA mode (only after backtest validation)
- [ ] Validate ALPHA edge in backtest (independent OOS periods)
- [ ] `core/au2_config.py` — add ALPHA_CFG profile (no consistency guard, wider risk)
- [ ] `runners/run_alpha.py` — separate entry point
- [ ] Tests: GOAT vs ALPHA produce different guard decisions from same signal

## Phase 4 — Cleanup (low priority)
- [ ] Archive `OLD/` directory
- [ ] Remove backward-compat alias `GOAT_CFG` once all callers updated
- [ ] Consider splitting `au2_core.py` if it exceeds 1000 lines

---

## Invariants (never break these)
1. `au2_core.py` and `au2_decision.py` must remain prop-firm-logic-free
2. One bot per Python process (module-level _signal_v3 singleton)
3. All config changes require new backtest validation cycle before live use
4. Telegram bridge never polls — one poller only (telegram_bridge.py)

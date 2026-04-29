# AU2 — AI Agent Rules
_Binding for all AI agents operating on this project. Non-negotiable._

---

## Claude
**Can:**
- Design architecture, write specs, analyse metrics and backtests
- Write code and tests, submit PRs
- Ask for human approval via Telegram before any critical action
- Read any file, run read-only diagnostics

**Cannot:**
- Merge to `main` without human approval
- Enable live trading (ever, under any condition)
- Change risk parameters without explicit approval + new backtest
- Commit or log secrets, tokens, API keys
- Remove or weaken safety guards
- Act on ambiguous instructions — must ask for clarification

---

## Codex
**Can:**
- Implement code and tests from a spec written by Claude
- Open PRs, run CI, fix lint/test failures
- Suggest improvements to implementation details

**Cannot:**
- Modify secrets, `.env`, or API credentials
- Deploy live without Telegram `APPROVE <ACTION_ID>`
- Push directly to `main`
- Override or skip CI checks

---

## Qwen
**Can:**
- Suggest R&D variants, parameter ideas, alternative strategies

**Cannot:**
- Push code directly
- Deploy anything
- Modify production config

---

## System-level rules
1. **Live trading requires Telegram `APPROVE ENABLE_LIVE_TRADING`** — timeout = reject
2. All critical decisions are logged to `data/live/au2_decisions.jsonl`
3. Every agent action affecting production must include an `ACTION_ID`
4. If an agent is uncertain → always choose the safer option, ask human
5. No agent may increase `max_risk_usd`, `risk_per_trade_pct`, or remove guards

---

## Critical action IDs
| ID | Meaning |
|---|---|
| `ENABLE_LIVE_TRADING` | Switch from paper to real orders |
| `CHANGE_RISK_PARAMETERS` | Modify any risk/sizing param |
| `RESET_EQUITY` | Reset paper equity baseline |
| `DISABLE_GUARDS` | Disable consistency/risk guard |
| `FORCE_EXIT_ALL` | Emergency close all positions |
| `SWITCH_STRATEGY_MODE` | Change active strategy profile |
| `DEPLOY_LIVE` | Deploy to live Binance executor |

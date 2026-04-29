# AU2 — Project Rules
_These rules apply to all contributors, human and AI._

---

## Git & Code
- **No direct commit to `main`** — PRs only
- PRs require CI to pass before merge
- Secrets live in `.env` only — never in repo, never in logs
- `.env` is in `.gitignore` — check before every commit
- `LIVE_ENABLED` must remain `false` by default in all configs

## Safety
- No agent may remove or weaken a safety guard
- No agent may increase risk parameters without:
  1. A new validated backtest (3 independent OOS periods)
  2. Explicit Telegram `APPROVE CHANGE_RISK_PARAMETERS`
- Live trading cannot be enabled without `APPROVE ENABLE_LIVE_TRADING`
- If a Telegram approval message doesn't match the exact format → **auto-reject**

## Deployment
- Paper deploy: push to `main` → GitHub Action → scp to server → restart `bot_au2qwen_goat`
- Live deploy: **disabled**. Requires separate executor, separate service, separate approval
- Server: `91.99.100.5`, service: `bot_au2qwen_goat`, workdir: `/root/bot_au2qwen/`

## Approvals
Any critical action requires this exact Telegram message from the authorized chat:

```
APPROVE <ACTION_ID>
```

Example: `APPROVE ENABLE_LIVE_TRADING`

**Timeout: 6 hours. Default on timeout: REJECT.**

## Versioning
- Bot source lives in `core/` and `runtime/` — do not rename
- Configs live in `core/au2_config.py` — Python, not YAML
- Test suite lives in `tests/` — must pass before any merge
- Research lives in `research/` — never imported by production code

## Architecture invariants
See `ARCHITECTURE_AU2_REFACTOR.md` for the full mapping.
1. `au2_core.py` must remain prop-firm-logic-free
2. `au2_decision.py` must remain prop-firm-logic-free
3. One bot per Python process (module-level singleton)
4. Telegram polling: one poller only (`telegram_bridge.py`)

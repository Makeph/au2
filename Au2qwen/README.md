# AU2QWEN — Autonomous Trading Bot

> **Paper trading only.** Live trading is disabled and requires explicit human approval.

---

## What is AU2?

AU2QWEN is a BTC/USDT scalping bot running on Binance Futures (paper mode).
It uses a regime-adaptive signal (V3), a 30s dead-market filter, and optional
GOAT Pay Later consistency guards for prop-firm compliance.

**Current mode:** Paper trading  
**Strategy:** GOAT V3 (validated PF 1.35–2.50 across 3 OOS periods)  
**Server:** Hetzner `91.99.100.5`, service `bot_au2qwen_goat`

---

## Phone workflow

```
You (phone)
  │
  ├─ Telegram → /goat /gtrades /gbilan /gdiag /gstop /gstart
  │              /gpatch <request> → Claude modifies code
  │
  ├─ GitHub (mobile) → review PRs, approve merges
  │
  └─ Telegram approval → APPROVE <ACTION_ID>
```

---

## Agent roles

| Agent | Role | Authority |
|---|---|---|
| **Claude** | Architecture, specs, analysis, code | Can write PRs, cannot merge to main |
| **Codex** | Implementation, tests | Can open PRs, cannot deploy live |
| **Qwen** | R&D suggestions only | No code push, no deploy |

See `AI_RULES.md` for binding rules.

---

## Safety rules (summary)

- `LIVE_ENABLED=false` in all configs — never change without `APPROVE ENABLE_LIVE_TRADING`
- No direct push to `main` — PR only
- CI must pass before merge (pytest + secret scan + live_enabled check)
- No secrets in code or logs
- Full rules: `PROJECT_RULES.md`

---

## Directory structure

```
Au2qwen/
├── core/           Signal engine, risk, config, guards, Telegram
├── runtime/        Executor, bot runner, state manager
├── research/       Backtests (never imported by production)
├── presets/        Strategy presets (au2fp prop firm configs)
├── tests/          Test suite (pytest)
├── scripts/        CLI tools (notify_telegram.py)
├── infra/
│   ├── systemd/    au2-paper.service unit file
│   └── deploy/     deploy_paper.sh
├── docs/           AGENT_MESSAGE_FORMAT.md
├── .github/
│   ├── workflows/  ci.yml, deploy-paper.yml
│   └── ISSUE_TEMPLATE/task.md
├── AI_RULES.md
├── PROJECT_RULES.md
├── ARCHITECTURE_AU2_REFACTOR.md
├── RISK_RULES_GOAT_PAYLATER.md
├── MIGRATION_PLAN.md
└── .env.example
```

---

## Telegram commands

| Command | Description |
|---|---|
| `/goat` | Real-time equity, PnL, position, market |
| `/gtrades [N]` | Last N trades |
| `/gbilan` | Daily report |
| `/gdiag` | Rejection stats, dead_market count |
| `/gstop` | Graceful stop (systemd auto-restarts) |
| `/gstart` | Start service |
| `/gpatch <request>` | Claude modifies code and restarts |
| `/gask <question>` | Claude answers, no file modification |
| `/ghelp` | Command list |

---

## Running tests

```bash
cd Au2qwen
python -m pytest tests/ -v
```

---

## Starting paper service (server)

```bash
bash infra/deploy/deploy_paper.sh
```

Or manually:
```bash
systemctl restart bot_au2qwen_goat
journalctl -u bot_au2qwen_goat -f
```

---

## GitHub Actions setup (required secrets)

In GitHub → Settings → Secrets → Actions:

| Secret | Value |
|---|---|
| `SERVER_IP` | `91.99.100.5` |
| `SSH_PRIVATE_KEY` | Your SSH private key for root@server |
| `TELEGRAM_BOT_TOKEN` | Bot token |
| `TELEGRAM_CHAT_ID` | Your chat ID |

---

## Sending a Telegram notification from CLI

```bash
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_ID=...
python scripts/notify_telegram.py "🚀 Deploy complete"
```

---

## Approving a critical action

From Telegram, send exactly:
```
APPROVE <ACTION_ID>
```

Example: `APPROVE DEPLOY_PAPER`

Timeout: 6h — default: REJECT. Full list of action IDs in `AI_RULES.md`.

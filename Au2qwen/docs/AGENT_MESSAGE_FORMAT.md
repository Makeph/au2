# AU2 — Agent Message Format
_All agent-to-human communication in Telegram and GitHub comments must follow this format.
If the format is not respected → action is auto-rejected._

---

## Standard message template

```
[AGENT: CLAUDE | CODEX | QWEN]
[TASK: <short-id>]
[STATUS: READY_FOR_REVIEW | IN_PROGRESS | BLOCKED | FAILED]

WHAT I DID:
- <concise bullet list>

CURRENT STATE:
- live_enabled: false
- equity: $X,XXX
- service: active/inactive
- branch: <branch-name>
- PR: <url or N/A>

METRICS (if applicable):
- PF: X.XX
- WR: XX%
- DD: X.X%
- Trades: N
- Errors: N

ISSUES / RISKS:
- <list or "None">

NEXT OPTIONS:
1. <option>
2. <option>
3. <option>

RECOMMENDATION:
-> <single clear recommendation>

ACTION REQUIRED:
APPROVE <ACTION_ID>
or
REJECT <ACTION_ID>

TIMEOUT: 6h — default: REJECT
```

---

## Rules

1. `live_enabled` must always appear in CURRENT STATE
2. `ACTION_ID` must match an ID from `AI_RULES.md`
3. Timeout field is mandatory — 6h default unless specified otherwise
4. If `STATUS: FAILED`, do NOT propose an action that enables live trading
5. Metrics section can be omitted for non-trading tasks (infra, docs, etc.)

---

## Example — valid approval request

```
[AGENT: CLAUDE]
[TASK: AU2-042]
[STATUS: READY_FOR_REVIEW]

WHAT I DID:
- Implemented early-loser exit (EXIT_NO_FOLLOW_THROUGH)
- Added 3 new tests, all passing
- Opened PR #42

CURRENT STATE:
- live_enabled: false
- equity: $9,452
- service: active (paper)
- branch: feat/early-exit
- PR: https://github.com/Makeph/au2/pull/42

METRICS:
- PF: 1.72 (backtest 24h OOS)
- WR: 54%
- DD: 2.1%
- Trades: 38
- Errors: 0

ISSUES / RISKS:
- Not yet validated over 3 independent periods

NEXT OPTIONS:
1. Merge PR and deploy to paper
2. Run additional backtest period first
3. Reject and iterate

RECOMMENDATION:
-> Option 2 — run one more OOS period before merging

ACTION REQUIRED:
APPROVE DEPLOY_PAPER
or
REJECT DEPLOY_PAPER

TIMEOUT: 6h — default: REJECT
```

---

## Telegram approval reply

Send exactly:
```
APPROVE DEPLOY_PAPER
```
or
```
REJECT DEPLOY_PAPER
```

Any other format → auto-rejected.

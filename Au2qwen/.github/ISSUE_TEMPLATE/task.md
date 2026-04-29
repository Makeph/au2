---
name: Agent Task
about: Structured task for AI agents (Claude/Codex/Qwen)
title: "[TASK: AU2-XXX] Short description"
labels: agent-task
assignees: ''
---

## Context
<!-- What is the current situation? What triggered this task? -->
- 

## Objective
<!-- What exactly should be done? Single, clear outcome. -->
- 

## Constraints (non-negotiable)
- [ ] No live trading
- [ ] No secrets in code or logs
- [ ] No direct push to `main` — PR only
- [ ] Tests must pass before merge
- [ ] `live_enabled` must remain `false`
- [ ] Risk parameters must not increase
- [ ] Guards must not be disabled

## Deliverable
- [ ] PR with changes
- [ ] Tests covering new behavior
- [ ] Short report (what changed, why, metrics if applicable)
- [ ] List of affected files

## Agent assignments
- **Claude**: architecture design / spec / analysis
- **Codex**: implementation / tests
- **Qwen**: R&D suggestions only (no direct code push)

## Approval required?
- [ ] YES — Action ID: `_____________`
- [ ] NO

## Reference
- Related file(s): 
- Related backtest: 
- Related Telegram alert: 

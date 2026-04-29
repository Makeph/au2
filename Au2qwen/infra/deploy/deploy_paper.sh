#!/usr/bin/env bash
# AU2 — Paper bot deploy script
# Run on the server: bash infra/deploy/deploy_paper.sh
# Never touches live service. Never enables live trading.
set -euo pipefail

BOT_DIR="/root/bot_au2qwen"
SERVICE="bot_au2qwen_goat"
VENV="$BOT_DIR/venv/bin/python"

echo "=== AU2 Paper Deploy — $(date -u '+%Y-%m-%d %H:%M:%S UTC') ==="
echo "Service : $SERVICE"
echo "Dir     : $BOT_DIR"
echo ""

# ── Safety check ─────────────────────────────────────────────────────────────
if grep -r "live_mode.*=.*True\|LIVE_ENABLED.*=.*true" "$BOT_DIR"/*.py 2>/dev/null; then
    echo "❌ live_mode=True detected in source — aborting"
    exit 1
fi
echo "✅ live_mode safe"

# ── Clear bytecode cache ──────────────────────────────────────────────────────
rm -rf "$BOT_DIR/__pycache__"
echo "✅ Cache cleared"

# ── Import sanity check ───────────────────────────────────────────────────────
cd "$BOT_DIR"
$VENV -c "
from au2_config import GOAT_CFG, GOAT_PAYLATER_CONSISTENCY_CFG
from au2_consistency_guard import ConsistencyGuard
from au2_live_executor import LiveExecutor
print('imports OK')
"
echo "✅ Imports OK"

# ── Restart service ───────────────────────────────────────────────────────────
systemctl restart "$SERVICE"
sleep 3

# ── Verify ────────────────────────────────────────────────────────────────────
if systemctl is-active --quiet "$SERVICE"; then
    echo "✅ $SERVICE is active"
else
    echo "❌ $SERVICE failed to start — last 20 lines:"
    journalctl -u "$SERVICE" -n 20 --no-pager
    exit 1
fi

echo ""
echo "=== Deploy complete — live trading: DISABLED ==="
journalctl -u "$SERVICE" -n 5 --no-pager --output=cat

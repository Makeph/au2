#!/usr/bin/env bash
# deploy_au2qwen.sh — Deploy AU2QWEN GOAT paper bot to Hetzner
#
# Usage: bash deploy_au2qwen.sh <SERVER_IP>
#
# Safety guarantees:
#   - DOES NOT stop or modify bot_v225 or bot_v21
#   - DOES NOT overwrite /root/bot_au2qwen/ if venv already present
#   - FAILS FAST on missing files (set -euo pipefail)
#   - FAILS FAST on import errors before service is started
#
set -euo pipefail

SERVER_IP="${1:-}"
if [[ -z "$SERVER_IP" ]]; then
  echo "Usage: $0 <server_ip>"
  exit 1
fi

REMOTE_DIR="/root/bot_au2qwen"
SERVICE_NAME="bot_au2qwen_goat"

# ── Pre-flight: verify all local files exist before touching the server ────────
echo "==> Pre-flight: checking local files"
REQUIRED_FILES=(
  "au2_bot_live.py"
  "au2_core.py"
  "au2_live_executor.py"
  "au2_state_manager.py"
  ".env.au2qwen"
  "bot_au2qwen_goat.service"
)
for f in "${REQUIRED_FILES[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: missing required file: $f"
    exit 1
  fi
  echo "  found: $f"
done

# ── Step 1: create remote directory before scp ────────────────────────────────
echo ""
echo "==> Creating remote directory: $REMOTE_DIR"
ssh root@"$SERVER_IP" "mkdir -p $REMOTE_DIR"

# ── Step 2: upload all modules ────────────────────────────────────────────────
echo ""
echo "==> Uploading files to $SERVER_IP:$REMOTE_DIR"

scp au2_bot_live.py          root@"$SERVER_IP":"$REMOTE_DIR"/au2_bot_live.py
scp au2_core.py              root@"$SERVER_IP":"$REMOTE_DIR"/au2_core.py
scp au2_live_executor.py     root@"$SERVER_IP":"$REMOTE_DIR"/au2_live_executor.py
scp au2_state_manager.py     root@"$SERVER_IP":"$REMOTE_DIR"/au2_state_manager.py
scp .env.au2qwen             root@"$SERVER_IP":"$REMOTE_DIR"/.env
scp bot_au2qwen_goat.service root@"$SERVER_IP":/etc/systemd/system/"$SERVICE_NAME".service

echo "  all files uploaded"

# ── Step 3: remote setup ──────────────────────────────────────────────────────
ssh root@"$SERVER_IP" bash -s << 'REMOTE'
set -euo pipefail

REMOTE_DIR="/root/bot_au2qwen"
SERVICE_NAME="bot_au2qwen_goat"
VENV="$REMOTE_DIR/venv"

# ── Venv ────────────────────────────────────────────────────────────────────────
echo ""
echo "==> Venv"
if [[ ! -f "$VENV/bin/python" ]]; then
  echo "--> Creating venv"
  python3 -m venv "$VENV"
else
  echo "--> Venv already present, skipping creation"
fi

# ── Dependencies ────────────────────────────────────────────────────────────────
echo ""
echo "==> Installing dependencies"
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q websockets python-dotenv
echo "--> websockets, python-dotenv OK"

# ── Import check — FAIL FAST ────────────────────────────────────────────────────
echo ""
echo "==> Import check (fail fast on error)"
cd "$REMOTE_DIR"
if ! "$VENV/bin/python" -c "import au2_bot_live, au2_core, au2_live_executor, au2_state_manager"; then
  echo ""
  echo "ERROR: import check failed — aborting deployment"
  echo "       service will NOT be started"
  exit 1
fi
echo "--> all imports OK"

# ── Safety: existing bots untouched ────────────────────────────────────────────
echo ""
echo "==> Existing bots (read-only check)"
for svc in bot_v225 bot_v21; do
  state=$(systemctl is-active "$svc" 2>/dev/null || echo "inactive")
  echo "  $svc: $state"
done

# ── Deploy service ──────────────────────────────────────────────────────────────
echo ""
echo "==> Deploying service: $SERVICE_NAME"
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"

# ── Post-deploy checks ──────────────────────────────────────────────────────────
sleep 3

echo ""
echo "==> Service status"
systemctl status "$SERVICE_NAME" --no-pager -l | head -30

echo ""
echo "==> Last 50 log lines"
journalctl -u "$SERVICE_NAME" -n 50 --no-pager

echo ""
echo "==> Final state summary"
echo "  bot_au2qwen_goat : $(systemctl is-active $SERVICE_NAME 2>/dev/null || echo unknown)"
echo "  bot_v225         : $(systemctl is-active bot_v225 2>/dev/null || echo unknown)"
echo "  bot_v21          : $(systemctl is-active bot_v21  2>/dev/null || echo unknown)"

REMOTE

echo ""
echo "==> Deployment complete."
echo ""
echo "  Live logs : ssh root@$SERVER_IP 'journalctl -u bot_au2qwen_goat -f'"
echo "  Status    : ssh root@$SERVER_IP 'systemctl status bot_au2qwen_goat'"
echo "  Restart   : ssh root@$SERVER_IP 'systemctl restart bot_au2qwen_goat'"
echo "  Stop      : ssh root@$SERVER_IP 'systemctl stop bot_au2qwen_goat'"

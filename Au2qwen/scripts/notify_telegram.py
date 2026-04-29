#!/usr/bin/env python3
"""AU2 — Minimal Telegram CLI notifier.

Usage:
    python scripts/notify_telegram.py "Your message here"

Reads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from environment.
Exits non-zero on failure. Never logs the token.

Designed to be called from GitHub Actions, deploy scripts, and cron jobs.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
import urllib.parse


def send(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
    }).encode()
    req = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        body = json.loads(resp.read())
    if not body.get("ok"):
        raise RuntimeError(f"Telegram API error: {body.get('description', 'unknown')}")


def main() -> int:
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    if not token:
        print("ERROR: TELEGRAM_BOT_TOKEN not set", file=sys.stderr)
        return 1
    if not chat_id:
        print("ERROR: TELEGRAM_CHAT_ID not set", file=sys.stderr)
        return 1
    if len(sys.argv) < 2:
        print("Usage: notify_telegram.py <message>", file=sys.stderr)
        return 1

    message = " ".join(sys.argv[1:])
    if not message.strip():
        print("ERROR: empty message", file=sys.stderr)
        return 1

    try:
        send(token, chat_id, message)
        return 0
    except Exception as exc:
        # Never print the token — exc may contain it in some edge cases
        print(f"ERROR: failed to send Telegram message: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

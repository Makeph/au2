"""Telegram notification helper for AU2QWEN bot.

Fire-and-forget: messages are sent in daemon threads so the trading loop
is never blocked by network I/O.

Configuration (via .env or environment):
    TELEGRAM_TOKEN   — bot token from @BotFather
    TELEGRAM_CHAT_ID — recipient chat/user ID

Usage:
    from au2_telegram import tg
    tg.entry("LONG", qty=0.12, price=84000, regime="CHOP", score=7.2, risk=35)
    tg.closed(pnl=12.50, reason="EXIT_BE_FALLBACK", equity=10050)
    tg.status(uptime_h=2.5, equity=10050, pnl=50, n_trades=8, pos="FLAT")
    tg.warning("FLOW SL rate 14% > 10%")
"""
from __future__ import annotations

import logging
import os
import threading
import time

import requests

log = logging.getLogger("au2_telegram")

_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
_API_URL = "https://api.telegram.org/bot{token}/sendMessage"

# Minimum seconds between status messages (avoid spam on frequent heartbeats)
_STATUS_INTERVAL = 3600.0
_last_status_ts: float = 0.0


# ── internal ────────────────────────────────────────────────────────────────

def _post(text: str) -> None:
    """Blocking send — always called from a daemon thread."""
    token   = os.getenv("TELEGRAM_TOKEN", _TOKEN)
    chat_id = os.getenv("TELEGRAM_CHAT_ID", _CHAT_ID)
    if not token or not chat_id:
        return
    try:
        resp = requests.post(
            _API_URL.format(token=token),
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        if not resp.ok:
            log.warning("Telegram API %d: %s", resp.status_code, resp.text[:120])
    except Exception as exc:
        log.warning("Telegram send failed: %s", exc)


def _send(text: str) -> None:
    """Fire-and-forget."""
    threading.Thread(target=_post, args=(text,), daemon=True).start()


# ── public API ───────────────────────────────────────────────────────────────

class TelegramNotifier:
    """Thin wrapper so callers can do `from au2_telegram import tg`."""

    def entry(self, side: str, qty: float, price: float,
              regime: str, score: float, risk: float) -> None:
        emoji = "🟢" if side.upper() == "LONG" else "🔴"
        _send(
            f"{emoji} <b>ENTRY {side.upper()}</b>\n"
            f"Price: <code>${price:,.2f}</code>   Qty: <code>{qty:.4f}</code>\n"
            f"Regime: <b>{regime}</b>   Score: {score:.2f}   Risk: ${risk:.2f}"
        )

    def closed(self, pnl: float, reason: str, equity: float,
               side: str = "") -> None:
        emoji = "✅" if pnl >= 0 else "❌"
        sign  = "+" if pnl >= 0 else ""
        _send(
            f"{emoji} <b>CLOSED</b>  {reason}\n"
            f"PnL: <code>{sign}${pnl:.2f}</code>   Equity: <code>${equity:,.2f}</code>"
        )

    def status(self, uptime_h: float, equity: float, pnl: float,
               n_trades: int, pos: str, approved_rate: float = 0.0,
               dead_market: int = 0) -> None:
        """Hourly status — silently skipped if called more often than STATUS_INTERVAL."""
        global _last_status_ts
        now = time.time()
        if now - _last_status_ts < _STATUS_INTERVAL:
            return
        _last_status_ts = now
        sign = "+" if pnl >= 0 else ""
        dead_str = f"\n🪨 Dead-market blocks: {dead_market:,}" if dead_market > 0 else ""
        _send(
            f"📊 <b>STATUS</b>  uptime {uptime_h:.1f}h\n"
            f"Equity: <code>${equity:,.2f}</code>   PnL: <code>{sign}${pnl:.2f}</code>\n"
            f"Trades: {n_trades}   Rate: {approved_rate:.1f}%   Pos: {pos}"
            f"{dead_str}"
        )

    def warning(self, text: str) -> None:
        _send(f"⚠️ <b>WARNING</b>  {text}")

    def started(self, symbol: str, equity: float, flow_be: float, chop_be: float) -> None:
        _send(
            f"🚀 <b>AU2QWEN started</b>  {symbol.upper()}\n"
            f"Equity: <code>${equity:,.2f}</code>\n"
            f"FLOW be_trigger: {flow_be} bps   CHOP be_trigger: {chop_be} bps"
        )

    def stopped(self, equity: float, pnl: float, n_trades: int) -> None:
        sign = "+" if pnl >= 0 else ""
        _send(
            f"🛑 <b>AU2QWEN stopped</b>\n"
            f"Equity: <code>${equity:,.2f}</code>   Session PnL: <code>{sign}${pnl:.2f}</code>\n"
            f"Trades this session: {n_trades}"
        )


tg = TelegramNotifier()

"""Telegram command handler for AU2QWEN bot.

Runs as an async task alongside the trading bot.
Polls Telegram getUpdates every 2 seconds and dispatches commands.

Commands
--------
/status   — real-time equity, PnL, position, vol, regime
/bilan    — daily P&L report
/diag     — rejection stats, dead_market, approval rate, flow metrics
/trades   — last 10 closed trades (equity progression)
/stop     — graceful shutdown (systemd auto-restarts)
/resume   — clear manual pause flag
/help     — command list

Automatic
---------
- Daily report at 00:05 UTC
- Telegram startup/shutdown/entry/close via au2_telegram.py (unchanged)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("au2_tgcmd")

_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


# ── HTTP helpers (sync, called via asyncio.to_thread) ────────────────────────

def _tg_get(method: str, params: dict) -> dict:
    token = os.getenv("TELEGRAM_TOKEN", _TOKEN)
    url   = f"https://api.telegram.org/bot{token}/{method}"
    data  = urllib.parse.urlencode(params).encode()
    try:
        with urllib.request.urlopen(url, data=data, timeout=10) as r:
            return json.loads(r.read())
    except Exception as exc:
        log.warning("tg_get %s failed: %s", method, exc)
        return {}


def _send_sync(text: str, parse_mode: str = "HTML") -> None:
    chat_id = os.getenv("TELEGRAM_CHAT_ID", _CHAT_ID)
    if not chat_id:
        return
    _tg_get("sendMessage", {"chat_id": chat_id, "text": text,
                            "parse_mode": parse_mode})


# ── Command handler ──────────────────────────────────────────────────────────

class TelegramCommandHandler:
    """Async Telegram polling loop. Inject into bot task dict as 'telegram'."""

    def __init__(self, bot: Any, start_equity: float) -> None:
        self.bot           = bot
        self.start_equity  = start_equity
        self._offset       = 0
        self._daily_date   = None        # date string of last daily report
        self._trade_log: list[dict] = [] # ring buffer of last 20 closes
        self._paused       = False

    # ── Public hook called by executor on each trade close ───────────────────

    def record_close(self, pnl: float, reason: str, equity: float,
                     regime: str, side: str) -> None:
        self._trade_log.append({
            "ts":     time.time(),
            "pnl":    pnl,
            "reason": reason,
            "equity": equity,
            "regime": regime,
            "side":   side,
        })
        if len(self._trade_log) > 50:
            self._trade_log.pop(0)

    # ── Main poll loop ───────────────────────────────────────────────────────

    async def run(self) -> None:
        log.info("Telegram command handler started (polling)")
        await asyncio.to_thread(_send_sync,
            "🤖 <b>AU2QWEN prêt</b> — envoie /help pour les commandes")
        while not self.bot.stop_event.is_set():
            try:
                await self._poll_once()
                await self._check_daily_report()
            except Exception as exc:
                log.warning("telegram poll error: %s", exc)
            await asyncio.sleep(2)

    async def _poll_once(self) -> None:
        resp = await asyncio.to_thread(
            _tg_get, "getUpdates",
            {"offset": self._offset, "timeout": 1, "limit": 10}
        )
        updates = resp.get("result", [])
        for upd in updates:
            self._offset = upd["update_id"] + 1
            await self._dispatch(upd)

    async def _dispatch(self, upd: dict) -> None:
        msg     = upd.get("message") or upd.get("edited_message", {})
        text    = (msg.get("text") or "").strip()
        chat_id = str(msg.get("chat", {}).get("id", ""))

        # Only accept messages from authorized chat
        authorized = os.getenv("TELEGRAM_CHAT_ID", _CHAT_ID)
        if chat_id != authorized or not text:
            return

        parts = text.split()
        cmd   = parts[0].lstrip("/").lower().split("@")[0]  # strip @botname
        args  = parts[1:]

        handlers = {
            "status":  self._cmd_status,
            "bilan":   self._cmd_daily,
            "daily":   self._cmd_daily,
            "diag":    self._cmd_diag,
            "trades":  self._cmd_trades,
            "stop":    self._cmd_stop,
            "pause":   self._cmd_pause,
            "resume":  self._cmd_resume,
            "help":    self._cmd_help,
            "start":   self._cmd_help,  # /start from Telegram itself
        }

        handler = handlers.get(cmd)
        if handler:
            await handler(args)
        else:
            await self._cmd_unknown(text)

    # ── Commands ─────────────────────────────────────────────────────────────

    async def _cmd_status(self, args: list) -> None:
        ex       = self.bot.executor
        eq       = ex.risk.current_equity
        pnl      = eq - self.start_equity
        sign     = "+" if pnl >= 0 else ""
        pnl_pct  = pnl / max(self.start_equity, 1) * 100
        uptime_h = (time.time() - self.bot._start_ts) / 3600

        pos_str = "FLAT 🟡"
        if ex.pm.pos:
            p       = ex.pm.pos
            held    = int(time.time() - p.entry_ts)
            unrealised = ""
            pos_str = (f"{'🟢' if p.side == 'LONG' else '🔴'} <b>{p.side}</b>"
                       f"  entry=${p.exec_price:,.2f}"
                       f"  qty={p.remaining_qty:.4f}"
                       f"  held={held}s")

        snap   = ex._diag.get("last_mkt_snapshot") or {}
        vol    = snap.get("vol",    0)
        regime = snap.get("regime", "?")
        score  = snap.get("score",  0)
        thr    = snap.get("thr",    0)
        dead   = ex._diag["rejection_counts"].get("dead_market", 0)

        approved = ex._diag["signals_approved"]
        sigs     = ex._diag["signals_nonflat"] or 1
        rate     = approved / sigs * 100

        await asyncio.to_thread(_send_sync, (
            f"📊 <b>STATUS</b>  uptime {uptime_h:.1f}h\n"
            f"Equity: <code>${eq:,.2f}</code>  "
            f"PnL: <code>{sign}${pnl:.2f} ({sign}{pnl_pct:.1f}%)</code>\n"
            f"Position: {pos_str}\n"
            f"Marché: regime=<b>{regime}</b>  vol={vol:.2f}bps  "
            f"score={score:.2f}/{thr:.2f}\n"
            f"Trades: {approved}  rate={rate:.1f}%  "
            f"🪨dead={dead:,}"
        ))

    async def _cmd_daily(self, args: list) -> None:
        await asyncio.to_thread(_send_sync, self._build_daily_report())

    async def _cmd_diag(self, args: list) -> None:
        ex  = self.bot.executor
        d   = ex._diag
        rej = dict(d["rejection_counts"])

        sc  = d["score_count"] or 1
        avg = d["score_abs_sum"] / sc
        pct = d["score_below_thr"] / sc * 100

        flow_total = (d.get("flow_exit_time", 0) +
                      d.get("flow_exit_be_fallback", 0) +
                      d.get("flow_exit_sl", 0))
        flow_str = ""
        if flow_total > 0:
            fsl  = d.get("flow_exit_sl", 0) / flow_total * 100
            fbe  = d.get("flow_exit_be_fallback", 0) / flow_total * 100
            ftime= d.get("flow_exit_time", 0) / flow_total * 100
            flow_str = (f"\n🌊 FLOW n={flow_total}"
                        f"  time={ftime:.0f}%  be={fbe:.0f}%  sl={fsl:.0f}%")
            if fsl > 10:
                flow_str += "  ⚠️"

        ticks = d["ticks_total"] or 1
        pct_in  = d["ticks_in_pos"]    / ticks * 100
        pct_rb  = d["ticks_risk_block"]/ ticks * 100

        rej_str = "  ".join(f"{k}={v:,}" for k, v in
                            sorted(rej.items(), key=lambda x: -x[1])[:6])

        await asyncio.to_thread(_send_sync, (
            f"🔬 <b>DIAG</b>\n"
            f"Ticks: {ticks:,}  in_pos={pct_in:.0f}%  rblock={pct_rb:.0f}%\n"
            f"Score: avg={avg:.2f}  below_thr={pct:.0f}%\n"
            f"Rejections: {rej_str}"
            f"{flow_str}"
        ))

    async def _cmd_trades(self, args: list) -> None:
        n = 10
        if args:
            try:
                n = min(int(args[0]), 20)
            except ValueError:
                pass

        trades = self._trade_log[-n:]
        if not trades:
            await asyncio.to_thread(_send_sync,
                "Aucun trade enregistré dans cette session.")
            return

        lines = []
        for t in reversed(trades):
            ts  = datetime.fromtimestamp(t["ts"], tz=timezone.utc).strftime("%H:%M")
            pnl = t["pnl"]
            em  = "✅" if pnl >= 0 else "❌"
            lines.append(
                f"{em} {ts}  {t['side']:<5}  {t['reason']:<20}"
                f"  <code>{pnl:+.2f}$</code>  eq={t['equity']:,.0f}"
            )

        gross_w = sum(t["pnl"] for t in trades if t["pnl"] > 0)
        gross_l = abs(sum(t["pnl"] for t in trades if t["pnl"] <= 0))
        pf      = gross_w / gross_l if gross_l > 0 else float("inf")
        wr      = sum(1 for t in trades if t["pnl"] > 0) / len(trades) * 100

        await asyncio.to_thread(_send_sync, (
            f"📋 <b>Derniers {len(trades)} trades</b>\n"
            f"<pre>{''.join(l + chr(10) for l in lines)}</pre>"
            f"WR={wr:.0f}%  PF={pf:.2f}  "
            f"net=${sum(t['pnl'] for t in trades):.2f}"
        ))

    async def _cmd_stop(self, args: list) -> None:
        await asyncio.to_thread(_send_sync,
            "🛑 <b>Arrêt gracieux déclenché.</b>\n"
            "systemd relancera automatiquement. /status dans 10s.")
        self.bot.stop_event.set()

    async def _cmd_pause(self, args: list) -> None:
        self._paused = True
        await asyncio.to_thread(_send_sync,
            "⏸ <b>Pause activée</b> — aucun nouveau trade.")

    async def _cmd_resume(self, args: list) -> None:
        self._paused = False
        await asyncio.to_thread(_send_sync,
            "▶️ <b>Reprise</b> — le bot accepte de nouveaux trades.")

    async def _cmd_help(self, args: list) -> None:
        await asyncio.to_thread(_send_sync, (
            "🤖 <b>AU2QWEN — Commandes</b>\n\n"
            "/status  — état temps réel\n"
            "/bilan   — rapport journalier\n"
            "/diag    — stats internes détaillées\n"
            "/trades [N]  — derniers N trades\n"
            "/stop    — arrêt gracieux\n"
            "/pause   — suspend les entrées\n"
            "/resume  — reprend les entrées\n"
            "/help    — cette aide\n\n"
            "Rapport automatique envoyé à 00:05 UTC."
        ))

    async def _cmd_unknown(self, text: str) -> None:
        await asyncio.to_thread(_send_sync,
            f"❓ Commande inconnue : <code>{text[:60]}</code>\n"
            "Essaie /help pour la liste des commandes.")

    # ── Daily report ─────────────────────────────────────────────────────────

    async def _check_daily_report(self) -> None:
        now  = datetime.now(timezone.utc)
        date = now.strftime("%Y-%m-%d")
        # Send at 00:05 UTC, once per day
        if now.hour == 0 and now.minute == 5 and self._daily_date != date:
            self._daily_date = date
            await asyncio.to_thread(_send_sync, self._build_daily_report())

    def _build_daily_report(self) -> str:
        ex       = self.bot.executor
        eq       = ex.risk.current_equity
        pnl      = eq - self.start_equity
        sign     = "+" if pnl >= 0 else ""
        pnl_pct  = pnl / max(self.start_equity, 1) * 100
        approved = ex._diag["signals_approved"]

        today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Trades from today only
        cutoff   = time.time() - 86_400
        today_trades = [t for t in self._trade_log if t["ts"] > cutoff]

        if today_trades:
            wins    = [t for t in today_trades if t["pnl"] > 0]
            wr      = len(wins) / len(today_trades) * 100
            gross_w = sum(t["pnl"] for t in wins)
            gross_l = abs(sum(t["pnl"] for t in today_trades if t["pnl"] <= 0))
            pf      = gross_w / gross_l if gross_l > 0 else float("inf")
            net_day = sum(t["pnl"] for t in today_trades)
            pf_str  = f"{pf:.2f}" if pf != float("inf") else "∞"
            trades_str = (
                f"Trades: {len(today_trades)}  WR: {wr:.0f}%  PF: {pf_str}\n"
                f"PnL journée: <code>{'+' if net_day >= 0 else ''}{net_day:.2f}$</code>"
            )
        else:
            trades_str = "Aucun trade aujourd'hui."

        dead    = ex._diag["rejection_counts"].get("dead_market", 0)
        sigs    = ex._diag["signals_nonflat"] or 1
        rate    = approved / sigs * 100

        return (
            f"📅 <b>BILAN {today}</b>\n\n"
            f"Equity: <code>${eq:,.2f}</code>\n"
            f"PnL session: <code>{sign}${pnl:.2f} ({sign}{pnl_pct:.1f}%)</code>\n"
            f"{trades_str}\n"
            f"Taux approbation: {rate:.1f}%  🪨dead={dead:,}"
        )

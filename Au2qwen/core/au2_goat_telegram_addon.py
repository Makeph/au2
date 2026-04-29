"""AU2QWEN GOAT Bot — Telegram command addon for telegram_bridge.py.

Registers /goat, /gtrades, /gdiag handlers on the existing telebot instance.
Called once at bridge startup: register_goat_commands(bot, guard_fn, chat_id)

State is read from:
  /root/bot_au2qwen/au2_live_state.json   — equity, position, loss_streak
  journalctl -u bot_au2qwen_goat          — recent trades + diag
"""
from __future__ import annotations

import json
import pathlib
import subprocess
import time
from datetime import datetime, timezone
from typing import Callable

GOAT_STATE_PATH = pathlib.Path("/root/bot_au2qwen/au2_live_state.json")
GOAT_SERVICE    = "bot_au2qwen_goat"
START_EQUITY    = 10_000.0   # reference equity for PnL calculation


# ── helpers ───────────────────────────────────────────────────────────────────

def _state() -> dict:
    try:
        return json.loads(GOAT_STATE_PATH.read_text())
    except Exception:
        return {}


def _service_status() -> str:
    try:
        r = subprocess.run(
            ["systemctl", "is-active", GOAT_SERVICE],
            capture_output=True, text=True, timeout=5,
        )
        return r.stdout.strip()
    except Exception:
        return "?"


def _recent_journal(grep: str, n: int = 40) -> list[str]:
    try:
        r = subprocess.run(
            ["journalctl", "-u", GOAT_SERVICE, "-n", "500",
             "--no-pager", "--output=cat"],
            capture_output=True, text=True, timeout=10,
        )
        lines = [l for l in r.stdout.split("\n") if grep in l]
        return lines[-n:]
    except Exception:
        return []


def _fmt_pos(pos: dict | None) -> str:
    if not pos:
        return "🟡 FLAT"
    side  = pos.get("side", "?")
    entry = pos.get("exec_price", 0)
    qty   = pos.get("remaining_qty", 0)
    reg   = pos.get("regime", "?")
    held  = int(time.time() - pos.get("entry_ts", time.time()))
    em    = "🟢" if side == "LONG" else "🔴"
    return f"{em} {side}  @${entry:,.2f}  qty={qty:.4f}  {reg}  {held}s"


def _parse_trade_line(line: str) -> dict | None:
    """Extract PnL and reason from a 'TRADE CLOSED' log line."""
    try:
        ts_str = line[:19]
        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc).timestamp()
        pnl    = float(line.split("PnL=$")[1].split(" ")[0].strip("|").strip())
        reason = line.split("Reason=")[1].split()[0].strip()
        return {"ts": ts, "pnl": pnl, "reason": reason}
    except Exception:
        return None


# ── command registration ──────────────────────────────────────────────────────

def register_goat_commands(bot, guard: Callable, chat_id: int) -> None:
    """Call once at bridge startup to register GOAT handlers."""

    @bot.message_handler(commands=["goat", "g"])
    def cmd_goat(msg):
        if not guard(msg):
            return
        state  = _state()
        svc    = _service_status()
        svc_em = "✅" if svc == "active" else "🔴"

        eq     = state.get("equity", 0.0)
        pnl    = eq - START_EQUITY
        streak = state.get("loss_streak", 0)
        sign   = "+" if pnl >= 0 else ""
        pnl_pct = pnl / START_EQUITY * 100
        pos    = _fmt_pos(state.get("open_position"))

        # Last heartbeat line for vol/regime
        hb_lines = _recent_journal("last_mkt")
        mkt_str = ""
        if hb_lines:
            last = hb_lines[-1]
            try:
                vol    = float(last.split("vol=")[1].split()[0])
                regime = last.split("regime=")[1].split()[0]
                score  = float(last.split("score=")[1].split()[0])
                dead   = 0
            except Exception:
                vol, regime, score = 0, "?", 0
            mkt_str = f"\nMarché: `{regime}`  vol=`{vol:.2f}bps`  score=`{score:.2f}`"

        # Dead market blocks
        rej_lines = _recent_journal("dead_market", n=5)
        dead_str = ""
        if rej_lines:
            try:
                last_rej = rej_lines[-1]
                dead_cnt = int(last_rej.split("dead_market': ")[1].split(",")[0].split("}")[0])
                dead_str = f"\n🪨 Dead-market blocks: `{dead_cnt:,}`"
            except Exception:
                pass

        bot.reply_to(msg,
            f"{svc_em} *GOAT Bot* — service `{svc}`\n\n"
            f"💰 Equity: `${eq:,.2f}`\n"
            f"📈 PnL: `{sign}${pnl:.2f} ({sign}{pnl_pct:.1f}%)`\n"
            f"📍 Position: {pos}\n"
            f"📉 Loss streak: `{streak}`"
            f"{mkt_str}"
            f"{dead_str}"
        )

    @bot.message_handler(commands=["gtrades"])
    def cmd_gtrades(msg):
        if not guard(msg):
            return
        parts = msg.text.split()
        n = 10
        if len(parts) > 1:
            try:
                n = min(int(parts[1]), 25)
            except ValueError:
                pass

        lines = _recent_journal("TRADE CLOSED", n=n)
        if not lines:
            bot.reply_to(msg, "Aucun trade enregistré.")
            return

        rows = []
        for line in reversed(lines):
            t = _parse_trade_line(line)
            if not t:
                continue
            ts  = datetime.fromtimestamp(t["ts"], tz=timezone.utc).strftime("%H:%M")
            em  = "✅" if t["pnl"] >= 0 else "❌"
            rows.append(f"{em} `{ts}`  {t['reason']:<20}  `{t['pnl']:+.2f}$`")

        trades = [_parse_trade_line(l) for l in lines]
        trades = [t for t in trades if t]
        if trades:
            wins = [t for t in trades if t["pnl"] > 0]
            wr   = len(wins) / len(trades) * 100
            net  = sum(t["pnl"] for t in trades)
            gw   = sum(t["pnl"] for t in wins)
            gl   = abs(sum(t["pnl"] for t in trades if t["pnl"] <= 0))
            pf   = gw / gl if gl > 0 else float("inf")
            pf_str = f"{pf:.2f}" if pf != float("inf") else "∞"
            footer = f"\nWR=`{wr:.0f}%`  PF=`{pf_str}`  net=`${net:.2f}`"
        else:
            footer = ""

        bot.reply_to(msg,
            f"📋 *Derniers {len(rows)} trades GOAT*\n"
            + "\n".join(rows)
            + footer
        )

    @bot.message_handler(commands=["gdiag"])
    def cmd_gdiag(msg):
        if not guard(msg):
            return
        hb_lines = _recent_journal("DIAG |")
        if not hb_lines:
            bot.reply_to(msg, "Pas encore de DIAG disponible.")
            return
        # Last 3 DIAG lines
        last3 = hb_lines[-3:]
        bot.reply_to(msg, "🔬 *GOAT DIAG*\n```\n" + "\n".join(
            l[30:] for l in last3  # strip timestamp prefix
        ) + "\n```")

    @bot.message_handler(commands=["gbilan"])
    def cmd_gbilan(msg):
        if not guard(msg):
            return
        state  = _state()
        eq     = state.get("equity", 0.0)
        pnl    = eq - START_EQUITY
        sign   = "+" if pnl >= 0 else ""
        pnl_pct = pnl / START_EQUITY * 100

        trades  = [_parse_trade_line(l) for l in _recent_journal("TRADE CLOSED", n=200)]
        trades  = [t for t in trades if t]

        today_cutoff = time.time() - 86_400
        today   = [t for t in trades if t["ts"] > today_cutoff]

        if today:
            wins    = [t for t in today if t["pnl"] > 0]
            wr      = len(wins) / len(today) * 100
            gw      = sum(t["pnl"] for t in wins)
            gl      = abs(sum(t["pnl"] for t in today if t["pnl"] <= 0))
            pf      = gw / gl if gl > 0 else float("inf")
            net_day = sum(t["pnl"] for t in today)
            pf_str  = f"{pf:.2f}" if pf != float("inf") else "∞"
            trades_str = (
                f"Trades 24h: `{len(today)}`  WR: `{wr:.0f}%`  PF: `{pf_str}`\n"
                f"PnL 24h: `{'+' if net_day >= 0 else ''}{net_day:.2f}$`"
            )
        else:
            trades_str = "Aucun trade dans les 24h."

        date_str = datetime.now(timezone.utc).strftime("%d/%m/%Y")
        bot.reply_to(msg,
            f"📅 *GOAT Bilan — {date_str}*\n\n"
            f"💰 Equity: `${eq:,.2f}`\n"
            f"📈 PnL session: `{sign}${pnl:.2f} ({sign}{pnl_pct:.1f}%)`\n"
            f"{trades_str}"
        )

    @bot.message_handler(commands=["gstop"])
    def cmd_gstop(msg):
        if not guard(msg):
            return
        try:
            subprocess.run(
                ["systemctl", "stop", GOAT_SERVICE],
                timeout=15, check=True,
            )
            bot.reply_to(msg, "🛑 *GOAT bot arrêté.* systemd le relancera automatiquement.")
        except Exception as e:
            bot.reply_to(msg, f"❌ Erreur: `{e}`")

    @bot.message_handler(commands=["gstart"])
    def cmd_gstart(msg):
        if not guard(msg):
            return
        try:
            subprocess.run(
                ["systemctl", "start", GOAT_SERVICE],
                timeout=15, check=True,
            )
            bot.reply_to(msg, "✅ *GOAT bot démarré.*")
        except Exception as e:
            bot.reply_to(msg, f"❌ Erreur: `{e}`")

    @bot.message_handler(commands=["gpatch", "gask", "gfix"])
    def cmd_gpatch(msg):
        if not guard(msg):
            return
        import os
        parts = msg.text.split(maxsplit=1)
        if len(parts) < 2:
            bot.reply_to(msg,
                "Usage: `/gpatch <modification souhaitée>`\n"
                "Ex: `/gpatch augmente le seuil dead_market à 7.0 bps`\n"
                "Ex: `/gask quel est le seuil min_vol_bps actuel ?`")
            return

        request  = parts[1].strip()
        cmd_name = parts[0].lstrip("/").lower()
        read_only = cmd_name == "gask"

        workdir  = os.getenv("GOAT_CLAUDE_WORKDIR", "/root/bot_au2qwen")
        api_key  = os.getenv("ANTHROPIC_API_KEY", "")

        # Inject bot context into the prompt
        state_eq = "?"
        try:
            st = json.loads(pathlib.Path("/root/bot_au2qwen/au2_live_state.json").read_text())
            state_eq = f"${st.get('equity', 0):,.2f}"
        except Exception:
            pass

        if read_only:
            prompt = (
                f"AU2QWEN GOAT bot context — workdir: {workdir}, equity: {state_eq}. "
                f"Do NOT modify any files. Just answer this question: {request}"
            )
        else:
            prompt = (
                f"AU2QWEN GOAT bot context — workdir: {workdir}, equity: {state_eq}. "
                f"Main files: au2_bot_live.py, au2_live_executor.py, au2_core.py, au2_config.py. "
                f"After any code change, run: systemctl restart bot_au2qwen_goat. "
                f"Task: {request}"
            )

        bot.reply_to(msg, f"🧠 Claude traite : `{request[:80]}`\n_max 3 min…_")

        try:
            import tempfile, shlex
            # Write prompt to temp file — avoids shell escaping issues
            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt",
                                             delete=False, dir="/tmp") as tf:
                tf.write(prompt)
                prompt_file = tf.name
            os.chmod(prompt_file, 0o644)

            # Run as claude_ops (non-root) — required by Claude Code security policy
            script = (
                f"ANTHROPIC_API_KEY={shlex.quote(api_key)} "
                f"claude --print --dangerously-skip-permissions "
                f'"$(cat {shlex.quote(prompt_file)})"'
            )
            result = subprocess.run(
                ["sudo", "-u", "claude_ops", "bash", "-c", script],
                capture_output=True, text=True, timeout=180,
                cwd=workdir,
            )
            os.unlink(prompt_file)
            output = (result.stdout or result.stderr or "Pas de réponse.").strip()
        except subprocess.TimeoutExpired:
            output = "⏱ Timeout 180s — découpe la demande en étapes plus petites."
        except FileNotFoundError:
            output = "❌ Claude CLI introuvable (`claude` not in PATH)."
        except Exception as exc:
            output = f"❌ Erreur: {exc}"

        # Telegram message limit = 4096 chars; send up to 4 chunks
        chunks = [output[i:i + 3800] for i in range(0, min(len(output), 15200), 3800)]
        for i, chunk in enumerate(chunks[:4]):
            prefix = "✅ " if i == 0 and result.returncode == 0 else ("❌ " if i == 0 else "")
            bot.send_message(
                msg.chat.id,
                f"{prefix}```\n{chunk}\n```",
                parse_mode="Markdown",
            )

    @bot.message_handler(commands=["ghelp"])
    def cmd_ghelp(msg):
        if not guard(msg):
            return
        bot.reply_to(msg,
            "🤖 *AU2QWEN GOAT — Commandes*\n\n"
            "/goat — état temps réel\n"
            "/gtrades [N] — derniers trades\n"
            "/gbilan — bilan journalier\n"
            "/gdiag — diagnostic interne\n"
            "/gstop — arrêt gracieux\n"
            "/gstart — démarrage\n"
            "/gpatch <demande> — modification de code par IA\n"
            "/gask <question> — question sans modification\n"
            "/ghelp — cette aide"
        )

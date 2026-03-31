# AU2 V22.5R — Phase 1: Contrarian Mean-Reversion
# Backtest-proven: CVD+Trend signal predicts END of move, not start.
# Strategy: FADE momentum. Score >= threshold → SHORT (sell the top).
# Config: T5.0 SL.30% TP.30% 300s hold. Maker entry. No fast fail.
# V22.5 ajoute une logique ADAPTATIVE sur 6 axes :
#   A. Thresholds par régime (TREND permissif / CHOP quasi-bloqué)
#   B. Confirmation cycles dynamiques (1 à 3 selon score+contexte)
#   C. Cooldown adaptatif (4s après TP, 15s après SL)
#   D. Filters_pass contextuel selon régime
#   E. Trend_ok bypass pour scores extrêmes (score ≥ 1.45×threshold)
#   F. Sizing avec qualité setup (premium/normal/weak)
#   G. Logs adaptatifs complets (adaptive_mode, threshold, cycles, cooldown)
#
# CHANGES V22 hérités (marqués # [V22]):
#   1. score_threshold 2.45 → 2.10          — seuil principal plus permissif
#   2. entry_revalidate_cycles 3 → 2        — confirmation plus rapide
#   3. realized_vol_min_bps 4.0 → 3.0       — plus de marchés qualifient
#   4. min_expected_edge_bps 4.0 → 2.5      — edge guard moins strict
#   5. setup_sweep_mult 0.60 → 0.75         — sweep moins pénalisé
#   6. setup_breakout_mult 0.85 → 0.95      — breakout presque neutre
#   7. flow_expand_vol_ratio 0.95 → 0.78    — flow_expanding plus facile
#   8. filters_pass +4ème chemin            — context_ok seul suffit si score fort
#   9. dynamic_thresholds : léger downside  — seuil peut baisser de 0.20 max
#  10. pause_after_losses 1800 → 600s       — reprise plus rapide
#  11. max_consecutive_losses 2 → 3         — tolérance pertes consécutives
#  12. max_daily_trades 20 → 35             — plafond journalier
#  13. trade_freq caps 3/5 → 5/8            — fréquence autorisée plus haute
#  14. cooldown 12 → 8s                     — entre trades
#  15. no_trade_windows : retire 10:00-13:30 — plage haute activité débloquée
#  16. anti_chase_max_move_bps 5.5 → 7.5    — anti-chase légèrement relâché
#  17. entry_signal_window 2.0 → 3.0s       — fenêtre confirmation plus large

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import signal
import sqlite3
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from enum import Enum
from typing import Deque, Dict, List, Optional, Tuple

import aiohttp
import websockets
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("au2")


@dataclass
class BotConfig:
    symbol: str = os.getenv("BOT_SYMBOL", "BTCUSDT")
    live_mode: bool = os.getenv("LIVE_MODE", "false").lower() == "true"

    api_key: str = os.getenv("BINANCE_API_KEY", "")
    api_secret: str = os.getenv("BINANCE_API_SECRET", "")

    rest_base: str = os.getenv("BINANCE_FAPI_REST", "https://fapi.binance.com")
    ws_base: str = os.getenv("BINANCE_FAPI_WS", "wss://fstream.binance.com/stream")

    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    db_path: str = os.getenv("DB_PATH", "au2_v22_5.db")

    leverage: int = int(os.getenv("BOT_LEVERAGE", "5"))
    risk_per_trade_bps: float = float(os.getenv("RISK_PER_TRADE_BPS", "48"))
    max_notional_usd: float = float(os.getenv("MAX_NOTIONAL_USD", "350"))
    min_notional_usd: float = float(os.getenv("MIN_NOTIONAL_USD", "20"))

    stop_loss_pct: float = float(os.getenv("STOP_LOSS_PCT", "0.30"))               # Phase1: 0.30% symmetric
    take_profit_1_pct: float = float(os.getenv("TAKE_PROFIT_1_PCT", "0.30"))     # Phase1: 0.30% symmetric with SL
    take_profit_2_pct: float = float(os.getenv("TAKE_PROFIT_2_PCT", "0.60"))     # Phase1: 2x TP1 for runner
    trailing_stop_pct: float = float(os.getenv("TRAILING_STOP_PCT", "0.15"))     # Phase1: trailing for runner

    score_long_threshold: float = float(os.getenv("SCORE_LONG_THRESHOLD", "5.00"))     # Phase1: backtested optimal
    score_short_threshold: float = float(os.getenv("SCORE_SHORT_THRESHOLD", "-5.00"))  # Phase1: symmetric
    cooldown_seconds: int = int(os.getenv("COOLDOWN_SECONDS", "45"))                   # Phase1: 45s between trades
    min_cvd_aligned: float = float(os.getenv("MIN_CVD_ALIGNED", "0.0"))  # Phase1: disabled

    oi_poll_seconds: int = int(os.getenv("OI_POLL_SECONDS", "5"))
    account_poll_seconds: int = int(os.getenv("ACCOUNT_POLL_SECONDS", "20"))
    oi_window_seconds: int = int(os.getenv("OI_WINDOW_SECONDS", "60"))

    kill_switch_daily_loss_pct: float = float(os.getenv("KILL_SWITCH_DAILY_LOSS_PCT", "2.0"))
    top_levels: int = int(os.getenv("TOP_LEVELS", "16"))
    warmup_seconds: int = int(os.getenv("WARMUP_SECONDS", "25"))
    signal_log_interval: float = float(os.getenv("SIGNAL_LOG_INTERVAL", "0.5"))

    soft_stop_fallback_after: int = int(os.getenv("SOFT_STOP_FALLBACK_AFTER", "30"))
    close_on_shutdown: bool = os.getenv("CLOSE_ON_SHUTDOWN", "false").lower() == "true"

    flow_expand_accel_min: float = float(os.getenv("FLOW_EXPAND_ACCEL_MIN", "0.40"))
    flow_expand_vol_ratio: float = float(os.getenv("FLOW_EXPAND_VOL_RATIO", "0.78"))  # [V22] 0.95→0.78
    trade_sessions: str = os.getenv("TRADE_SESSIONS", "00:00-23:59")

    context_window_s: int = int(os.getenv("CONTEXT_WINDOW_S", "120"))
    context_exclude_last_s: int = int(os.getenv("CONTEXT_EXCLUDE_LAST_S", "5"))
    context_breakout_bps: float = float(os.getenv("CONTEXT_BREAKOUT_BPS", "9.0"))
    context_sweep_window_s: int = int(os.getenv("CONTEXT_SWEEP_WINDOW_S", "20"))
    context_sweep_liq_min: float = float(os.getenv("CONTEXT_SWEEP_LIQ_MIN", "3.0"))
    context_extreme_prox_bps: float = float(os.getenv("CONTEXT_EXTREME_PROX_BPS", "6.0"))
    pullback_min_dev_bps: float = float(os.getenv("PULLBACK_MIN_DEV_BPS", "5.0"))
    pullback_max_dev_bps: float = float(os.getenv("PULLBACK_MAX_DEV_BPS", "18.0"))
    pullback_cvd_reclaim_min: float = float(os.getenv("PULLBACK_CVD_RECLAIM_MIN", "3.0"))


    realized_vol_window_s: int = int(os.getenv("REALIZED_VOL_WINDOW_S", "30"))
    realized_vol_min_bps: float = float(os.getenv("REALIZED_VOL_MIN_BPS", "3.0"))   # [V22] 4.0→3.0
    realized_vol_max_bps: float = float(os.getenv("REALIZED_VOL_MAX_BPS", "110.0"))
    microtrend_window_s: int = int(os.getenv("MICROTREND_WINDOW_S", "20"))
    vwap_window_s: int = int(os.getenv("VWAP_WINDOW_S", "30"))
    vwap_max_dev_bps: float = float(os.getenv("VWAP_MAX_DEV_BPS", "40.0"))
    trend_confirm_bps: float = float(os.getenv("TREND_CONFIRM_BPS", "2.2"))

    liquidity_shift_min: float = float(os.getenv("LIQUIDITY_SHIFT_MIN", "0.08"))
    liquidity_pull_weight: float = float(os.getenv("LIQUIDITY_PULL_WEIGHT", "1.25"))
    liquidity_stack_weight: float = float(os.getenv("LIQUIDITY_STACK_WEIGHT", "1.05"))

    flow_flip_persist_cycles: int = int(os.getenv("FLOW_FLIP_PERSIST_CYCLES", "2"))
    flip_score_threshold: float = float(os.getenv("FLIP_SCORE_THRESHOLD", "1.35"))
    entry_signal_window_seconds: float = float(os.getenv("ENTRY_SIGNAL_WINDOW_SECONDS", "3.0"))  # [V22] NEW

    tp1_size_ratio: float = float(os.getenv("TP1_SIZE_RATIO", "0.55"))
    tp2_size_ratio: float = float(os.getenv("TP2_SIZE_RATIO", "0.25"))
    runner_size_ratio: float = float(os.getenv("RUNNER_SIZE_RATIO", "0.30"))
    breakeven_after_tp1: bool = os.getenv("BREAKEVEN_AFTER_TP1", "true").lower() == "true"
    entry_revalidate_cycles: int = int(os.getenv("ENTRY_REVALIDATE_CYCLES", "2"))   # [V22] 3→2
    anti_chase_max_move_bps: float = float(os.getenv("ANTI_CHASE_MAX_MOVE_BPS", "7.5"))  # [V22] 5.5→7.5
    anti_chase_max_spread_bps: float = float(os.getenv("ANTI_CHASE_MAX_SPREAD_BPS", "3.0"))
    anti_chase_min_pressure_ratio: float = float(os.getenv("ANTI_CHASE_MIN_PRESSURE_RATIO", "1.05"))
    no_trade_windows: str = os.getenv("NO_TRADE_WINDOWS", "00:00-00:15,23:45-23:59")  # [V22] retire 10:00-13:30
    session_report_on_shutdown: bool = os.getenv("SESSION_REPORT_ON_SHUTDOWN", "true").lower() == "true"
    reconcile_poll_seconds: float = float(os.getenv("RECONCILE_POLL_SECONDS", "5"))
    max_position_hold_seconds: int = int(os.getenv("MAX_POSITION_HOLD_SECONDS", "300"))  # Phase1: 300s (5min) for mean reversion
    pnl_hold_extension_seconds: int = int(os.getenv("PNL_HOLD_EXTENSION_SECONDS", "25"))
    pnl_hold_min_bps: float = float(os.getenv("PNL_HOLD_MIN_BPS", "6.0"))
    runner_trend_hold_bonus_seconds: int = int(os.getenv("RUNNER_TREND_HOLD_BONUS_SECONDS", "20"))
    max_ws_lag_ms: float = float(os.getenv("MAX_WS_LAG_MS", "500.0"))

    dangerous_windows: str = os.getenv("DANGEROUS_WINDOWS", "13:25-13:40")
    dangerous_spread_bps: float = float(os.getenv("DANGEROUS_SPREAD_BPS", "3.8"))
    queue_depletion_min: float = float(os.getenv("QUEUE_DEPLETION_MIN", "0.10"))
    queue_rebuild_min: float = float(os.getenv("QUEUE_REBUILD_MIN", "0.08"))
    queue_edge_weight: float = float(os.getenv("QUEUE_EDGE_WEIGHT", "0.90"))
    setup_breakout_mult: float = float(os.getenv("SETUP_BREAKOUT_MULT", "0.95"))  # [V22] 0.85→0.95
    setup_sweep_mult: float = float(os.getenv("SETUP_SWEEP_MULT", "0.75"))        # [V22] 0.60→0.75
    setup_extreme_mult: float = float(os.getenv("SETUP_EXTREME_MULT", "1.18"))
    setup_liquidity_mult: float = float(os.getenv("SETUP_LIQUIDITY_MULT", "1.02"))
    # ATTENTION : NO_TRADE_WINDOWS et DANGEROUS_WINDOWS sont interprétées dans cette timezone.
    # Europe/Paris = UTC+1 hiver / UTC+2 été. Sur un VPS en UTC, mettre BOT_TIMEZONE=UTC.
    timezone_name: str = os.getenv("BOT_TIMEZONE", "Europe/Paris")

    taker_fee_bps: float = float(os.getenv("TAKER_FEE_BPS", "4.5"))
    maker_fee_bps: float = float(os.getenv("MAKER_FEE_BPS", "1.8"))
    expected_entry_slippage_bps: float = float(os.getenv("EXPECTED_ENTRY_SLIPPAGE_BPS", "1.4"))
    expected_exit_slippage_bps: float = float(os.getenv("EXPECTED_EXIT_SLIPPAGE_BPS", "1.8"))
    max_entry_slippage_bps: float = float(os.getenv("MAX_ENTRY_SLIPPAGE_BPS", "5.0"))
    min_expected_edge_bps: float = float(os.getenv("MIN_EXPECTED_EDGE_BPS", "2.5"))  # [V22] 4.0→2.5

    max_consecutive_losses: int = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "3"))        # V23: 2→3 (less pausing)
    pause_after_losses_seconds: int = int(os.getenv("PAUSE_AFTER_LOSSES_SECONDS", "900"))  # V23: 1200→900 (15min pause)

    dynamic_threshold_vol_weight: float = float(os.getenv("DYNAMIC_THRESHOLD_VOL_WEIGHT", "0.006"))
    dynamic_threshold_spread_weight: float = float(os.getenv("DYNAMIC_THRESHOLD_SPREAD_WEIGHT", "0.08"))
    dynamic_threshold_queue_weight: float = float(os.getenv("DYNAMIC_THRESHOLD_QUEUE_WEIGHT", "0.20"))
    breakeven_buffer_bps: float = float(os.getenv("BREAKEVEN_BUFFER_BPS", "1.5"))

    burst_after_win_seconds: int = int(os.getenv("BURST_AFTER_WIN_SECONDS", "180"))
    burst_cooldown_seconds: int = int(os.getenv("BURST_COOLDOWN_SECONDS", "1"))
    burst_threshold_bonus: float = float(os.getenv("BURST_THRESHOLD_BONUS", "0.22"))
    burst_size_mult: float = float(os.getenv("BURST_SIZE_MULT", "1.12"))
    scalp_fast_fail_seconds: int = int(os.getenv("SCALP_FAST_FAIL_SECONDS", "0"))  # Phase1: DISABLED (no fast fail for contrarian)
    scalp_fast_fail_score_flip: float = float(os.getenv("SCALP_FAST_FAIL_SCORE_FLIP", "0.45"))
    scalp_take_profit_bps: float = float(os.getenv("SCALP_TAKE_PROFIT_BPS", "12.0"))
    scalp_fade_exit_score: float = float(os.getenv("SCALP_FADE_EXIT_SCORE", "0.50"))
    allow_chop_reversion: bool = os.getenv("ALLOW_CHOP_REVERSION", "false").lower() == "true"
    max_daily_trades: int = int(os.getenv("MAX_DAILY_TRADES", "10"))               # V23: 15→10
    trade_freq_lookback_seconds: int = int(os.getenv("TRADE_FREQ_LOOKBACK_SECONDS", "600"))
    trade_freq_soft_cap: int = int(os.getenv("TRADE_FREQ_SOFT_CAP", "2"))
    trade_freq_hard_cap: int = int(os.getenv("TRADE_FREQ_HARD_CAP", "3"))        # V23: 4→3
    setup_min_trades_for_blacklist: int = int(os.getenv("SETUP_MIN_TRADES_FOR_BLACKLIST", "5"))
    setup_blacklist_pnl_usd: float = float(os.getenv("SETUP_BLACKLIST_PNL_USD", "-0.5"))
    setup_cooldown_seconds: int = int(os.getenv("SETUP_COOLDOWN_SECONDS", "600"))
    chop_reversion_min_vol_bps: float = float(os.getenv("CHOP_REVERSION_MIN_VOL_BPS", "12.0"))
    chop_reversion_max_spread_bps: float = float(os.getenv("CHOP_REVERSION_MAX_SPREAD_BPS", "2.0"))
    chop_reversion_min_queue_edge: float = float(os.getenv("CHOP_REVERSION_MIN_QUEUE_EDGE", "0.4"))
    micro_move_hard_limit_bps: float = float(os.getenv("MICRO_MOVE_HARD_LIMIT_BPS", "15.0"))
    # Bloquer entrée si tendance forte contre le signal (calibré sur données réelles)
    # Sur 46 trades : 13 contre-tendance = 100% des gains annulés
    trend_adverse_bps: float = float(os.getenv("TREND_ADVERSE_BPS", "2.2"))
    queue_edge_min_abs: float = float(os.getenv("QUEUE_EDGE_MIN_ABS", "0.05"))
    max_same_side_reentries: int = int(os.getenv("MAX_SAME_SIDE_REENTRIES", "3"))
    reentry_window_seconds: int = int(os.getenv("REENTRY_WINDOW_SECONDS", "900"))

    # ── V22.5 : Adaptive thresholds par régime ──────────────────────────────
    # Multiplicateurs appliqués sur score_long_threshold de base.
    # TREND propre → plus permissif. CHOP → quasi-blocage.
    adaptive_threshold_trend_mult: float     = float(os.getenv("ADAPTIVE_THRESHOLD_TREND_MULT",      "0.88"))
    adaptive_threshold_flow_mult: float      = float(os.getenv("ADAPTIVE_THRESHOLD_FLOW_MULT",       "0.93"))
    adaptive_threshold_mean_revert_mult: float = float(os.getenv("ADAPTIVE_THRESHOLD_MEAN_REVERT_MULT", "1.05"))
    adaptive_threshold_liquidation_mult: float = float(os.getenv("ADAPTIVE_THRESHOLD_LIQUIDATION_MULT", "0.95"))
    adaptive_threshold_chop_mult: float      = float(os.getenv("ADAPTIVE_THRESHOLD_CHOP_MULT",       "1.50"))

    # ── V22.5 : Confirmation cycles dynamiques ──────────────────────────────
    # Si score ≥ threshold × fast_ratio ET queue_edge ≥ fast_queue_min ET bon régime → 1 cycle.
    adaptive_cycles_fast_score_ratio: float  = float(os.getenv("ADAPTIVE_CYCLES_FAST_SCORE_RATIO",   "1.35"))
    adaptive_cycles_fast_queue_min: float    = float(os.getenv("ADAPTIVE_CYCLES_FAST_QUEUE_MIN",     "0.30"))
    # Si score ≥ threshold × normal_ratio → 2 cycles. Sinon → cycles config (2 par défaut).
    adaptive_cycles_normal_score_ratio: float = float(os.getenv("ADAPTIVE_CYCLES_NORMAL_SCORE_RATIO", "1.10"))

    # ── V22.5 : Cooldown adaptatif ──────────────────────────────────────────
    # Remplace le cooldown fixe selon la raison de la dernière sortie.
    cooldown_after_tp_seconds: int           = int(os.getenv("COOLDOWN_AFTER_TP_SECONDS",            "4"))
    cooldown_after_sl_seconds: int           = int(os.getenv("COOLDOWN_AFTER_SL_SECONDS",            "15"))
    cooldown_after_fast_fail_seconds: int    = int(os.getenv("COOLDOWN_AFTER_FAST_FAIL_SECONDS",     "20"))  # V22.5R: 12→20
    cooldown_after_time_stop_seconds: int    = int(os.getenv("COOLDOWN_AFTER_TIME_STOP_SECONDS",     "15"))  # V22.5R: 10→15

    # ── V22.5 : Trend bypass pour scores extrêmes ──────────────────────────
    # Si score ≥ adaptive_threshold × ce ratio, trend_side=FLAT toléré en TREND.
    trend_bypass_score_ratio: float          = float(os.getenv("TREND_BYPASS_SCORE_RATIO",           "1.45"))

    # ── V22.5 : Setup quality sizing ───────────────────────────────────────
    # Multiplicateurs sur la taille selon la qualité du setup.
    setup_premium_size_mult: float           = float(os.getenv("SETUP_PREMIUM_SIZE_MULT",            "1.05"))
    setup_acceptable_size_mult: float        = float(os.getenv("SETUP_ACCEPTABLE_SIZE_MULT",         "0.80"))
    setup_weak_size_mult: float              = float(os.getenv("SETUP_WEAK_SIZE_MULT",               "0.65"))


def now_ms() -> int:
    return int(time.time() * 1000)


def pct_change(a: float, b: float) -> float:
    return 0.0 if not a else (b - a) / a * 100.0


def round_down(value: float, step: float) -> float:
    return value if step <= 0 else math.floor(value / step) * step


def round_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return price
    return round(round(price / tick) * tick, 8)


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def safe_ratio(a: float, b: float) -> float:
    return 0.0 if abs(b) < 1e-12 else a / b


def _parse_sessions(sessions_str: str) -> List[Tuple[int, int]]:
    result = []
    for part in sessions_str.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            start_s, end_s = part.split("-")
            sh, sm = map(int, start_s.split(":"))
            eh, em = map(int, end_s.split(":"))
            result.append((sh * 60 + sm, eh * 60 + em))
        except Exception:
            log.warning("session invalide ignorée: %r", part)
    return result if result else [(0, 1439)]


class PosState(str, Enum):
    NONE = "NONE"
    PENDING_ENTRY = "PENDING_ENTRY"
    OPEN = "OPEN"
    PENDING_CLOSE = "PENDING_CLOSE"


@dataclass
class BracketOrders:
    sl_order_id: int
    tp_order_ids: List[int]
    sl_price: float
    tp_prices: List[float]
    sl_filled_qty: float = 0.0  # cumulatif Binance déjà comptabilisé


@dataclass
class Position:
    side: str
    entry_price: float
    qty: float
    opened_at: float
    entry_order_id: int = 0
    bracket: Optional[BracketOrders] = field(default=None)
    state: PosState = PosState.OPEN
    remaining_qty: float = 0.0
    tp1_done: bool = False
    tp2_done: bool = False
    runner_active: bool = False
    peak_price: float = 0.0
    trough_price: float = 0.0
    flip_counter: int = 0
    signal_mid: float = 0.0  # mid au moment du signal d'entrée (mesure slippage)
    setup_name: str = ""


class Journal:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-8000")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS fills(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER,
                side TEXT,
                price REAL,
                qty REAL,
                pnl REAL,
                mode TEXT,
                reason TEXT,
                order_id INTEGER,
                slippage_bps REAL DEFAULT 0.0
            );
            CREATE TABLE IF NOT EXISTS orders(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER,
                order_id INTEGER UNIQUE,
                order_type TEXT,
                side TEXT,
                stop_price REAL,
                qty REAL,
                status TEXT,
                fill_price REAL,
                mode TEXT
            );
            CREATE TABLE IF NOT EXISTS signals(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER,
                price REAL,
                raw_score REAL,
                score REAL,
                signal TEXT,
                cvd_delta_5s REAL,
                ofi REAL,
                imbalance REAL,
                liquidation_score REAL,
                oi_delta_pct REAL,
                absorption INTEGER,
                filter_pass INTEGER,
                flow_expanding INTEGER,
                time_ok INTEGER,
                context_ok INTEGER,
                regime_ok INTEGER,
                trend_ok INTEGER,
                context_reason TEXT,
                realized_vol_bps REAL,
                vwap_dev_bps REAL,
                trend_bps REAL,
                liquidity_shift REAL,
                dangerous_time_ok INTEGER,
                queue_edge REAL,
                bid_depletion REAL,
                ask_depletion REAL,
                bid_rebuild REAL,
                ask_rebuild REAL,
                setup_name TEXT,
                calibrated_score REAL
            );
            CREATE TABLE IF NOT EXISTS equity(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER,
                equity REAL,
                realized_pnl REAL,
                unrealized_pnl REAL,
                daily_loss_pct REAL
            );
            CREATE TABLE IF NOT EXISTS runtime_state(
                state_key TEXT PRIMARY KEY,
                state_value TEXT,
                ts INTEGER
            );
            """
        )
        self.conn.commit()
        # Migration : colonnes ajoutées en v18 (silencieux si déjà présentes)
        for col_def in ("ALTER TABLE fills ADD COLUMN slippage_bps REAL DEFAULT 0.0",):
            try:
                self.conn.execute(col_def)
                self.conn.commit()
            except Exception:
                pass
        self._queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None
        self.started_ts = int(time.time())

    def start(self):
        self._worker_task = asyncio.create_task(self._flush_worker(), name="journal")

    async def _flush_worker(self):
        while True:
            try:
                rows = [await self._queue.get()]
                while not self._queue.empty():
                    rows.append(self._queue.get_nowait())
                for sql, args in rows:
                    self.conn.execute(sql, args)
                self.conn.commit()
            except asyncio.CancelledError:
                while not self._queue.empty():
                    sql, args = self._queue.get_nowait()
                    self.conn.execute(sql, args)
                self.conn.commit()
                raise
            except Exception as exc:
                log.warning("journal flush error: %s", exc)

    def log_fill(self, side: str, price: float, qty: float, pnl: float, mode: str, reason: str, order_id: int = 0, slippage_bps: float = 0.0):
        self._queue.put_nowait((
            "INSERT INTO fills(ts,side,price,qty,pnl,mode,reason,order_id,slippage_bps) VALUES(?,?,?,?,?,?,?,?,?)",
            (int(time.time()), side, price, qty, pnl, mode, reason, order_id, round(slippage_bps, 4)),
        ))

    def log_order(self, order_id: int, order_type: str, side: str, stop_price: float, qty: float, status: str, fill_price: float = 0.0, mode: str = "PAPER"):
        self._queue.put_nowait((
            "INSERT OR REPLACE INTO orders(ts,order_id,order_type,side,stop_price,qty,status,fill_price,mode) VALUES(?,?,?,?,?,?,?,?,?)",
            (int(time.time()), order_id, order_type, side, stop_price, qty, status, fill_price, mode),
        ))

    def update_order_status(self, order_id: int, status: str, fill_price: float = 0.0):
        self._queue.put_nowait((
            "UPDATE orders SET status=?, fill_price=?, ts=? WHERE order_id=?",
            (status, fill_price, int(time.time()), order_id),
        ))

    def log_signal(self, **k):
        self._queue.put_nowait((
            "INSERT INTO signals(ts,price,raw_score,score,signal,cvd_delta_5s,ofi,imbalance,liquidation_score,oi_delta_pct,absorption,filter_pass,flow_expanding,time_ok,context_ok,regime_ok,trend_ok,context_reason,realized_vol_bps,vwap_dev_bps,trend_bps,liquidity_shift,dangerous_time_ok,queue_edge,bid_depletion,ask_depletion,bid_rebuild,ask_rebuild,setup_name,calibrated_score) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                int(time.time()),
                k["price"], k["raw_score"], k["score"], k["signal"],
                k["cvd_delta_5s"], k["ofi"], k["imbalance"], k["liquidation_score"],
                k["oi_delta_pct"], int(k["absorption"]), int(k["filter_pass"]),
                int(k["flow_expanding"]), int(k["time_ok"]), int(k["context_ok"]),
                int(k["regime_ok"]), int(k["trend_ok"]), k["context_reason"],
                k["realized_vol_bps"], k["vwap_dev_bps"], k["trend_bps"], k["liquidity_shift"],
                int(k["dangerous_time_ok"]), k["queue_edge"], k["bid_depletion"], k["ask_depletion"],
                k["bid_rebuild"], k["ask_rebuild"], k["setup_name"], k["calibrated_score"],
            ),
        ))

    def log_equity(self, equity: float, realized_pnl: float, unrealized_pnl: float, daily_loss_pct: float):
        self._queue.put_nowait((
            "INSERT INTO equity(ts,equity,realized_pnl,unrealized_pnl,daily_loss_pct) VALUES(?,?,?,?,?)",
            (int(time.time()), equity, realized_pnl, unrealized_pnl, daily_loss_pct),
        ))

    def save_runtime_state(self, key: str, value: str):
        self._queue.put_nowait((
            "INSERT OR REPLACE INTO runtime_state(state_key,state_value,ts) VALUES(?,?,?)",
            (key, value, int(time.time())),
        ))

    def load_runtime_state(self, key: str) -> Optional[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT state_value FROM runtime_state WHERE state_key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

    def session_report(self) -> dict:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT COUNT(*), COALESCE(SUM(pnl),0.0), COALESCE(AVG(pnl),0.0), "
            "COALESCE(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END),0), "
            "COALESCE(SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END),0) "
            "FROM fills WHERE ts >= ?",
            (self.started_ts,),
        )
        trades, pnl, avg_pnl, wins, losses = cur.fetchone()
        cur.execute(
            "SELECT reason, COUNT(*), COALESCE(SUM(pnl),0.0) FROM fills "
            "WHERE ts >= ? GROUP BY reason ORDER BY COUNT(*) DESC, SUM(pnl) DESC LIMIT 6",
            (self.started_ts,),
        )
        reasons = cur.fetchall()
        cur.execute(
            "SELECT strftime('%H', ts, 'unixepoch') AS hh, COUNT(*), COALESCE(SUM(pnl),0.0) "
            "FROM fills WHERE ts >= ? GROUP BY hh ORDER BY hh",
            (self.started_ts,),
        )
        hours = cur.fetchall()
        cur.execute(
            "SELECT context_reason, COUNT(*) FROM signals WHERE ts >= ? AND signal != 'FLAT' "
            "GROUP BY context_reason ORDER BY COUNT(*) DESC LIMIT 6",
            (self.started_ts,),
        )
        contexts = cur.fetchall()
        cur.execute(
            "SELECT setup_name, COUNT(*), ROUND(AVG(calibrated_score),3) FROM signals WHERE ts >= ? AND signal != 'FLAT' "
            "GROUP BY setup_name ORDER BY COUNT(*) DESC LIMIT 8",
            (self.started_ts,),
        )
        setups = cur.fetchall()
        return {
            "trades": int(trades or 0),
            "pnl": float(pnl or 0.0),
            "avg_pnl": float(avg_pnl or 0.0),
            "wins": int(wins or 0),
            "losses": int(losses or 0),
            "reasons": reasons,
            "hours": hours,
            "contexts": contexts,
            "setups": setups,
        }


class BinanceREST:
    def __init__(self, cfg: BotConfig, session: aiohttp.ClientSession):
        self.cfg = cfg
        self.session = session

    def _sign(self, params: Dict[str, str]) -> str:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        sig = hmac.new(self.cfg.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        return f"{query}&signature={sig}"

    async def req(self, method: str, path: str, params: Optional[Dict[str, str]] = None, signed: bool = False) -> dict:
        params = params or {}
        headers: Dict[str, str] = {}
        url = self.cfg.rest_base + path
        body = None
        if signed:
            params["timestamp"] = str(now_ms())
            params["recvWindow"] = "5000"
            headers["X-MBX-APIKEY"] = self.cfg.api_key
            sq = self._sign(params)
            if method == "GET":
                url += "?" + sq
            else:
                body = sq
                headers["Content-Type"] = "application/x-www-form-urlencoded"
        elif params:
            url += "?" + "&".join(f"{k}={v}" for k, v in params.items())

        async with self.session.request(method, url, data=body, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            txt = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"{method} {path} -> {resp.status} {txt}")
            return json.loads(txt) if txt else {}

    async def exchange_info(self) -> dict:
        return await self.req("GET", "/fapi/v1/exchangeInfo", {"symbol": self.cfg.symbol})

    async def depth_snapshot(self) -> dict:
        return await self.req("GET", "/fapi/v1/depth", {"symbol": self.cfg.symbol, "limit": "1000"})

    async def open_interest(self) -> dict:
        return await self.req("GET", "/fapi/v1/openInterest", {"symbol": self.cfg.symbol})

    async def account(self) -> dict:
        return await self.req("GET", "/fapi/v3/account", signed=True)

    async def position_risk(self) -> List[dict]:
        return await self.req("GET", "/fapi/v3/positionRisk", {"symbol": self.cfg.symbol}, signed=True)

    async def open_orders(self) -> List[dict]:
        return await self.req("GET", "/fapi/v1/openOrders", {"symbol": self.cfg.symbol}, signed=True)

    async def change_leverage(self) -> dict:
        return await self.req("POST", "/fapi/v1/leverage", {"symbol": self.cfg.symbol, "leverage": str(self.cfg.leverage)}, signed=True)

    async def new_order(self, side: str, order_type: str, quantity: float,
                        stop_price: float = 0.0, reduce_only: bool = False,
                        client_order_id: str = "", price: float = 0.0,
                        time_in_force: str = "") -> dict:
        params: Dict[str, str] = {
            "symbol": self.cfg.symbol,
            "side": side,
            "type": order_type,
            "quantity": str(quantity),
            "newOrderRespType": "RESULT",
        }
        if price > 0:
            params["price"] = str(price)
        if time_in_force:
            params["timeInForce"] = time_in_force
        if stop_price:
            params["stopPrice"] = str(stop_price)
        if reduce_only:
            params["reduceOnly"] = "true"
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return await self.req("POST", "/fapi/v1/order", params, signed=True)

    async def cancel_order(self, order_id: int) -> dict:
        return await self.req("DELETE", "/fapi/v1/order", {"symbol": self.cfg.symbol, "orderId": str(order_id)}, signed=True)

    async def cancel_all_orders(self) -> dict:
        return await self.req("DELETE", "/fapi/v1/allOpenOrders", {"symbol": self.cfg.symbol}, signed=True)

    async def new_listen_key(self) -> dict:
        return await self.req("POST", "/fapi/v1/listenKey", signed=True)

    async def keepalive_listen_key(self, listen_key: str) -> dict:
        return await self.req("PUT", "/fapi/v1/listenKey", {"listenKey": listen_key}, signed=True)


class LocalBook:
    def __init__(self):
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id = 0
        self.ready = False
        self._synced = False  # True après le premier diff accepté post-snapshot

    def load_snapshot(self, snap: dict):
        self.bids = {float(p): float(q) for p, q in snap["bids"] if float(q) > 0}
        self.asks = {float(p): float(q) for p, q in snap["asks"] if float(q) > 0}
        self.last_update_id = int(snap["lastUpdateId"])
        self.ready = True
        self._synced = False  # reset : prochain diff doit passer la validation initiale

    def apply_diff(self, data: dict) -> bool:
        if not self.ready:
            return False
        u = int(data["u"])    # final update ID de ce message
        U = int(data["U"])    # premier update ID de ce message
        pu = int(data.get("pu", 0))  # final update ID du message précédent

        if not self._synced:
            # Algorithme Binance Futures : ignorer tant que u <= lastUpdateId,
            # accepter le premier message où U <= lastUpdateId + 1 <= u
            if u <= self.last_update_id:
                return True  # encore dans le passé — ignorer silencieusement
            if U > self.last_update_id + 1:
                return False  # trou de séquence — refaire snapshot
            self._synced = True
        else:
            # Messages suivants : pu doit enchaîner avec notre dernier u
            if pu != self.last_update_id:
                self._synced = False
                return False  # trou — refaire snapshot

        for p, q in data["b"]:
            p, q = float(p), float(q)
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q
        for p, q in data["a"]:
            p, q = float(p), float(q)
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q
        self.last_update_id = u
        return True

    def best_bid_ask(self) -> Tuple[float, float]:
        return (max(self.bids) if self.bids else 0.0, min(self.asks) if self.asks else 0.0)

    def top(self, n: int):
        bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return bids, asks


@dataclass
class FlowState:
    mid: float = 0.0
    best_bid: float = 0.0
    best_ask: float = 0.0
    spread_bps: float = 0.0
    imbalance: float = 0.0
    ofi: float = 0.0
    cvd: float = 0.0
    cvd_delta_5s: float = 0.0
    liquidation_score: float = 0.0
    funding_rate: float = 0.0
    open_interest: float = 0.0
    oi_delta_pct: float = 0.0
    absorption: bool = False
    raw_score: float = 0.0
    score: float = 0.0
    signal: str = "FLAT"
    warmed_up: bool = False
    flow_expanding: bool = False
    time_ok: bool = False
    context_ok: bool = False
    regime_ok: bool = False
    trend_ok: bool = False
    context_reason: str = ""
    filters_pass: bool = False
    realized_vol_bps: float = 0.0
    vwap_dev_bps: float = 0.0
    trend_bps: float = 0.0
    trend_side: str = "FLAT"
    liquidity_shift: float = 0.0
    regime: str = "CHOP"
    no_trade_ok: bool = True
    micro_move_1s_bps: float = 0.0
    book_pressure_ratio: float = 1.0
    dangerous_time_ok: bool = True
    queue_edge: float = 0.0
    bid_depletion: float = 0.0
    ask_depletion: float = 0.0
    bid_rebuild: float = 0.0
    ask_rebuild: float = 0.0
    setup_name: str = ""
    calibrated_score: float = 0.0
    adaptive_mode: str = "neutral"       # V22.5: trend_aggressive|flow_normal|mean_revert_selective|liquidation_special|chop_defensive
    adaptive_threshold: float = 0.0      # V22.5: seuil effectif après ajustement régime
    # Impulse detection (reactive entry)
    move_1s_bps: float = 0.0
    move_2s_bps: float = 0.0
    is_impulsive: bool = False


class FlowEngine:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.book = LocalBook()
        self.cvd = 0.0
        self.cvd_hist: Deque[Tuple[float, float]] = deque(maxlen=10000)
        self.liq_hist: Deque[Tuple[float, float, str]] = deque(maxlen=1000)
        self.oi_hist: Deque[Tuple[float, float]] = deque(maxlen=500)
        self.price_hist: Deque[Tuple[float, float]] = deque(maxlen=12000)
        self.vol_hist: Deque[Tuple[float, float]] = deque(maxlen=12000)

        self.last_bid_vol = 0.0
        self.last_ask_vol = 0.0
        self.last_best_bid = 0.0
        self.last_best_ask = 0.0
        self.last_best_bid_qty = 0.0
        self.last_best_ask_qty = 0.0

        self.funding_rate = 0.0
        self.last_trade_price = 0.0
        self._start_time = time.time()
        self._sessions = _parse_sessions(cfg.trade_sessions)
        self._no_trade_windows = _parse_sessions(cfg.no_trade_windows) if cfg.no_trade_windows.strip() else []
        self._dangerous_windows = _parse_sessions(cfg.dangerous_windows) if cfg.dangerous_windows.strip() else []
        self._tz = ZoneInfo(cfg.timezone_name)

    def on_trade(self, price: float, qty: float, is_sell: bool):
        self.last_trade_price = price
        self.cvd += -qty if is_sell else qty
        ts = time.time()
        self.cvd_hist.append((ts, self.cvd))
        self.price_hist.append((ts, price))
        self.vol_hist.append((ts, qty))

    def on_liquidation(self, qty: float, side: str):
        self.liq_hist.append((time.time(), qty, side))

    def on_open_interest(self, oi: float):
        self.oi_hist.append((time.time(), oi))

    def on_funding(self, rate: float):
        self.funding_rate = rate

    def _recent_price(self, seconds: float) -> Optional[float]:
        target = time.time() - seconds
        candidate = None
        for ts, px in self.price_hist:
            if ts <= target:
                candidate = px
            else:
                break
        if candidate is not None:
            return candidate
        return self.price_hist[0][1] if self.price_hist else None

    def _compute_impulse(self, mid: float) -> Tuple[float, float, bool]:
        """Detect real-time price impulse. Returns (move_1s_bps, move_2s_bps, is_impulsive)."""
        px_1s = self._recent_price(1.0)
        px_2s = self._recent_price(2.0)
        move_1s = ((mid - px_1s) / mid * 10000.0) if (px_1s and mid) else 0.0
        move_2s = ((mid - px_2s) / mid * 10000.0) if (px_2s and mid) else 0.0
        is_impulsive = abs(move_1s) >= 1.2 or abs(move_2s) >= 2.0
        return move_1s, move_2s, is_impulsive

    def _realized_vol_bps(self, now: float, mid: float) -> float:
        cutoff = now - self.cfg.realized_vol_window_s
        prices = [px for ts, px in self.price_hist if ts >= cutoff]
        if len(prices) < 6 or mid <= 0:
            return 0.0
        rets = []
        for a, b in zip(prices, prices[1:]):
            if a > 0 and b > 0:
                rets.append(math.log(b / a))
        if len(rets) < 5:
            return 0.0
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / len(rets)
        return math.sqrt(var) * math.sqrt(len(rets)) * 10000.0

    def _trend_metrics(self, now: float, mid: float) -> Tuple[float, str, float]:
        trend_base = None
        vwap_cutoff = now - self.cfg.vwap_window_s
        trend_cutoff = now - self.cfg.microtrend_window_s
        pv = 0.0
        vol = 0.0
        for ts, px in self.price_hist:
            if trend_base is None and ts >= trend_cutoff:
                trend_base = px
                break
        vol_events = [(ts, q) for ts, q in self.vol_hist if ts >= vwap_cutoff]
        price_events = [(ts, px) for ts, px in self.price_hist if ts >= vwap_cutoff]
        if vol_events and price_events:
            i = 0
            last_px = price_events[0][1]
            for ts, q in vol_events:
                while i + 1 < len(price_events) and price_events[i + 1][0] <= ts:
                    i += 1
                    last_px = price_events[i][1]
                pv += last_px * q
                vol += q
        vwap = pv / vol if vol > 0 else mid
        trend_bps = ((mid - trend_base) / mid * 10000.0) if trend_base and mid else 0.0
        vwap_dev_bps = ((mid - vwap) / mid * 10000.0) if mid else 0.0
        if trend_bps >= self.cfg.trend_confirm_bps:
            trend_side = "LONG"
        elif trend_bps <= -self.cfg.trend_confirm_bps:
            trend_side = "SHORT"
        else:
            trend_side = "FLAT"
        return trend_bps, trend_side, vwap_dev_bps

    def _flow_expanding(self, raw_signal: str, cvd_delta_5s: float, now: float) -> bool:
        if raw_signal == "LONG" and cvd_delta_5s <= 0:
            return False
        if raw_signal == "SHORT" and cvd_delta_5s >= 0:
            return False
        cutoff5 = now - 5.0
        cutoff10 = now - 10.0
        cvd_at_5 = cvd_at_10 = None
        for ts, cvd in self.cvd_hist:
            if cvd_at_10 is None and ts >= cutoff10:
                cvd_at_10 = cvd
            if cvd_at_5 is None and ts >= cutoff5:
                cvd_at_5 = cvd
            if cvd_at_5 is not None and cvd_at_10 is not None:
                break
        prev = (cvd_at_5 - cvd_at_10) if (cvd_at_5 is not None and cvd_at_10 is not None) else 0.0
        accel_ok = True if cvd_at_10 is None else abs(cvd_delta_5s) >= abs(prev) * self.cfg.flow_expand_accel_min
        cutoff60 = now - 60.0
        vol_5s = sum(v for ts, v in self.vol_hist if ts >= cutoff5)
        vol_60s = sum(v for ts, v in self.vol_hist if ts >= cutoff60)
        avg_5s = vol_60s / 12.0 if vol_60s > 0 else 0.0
        vol_ok = True if avg_5s <= 0 else vol_5s >= avg_5s * self.cfg.flow_expand_vol_ratio
        return accel_ok and vol_ok

    def _time_ok(self, now: float) -> bool:
        dt = datetime.fromtimestamp(now, tz=self._tz)
        cur = dt.hour * 60 + dt.minute
        for start, end in self._sessions:
            if start <= end:
                if start <= cur <= end:
                    return True
            else:
                if cur >= start or cur <= end:
                    return True
        return False

    def _no_trade_ok(self, now: float) -> bool:
        if not self._no_trade_windows:
            return True
        dt = datetime.fromtimestamp(now, tz=self._tz)
        cur = dt.hour * 60 + dt.minute
        for start, end in self._no_trade_windows:
            if start <= end:
                if start <= cur <= end:
                    return False
            else:
                if cur >= start or cur <= end:
                    return False
        return True

    def _dangerous_time_ok(self, now: float, spread_bps: float) -> bool:
        if spread_bps >= self.cfg.dangerous_spread_bps:
            return False
        if not self._dangerous_windows:
            return True
        dt = datetime.fromtimestamp(now, tz=self._tz)
        cur = dt.hour * 60 + dt.minute
        for start, end in self._dangerous_windows:
            if start <= end:
                if start <= cur <= end:
                    return False
            else:
                if cur >= start or cur <= end:
                    return False
        return True

    def _queue_metrics(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]) -> Tuple[float, float, float, float, float]:
        bid3 = sum(q for _, q in bids[:3])
        ask3 = sum(q for _, q in asks[:3])
        prev_bid = max(min(self.last_bid_vol, bid3 if bid3 > 0 else self.last_bid_vol), 1e-9)
        prev_ask = max(min(self.last_ask_vol, ask3 if ask3 > 0 else self.last_ask_vol), 1e-9)
        bid_depletion = max(prev_bid - bid3, 0.0) / prev_bid
        ask_depletion = max(prev_ask - ask3, 0.0) / prev_ask
        bid_rebuild = max(bid3 - prev_bid, 0.0) / prev_bid
        ask_rebuild = max(ask3 - prev_ask, 0.0) / prev_ask
        queue_edge = clamp((ask_depletion + bid_rebuild) - (bid_depletion + ask_rebuild), -2.0, 2.0)
        return queue_edge, bid_depletion, ask_depletion, bid_rebuild, ask_rebuild

    def _setup_multiplier(self, context_reason: str, regime: str) -> Tuple[str, float]:
        # V23 Phase 1: No setup multipliers. Score speaks for itself.
        # Keep setup NAME for analytics only (no score modification).
        if context_reason == "breakout" and regime == "TREND":
            return "breakout_trend", 1.0
        if context_reason == "extreme":
            return "extreme_trend", 1.0
        if context_reason == "liquidity_shift":
            return "liquidity_shift", 1.0
        if regime == "TREND":
            return "generic_trend", 1.0
        if regime == "FLOW":
            return "flow_continuation", 1.0
        return f"{context_reason or 'generic'}_{regime.lower()}", 1.0

    def _adaptive_threshold(
        self,
        regime: str,
        realized_vol_bps: float,
        spread_bps: float,
        queue_edge: float,
    ) -> Tuple[float, float, str]:
        """V23 Phase 1 — Fixed threshold. No regime adjustments.
        Regime is still used for regime_ok gate (blocks CHOP), but
        threshold is the same for all regimes. Simplicity > complexity.
        """
        base = abs(self.cfg.score_long_threshold)  # 4.00
        mode = f"{regime.lower()}_fixed"
        return base, -base, mode

    def _classify_regime(
        self,
        raw_signal: str,
        realized_vol_bps: float,
        spread_bps: float,
        trend_bps: float,
        vwap_dev_bps: float,
        liq_score: float,
        liquidity_shift: float,
    ) -> str:
        if spread_bps > max(self.cfg.anti_chase_max_spread_bps * 1.35, 4.0):
            return "CHOP"
        if abs(liq_score) >= max(self.cfg.context_sweep_liq_min * 2.4, 8.0) and realized_vol_bps >= self.cfg.realized_vol_min_bps:
            return "LIQUIDATION"
        if abs(trend_bps) >= self.cfg.trend_confirm_bps * 1.35 and realized_vol_bps >= self.cfg.realized_vol_min_bps * 1.15 and abs(liquidity_shift) >= self.cfg.liquidity_shift_min:
            return "TREND"
        if realized_vol_bps < self.cfg.realized_vol_min_bps:
            return "CHOP"
        if abs(vwap_dev_bps) >= self.cfg.vwap_max_dev_bps * 0.32 and abs(trend_bps) < self.cfg.trend_confirm_bps * 0.85:
            return "MEAN_REVERT"
        if raw_signal != "FLAT" and abs(liquidity_shift) >= self.cfg.liquidity_shift_min:
            return "FLOW"
        return "CHOP"

    def _context(self, raw_signal: str, mid: float, now: float, trend_side: str, vwap_dev_bps: float, liquidity_shift: float, cvd_delta_5s: float) -> Tuple[bool, str]:
        if not mid:
            return False, ""
        cutoff_old = now - self.cfg.context_window_s
        cutoff_new = now - self.cfg.context_exclude_last_s
        range_prices = [px for ts, px in self.price_hist if cutoff_old <= ts <= cutoff_new]
        if not range_prices:
            return True, "no_history"
        range_high = max(range_prices)
        range_low = min(range_prices)
        range_span_bps = (range_high - range_low) / mid * 10000 if mid else 0.0
        if range_span_bps >= self.cfg.context_breakout_bps:
            if raw_signal == "LONG" and mid >= range_high:
                return True, "breakout"
            if raw_signal == "SHORT" and mid <= range_low:
                return True, "breakout"
        sweep_cutoff = now - self.cfg.context_sweep_window_s
        liq_buy = sum(qty for ts, qty, side in self.liq_hist if ts >= sweep_cutoff and side == "BUY")
        liq_sell = sum(qty for ts, qty, side in self.liq_hist if ts >= sweep_cutoff and side == "SELL")
        if raw_signal == "LONG" and liq_buy >= self.cfg.context_sweep_liq_min:
            return True, "sweep"
        if raw_signal == "SHORT" and liq_sell >= self.cfg.context_sweep_liq_min:
            return True, "sweep"
        # pullback context SUPPRIMÉ (données: avg_cal=-5.795, 100% perdant)
        near_low = abs(mid - range_low) / mid * 10000 <= self.cfg.context_extreme_prox_bps
        near_high = abs(mid - range_high) / mid * 10000 <= self.cfg.context_extreme_prox_bps
        if raw_signal == "LONG" and near_low:
            return True, "extreme"
        if raw_signal == "SHORT" and near_high:
            return True, "extreme"
        return False, ""

    def _liquidity_shift(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]], best_bid: float, best_ask: float) -> float:
        bid_vol = sum(q for _, q in bids)
        ask_vol = sum(q for _, q in asks)
        best_bid_qty = bids[0][1] if bids else 0.0
        best_ask_qty = asks[0][1] if asks else 0.0

        bid_pull = max(self.last_bid_vol - bid_vol, 0.0)
        ask_pull = max(self.last_ask_vol - ask_vol, 0.0)
        bid_stack = max(bid_vol - self.last_bid_vol, 0.0)
        ask_stack = max(ask_vol - self.last_ask_vol, 0.0)

        bullish = 0.0
        bearish = 0.0
        if best_bid >= self.last_best_bid:
            bullish += safe_ratio(bid_stack, max(self.last_bid_vol, 1e-9)) * self.cfg.liquidity_stack_weight
        if best_ask <= self.last_best_ask or self.last_best_ask == 0:
            bullish += safe_ratio(ask_pull, max(self.last_ask_vol, 1e-9)) * self.cfg.liquidity_pull_weight
        if best_ask <= self.last_best_ask:
            bearish += safe_ratio(ask_stack, max(self.last_ask_vol, 1e-9)) * self.cfg.liquidity_stack_weight
        if best_bid >= self.last_best_bid or self.last_best_bid == 0:
            bearish += safe_ratio(bid_pull, max(self.last_bid_vol, 1e-9)) * self.cfg.liquidity_pull_weight

        bullish += safe_ratio(best_bid_qty - self.last_best_bid_qty, max(self.last_best_bid_qty, 1e-9)) * 0.35
        bearish += safe_ratio(best_ask_qty - self.last_best_ask_qty, max(self.last_best_ask_qty, 1e-9)) * 0.35

        self.last_best_bid = best_bid
        self.last_best_ask = best_ask
        self.last_best_bid_qty = best_bid_qty
        self.last_best_ask_qty = best_ask_qty
        return clamp(bullish - bearish, -2.5, 2.5)

    def build_state(self) -> FlowState:
        now = time.time()
        warmed_up = (now - self._start_time) >= self.cfg.warmup_seconds

        bids, asks = self.book.top(self.cfg.top_levels)
        bid_vol = sum(q for _, q in bids)
        ask_vol = sum(q for _, q in asks)
        best_bid, best_ask = self.book.best_bid_ask()
        mid = (best_bid + best_ask) / 2 if (best_bid and best_ask) else self.last_trade_price
        spread_bps = (best_ask - best_bid) / mid * 10000 if (mid and best_bid and best_ask) else 0.0
        imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol) if (bid_vol + ask_vol) else 0.0
        book_pressure_ratio = safe_ratio(bid_vol, max(ask_vol, 1e-9)) if ask_vol > 0 else 1.0
        ofi = (bid_vol - self.last_bid_vol) - (ask_vol - self.last_ask_vol)
        queue_edge, bid_depletion, ask_depletion, bid_rebuild, ask_rebuild = self._queue_metrics(bids, asks)
        liquidity_shift = self._liquidity_shift(bids, asks, best_bid, best_ask)
        self.last_bid_vol, self.last_ask_vol = bid_vol, ask_vol

        cutoff5 = now - 5.0
        cvd_base = None
        price_base = None
        for ts, cvd in self.cvd_hist:
            if ts >= cutoff5:
                cvd_base = cvd
                break
        for ts, px in self.price_hist:
            if ts >= cutoff5:
                price_base = px
                break
        cvd_delta_5s = self.cvd - cvd_base if cvd_base is not None else 0.0
        price_delta_5s = mid - price_base if price_base is not None else 0.0

        liq_score = sum((qty if side == "SELL" else -qty) for ts, qty, side in self.liq_hist if ts >= cutoff5)
        oi = self.oi_hist[-1][1] if self.oi_hist else 0.0
        oi_delta_pct = 0.0
        if self.oi_hist:
            cutoff_oi = now - self.cfg.oi_window_seconds
            base_oi = next((v for ts, v in self.oi_hist if ts >= cutoff_oi), None)
            if base_oi is not None:
                oi_delta_pct = pct_change(base_oi, oi)

        px_tol = mid * 0.00025 if mid else 5.0
        strong_abs = abs(cvd_delta_5s) > 15 and abs(price_delta_5s) < px_tol
        weak_abs = abs(cvd_delta_5s) > 8 and abs(price_delta_5s) < px_tol * 1.6
        absorption = strong_abs or weak_abs

        realized_vol_bps = self._realized_vol_bps(now, mid)
        trend_bps, trend_side, vwap_dev_bps = self._trend_metrics(now, mid)
        px_1s = self._recent_price(1.0)
        micro_move_1s_bps = ((mid - px_1s) / mid * 10000.0) if (px_1s and mid) else 0.0

        # ── Scoring V23 Phase 1 — 3 components only ──────────────────
        # CVD: who is aggressive (buyers vs sellers). THE signal.
        # Trend: is price actually moving? Confirmation.
        # Volume: is the market active? Filter.
        # Everything else is noise at this timeframe.
        cvd_score = clamp(cvd_delta_5s / 6.0, -3.0, 3.0)
        trend_component = clamp(trend_bps / 5.0, -2.0, 2.0)
        vol_score = clamp((realized_vol_bps - 5.0) / 10.0, -0.5, 0.5)
        raw_score = (
            2.00 * cvd_score           # CVD dominant (50% of max score)
            + 1.50 * trend_component   # Trend confirmation (33%)
            + vol_score                # Volume activity bonus/penalty
        )
        score = clamp(raw_score, -10.0, 10.0)

        # ── Étape 1 : threshold provisoire (sans régime) pour raw_signal ──
        # V23: provisional threshold = base (fixed, no adjustments)
        base = abs(self.cfg.score_long_threshold)  # 4.00
        prov_threshold = base

        raw_signal = "FLAT"
        if warmed_up and not strong_abs:
            if score >= prov_threshold:
                raw_signal = "LONG"
            elif score <= -prov_threshold:
                raw_signal = "SHORT"

        time_ok = self._time_ok(now)
        no_trade_ok = self._no_trade_ok(now)
        dangerous_time_ok = self._dangerous_time_ok(now, spread_bps)

        # ── Étape 2 : régime + threshold adaptatif ───────────────────────
        regime = self._classify_regime(raw_signal, realized_vol_bps, spread_bps, trend_bps, vwap_dev_bps, liq_score, liquidity_shift)
        long_threshold, short_threshold, adaptive_mode = self._adaptive_threshold(regime, realized_vol_bps, spread_bps, queue_edge)
        regime_ok = self.cfg.realized_vol_min_bps <= realized_vol_bps <= self.cfg.realized_vol_max_bps and regime != "CHOP"

        # ── Étape 3 : trend_ok — Phase1 CONTRARIAN ─────────────────────
        # In contrarian mode, we WANT to enter against momentum.
        # trend_ok = True always. The score threshold itself ensures
        # the momentum is strong enough to fade.
        trend_ok = True

        # ── Étape 4 : filters_pass V23 Phase 1 — simplified ────────────
        # Score itself is the filter. If CVD+trend+vol reached threshold,
        # and regime is not CHOP, that's enough. No context/flow gates.
        flow_expanding = self._flow_expanding(raw_signal, cvd_delta_5s, now) if raw_signal != "FLAT" else False
        context_ok = False
        context_reason = ""
        if raw_signal != "FLAT":
            context_ok, context_reason = self._context(raw_signal, mid, now, trend_side, vwap_dev_bps, liquidity_shift, cvd_delta_5s)
        filters_pass = (
            raw_signal != "FLAT"
            and time_ok
            and no_trade_ok
            and regime_ok
            and trend_ok
            and dangerous_time_ok
        )

        # Phase1: setup_mult is always 1.0, no queue_edge penalty
        setup_name, setup_mult = self._setup_multiplier(context_reason, regime)
        calibrated_score = score  # Phase1: score IS the calibrated score
        # ── CONTRARIAN SIGNAL ──────────────────────────────────────────
        # CVD+Trend predict END of move → FADE the momentum.
        # Strong bullish score → SELL. Strong bearish score → BUY.
        calibrated_signal = "FLAT"
        if warmed_up and calibrated_score >= long_threshold:
            calibrated_signal = "SHORT"   # FADE: bullish momentum exhaustion → sell
        elif warmed_up and calibrated_score <= short_threshold:
            calibrated_signal = "LONG"    # FADE: bearish momentum exhaustion → buy
        signal = calibrated_signal if filters_pass else "FLAT"
        # Impulse detection
        imp_1s, imp_2s, imp_active = self._compute_impulse(mid)
        return FlowState(
            mid=mid, best_bid=best_bid, best_ask=best_ask, spread_bps=spread_bps,
            imbalance=imbalance, ofi=ofi, cvd=self.cvd, cvd_delta_5s=cvd_delta_5s,
            liquidation_score=liq_score, funding_rate=self.funding_rate, open_interest=oi,
            oi_delta_pct=oi_delta_pct, absorption=absorption, raw_score=raw_score, score=score,
            signal=signal, warmed_up=warmed_up, flow_expanding=flow_expanding, time_ok=time_ok,
            context_ok=context_ok, regime_ok=regime_ok, trend_ok=trend_ok,
            context_reason=context_reason, filters_pass=filters_pass,
            realized_vol_bps=realized_vol_bps, vwap_dev_bps=vwap_dev_bps,
            trend_bps=trend_bps, trend_side=trend_side, liquidity_shift=liquidity_shift,
            regime=regime, no_trade_ok=no_trade_ok, micro_move_1s_bps=micro_move_1s_bps,
            book_pressure_ratio=book_pressure_ratio, dangerous_time_ok=dangerous_time_ok,
            queue_edge=queue_edge, bid_depletion=bid_depletion, ask_depletion=ask_depletion,
            bid_rebuild=bid_rebuild, ask_rebuild=ask_rebuild, setup_name=setup_name,
            calibrated_score=calibrated_score,
            adaptive_mode=adaptive_mode, adaptive_threshold=long_threshold,
            move_1s_bps=imp_1s, move_2s_bps=imp_2s, is_impulsive=imp_active,
        )


class Trader:
    def __init__(self, cfg: BotConfig, rest: BinanceREST, journal: Journal):
        self.cfg = cfg
        self.rest = rest
        self.journal = journal
        self.position: Optional[Position] = None
        self.last_trade_ts = 0.0
        self.realized_pnl = 0.0
        self.day_anchor = int(time.time() // 86400)
        self.day_start_equity = 0.0
        self.latest_equity = 0.0
        self.available_balance = 0.0
        self.qty_step = 0.001
        self.min_qty = 0.001
        self.tick_size = 0.1
        self.user_stream_last_alive = time.time()
        self._close_lock = asyncio.Lock()
        self.entry_signal_side = ""
        self.entry_signal_count = 0
        self.entry_signal_ts = 0.0
        self._reconcile_error_count = 0
        self.consecutive_losses = 0
        self.pause_until_ts = 0.0
        self.fees_paid = 0.0
        self.last_win_ts = 0.0
        self.trade_count_day = 0
        self.reentry_side = ""
        self.reentry_count = 0
        self.reentry_ts = 0.0
        self.recent_trades: Deque[float] = deque(maxlen=64)
        self.setup_stats: Dict[str, Dict[str, float]] = defaultdict(lambda: {"pnl": 0.0, "trades": 0.0})
        self.setup_pause_until: Dict[str, float] = {}
        # V22.5 : tracking sortie pour cooldown adaptatif
        self.last_exit_reason: str = ""
        self.last_exit_pnl: float = 0.0

    def _trade_day_key(self) -> str:
        return f"trade_count_day:{self.day_anchor}"

    def persist_daily_state(self):
        self.journal.save_runtime_state(self._trade_day_key(), str(int(self.trade_count_day)))

    def restore_daily_state(self):
        raw = self.journal.load_runtime_state(self._trade_day_key())
        if raw is None:
            return
        try:
            self.trade_count_day = max(int(raw), 0)
        except Exception:
            self.trade_count_day = 0

    def update_symbol_filters(self, exchange_info: dict):
        sym = next((s for s in exchange_info["symbols"] if s["symbol"] == self.cfg.symbol), None)
        if not sym:
            log.error("symbole %s introuvable", self.cfg.symbol)
            return
        for flt in sym["filters"]:
            if flt["filterType"] == "LOT_SIZE":
                self.qty_step = float(flt["stepSize"])
                self.min_qty = float(flt["minQty"])
            elif flt["filterType"] == "PRICE_FILTER":
                self.tick_size = float(flt["tickSize"])
        log.info("filtres: qty_step=%s min_qty=%s tick_size=%s", self.qty_step, self.min_qty, self.tick_size)

    def update_account_snapshot(self, equity: float, available: float):
        day = int(time.time() // 86400)
        if day != self.day_anchor:
            self.day_anchor = day
            self.day_start_equity = equity
            self.trade_count_day = 0
            self.persist_daily_state()
            self.reentry_side = ""
            self.reentry_count = 0
            self.reentry_ts = 0.0
        if not self.day_start_equity:
            self.day_start_equity = equity
        self.latest_equity = equity
        self.available_balance = available

    def daily_loss_pct(self) -> float:
        if not self.day_start_equity:
            return 0.0
        return max(self.day_start_equity - self.latest_equity, 0.0) / self.day_start_equity * 100.0

    def expected_roundtrip_cost_bps(self, state: FlowState) -> float:
        return max(
            self.cfg.taker_fee_bps * 2.0 + self.cfg.expected_entry_slippage_bps + self.cfg.expected_exit_slippage_bps + state.spread_bps * 0.35,
            0.0,
        )

    def expected_edge_bps(self, state: FlowState) -> float:
        """V23 Phase 1 — Simple edge: score strength minus costs."""
        gross = abs(state.calibrated_score) * 1.5
        return gross - self.expected_roundtrip_cost_bps(state)

    def trade_fee_usd(self, notional: float, is_maker: bool = False) -> float:
        fee_bps = self.cfg.maker_fee_bps if is_maker else self.cfg.taker_fee_bps
        return max(notional, 0.0) * fee_bps / 10000.0

    def recent_trade_count(self, lookback: Optional[int] = None) -> int:
        now = time.time()
        cutoff = now - float(lookback or self.cfg.trade_freq_lookback_seconds)
        return sum(1 for ts in self.recent_trades if ts >= cutoff)

    def frequency_size_mult(self) -> float:
        freq = self.recent_trade_count()
        if freq > self.cfg.trade_freq_hard_cap:
            return 0.35
        if freq > self.cfg.trade_freq_soft_cap:
            return 0.65
        return 1.0

    def burst_mode(self, state: Optional[FlowState] = None) -> bool:
        if not (self.last_win_ts > 0 and (time.time() - self.last_win_ts) <= self.cfg.burst_after_win_seconds):
            return False
        if self.consecutive_losses > 0 or self.trade_count_day < 3:
            return False
        if state is None:
            return True
        return (
            state.realized_vol_bps > 15.0
            and state.regime in ("TREND", "FLOW", "LIQUIDATION")
            and abs(state.queue_edge) > 0.25
        )

    def register_new_entry(self, side: str):
        now = time.time()
        self.trade_count_day += 1
        self.persist_daily_state()
        self.recent_trades.append(now)
        if side == self.reentry_side and now - self.reentry_ts <= self.cfg.reentry_window_seconds:
            self.reentry_count += 1
        else:
            self.reentry_side = side
            self.reentry_count = 1
        self.reentry_ts = now

    def record_setup_pnl(self, setup_name: str, pnl_after_costs: float):
        if not setup_name:
            return
        stats = self.setup_stats[setup_name]
        stats["pnl"] += pnl_after_costs
        stats["trades"] += 1
        if stats["trades"] >= self.cfg.setup_min_trades_for_blacklist and stats["pnl"] <= self.cfg.setup_blacklist_pnl_usd:
            self.setup_pause_until[setup_name] = max(
                self.setup_pause_until.get(setup_name, 0.0),
                time.time() + self.cfg.setup_cooldown_seconds,
            )

    def _adaptive_cooldown(self) -> int:
        """V23 Phase 1 — Fixed cooldown. Simple."""
        return self.cfg.cooldown_seconds  # 30s

    def _adaptive_cycles(self, state: "FlowState") -> int:
        """V22.5 — Nombre de cycles de confirmation requis selon le contexte.

        Score fort + queue edge fort + bon régime → 1 cycle (entrée rapide).
        Score normal → cycles config (2 par défaut).
        Contexte ambigu → max(cycles_config, 3).
        """
        threshold = state.adaptive_threshold if state.adaptive_threshold > 0 else self.cfg.score_long_threshold
        score_abs = abs(state.calibrated_score)

        # Entrée rapide : signal très fort dans un bon régime avec microstructure alignée
        fast_conditions = (
            score_abs >= threshold * self.cfg.adaptive_cycles_fast_score_ratio
            and abs(state.queue_edge) >= self.cfg.adaptive_cycles_fast_queue_min
            and state.regime in ("TREND", "FLOW", "LIQUIDATION")
            and state.trend_ok
        )
        if fast_conditions:
            return 1

        # Entrée normale : signal clairement au-dessus du seuil
        if score_abs >= threshold * self.cfg.adaptive_cycles_normal_score_ratio:
            return max(1, self.cfg.entry_revalidate_cycles)

        # Contexte ambigu : exiger un cycle de plus
        return max(self.cfg.entry_revalidate_cycles, 2)

    def _setup_quality_mult(self, setup_name: str) -> float:
        """V22.5 — Multiplicateur de sizing selon la qualité du setup.

        Premium (données: avg_cal +1.2 à +1.4) → 1.05x.
        Acceptable mais pas premium → 0.80x.
        Faible / toxique historiquement → 0.65x.
        """
        if setup_name in ("extreme_trend", "generic_trend"):
            return self.cfg.setup_premium_size_mult   # 1.05
        if setup_name in ("flow_continuation",):
            return 1.0                                # neutre
        if setup_name in ("breakout_trend", "liquidity_shift"):
            return self.cfg.setup_acceptable_size_mult  # 0.80
        if setup_name in ("sweep_liq",):
            return self.cfg.setup_weak_size_mult      # 0.65
        return 1.0  # fallback générique

    def is_setup_blocked(self, setup_name: str) -> bool:
        if not setup_name:
            return False
        return time.time() < self.setup_pause_until.get(setup_name, 0.0)

    def register_trade_result(self, pnl_after_costs: float):
        now = time.time()
        if pnl_after_costs < 0:
            self.consecutive_losses += 1
            if self.consecutive_losses >= self.cfg.max_consecutive_losses:
                self.pause_until_ts = max(self.pause_until_ts, now + self.cfg.pause_after_losses_seconds)
        else:
            self.consecutive_losses = 0
            self.last_win_ts = now

    def position_size(self, price: float, state: FlowState) -> float:
        """V23 Phase 1 — Simple risk-based sizing. 3 multipliers max."""
        equity_base = self.latest_equity if self.latest_equity > 0 else (self.available_balance if self.available_balance > 0 else self.cfg.max_notional_usd)
        risk_usd = max(equity_base * self.cfg.risk_per_trade_bps / 10000.0, self.cfg.min_notional_usd * 0.01)
        # Confidence: higher score → slightly larger size
        confidence = clamp(abs(state.calibrated_score) / 5.0, 0.70, 1.15)
        # Loss reduction: size down after losses
        loss_mult = 0.60 if self.consecutive_losses >= 2 else 0.80 if self.consecutive_losses == 1 else 1.0
        # Spread penalty: wider spread → smaller size
        spread_penalty = clamp(3.0 / max(state.spread_bps, 0.5), 0.50, 1.0)
        risk_usd *= confidence * loss_mult * spread_penalty
        stop_d = max(price * self.cfg.stop_loss_pct / 100.0, self.tick_size, 1e-8)
        qty_from_risk = risk_usd / stop_d
        max_qty = self.cfg.max_notional_usd / max(price, 1e-8)
        qty = min(qty_from_risk, max_qty)
        if qty * price < self.cfg.min_notional_usd:
            qty = self.cfg.min_notional_usd / max(price, 1e-8)
        qty = max(qty, self.min_qty)
        qty = round_down(qty, self.qty_step)
        return max(qty, self.min_qty)

    def _split_qty(self, qty: float) -> Tuple[float, float, float]:
        tp1 = round_down(qty * self.cfg.tp1_size_ratio, self.qty_step)
        tp2 = round_down(qty * self.cfg.tp2_size_ratio, self.qty_step)
        runner = round_down(qty - tp1 - tp2, self.qty_step)
        if runner < 0:
            runner = 0.0
        total = tp1 + tp2 + runner
        if total < qty:
            runner = round_down(runner + (qty - total), self.qty_step)
        if tp1 < self.min_qty:
            tp1 = 0.0
        if runner < self.min_qty:
            runner = max(round_down(qty - tp1 - tp2, self.qty_step), 0.0)
        if tp1 + tp2 + runner <= 0:
            return qty, 0.0, 0.0
        return tp1, tp2, runner

    def bracket_prices(self, pos: Position) -> Tuple[float, float, float]:
        if pos.side == "LONG":
            sl = pos.entry_price * (1 - self.cfg.stop_loss_pct / 100.0)
            tp1 = pos.entry_price * (1 + self.cfg.take_profit_1_pct / 100.0)
            tp2 = pos.entry_price * (1 + self.cfg.take_profit_2_pct / 100.0)
        else:
            sl = pos.entry_price * (1 + self.cfg.stop_loss_pct / 100.0)
            tp1 = pos.entry_price * (1 - self.cfg.take_profit_1_pct / 100.0)
            tp2 = pos.entry_price * (1 - self.cfg.take_profit_2_pct / 100.0)
        return round_to_tick(sl, self.tick_size), round_to_tick(tp1, self.tick_size), round_to_tick(tp2, self.tick_size)

    async def _replace_stop_order(self, pos: Position, new_sl_price: float):
        if not self.cfg.live_mode:
            if pos and pos.bracket:
                pos.bracket.sl_price = new_sl_price
            return
        if not pos or not pos.bracket:
            return
        close_side = "SELL" if pos.side == "LONG" else "BUY"
        remaining_qty = max(round_down(pos.remaining_qty, self.qty_step), 0.0)
        if remaining_qty < self.min_qty:
            return
        old_id = pos.bracket.sl_order_id
        try:
            res = await self.rest.new_order(close_side, "STOP_MARKET", remaining_qty, stop_price=new_sl_price, reduce_only=True)
        except Exception as exc:
            log.warning("replace stop new failed: %s", exc)
            return
        new_id = int(res["orderId"])
        pos.bracket.sl_order_id = new_id
        pos.bracket.sl_price = new_sl_price
        self.journal.log_order(new_id, "SL", close_side, new_sl_price, remaining_qty, "PLACED", mode="LIVE")
        if old_id and old_id != new_id:
            try:
                await self.rest.cancel_order(old_id)
                self.journal.update_order_status(old_id, "REPLACED")
            except Exception as exc:
                if "-2011" not in str(exc):
                    log.warning("replace stop cancel %s: %s", old_id, exc)

    async def sync_reduce_only_brackets(self, pos: Position):
        if not pos or not pos.bracket:
            return
        remaining_qty = max(round_down(pos.remaining_qty, self.qty_step), 0.0)
        if remaining_qty < self.min_qty:
            return
        if not self.cfg.live_mode:
            return
        tp1_qty, tp2_qty, _ = self._split_qty(remaining_qty)
        close_side = "SELL" if pos.side == "LONG" else "BUY"
        existing_tp_ids = list(pos.bracket.tp_order_ids)
        for oid in existing_tp_ids:
            if not oid:
                continue
            try:
                await self.rest.cancel_order(oid)
                self.journal.update_order_status(oid, "REPLACED")
            except Exception as exc:
                if "-2011" not in str(exc):
                    log.warning("sync tp cancel %s: %s", oid, exc)
        pos.bracket.tp_order_ids = []
        tp_prices = list(pos.bracket.tp_prices[:2])
        while len(tp_prices) < 2:
            tp_prices.append(pos.entry_price)
        for qty, price, label in ((tp1_qty, tp_prices[0], "TP1"), (tp2_qty, tp_prices[1], "TP2")):
            if qty < self.min_qty:
                continue
            try:
                res = await self.rest.new_order(close_side, "TAKE_PROFIT_MARKET", qty, stop_price=price, reduce_only=True)
                oid = int(res["orderId"])
                pos.bracket.tp_order_ids.append(oid)
                self.journal.log_order(oid, label, close_side, price, qty, "PLACED", mode="LIVE")
            except Exception as exc:
                log.warning("sync %s failed: %s", label, exc)
        if pos.bracket.sl_price > 0:
            await self._replace_stop_order(pos, pos.bracket.sl_price)

    async def place_bracket_orders(self, pos: Position):
        if not self.cfg.live_mode:
            return
        sl_price, tp1_price, tp2_price = self.bracket_prices(pos)
        close_side = "SELL" if pos.side == "LONG" else "BUY"
        tp1_qty, tp2_qty, _runner_qty = self._split_qty(pos.qty)
        sl_id = 0
        tp_ids: List[int] = []
        try:
            res = await self.rest.new_order(close_side, "STOP_MARKET", pos.qty, stop_price=sl_price, reduce_only=True)
            sl_id = int(res["orderId"])
            self.journal.log_order(sl_id, "SL", close_side, sl_price, pos.qty, "PLACED", mode="LIVE")
        except Exception as exc:
            log.error("STOP_MARKET failed: %s — position ouverte sans stop !", exc)
        for qty, price, label in ((tp1_qty, tp1_price, "TP1"), (tp2_qty, tp2_price, "TP2")):
            if qty < self.min_qty:
                continue
            try:
                res = await self.rest.new_order(close_side, "TAKE_PROFIT_MARKET", qty, stop_price=price, reduce_only=True)
                oid = int(res["orderId"])
                tp_ids.append(oid)
                self.journal.log_order(oid, label, close_side, price, qty, "PLACED", mode="LIVE")
            except Exception as exc:
                log.error("%s failed: %s", label, exc)
        pos.bracket = BracketOrders(sl_id=sl_id, tp_order_ids=tp_ids, sl_price=sl_price, tp_prices=[tp1_price, tp2_price])

    async def cancel_bracket_orders(self, pos: Position):
        if not self.cfg.live_mode or not pos.bracket:
            return
        ids = [pos.bracket.sl_order_id] + list(pos.bracket.tp_order_ids)
        for oid in ids:
            if not oid:
                continue
            try:
                await self.rest.cancel_order(oid)
                self.journal.update_order_status(oid, "CANCELLED")
            except Exception as exc:
                if "-2011" not in str(exc):
                    log.warning("cancel %s: %s", oid, exc)

    async def reconcile(self):
        if not self.cfg.live_mode:
            return
        try:
            risks = await self.rest.position_risk()
            orders = await self.rest.open_orders()
            self._reconcile_error_count = 0
        except Exception as exc:
            self._reconcile_error_count += 1
            log.error("reconcile impossible (%d): %s", self._reconcile_error_count, exc)
            return

        pos_data = next((r for r in risks if abs(float(r.get("positionAmt", 0))) > 1e-9), None)

        # ── Aucune position exchange ──────────────────────────────────────────
        if pos_data is None:
            if self.position is not None:
                log.warning("reconcile: position locale %s %.4f sans contrepartie exchange — reset",
                            self.position.side, self.position.remaining_qty)
                self.position = None
            if orders:
                try:
                    await self.rest.cancel_all_orders()
                except Exception as exc:
                    log.error("reconcile cancel_all: %s", exc)
            return

        # ── Vérité exchange ───────────────────────────────────────────────────
        amt = float(pos_data["positionAmt"])
        ex_side = "LONG" if amt > 0 else "SHORT"
        ex_entry = float(pos_data["entryPrice"])
        ex_qty = abs(amt)
        local = self.position

        # Détecter une divergence nécessitant un reset autoritaire
        side_ok = local is not None and local.side == ex_side
        qty_ok = local is not None and abs(local.remaining_qty - ex_qty) <= self.qty_step * 2

        if not side_ok or not qty_ok:
            if local is not None:
                log.warning(
                    "reconcile: divergence locale(%s %.4f) vs exchange(%s %.4f) — reset autoritaire",
                    local.side, local.remaining_qty, ex_side, ex_qty,
                )
            # Préserver les flags TP uniquement si même côté ET qty cohérente
            preserve = side_ok and qty_ok
            self.position = Position(
                side=ex_side,
                entry_price=ex_entry,
                qty=ex_qty,
                remaining_qty=ex_qty,
                opened_at=local.opened_at if preserve else time.time(),
                peak_price=local.peak_price if preserve else ex_entry,
                trough_price=local.trough_price if preserve else ex_entry,
                tp1_done=local.tp1_done if preserve else False,
                tp2_done=local.tp2_done if preserve else False,
                runner_active=local.runner_active if preserve else False,
                signal_mid=local.signal_mid if preserve else 0.0,
                state=PosState.OPEN,
            )
        else:
            # Resynchroniser le prix d'entrée (peut dériver après partials)
            self.position.entry_price = ex_entry

        # ── Brackets ─────────────────────────────────────────────────────────
        close_side = "SELL" if ex_side == "LONG" else "BUY"
        sl_order = next((o for o in orders
                         if o["type"] == "STOP_MARKET"
                         and o.get("reduceOnly")
                         and o["side"] == close_side), None)
        tp_orders = [o for o in orders
                     if o["type"] == "TAKE_PROFIT_MARKET"
                     and o.get("reduceOnly")
                     and o["side"] == close_side]
        if sl_order or tp_orders:
            existing_sl_filled = (self.position.bracket.sl_filled_qty
                                  if self.position.bracket else 0.0)
            self.position.bracket = BracketOrders(
                sl_order_id=int(sl_order["orderId"]) if sl_order else 0,
                tp_order_ids=[int(o["orderId"]) for o in tp_orders],
                sl_price=float(sl_order["stopPrice"]) if sl_order else 0.0,
                tp_prices=[float(o["stopPrice"]) for o in tp_orders],
                sl_filled_qty=existing_sl_filled,
            )
        else:
            await self.place_bracket_orders(self.position)

    async def _close_partial(self, pos: Position, price: float, qty: float, reason: str):
        if qty < self.min_qty or pos.remaining_qty < self.min_qty:
            return
        qty = min(qty, pos.remaining_qty)
        exchange_side = "SELL" if pos.side == "LONG" else "BUY"
        exit_price = price
        oid = 0
        if self.cfg.live_mode:
            res = await self.rest.new_order(exchange_side, "MARKET", qty, reduce_only=True)
            exit_price = float(res.get("avgPrice") or res.get("price") or price)
            qty = float(res.get("executedQty") or qty)
            oid = int(res.get("orderId", 0))
            self.journal.log_order(oid, "PARTIAL_CLOSE", exchange_side, 0.0, qty, "FILLED", exit_price, "LIVE")
        gross_pnl = ((exit_price - pos.entry_price) * qty if pos.side == "LONG" else (pos.entry_price - exit_price) * qty)
        ref = pos.signal_mid if pos.signal_mid else pos.entry_price
        slippage = ((exit_price - ref) / max(ref, 1e-8) * 10000
                    if pos.side == "LONG"
                    else (ref - exit_price) / max(ref, 1e-8) * 10000)
        fees = self.trade_fee_usd(pos.entry_price * qty) + self.trade_fee_usd(exit_price * qty)
        pnl = gross_pnl - fees
        self.fees_paid += fees
        self.realized_pnl += pnl
        self.record_setup_pnl(pos.setup_name, pnl)
        self.register_trade_result(pnl)
        self.last_exit_reason = reason   # V22.5
        self.last_exit_pnl = pnl
        pos.remaining_qty = max(round_down(pos.remaining_qty - qty, self.qty_step), 0.0)
        self.journal.log_fill(pos.side, exit_price, qty, pnl,
                              "LIVE" if self.cfg.live_mode else "PAPER",
                              reason, oid, slippage_bps=slippage)
        if pos.remaining_qty < self.min_qty:
            self.position = None
            self.last_trade_ts = time.time()
            return
        if pos.bracket:
            await self.sync_reduce_only_brackets(pos)

    async def handle_order_update(self, data: dict):
        self.user_stream_last_alive = time.time()
        order = data.get("o", {})
        status = order.get("X")
        oid = int(order.get("i", 0))
        qty_filled = float(order.get("z", 0))   # cumulatif Binance
        avg_price = float(order.get("ap", 0) or order.get("L", 0))
        reduce_only = order.get("R", False)
        if status not in ("FILLED", "PARTIALLY_FILLED"):
            if status in ("CANCELED", "REJECTED", "EXPIRED") and oid:
                self.journal.update_order_status(oid, status)
            return

        # ── Entry fill ───────────────────────────────────────────────────────
        if not reduce_only and self.position and self.position.state == PosState.PENDING_ENTRY:
            self.position.entry_price = avg_price
            self.position.qty = qty_filled
            self.position.remaining_qty = qty_filled
            self.position.entry_order_id = oid
            self.position.state = PosState.OPEN
            self.position.peak_price = avg_price
            self.position.trough_price = avg_price
            self.journal.log_order(oid, "ENTRY", order.get("S", ""), 0.0, qty_filled, "FILLED", avg_price, "LIVE")
            await self.place_bracket_orders(self.position)
            return

        if not reduce_only or not self.position or not self.position.bracket:
            return
        pos = self.position

        # ── SL fill (partiel ou total) ────────────────────────────────────────
        if oid == pos.bracket.sl_order_id:
            exit_price = avg_price if avg_price else pos.bracket.sl_price
            # qty_filled est CUMULATIF — calculer l'incrément depuis le dernier event
            incremental = max(round_down(qty_filled - pos.bracket.sl_filled_qty, self.qty_step), 0.0)
            if incremental >= self.min_qty:
                pnl = ((exit_price - pos.entry_price) * incremental
                       if pos.side == "LONG"
                       else (pos.entry_price - exit_price) * incremental)
                slippage = ((exit_price - pos.bracket.sl_price) / max(pos.bracket.sl_price, 1e-8) * 10000
                            if pos.side == "LONG"
                            else (pos.bracket.sl_price - exit_price) / max(pos.bracket.sl_price, 1e-8) * 10000)
                self.realized_pnl += pnl
                self.record_setup_pnl(pos.setup_name, pnl)
                pos.remaining_qty = max(round_down(pos.remaining_qty - incremental, self.qty_step), 0.0)
                pos.bracket.sl_filled_qty = qty_filled
                fill_reason = "SL_PARTIAL" if status == "PARTIALLY_FILLED" else "SL"
                self.journal.update_order_status(oid, status, exit_price)
                self.journal.log_fill(pos.side, exit_price, incremental, pnl,
                                      "LIVE", fill_reason, oid, slippage_bps=slippage)

            if status == "PARTIALLY_FILLED":
                # Position toujours ouverte, attendre le FILLED final
                return

            # FILLED : clôture complète
            for other_id in pos.bracket.tp_order_ids:
                try:
                    await self.rest.cancel_order(other_id)
                    self.journal.update_order_status(other_id, "CANCELLED")
                except Exception:
                    pass
            self.position = None
            self.last_trade_ts = time.time()
            return

        # ── TP fill ───────────────────────────────────────────────────────────
        if oid in pos.bracket.tp_order_ids:
            tp_idx = pos.bracket.tp_order_ids.index(oid) if oid in pos.bracket.tp_order_ids else 0
            tp_ref = pos.bracket.tp_prices[tp_idx] if tp_idx < len(pos.bracket.tp_prices) else pos.entry_price
            exit_price = avg_price if avg_price else tp_ref
            prev_qty = pos.remaining_qty
            qty = min(qty_filled if qty_filled else prev_qty, prev_qty)
            gross_pnl = ((exit_price - pos.entry_price) * qty
                         if pos.side == "LONG"
                         else (pos.entry_price - exit_price) * qty)
            slippage = ((tp_ref - exit_price) / max(tp_ref, 1e-8) * 10000
                        if pos.side == "LONG"
                        else (exit_price - tp_ref) / max(tp_ref, 1e-8) * 10000)
            fees = self.trade_fee_usd(pos.entry_price * qty) + self.trade_fee_usd(exit_price * qty)
            pnl = gross_pnl - fees
            self.fees_paid += fees
            self.realized_pnl += pnl
            self.record_setup_pnl(pos.setup_name, pnl)
            pos.remaining_qty = max(round_down(prev_qty - qty, self.qty_step), 0.0)
            self.journal.update_order_status(oid, "FILLED", exit_price)
            label = "TP1" if not pos.tp1_done else "TP2"
            self.journal.log_fill(pos.side, exit_price, qty, pnl, "LIVE", label, oid, slippage_bps=slippage)
            if not pos.tp1_done:
                pos.tp1_done = True
            elif not pos.tp2_done:
                pos.tp2_done = True
            if pos.remaining_qty < self.min_qty:
                if pos.bracket.sl_order_id:
                    try:
                        await self.rest.cancel_order(pos.bracket.sl_order_id)
                    except Exception:
                        pass
                self.register_trade_result(pnl)
                self.position = None
                self.last_trade_ts = time.time()

    def can_enter(self) -> bool:
        if self.position is not None:
            return False
        now = time.time()
        if now < self.pause_until_ts:
            return False
        cooldown = self.cfg.burst_cooldown_seconds if self.burst_mode(None) else self._adaptive_cooldown()  # V22.5
        if now - self.last_trade_ts < cooldown:
            return False
        if self.daily_loss_pct() >= self.cfg.kill_switch_daily_loss_pct:
            return False
        if self.trade_count_day >= self.cfg.max_daily_trades:
            return False
        if self.recent_trade_count() > self.cfg.trade_freq_hard_cap:
            return False
        return True

    def execution_guard(self, side: str, state: FlowState) -> Tuple[bool, str]:
        """Phase 1 CONTRARIAN — 3 safety checks only.
        We WANT to enter against momentum, so no trend_adverse check.
        """
        # 1. Spread too wide → bad execution (critical for maker entry)
        if state.spread_bps > 4.0:
            return False, "spread"
        # 2. No volatility → no mean reversion possible
        if state.realized_vol_bps < self.cfg.realized_vol_min_bps:
            return False, "vol"
        # 3. Micro move too large → flash crash/pump, don't catch falling knife
        if abs(state.micro_move_1s_bps) > self.cfg.micro_move_hard_limit_bps:
            return False, "micro_move"
        return True, ""

    def register_entry_signal(self, side: str) -> int:
        now = time.time()
        signal_window = 1.2 if self.burst_mode(None) else self.cfg.entry_signal_window_seconds  # [V22] 2.0→3.0
        if side == self.entry_signal_side and now - self.entry_signal_ts <= signal_window:
            self.entry_signal_count += 1
        else:
            self.entry_signal_side = side
            self.entry_signal_count = 1
        self.entry_signal_ts = now
        return self.entry_signal_count

    def reset_entry_signal(self):
        self.entry_signal_side = ""
        self.entry_signal_count = 0
        self.entry_signal_ts = 0.0

    async def open_position(self, side: str, price: float, state: FlowState, reason: str):
        qty = self.position_size(price, state)
        if qty < self.min_qty:
            return
        exchange_side = "BUY" if side == "LONG" else "SELL"
        self.position = Position(
            side=side, entry_price=price, qty=qty, remaining_qty=qty,
            opened_at=time.time(), state=PosState.PENDING_ENTRY,
            peak_price=price, trough_price=price,
            signal_mid=state.mid,
            setup_name=state.setup_name,
        )
        self.last_trade_ts = time.time()
        self.register_new_entry(side)
        if self.cfg.live_mode:
            # Phase1 CONTRARIAN: Use LIMIT GTX (post-only) for maker fees.
            # Place limit at best bid (for BUY) or best ask (for SELL).
            # If it would cross the spread, GTX rejects it → we retry next tick.
            limit_price = state.best_bid if side == "LONG" else state.best_ask
            limit_price = round_to_tick(limit_price, self.tick_size)
            try:
                res = await self.rest.new_order(
                    exchange_side, "LIMIT", qty,
                    price=limit_price, time_in_force="GTX",
                )
                status = res.get("status", "")
                if status == "EXPIRED":
                    # GTX rejected: would have been taker. Skip this entry.
                    log.info("LIMIT GTX rejected (would cross) — skipping entry")
                    self.position = None
                    return
                fill_price = float(res.get("avgPrice") or res.get("price") or limit_price)
                fill_qty = float(res.get("executedQty") or qty)
                oid = int(res.get("orderId", 0))
                if fill_qty <= 0:
                    # Order placed but not filled yet — wait for fill via user stream
                    self.position.entry_order_id = oid
                    self.position.entry_price = limit_price
                    log.info("LIMIT entry placed oid=%d price=%.2f qty=%.4f — awaiting fill",
                             oid, limit_price, qty)
                    # Cancel after 2s if not filled
                    asyncio.get_event_loop().call_later(
                        2.0, lambda: asyncio.ensure_future(self._cancel_unfilled_entry(oid))
                    )
                    return
            except Exception as exc:
                log.error("open_position LIMIT failed: %s — falling back to MARKET", exc)
                # Fallback to MARKET if LIMIT fails
                try:
                    res = await self.rest.new_order(exchange_side, "MARKET", qty)
                    fill_price = float(res.get("avgPrice") or res.get("price") or price)
                    fill_qty = float(res.get("executedQty") or qty)
                    oid = int(res.get("orderId", 0))
                except Exception as exc2:
                    log.error("open_position MARKET fallback failed: %s", exc2)
                    self.position = None
                    return
            self.position.entry_price = fill_price
            self.position.qty = fill_qty
            self.position.remaining_qty = fill_qty
            self.position.entry_order_id = oid
            self.position.state = PosState.OPEN
            self.position.peak_price = fill_price
            self.position.trough_price = fill_price
            self.journal.log_order(oid, "ENTRY", exchange_side, 0.0, fill_qty, "FILLED", fill_price, "LIVE")
            await self.place_bracket_orders(self.position)
        else:
            self.position.state = PosState.OPEN
            sl, tp1, tp2 = self.bracket_prices(self.position)
            self.position.bracket = BracketOrders(0, [], sl, [tp1, tp2])

    async def _cancel_unfilled_entry(self, order_id: int):
        """Cancel a LIMIT entry order if still unfilled after timeout."""
        try:
            if self.position and self.position.state == PosState.PENDING_ENTRY and self.position.entry_order_id == order_id:
                await self.rest.cancel_order(order_id)
                log.info("Cancelled unfilled LIMIT entry oid=%d", order_id)
                self.position = None
        except Exception as exc:
            log.debug("cancel_unfilled_entry oid=%d: %s", order_id, exc)

    async def close_position(self, price: float, reason: str):
        async with self._close_lock:
            pos = self.position
            if not pos or pos.state == PosState.PENDING_CLOSE:
                return
            pos.state = PosState.PENDING_CLOSE
            await self.cancel_bracket_orders(pos)
            exchange_side = "SELL" if pos.side == "LONG" else "BUY"
            exit_price = price
            qty = pos.remaining_qty or pos.qty
            oid = 0
            if self.cfg.live_mode:
                try:
                    res = await self.rest.new_order(exchange_side, "MARKET", qty, reduce_only=True)
                    exit_price = float(res.get("avgPrice") or res.get("price") or price)
                    qty = float(res.get("executedQty") or qty)
                    oid = int(res.get("orderId", 0))
                    self.journal.log_order(oid, "CLOSE", exchange_side, 0.0, qty, "FILLED", exit_price, "LIVE")
                except Exception as exc:
                    log.error("close_position failed: %s", exc)
                    pos.state = PosState.OPEN
                    await self.reconcile()
                    return
            gross_pnl = ((exit_price - pos.entry_price) * qty if pos.side == "LONG" else (pos.entry_price - exit_price) * qty)
            ref = pos.signal_mid if pos.signal_mid else pos.entry_price
            slippage = ((exit_price - ref) / max(ref, 1e-8) * 10000
                        if pos.side == "LONG"
                        else (ref - exit_price) / max(ref, 1e-8) * 10000)
            fees = self.trade_fee_usd(pos.entry_price * qty) + self.trade_fee_usd(exit_price * qty)
            pnl = gross_pnl - fees
            self.fees_paid += fees
            self.realized_pnl += pnl
            self.record_setup_pnl(pos.setup_name, pnl)
            self.register_trade_result(pnl)
            self.last_exit_reason = reason   # V22.5 : tracking pour cooldown adaptatif
            self.last_exit_pnl = pnl
            self.journal.log_fill(pos.side, exit_price, qty, pnl,
                                  "LIVE" if self.cfg.live_mode else "PAPER",
                                  reason, oid, slippage_bps=slippage)
            self.position = None
            self.last_trade_ts = time.time()

    async def _manage_partials_and_trailing(self, state: FlowState):
        pos = self.position
        if not pos or pos.state != PosState.OPEN or not pos.bracket:
            return
        price = state.mid
        sl_price = pos.bracket.sl_price
        tp1_price = pos.bracket.tp_prices[0] if pos.bracket.tp_prices else pos.entry_price
        tp2_price = pos.bracket.tp_prices[1] if len(pos.bracket.tp_prices) > 1 else tp1_price
        tp1_qty, tp2_qty, runner_qty = self._split_qty(pos.qty)

        if pos.side == "LONG":
            pos.peak_price = max(pos.peak_price, price)
            if not pos.tp1_done and price >= tp1_price:
                await self._close_partial(pos, price, tp1_qty or pos.remaining_qty * 0.4, "TP1")
                pos.tp1_done = True
                if self.position and self.cfg.breakeven_after_tp1:
                    be_buffer = pos.entry_price * self.cfg.breakeven_buffer_bps / 10000.0
                    new_sl = max(pos.entry_price + be_buffer, sl_price)
                    if new_sl > pos.bracket.sl_price:
                        await self._replace_stop_order(pos, new_sl)
            if self.position and not pos.tp2_done and price >= tp2_price:
                await self._close_partial(pos, price, tp2_qty or max(pos.remaining_qty - runner_qty, 0.0), "TP2")
                if self.position:
                    pos.tp2_done = True
                    pos.runner_active = True
            if self.position and pos.runner_active and runner_qty > 0:
                trail = round_to_tick(pos.peak_price * (1 - self.cfg.trailing_stop_pct / 100.0), self.tick_size)
                if trail > pos.bracket.sl_price:
                    await self._replace_stop_order(pos, trail)
                if price <= pos.bracket.sl_price:
                    await self.close_position(price, "runner trail")
        else:
            pos.trough_price = min(pos.trough_price, price)
            if not pos.tp1_done and price <= tp1_price:
                await self._close_partial(pos, price, tp1_qty or pos.remaining_qty * 0.4, "TP1")
                pos.tp1_done = True
                if self.position and self.cfg.breakeven_after_tp1:
                    be_buffer = pos.entry_price * self.cfg.breakeven_buffer_bps / 10000.0
                    new_sl = min(pos.entry_price - be_buffer, sl_price)
                    if new_sl < pos.bracket.sl_price or pos.bracket.sl_price <= 0:
                        await self._replace_stop_order(pos, new_sl)
            if self.position and not pos.tp2_done and price <= tp2_price:
                await self._close_partial(pos, price, tp2_qty or max(pos.remaining_qty - runner_qty, 0.0), "TP2")
                if self.position:
                    pos.tp2_done = True
                    pos.runner_active = True
            if self.position and pos.runner_active and runner_qty > 0:
                trail = round_to_tick(pos.trough_price * (1 + self.cfg.trailing_stop_pct / 100.0), self.tick_size)
                if trail < pos.bracket.sl_price or pos.bracket.sl_price <= 0:
                    await self._replace_stop_order(pos, trail)
                if price >= pos.bracket.sl_price:
                    await self.close_position(price, "runner trail")

    async def _fast_fail_exit(self, state: FlowState) -> bool:
        """Phase 1 CONTRARIAN — Fast fail DISABLED.
        In mean-reversion, the initial move AGAINST us is expected.
        We wait for the reversion. SL handles max risk.
        """
        return False

    async def on_state(self, state: FlowState):
        if not state.mid:
            return
        if self.position is None:
            if self.can_enter() and state.signal in ("LONG", "SHORT"):
                # Phase1 CONTRARIAN: signal is already flipped in build_state
                ok, reason = self.execution_guard(state.signal, state)
                if ok:
                    seen = self.register_entry_signal(state.signal)
                    needed = self.cfg.entry_revalidate_cycles  # Phase1: fixed 2 cycles
                    if seen >= needed:
                        self.reset_entry_signal()
                        log.info(
                            "ENTRY CONTRARIAN %s | score=%.2f (fading) "
                            "regime=%s cvd5=%.1f trend=%.1f vol=%.1f spread=%.2f",
                            state.signal,
                            state.calibrated_score,
                            state.regime, state.cvd_delta_5s,
                            state.trend_bps, state.realized_vol_bps,
                            state.spread_bps,
                        )
                        await self.open_position(
                            state.signal, state.mid, state,
                            f"contrarian {state.signal.lower()} [fade {state.regime.lower()}]",
                        )
                    else:
                        log.debug(
                            "CONFIRM %s %d/%d | score=%.2f regime=%s",
                            state.signal, seen, needed,
                            state.calibrated_score, state.regime,
                        )
                else:
                    self.reset_entry_signal()
                    log.info(
                        "BLOCKED %s | reason=%s score=%.2f "
                        "regime=%s spread=%.2f vol=%.1f trend=%.2f",
                        state.signal, reason,
                        state.calibrated_score,
                        state.regime, state.spread_bps,
                        state.realized_vol_bps, state.trend_bps,
                    )
            else:
                if state.signal == "FLAT" and state.raw_score != 0.0:
                    # Log pourquoi le signal est FLAT (filtre pré-entrée)
                    block_reasons = []
                    if not state.regime_ok:
                        block_reasons.append(f"regime={state.regime}")
                    if not state.trend_ok:
                        block_reasons.append(f"trend_adverse={state.trend_bps:.1f}bps")
                    if not state.filters_pass:
                        fe = "fe=Y" if state.flow_expanding else "fe=N"
                        ct = f"ctx={state.context_reason or 'none'}"
                        block_reasons.append(f"filters({fe},{ct})")
                    if block_reasons:
                        log.debug(
                            "FLAT score=%.3f cal=%.3f thr=%.3f | %s",
                            state.score, state.calibrated_score,
                            state.adaptive_threshold, ", ".join(block_reasons),
                        )
                self.reset_entry_signal()
            return

        pos = self.position
        if pos.state != PosState.OPEN:
            return

        if await self._fast_fail_exit(state):
            return

        await self._manage_partials_and_trailing(state)
        if self.position is None:
            return
        pos = self.position

        # Phase1 CONTRARIAN: Simple exits — SL, TP (via partials), time stop.
        # No early exit. No flow flip. We WAIT for the reversion.
        hold_limit = self.cfg.max_position_hold_seconds  # 300s
        if time.time() - pos.opened_at >= hold_limit:
            await self.close_position(state.mid, "time stop")
            return

        if time.time() - self.user_stream_last_alive < self.cfg.soft_stop_fallback_after:
            return
        sl = pos.bracket.sl_price if pos.bracket else pos.entry_price * (1 - self.cfg.stop_loss_pct / 100 if pos.side == "LONG" else 1 + self.cfg.stop_loss_pct / 100)
        if pos.side == "LONG" and state.mid <= sl:
            await self.close_position(state.mid, "soft stop [fallback]")
        elif pos.side == "SHORT" and state.mid >= sl:
            await self.close_position(state.mid, "soft stop [fallback]")


class Au2Bot:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.stop_event = asyncio.Event()
        self.session: Optional[aiohttp.ClientSession] = None
        self.rest: Optional[BinanceREST] = None
        self.journal = Journal(cfg.db_path)
        self.flow = FlowEngine(cfg)
        self.trader: Optional[Trader] = None
        self.listen_key: Optional[str] = None
        self._last_signal_log_ts = 0.0
        self._last_logged_signal = ""
        self._ws_lag_ms: float = 0.0
        # "latest state" channel : le WS dépose, strategy_loop consomme
        self._pending_state: Optional[FlowState] = None
        self._state_ready: asyncio.Event = asyncio.Event()
        # Runtime stability counters
        self._start_ts: float = time.time()
        self._error_count: int = 0
        self._restart_count: int = 0
        self._kill_switch_active: bool = False

    async def setup(self):
        logging.basicConfig(
            level=getattr(logging, self.cfg.log_level.upper(), logging.INFO),
            format="%(asctime)s | %(levelname)s | %(message)s",
        )
        self.session = aiohttp.ClientSession()
        self.rest = BinanceREST(self.cfg, self.session)
        self.trader = Trader(self.cfg, self.rest, self.journal)
        self.journal.start()
        self.trader.restore_daily_state()

        # Retry exchange_info with backoff
        for attempt in range(1, 11):
            try:
                info = await self.rest.exchange_info()
                self.trader.update_symbol_filters(info)
                break
            except Exception as exc:
                log.warning("setup exchange_info attempt %d/10: %s", attempt, exc)
                if attempt == 10:
                    raise
                await asyncio.sleep(min(2 ** attempt, 30))

        # Retry depth snapshot with backoff
        for attempt in range(1, 11):
            try:
                snap = await self.rest.depth_snapshot()
                self.flow.book.load_snapshot(snap)
                break
            except Exception as exc:
                log.warning("setup depth_snapshot attempt %d/10: %s", attempt, exc)
                if attempt == 10:
                    raise
                await asyncio.sleep(min(2 ** attempt, 30))

        if self.cfg.live_mode:
            # Retry leverage + reconcile with backoff
            for attempt in range(1, 11):
                try:
                    await self.rest.change_leverage()
                    await self.trader.reconcile()
                    break
                except Exception as exc:
                    log.warning("setup leverage/reconcile attempt %d/10: %s", attempt, exc)
                    if attempt == 10:
                        raise
                    await asyncio.sleep(min(2 ** attempt, 30))

        log.info("Au2 V22.5 démarré symbol=%s live=%s", self.cfg.symbol, self.cfg.live_mode)

    # ── Polls ─────────────────────────────────────────────────────────────────

    async def poll_open_interest(self):
        assert self.rest
        while not self.stop_event.is_set():
            try:
                res = await self.rest.open_interest()
                self.flow.on_open_interest(float(res["openInterest"]))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("OI poll: %s", exc, exc_info=True)
            await asyncio.sleep(self.cfg.oi_poll_seconds)

    async def poll_reconcile(self):
        assert self.trader
        while not self.stop_event.is_set():
            try:
                if self.cfg.live_mode:
                    await self.trader.reconcile()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("reconcile poll: %s", exc, exc_info=True)
            await asyncio.sleep(self.cfg.reconcile_poll_seconds)

    async def poll_account(self):
        assert self.rest and self.trader
        while not self.stop_event.is_set():
            try:
                if self.cfg.live_mode:
                    acct = await self.rest.account()
                    equity = float(acct["totalWalletBalance"])
                    avail = float(acct["availableBalance"])
                else:
                    px = self.flow.last_trade_price or 0.0
                    unreal = 0.0
                    if self.trader.position and px:
                        p = self.trader.position
                        unreal = ((px - p.entry_price) * p.remaining_qty if p.side == "LONG"
                                  else (p.entry_price - px) * p.remaining_qty)
                    equity = max(self.cfg.max_notional_usd + self.trader.realized_pnl + unreal, 1.0)
                    avail = self.cfg.max_notional_usd + self.trader.realized_pnl
                self.trader.update_account_snapshot(equity, avail)
                unreal = 0.0
                if self.trader.position and self.flow.last_trade_price:
                    p = self.trader.position
                    lp = self.flow.last_trade_price
                    unreal = ((lp - p.entry_price) * p.remaining_qty if p.side == "LONG"
                              else (p.entry_price - lp) * p.remaining_qty)
                self.journal.log_equity(equity, self.trader.realized_pnl, unreal, self.trader.daily_loss_pct())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("account poll: %s", exc, exc_info=True)
            await asyncio.sleep(self.cfg.account_poll_seconds)

    async def keepalive_user_stream(self):
        assert self.rest
        while not self.stop_event.is_set():
            try:
                if self.cfg.live_mode and self.listen_key:
                    await self.rest.keepalive_listen_key(self.listen_key)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("keepalive: %s", exc, exc_info=True)
            await asyncio.sleep(30 * 60)  # 30 min — Binance expire à 60 min

    # ── WebSockets ────────────────────────────────────────────────────────────

    async def run_user_stream(self):
        assert self.rest and self.trader
        if not self.cfg.live_mode:
            await self.stop_event.wait()  # stay alive, don't exit
            return
        while not self.stop_event.is_set():
            try:
                lk = await self.rest.new_listen_key()
                self.listen_key = lk["listenKey"]
                ws_host = self.cfg.ws_base.replace("/stream", "")
                url = f"{ws_host}/ws/{self.listen_key}"
                async with websockets.connect(url, ping_interval=60, ping_timeout=40, max_size=None) as ws:
                    await self.trader.reconcile()
                    self.trader.user_stream_last_alive = time.time()
                    while not self.stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
                        except asyncio.TimeoutError:
                            log.warning("user stream: timeout recv — reconnexion")
                            break
                        data = json.loads(raw)
                        evt = data.get("e")
                        self.trader.user_stream_last_alive = time.time()
                        if evt == "listenKeyExpired":
                            log.warning("listenKey expiré — reconnexion user stream")
                            break
                        elif evt == "ORDER_TRADE_UPDATE":
                            await self.trader.handle_order_update(data)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._error_count += 1
                log.error("user stream error (#%d): %s", self._error_count, exc, exc_info=True)
                await asyncio.sleep(3)

    async def run_market_stream(self):
        """Ingestion pure : données → FlowEngine → _pending_state. Pas de logique métier."""
        assert self.trader
        sym = self.cfg.symbol.lower()
        streams = "/".join([
            f"{sym}@aggTrade",
            f"{sym}@depth@100ms",
            f"{sym}@forceOrder",
            f"{sym}@markPrice@1s",
        ])
        url = f"{self.cfg.ws_base}?streams={streams}"
        _last_snapshot_ts: float = 0.0
        _SNAPSHOT_COOLDOWN = 10.0  # min 10s entre deux depth_snapshot REST
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(url, ping_interval=60, ping_timeout=40, max_size=None) as ws:
                    log.info("market stream connecté")
                    while not self.stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            log.warning("market stream: timeout recv — reconnexion")
                            break
                        recv_ts = time.time()
                        msg = json.loads(raw)
                        stream = msg.get("stream", "")
                        data = msg.get("data", {})

                        # Mesure lag WS — filtre le burst initial Binance (timestamps > 60s dans le passé)
                        event_ts_ms = float(data.get("E") or 0.0)
                        if event_ts_ms > 0:
                            lag = recv_ts * 1000.0 - event_ts_ms
                            if 0 <= lag < 60_000:
                                self._ws_lag_ms = lag
                                if self.cfg.max_ws_lag_ms > 0 and lag > self.cfg.max_ws_lag_ms:
                                    log.warning("ws_lag élevé: %.0f ms", lag)

                        if stream.endswith("@aggTrade"):
                            self.flow.on_trade(float(data["p"]), float(data["q"]), bool(data["m"]))
                        elif "@depth" in stream:
                            if not self.flow.book.apply_diff(data):
                                now = time.time()
                                if now - _last_snapshot_ts >= _SNAPSHOT_COOLDOWN:
                                    assert self.rest
                                    log.info("book désync — resync snapshot")
                                    try:
                                        self.flow.book.load_snapshot(await self.rest.depth_snapshot())
                                    except Exception as exc:
                                        log.warning("depth_snapshot: %s", exc)
                                    _last_snapshot_ts = time.time()
                                continue
                        elif stream.endswith("@forceOrder"):
                            o = data.get("o", {})
                            self.flow.on_liquidation(float(o.get("q", 0)), o.get("S", ""))
                        elif stream.endswith("@markPrice@1s"):
                            self.flow.on_funding(float(data.get("r", 0)))

                        # Déposer le state courant pour strategy_loop
                        self._pending_state = self.flow.build_state()
                        self._state_ready.set()

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._error_count += 1
                log.error("market stream error (#%d): %s", self._error_count, exc, exc_info=True)
                await asyncio.sleep(3)

    # ── Heartbeat ──────────────────────────────────────────────────────────────

    async def heartbeat_loop(self):
        """Periodic heartbeat log — proves the bot is alive."""
        interval = 300  # every 5 minutes
        while not self.stop_event.is_set():
            await asyncio.sleep(interval)
            uptime_s = time.time() - self._start_ts
            uptime_h = uptime_s / 3600
            pos_info = "FLAT"
            if self.trader and self.trader.position:
                p = self.trader.position
                pos_info = f"{p.side} qty={p.remaining_qty}"
            log.info(
                "HEARTBEAT | uptime=%.1fh errors=%d kill_switch=%s pos=%s",
                uptime_h, self._error_count, self._kill_switch_active, pos_info,
            )

    # ── Boucle stratégie ──────────────────────────────────────────────────────

    def _should_log_signal(self, state: FlowState) -> bool:
        now = time.time()
        if state.signal != self._last_logged_signal or now - self._last_signal_log_ts >= self.cfg.signal_log_interval:
            self._last_signal_log_ts = now
            self._last_logged_signal = state.signal
            return True
        return False

    async def strategy_loop(self):
        """Consomme le dernier FlowState produit par run_market_stream.
        Toute la logique métier (kill switch, signal log, on_state) est ici."""
        assert self.trader
        while not self.stop_event.is_set():
            # Attendre un nouveau state (timeout pour rester interruptible)
            try:
                await asyncio.wait_for(self._state_ready.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise

            self._state_ready.clear()
            state = self._pending_state
            if state is None:
                continue

            try:
                # Log lag si anormal
                if self._ws_lag_ms > self.cfg.max_ws_lag_ms:
                    log.warning("ws_lag élevé: %.0f ms", self._ws_lag_ms)

                # Kill switch — close position + pause, but do NOT stop the bot
                if self.trader.daily_loss_pct() >= self.cfg.kill_switch_daily_loss_pct:
                    if not self._kill_switch_active:
                        self._kill_switch_active = True
                        log.warning(
                            "KILL SWITCH déclenché daily_loss=%.2f%% — fermeture position, pause trading.",
                            self.trader.daily_loss_pct(),
                        )
                        if self.trader.position and self.flow.last_trade_price:
                            try:
                                await self.trader.close_position(self.flow.last_trade_price, "kill_switch")
                            except Exception as exc:
                                log.error("kill switch close_position: %s", exc, exc_info=True)
                    # Stay alive but skip trading logic while kill switch active
                    continue

                # Log signal périodique
                if self._should_log_signal(state):
                    self.journal.log_signal(
                        price=state.mid, raw_score=state.raw_score, score=state.score, signal=state.signal,
                        cvd_delta_5s=state.cvd_delta_5s, ofi=state.ofi, imbalance=state.imbalance,
                        liquidation_score=state.liquidation_score, oi_delta_pct=state.oi_delta_pct,
                        absorption=state.absorption, filter_pass=state.filters_pass,
                        flow_expanding=state.flow_expanding, time_ok=state.time_ok,
                        context_ok=state.context_ok, regime_ok=state.regime_ok, trend_ok=state.trend_ok,
                        context_reason=state.context_reason, realized_vol_bps=state.realized_vol_bps,
                        vwap_dev_bps=state.vwap_dev_bps, trend_bps=state.trend_bps,
                        liquidity_shift=state.liquidity_shift, dangerous_time_ok=state.dangerous_time_ok,
                        queue_edge=state.queue_edge, bid_depletion=state.bid_depletion,
                        ask_depletion=state.ask_depletion, bid_rebuild=state.bid_rebuild,
                        ask_rebuild=state.ask_rebuild, setup_name=state.setup_name,
                        calibrated_score=state.calibrated_score,
                    )

                await self.trader.on_state(state)

                log.debug(
                    "px=%.2f raw=%+.2f sc=%+.2f sig=%s vol=%.1f trend=%+.1f vwap=%+.1f "
                    "liqShift=%+.2f ctx=%s flow=%d reg=%d tr=%d lag=%.0fms",
                    state.mid, state.raw_score, state.score, state.signal, state.realized_vol_bps,
                    state.trend_bps, state.vwap_dev_bps, state.liquidity_shift,
                    state.context_reason or "-", int(state.flow_expanding),
                    int(state.regime_ok), int(state.trend_ok), self._ws_lag_ms,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("strategy_loop: %s", exc, exc_info=True)
                self._error_count += 1
                await asyncio.sleep(1)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def shutdown(self, tasks: List[asyncio.Task]):
        """Arrêt propre : flatten optionnel → cancel tasks → flush journal → session report."""
        if self.cfg.close_on_shutdown and self.trader and self.trader.position and self.flow.last_trade_price:
            try:
                await self.trader.close_position(self.flow.last_trade_price, "shutdown")
            except Exception as exc:
                log.error("shutdown flatten: %s", exc)

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        if self.journal._worker_task:
            self.journal._worker_task.cancel()
            try:
                await self.journal._worker_task
            except asyncio.CancelledError:
                pass

        if self.cfg.session_report_on_shutdown:
            rep = self.journal.session_report()
            wr = (rep["wins"] / rep["trades"] * 100.0) if rep["trades"] else 0.0
            log.info("session report | trades=%d wins=%d losses=%d winrate=%.1f%% pnl=%.4f avg=%.4f",
                     rep["trades"], rep["wins"], rep["losses"], wr, rep["pnl"], rep["avg_pnl"])
            if rep["reasons"]:
                log.info("session reasons | %s", " | ".join(f"{r}:{c}:{pnl:.4f}" for r, c, pnl in rep["reasons"]))
            if rep["hours"]:
                log.info("session hours | %s", " | ".join(f"{hh}h:{c}:{pnl:.4f}" for hh, c, pnl in rep["hours"]))
            if rep["contexts"]:
                log.info("session contexts | %s", " | ".join(f"{ctx or '-'}:{c}" for ctx, c in rep["contexts"]))

        if self.session:
            await self.session.close()

    def _make_tasks(self) -> dict:
        """Create all worker tasks. Returns {name: task}."""
        task_defs = [
            ("market", self.run_market_stream),
            ("strategy", self.strategy_loop),
            ("oi", self.poll_open_interest),
            ("acct", self.poll_account),
            ("reconcile", self.poll_reconcile),
            ("keepalive", self.keepalive_user_stream),
            ("user", self.run_user_stream),
            ("heartbeat", self.heartbeat_loop),
        ]
        return {name: asyncio.create_task(coro(), name=name) for name, coro in task_defs}

    async def run(self):
        await self.setup()
        task_map = self._make_tasks()

        # Supervisor loop — restarts dead tasks, never exits until stop_event
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=10.0)
                break  # stop_event was set → shutdown
            except asyncio.TimeoutError:
                pass  # check tasks every 10s

            # Revive any crashed tasks
            for name, task in list(task_map.items()):
                if task.done():
                    exc = task.exception() if not task.cancelled() else None
                    if exc:
                        self._error_count += 1
                        log.error("task %s died (#%d): %s — restarting",
                                  name, self._error_count, exc, exc_info=exc)
                    else:
                        log.warning("task %s exited cleanly — restarting", name)
                    # Re-create the task
                    coro_map = {
                        "market": self.run_market_stream,
                        "strategy": self.strategy_loop,
                        "oi": self.poll_open_interest,
                        "acct": self.poll_account,
                        "reconcile": self.poll_reconcile,
                        "keepalive": self.keepalive_user_stream,
                        "user": self.run_user_stream,
                        "heartbeat": self.heartbeat_loop,
                    }
                    if name in coro_map:
                        task_map[name] = asyncio.create_task(coro_map[name](), name=name)

        await self.shutdown(list(task_map.values()))


async def amain():
    """IMMORTAL entry point — this function must NEVER return.
    The process must only die via SIGKILL or manual `systemctl stop`.
    Every exception, every clean return from run(), triggers a restart."""
    cfg = BotConfig()
    loop = asyncio.get_running_loop()
    restart_count = 0
    _sigterm_received = False

    def _on_signal():
        nonlocal _sigterm_received
        _sigterm_received = True
        bot.stop_event.set()

    while True:
        _sigterm_received = False
        bot = Au2Bot(cfg)
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _on_signal)
            except NotImplementedError:
                pass
        try:
            await bot.run()
        except KeyboardInterrupt:
            log.info("KeyboardInterrupt — but staying alive, restarting in 5s.")
        except BaseException:
            log.error("CRASH in bot.run()", exc_info=True)

        # Cleanup session
        if bot.session and not bot.session.closed:
            try:
                await bot.session.close()
            except Exception:
                pass

        # ONLY exit on explicit SIGTERM/SIGINT (systemctl stop)
        if _sigterm_received:
            log.info("SIGTERM/SIGINT received — exiting process.")
            return

        restart_count += 1
        log.warning("bot.run() ended unexpectedly (#%d) — restarting in 5s.", restart_count)
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(amain())

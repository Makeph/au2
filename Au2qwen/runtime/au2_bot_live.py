#!/usr/bin/env python3
"""AU2QWEN — Paper Trading Runtime (GOAT Mode)
Runtime wrapper around LiveExecutor.
Paper trading only: execute_entry / execute_partial log and do not place real orders.
Architecture:
Au2QwenBot                  — runtime shell (WS feed + task supervisor)
├── run_market_stream()     — Binance aggTrade + markPrice@1s → state dict
├── strategy_loop()         — calls executor.process_tick() on each state
└── heartbeat_loop()        — periodic alive log
PaperExecutor(LiveExecutor) — concrete subclass, paper execution stubs
MarketState                 — minimal rolling-window state builder from WS ticks
Entry: amain() → Au2QwenBot.run() — immortal, only exits on SIGTERM.
"""
from __future__ import annotations
import sys, pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

import asyncio
import json
import logging
import os
import signal
import time
from collections import deque
from typing import Deque, List, Optional, Tuple
import websockets
from dotenv import load_dotenv
from au2_core import PositionFill, Regime, SignalProcessor
from au2_live_executor import LiveExecutor
from au2_config import GOAT_CFG, GOAT_OVERLAY_CFG   # ← single source of truth
load_dotenv()
log = logging.getLogger("au2qwen")

# ── Minimal Market State Builder ───────────────────────────────────────────────
class MarketState:
    """Accumulates aggTrade ticks over a rolling window.
    Produces the state dict expected by LiveExecutor.process_tick():
    ts, mid, regime, cvd_delta_5s, trend_bps, realized_vol_bps, spread_bps
    """
    def __init__(self, window_s: float = 5.0, assume_spread_bps: float = 0.0) -> None:
        self._window_s = window_s
        self._assume_spread_bps = assume_spread_bps  # ✅ M2
        self._trades: Deque[Tuple[float, float, float, bool]] = deque()
        self._prices_30s: Deque[Tuple[float, float]] = deque()

    def on_agg_trade(self, price: float, qty: float, is_buyer_maker: bool, ts: float) -> None:
        self._trades.append((ts, price, qty, is_buyer_maker))
        cutoff = ts - self._window_s
        while self._trades and self._trades[0][0] < cutoff:
            self._trades.popleft()
        self._prices_30s.append((ts, price))
        cutoff_30s = ts - 30.0
        while self._prices_30s and self._prices_30s[0][0] < cutoff_30s:
            self._prices_30s.popleft()
        # Keep a 5-min price window for trend_30s_bps (V3 macro-trend feature)
        if not hasattr(self, "_prices_5m"):
            self._prices_5m: Deque[Tuple[float, float]] = deque()
        self._prices_5m.append((ts, price))
        cutoff_5m = ts - 300.0
        while self._prices_5m and self._prices_5m[0][0] < cutoff_5m:
            self._prices_5m.popleft()

    def on_mark_price(self, mark: float) -> None:
        if mark > 0:
            self._mark_price = mark

    def build(self) -> dict:
        now = time.time()
        mid = getattr(self, "_mark_price", 0.0)
        prices: List[float] = [p for _, p, _, _ in self._trades]
        qtys:   List[float] = [q for _, _, q, _ in self._trades]
        makers: List[bool]  = [m for _, _, _, m in self._trades]

        cvd = sum(q if not m else -q for q, m in zip(qtys, makers))
        trend_bps = 0.0
        if len(prices) >= 2 and prices[0] > 0:
            trend_bps = (prices[-1] - prices[0]) / prices[0] * 10_000

        realized_vol_bps = 0.0
        if prices:
            hi = max(prices)
            lo = min(prices)
            mid_ref = prices[-1] if prices[-1] > 0 else prices[0]
            realized_vol_bps = (hi - lo) / mid_ref * 10_000

        mid_ref = prices[-1] if prices else (mid if mid > 0 else 1.0)

        range_30s_bps = 0.0
        if len(self._prices_30s) >= 2:
            p30 = [p for _, p in self._prices_30s]
            range_30s_bps = (max(p30) - min(p30)) / max(mid_ref, 1.0) * 10_000

        # ── trend_30s_bps: 30-second directional trend for V3 macro-trend filter ──
        # This is the slope of price over the last 30 s, expressed in bps.
        # Without this, V3 RegimeAdaptiveSignal always receives trend30=0.0 and
        # cannot detect macro-trend direction → effectively degrades to V2/linear.
        trend_30s_bps = 0.0
        if len(self._prices_30s) >= 2:
            p0, p1 = self._prices_30s[0][1], self._prices_30s[-1][1]
            if p0 > 0:
                trend_30s_bps = (p1 - p0) / p0 * 10_000

        # ── Regime: single classification path, identical to backtest ─────────
        # The previous live-specific override (FLOW → CHOP when range<40 bps) was
        # removed because it creates a divergence not present in the backtest.
        regime = SignalProcessor.classify_regime(realized_vol_bps, trend_bps, cvd)

        return {
            "ts":               now,
            "mid":              mid,
            "regime":           regime,
            "cvd_delta_5s":     cvd,
            "trend_bps":        trend_bps,
            "realized_vol_bps": realized_vol_bps,
            "range_30s_bps":    range_30s_bps,
            "trend_30s_bps":    trend_30s_bps,   # ← V3 macro-trend (was missing)
            "spread_bps":       self._assume_spread_bps,
        }

# ── Paper Executor Subclass ────────────────────────────────────────────────────
class PaperExecutor(LiveExecutor):
    """Concrete LiveExecutor for paper trading.
    execute_entry and execute_partial log only — no real orders placed.
    """
    async def execute_entry(self, price: float, qty: float, side: str) -> None:
        log.info("PAPER ENTRY  | %-5s qty=%.6f @ %.2f", side, qty, price)

    async def execute_partial(self, fill: PositionFill) -> None:
        log.info(
            "PAPER EXIT   | %-5s qty=%.6f @ %.2f | pnl=$%.2f | %s",
            fill.side, fill.qty, fill.price, fill.pnl_usd, fill.event.name,
        )

# ── Bot Runtime ────────────────────────────────────────────────────────────────
class Au2QwenBot:
    """Runtime shell: WS market feed + task supervisor + paper executor."""
    def __init__(self) -> None:
        self.symbol    = os.getenv("BOT_SYMBOL", "BTCUSDT").lower()
        self.ws_base   = os.getenv("BINANCE_FAPI_WS", "wss://fstream.binance.com/stream")
        start_equity   = float(os.getenv("START_EQUITY", "10000"))
        self.executor  = PaperExecutor(GOAT_CFG, start_equity=start_equity, live_mode=False)
        self.market    = MarketState(window_s=5.0, assume_spread_bps=GOAT_CFG.assume_spread_bps)
        self.stop_event = asyncio.Event()
        self._start_ts    : float = time.time()
        self._error_count : int   = 0

    async def run_market_stream(self) -> None:
        streams = f"{self.symbol}@aggTrade/{self.symbol}@markPrice@1s"
        url = f"{self.ws_base}?streams={streams}"
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(
                    url, ping_interval=60, ping_timeout=40, max_size=None
                ) as ws:
                    log.info("market stream connected | %s", self.symbol.upper())
                    while not self.stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            log.warning("market stream: recv timeout — reconnecting")
                            break
                        msg    = json.loads(raw)
                        stream = msg.get("stream", "")
                        data   = msg.get("data", {})
                        if stream.endswith("@aggTrade"):
                            self.market.on_agg_trade(
                                float(data["p"]),
                                float(data["q"]),
                                bool(data["m"]),
                                time.time(),
                            )
                        elif stream.endswith("@markPrice@1s"):
                            self.market.on_mark_price(float(data.get("p", 0) or 0))
                        state = self.market.build()
                        if state["mid"] > 0:
                            self.executor.push_state(state)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._error_count += 1
                log.error("market stream error (#%d): %s", self._error_count, exc, exc_info=True)
                await asyncio.sleep(3)

    async def strategy_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self.executor.process_tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("strategy_loop: %s", exc, exc_info=True)
                self._error_count += 1
                await asyncio.sleep(1)

    async def heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            await asyncio.sleep(300)
            uptime_h = (time.time() - self._start_ts) / 3600
            eq = self.executor.risk.current_equity
            pos = "FLAT"
            if self.executor.pm.pos:
                p = self.executor.pm.pos
                pos = f"{p.side} qty={p.remaining_qty:.4f}"
            log.info(
                "HEARTBEAT | uptime=%.1fh errors=%d equity=$%.2f pos=%s",
                uptime_h, self._error_count, eq, pos,
            )

    async def run(self) -> None:
        logging.basicConfig(
            level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )
        log.info("AU2QWEN paper bot starting | GOAT | symbol=%s", self.symbol.upper())
        _task_coros = {
            "market":    self.run_market_stream,
            "strategy":  self.strategy_loop,
            "heartbeat": self.heartbeat_loop,
        }
        tasks = {
            name: asyncio.create_task(coro(), name=name)
            for name, coro in _task_coros.items()
        }
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=10.0)
                break
            except asyncio.TimeoutError:
                pass
            for name, task in list(tasks.items()):
                if task.done():
                    exc = task.exception() if not task.cancelled() else None
                    if exc:
                        self._error_count += 1
                        log.error("task %s crashed (#%d): %s — restarting",
                                  name, self._error_count, exc, exc_info=exc)
                    else:
                        log.warning("task %s exited cleanly — restarting", name)
                    tasks[name] = asyncio.create_task(_task_coros[name](), name=name)
        for t in tasks.values():
            t.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        log.info("AU2QWEN shutdown complete.")

# ── Immortal Entrypoint ────────────────────────────────────────────────────────
async def amain() -> None:
    """Immortal entrypoint — only exits on SIGTERM / SIGINT.
    Every crash or clean exit from bot.run() triggers a restart after 5s."""
    loop          = asyncio.get_running_loop()
    restart_count = 0
    _sigterm_recv = False
    # ✅ H3: Instanciation unique pour préserver l'état (positions, equity, compteurs)
    bot: Au2QwenBot = Au2QwenBot()

    def _on_signal() -> None:
        nonlocal _sigterm_recv
        _sigterm_recv = True
        if bot:
            bot.stop_event.set()

    while True:
        _sigterm_recv = False
        bot.stop_event.clear()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _on_signal)
            except NotImplementedError:
                pass
        try:
            await bot.run()
        except KeyboardInterrupt:
            log.info("KeyboardInterrupt — restarting in 5s.")
        except BaseException:
            log.error("CRASH in bot.run()", exc_info=True)
            if _sigterm_recv:
                log.info("SIGTERM received — exiting process.")
                return
        restart_count += 1
        log.warning("bot.run() ended unexpectedly (#%d) — restarting in 5s.", restart_count)
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(amain())
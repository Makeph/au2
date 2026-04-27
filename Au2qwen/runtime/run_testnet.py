#!/usr/bin/env python3
"""AU2QWEN — Binance USDM Testnet runner.

Pre-flight sequence (runs before the bot loop):
  1. Ping testnet REST endpoint
  2. Verify HMAC auth via GET /fapi/v2/account
  3. Check available USDT balance (warn if < 100 USDT)
  4. Place a GTX test order far from market (should expire immediately)
  5. Confirm order was rejected/expired cleanly (GTX behaves correctly)
  6. Reconcile open positions (H3)
  7. Start full bot loop

Environment variables
---------------------
  BINANCE_API_KEY       testnet API key
  BINANCE_API_SECRET    testnet API secret
  BINANCE_BASE_URL      (auto-set to testnet — do not override here)
  START_EQUITY          paper equity tracking (default 10 000)
  LOG_LEVEL             DEBUG | INFO (default INFO)
  BOT_SYMBOL            symbol override (default BTCUSDT)

Usage
-----
  export BINANCE_API_KEY=<testnet_key>
  export BINANCE_API_SECRET=<testnet_secret>
  python runtime/run_testnet.py

  # or via .env file:
  BINANCE_API_KEY=xxx BINANCE_API_SECRET=yyy python runtime/run_testnet.py

Note on market data
-------------------
Binance testnet does NOT have a separate market-data WebSocket.
The bot connects to the production aggTrade / markPrice streams (real market data).
Order placement and account state use the testnet REST endpoint.
"""
from __future__ import annotations

import sys, pathlib, os
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime", _ROOT / "presets"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

# ── Point BinanceExecutor at testnet BEFORE any import resolves the env var ──
os.environ.setdefault("BINANCE_BASE_URL", "https://testnet.binancefuture.com")

import asyncio
import logging
import signal
import time

from dotenv import load_dotenv

# Load .env.testnet (testnet keys) before .env (prod keys)
_TESTNET_ENV = _ROOT.parent / ".env.testnet"
_PROD_ENV    = _ROOT.parent / ".env"
if _TESTNET_ENV.exists():
    load_dotenv(_TESTNET_ENV, override=True)
elif _PROD_ENV.exists():
    load_dotenv(_PROD_ENV, override=False)

from au2_config import GOAT_VALIDATED_CFG
from au2_bot_live import MarketState, Au2QwenBot
from au2_binance import BinanceExecutor, TESTNET_BASE
from au2_state_manager import StatePersistence

log = logging.getLogger("au2_testnet")

_STATE_PATH     = str(_ROOT / "data" / "live" / "au2_testnet_state.json")
_DECISIONS_PATH = str(_ROOT / "data" / "live" / "au2_testnet_decisions.jsonl")


# ─────────────────────────────────────────────────────────────────────────────
# Testnet executor — minimal risk sizing
# ─────────────────────────────────────────────────────────────────────────────

import dataclasses

# Override risk sizing for testnet: smallest possible position (~0.001 BTC)
_TESTNET_CFG = dataclasses.replace(
    GOAT_VALIDATED_CFG,
    risk_per_trade_pct=0.01,   # 0.01% → ~$1 risk on $10k equity
    max_risk_usd=5.0,          # hard cap $5 per trade
    min_risk_usd=0.5,
)


class TestnetExecutor(BinanceExecutor):
    """BinanceExecutor wired to testnet with isolated state file."""

    def _load_or_init(self, equity: float) -> None:
        self.state_mgr = StatePersistence(_STATE_PATH)
        saved = self.state_mgr.load()
        if saved and saved.get("ts", 0) > time.time() - 86_400:
            log.info("Testnet state restored: equity=$%.2f  streak=%d",
                     saved["equity"], saved.get("loss_streak", 0))
            self.risk.current_equity     = saved["equity"]
            self.last_trade_ts           = saved["last_trade_ts"]
            self.risk.consecutive_losses = saved.get("loss_streak", 0)
            self.risk.day_start_equity   = saved["equity"]
        else:
            log.info("Testnet fresh start.")


# ─────────────────────────────────────────────────────────────────────────────
# Pre-flight checks
# ─────────────────────────────────────────────────────────────────────────────

async def preflight(executor: TestnetExecutor) -> bool:
    """Run all pre-flight checks. Returns True if safe to start the bot."""
    base = executor._base_url
    log.info("═══ PRE-FLIGHT CHECKS (testnet: %s) ═══", base)

    # 1. Ping ─────────────────────────────────────────────────────────────────
    log.info("[1/5] Ping...")
    try:
        async with executor._session.get(f"{base}/fapi/v1/ping") as r:
            if r.status != 200:
                log.error("Ping failed: HTTP %s", r.status)
                return False
        log.info("      ✓ Reachable")
    except Exception as exc:
        log.error("Ping exception: %s", exc)
        return False

    # 2. Auth — GET /fapi/v2/account ──────────────────────────────────────────
    log.info("[2/5] Auth check...")
    try:
        signed = executor._signed_params({})
        async with executor._session.get(f"{base}/fapi/v2/account", params=signed) as r:
            data = await r.json()
        if "code" in data and data["code"] < 0:
            log.error("Auth failed: %s", data.get("msg"))
            return False
        total_wallet = float(data.get("totalWalletBalance", 0))
        log.info("      ✓ Authenticated | walletBalance=%.2f USDT", total_wallet)
    except Exception as exc:
        log.error("Auth exception: %s", exc)
        return False

    # 3. Balance check ────────────────────────────────────────────────────────
    log.info("[3/5] Balance check (min 100 USDT recommended)...")
    if total_wallet < 100:
        log.warning("      ⚠ Low balance: %.2f USDT — testnet orders may fail sizing", total_wallet)
    else:
        log.info("      ✓ Balance OK: %.2f USDT", total_wallet)

    # 4. GTX order test (far from market — should EXPIRE immediately) ─────────
    log.info("[4/5] GTX test order (should expire instantly)...")
    try:
        # Fetch current mark price
        async with executor._session.get(
            f"{base}/fapi/v1/premiumIndex",
            params={"symbol": executor._symbol}
        ) as r:
            px_data = await r.json()
        mark_price = float(px_data.get("markPrice", 0))
        if mark_price <= 0:
            log.error("Could not fetch mark price")
            return False

        # Place GTX BUY limit 10% below market — guaranteed to expire
        test_price = round(mark_price * 0.90, 1)
        result = await executor._post_order(
            side="BUY",
            order_type="LIMIT",
            qty=0.001,
            price=test_price,
            timeInForce="GTX",
        )
        if result.is_rejected:
            log.info("      ✓ GTX expired as expected (orderId=%s status=%s)",
                     result.order_id, result.status)
        elif result.is_open:
            log.warning("      ⚠ GTX order is open (not expired) — cancelling")
            await executor._delete_order(result.order_id)
        else:
            log.info("      ✓ GTX order status=%s orderId=%s", result.status, result.order_id)
    except Exception as exc:
        log.error("GTX test failed: %s", exc)
        return False

    # 5. Position reconciliation (H3) ─────────────────────────────────────────
    log.info("[5/5] Position reconciliation (H3)...")
    try:
        await executor.reconcile_on_start()
        log.info("      ✓ Reconciliation complete")
    except Exception as exc:
        log.error("Reconciliation failed: %s", exc)
        return False

    log.info("═══ PRE-FLIGHT PASSED — starting bot loop ═══")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Bot
# ─────────────────────────────────────────────────────────────────────────────

class TestnetBot(Au2QwenBot):
    """Au2QwenBot wired to BinanceExecutor (testnet)."""

    def __init__(self) -> None:
        self.symbol     = os.getenv("BOT_SYMBOL", "BTCUSDT").lower()
        self.ws_base    = os.getenv("BINANCE_FAPI_WS", "wss://fstream.binance.com/stream")
        start_equity    = float(os.getenv("START_EQUITY", "10000"))

        self.executor   = TestnetExecutor(
            _TESTNET_CFG,
            start_equity=start_equity,
            live_mode=True,
            decision_log_path=_DECISIONS_PATH,
        )
        self.market     = MarketState(
            window_s=5.0,
            assume_spread_bps=_TESTNET_CFG.assume_spread_bps,
        )
        self.stop_event    = asyncio.Event()
        self._start_ts     = time.time()
        self._start_equity = start_equity
        self._error_count  = 0

    async def run(self) -> None:
        logging.basicConfig(
            level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
            format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        )
        log.info("AU2QWEN TESTNET | symbol=%s | equity=$%.0f | base=%s",
                 self.symbol.upper(), self._start_equity, TESTNET_BASE)

        # Open aiohttp session first (needed by preflight)
        import aiohttp
        self.executor._session = aiohttp.ClientSession(
            headers={"X-MBX-APIKEY": self.executor._api_key}
        )

        try:
            ok = await preflight(self.executor)
            if not ok:
                log.critical("Pre-flight failed — aborting.")
                return
        except Exception as exc:
            log.critical("Pre-flight exception: %s", exc, exc_info=True)
            return

        _task_coros = {
            "market":    self.run_market_stream,
            "strategy":  self.strategy_loop,
            "heartbeat": self.heartbeat_loop,
        }
        tasks = {n: asyncio.create_task(c(), name=n) for n, c in _task_coros.items()}

        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=10.0)
                break
            except asyncio.TimeoutError:
                pass
            for name, task in list(tasks.items()):
                if task.done():
                    exc = task.exception() if not task.cancelled() else None
                    log.error("task %s crashed — restarting: %s", name, exc)
                    tasks[name] = asyncio.create_task(_task_coros[name](), name=name)

        for t in tasks.values():
            t.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        await self.executor.close()
        log.info("Testnet bot shutdown complete.")


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

async def amain() -> None:
    if not os.environ.get("BINANCE_API_KEY") or not os.environ.get("BINANCE_API_SECRET"):
        print("ERROR: BINANCE_API_KEY and BINANCE_API_SECRET must be set.")
        print("       Get testnet keys at: https://testnet.binancefuture.com")
        sys.exit(1)

    loop = asyncio.get_running_loop()
    bot  = TestnetBot()

    def _stop():
        log.info("Signal received — stopping.")
        bot.stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    await bot.run()


if __name__ == "__main__":
    asyncio.run(amain())

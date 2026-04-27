#!/usr/bin/env python3
"""AU2QWEN — Binance USDM Futures executor interface.

Contract
--------
BinanceExecutor subclasses LiveExecutor and overrides three async methods:

  execute_entry(price, qty, side)
      Post a GTX (post-only) limit order at `price`.
      If GTX is rejected (price moved), the entry is silently skipped — do NOT retry.

  execute_partial(fill)
      Place the appropriate reduce-only order for a TP or SL event:
        TP1_HIT / TP2_HIT / EXIT_TIME / EXIT_BE_FALLBACK → reduceOnly limit (maker)
        EXIT_SL                                          → reduceOnly market (taker)
      Cancel any resting order for the same position leg before placing the new one.

  _cancel_open_order(order_id)
      DELETE /fapi/v1/order.  Best-effort — log failures, do not raise.

Fill confirmation strategy
--------------------------
We do NOT subscribe to the user-data WS stream in the first implementation.
Instead, each order write records its order_id in _open_order_ids.
The PositionManager drives all state transitions from market prices (backtest-identical).
Real fill confirmation can be layered in a later pass via user-data stream.

Lifecycle
---------
  executor = BinanceExecutor(cfg, start_equity=10_000)
  await executor.start()          # opens aiohttp session
  ...
  await executor.close()          # cancels all open orders, closes session

Fee model (from config)
-----------------------
  entry         : maker_fee_bps  (0.2 bps, GTX limit)
  TP1/TP2/time  : maker_fee_bps  (0.2 bps, reduceOnly limit)
  SL            : taker_fee_bps  (0.5 bps, reduceOnly market)
  round-trip    : 0.7 bps

API endpoints
-------------
  POST   /fapi/v1/order           new order
  DELETE /fapi/v1/order           cancel order
  GET    /fapi/v2/positionRisk    position query (crash-recovery, H3)
  GET    /fapi/v1/openOrders      list resting orders (startup reconciliation)
  GET    /fapi/v1/userTrades      trade history (crash-close PnL recovery, H3-C)
"""
from __future__ import annotations

import sys, pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

import asyncio
import hashlib
import hmac
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlencode

from au2_live_executor import LiveExecutor
from au2_core import (
    PositionFill, PositionEvent, CoreConfig,
    PositionState, REGIME_PROFILES, Regime,
)

log = logging.getLogger("au2_binance")

FAPI_BASE    = "https://fapi.binance.com"
TESTNET_BASE = "https://testnet.binancefuture.com"
SYMBOL       = "BTCUSDT"

# BTCUSDT USDM precision — update if exchange changes filters
_TICK_SIZE = 0.1    # price step (1 decimal place)
_STEP_SIZE = 0.001  # qty step   (3 decimal places)
_MIN_QTY   = 0.001  # minimum order quantity in BTC

def _round_tick(price: float) -> float:
    return round(round(price / _TICK_SIZE) * _TICK_SIZE, 1)

def _round_step(qty: float) -> float:
    return round(round(qty / _STEP_SIZE) * _STEP_SIZE, 3)


# ─────────────────────────────────────────────────────────────────────────────
# Data types
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class OrderResult:
    """Normalised response from a Binance order operation."""
    order_id:   str
    client_id:  str
    status:     str          # NEW | FILLED | CANCELED | EXPIRED | PARTIALLY_FILLED
    side:       str          # BUY | SELL
    price:      float
    qty:        float
    filled_qty: float = 0.0
    reject_reason: Optional[str] = None

    @property
    def is_open(self) -> bool:
        return self.status in ("NEW", "PARTIALLY_FILLED")

    @property
    def is_rejected(self) -> bool:
        return self.status == "EXPIRED" or self.reject_reason is not None


@dataclass
class _PositionLeg:
    """Tracks a single resting order attached to an open position."""
    event:    PositionEvent
    order_id: str
    price:    float
    qty:      float


# ─────────────────────────────────────────────────────────────────────────────
# BinanceExecutor
# ─────────────────────────────────────────────────────────────────────────────

class BinanceExecutor(LiveExecutor):
    """LiveExecutor that routes orders to Binance USDM Futures.

    All three order methods must be implemented before using with live_mode=True.
    The interface is fully defined here; HTTP + auth is wired in _post_order / _delete_order.
    """

    def __init__(
        self,
        cfg:              CoreConfig,
        start_equity:     float = 10_000.0,
        live_mode:        bool  = True,
        symbol:           str   = SYMBOL,
        decision_log_path: Optional[str] = None,
    ) -> None:
        super().__init__(cfg, start_equity=start_equity, live_mode=live_mode,
                         decision_log_path=decision_log_path)

        self._symbol     = symbol
        self._base_url   = os.environ.get("BINANCE_BASE_URL", FAPI_BASE).rstrip("/")
        self._api_key    = os.environ.get("BINANCE_API_KEY",    "")
        self._api_secret = os.environ.get("BINANCE_API_SECRET", "")
        self._session    = None  # aiohttp.ClientSession — set in start()

        # position_leg_key → _PositionLeg (cleared on position close)
        self._open_legs: dict[str, _PositionLeg] = {}
        # order_id of a GTX entry currently in the fill-check poll loop (None otherwise)
        self._pending_entry_order_id: Optional[str] = None

        if live_mode and not (self._api_key and self._api_secret):
            raise ValueError(
                "BinanceExecutor: live_mode=True but BINANCE_API_KEY / "
                "BINANCE_API_SECRET not set in environment."
            )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Open aiohttp session.  Must be called before process_tick()."""
        import aiohttp
        self._session = aiohttp.ClientSession(
            headers={"X-MBX-APIKEY": self._api_key},
        )
        log.info("BinanceExecutor: session opened (symbol=%s)", self._symbol)
        await self.reconcile_on_start()

    async def close(self) -> None:
        """Cancel all resting orders, then close the HTTP session."""
        for leg in list(self._open_legs.values()):
            try:
                await self._cancel_open_order(leg.order_id)
            except Exception as exc:
                log.warning("close(): failed to cancel %s — %s", leg.order_id, exc)
        if self._session:
            await self._session.close()
            self._session = None
        log.info("BinanceExecutor: session closed")

    # ── Order routing (overrides LiveExecutor stubs) ──────────────────────────

    async def execute_entry(self, price: float, qty: float, side: str) -> None:
        """Post a GTX (post-only maker) limit order at `price`.

        Parameters
        ----------
        price : limit price in USDT (rounded to tick size before sending)
        qty   : position size in BTC (rounded to step size before sending)
        side  : "LONG" → BUY, "SHORT" → SELL

        Behaviour
        ---------
        - timeInForce=GTX: order is cancelled immediately if it would cross.
          Do NOT retry on GTX rejection — the entry opportunity is gone.
        - On success: store order_id in self._open_legs["ENTRY"].
        - On rejection: log and return silently.
        """
        # Cancel any order left resting from a previous timed-out execute_entry call.
        if self._pending_entry_order_id:
            log.warning("execute_entry: cancelling orphan resting order %s before new attempt",
                        self._pending_entry_order_id)
            await self._cancel_open_order(self._pending_entry_order_id)
            self._pending_entry_order_id = None
            self._cancel_phantom_position()
            return

        bn_side = "BUY" if side == "LONG" else "SELL"
        # Offset 0.5 bps away from market so GTX lands as maker (not at mid).
        # SELL entry: limit above mid → resting on the ask side → maker fill.
        # BUY  entry: limit below mid → resting on the bid side → maker fill.
        _GTX_OFFSET = price * 0.00005  # 0.5 bps
        if bn_side == "SELL":
            price_r = _round_tick(price + _GTX_OFFSET)
        else:
            price_r = _round_tick(price - _GTX_OFFSET)
        qty_r   = _round_step(qty)

        if qty_r < _MIN_QTY:
            log.warning("execute_entry: qty %.4f < min %.4f — skipped", qty_r, _MIN_QTY)
            return

        try:
            result = await self._post_order(
                side=bn_side,
                order_type="LIMIT",
                qty=qty_r,
                price=price_r,
                timeInForce="GTX",
            )
        except RuntimeError as exc:
            log.error("execute_entry: order rejected by exchange — %s", exc)
            self._cancel_phantom_position()
            return

        if result.is_rejected:
            log.info("execute_entry: GTX expired (price moved) — entry skipped")
            self._cancel_phantom_position()
            return

        # GTX accepted as NEW — poll for fill confirmation (up to 4s, 8 × 0.5s)
        if result.status == "NEW":
            self._pending_entry_order_id = result.order_id
            filled = False
            for attempt in range(8):
                await asyncio.sleep(0.5)
                try:
                    result = await self._get_order_status(result.order_id)
                except RuntimeError as exc:
                    log.warning("execute_entry: poll %d failed — %s", attempt + 1, exc)
                    break
                if result.status == "FILLED":
                    filled = True
                    break
                if result.status not in ("NEW", "PARTIALLY_FILLED"):
                    # CANCELED / EXPIRED / REJECTED
                    break

            self._pending_entry_order_id = None
            if not filled:
                log.warning(
                    "execute_entry: GTX order %s not filled after 4s (status=%s) — cancelling",
                    result.order_id, result.status,
                )
                await self._cancel_open_order(result.order_id)
                self._cancel_phantom_position()
                return

        self._open_legs["ENTRY"] = _PositionLeg(
            event=PositionEvent.OPEN,
            order_id=result.order_id,
            price=price_r,
            qty=qty_r,
        )
        log.info("[ORDER] ENTRY %s %.4f BTC @ %.1f | id=%s | filled=%.4f",
                 bn_side, qty_r, price_r, result.order_id, result.filled_qty)

    async def execute_partial(self, fill: PositionFill) -> None:
        """Route a position-exit event to the correct Binance order type.

        Event mapping
        -------------
        TP1_HIT / TP2_HIT / EXIT_TIME / EXIT_BE_FALLBACK
            → POST /fapi/v1/order  side=opposite  type=LIMIT  reduceOnly=true  timeInForce=GTC
              price=fill.price  qty=fill.qty
        EXIT_SL
            → POST /fapi/v1/order  side=opposite  type=MARKET  reduceOnly=true
              qty=fill.qty

        Before placing any new order, cancel the resting order for this leg (if any)
        via _cancel_open_order(), then place the new one.

        Parameters
        ----------
        fill : PositionFill from PositionManager — contains event, price, qty, side
        """
        _TERMINAL = {PositionEvent.EXIT_SL, PositionEvent.EXIT_TIME, PositionEvent.EXIT_BE_FALLBACK}
        _TAKER    = {PositionEvent.EXIT_SL, PositionEvent.EXIT_BE_FALLBACK}

        close_side = "SELL" if fill.side == "LONG" else "BUY"
        leg_key    = fill.event.value
        qty_r      = _round_step(fill.qty)

        # Cancel resting orders — all of them on terminal events, just this leg otherwise
        if fill.event in _TERMINAL:
            for leg in list(self._open_legs.values()):
                await self._cancel_open_order(leg.order_id)
            self._open_legs.clear()
        else:
            existing = self._open_legs.pop(leg_key, None)
            if existing:
                await self._cancel_open_order(existing.order_id)

        if qty_r < _MIN_QTY:
            log.warning("execute_partial %s: qty %.4f < min — skipped", fill.event.value, qty_r)
            return

        try:
            if fill.event in _TAKER:
                result = await self._post_order(
                    side=close_side,
                    order_type="MARKET",
                    qty=qty_r,
                    reduceOnly="true",
                )
            else:
                price_r = _round_tick(fill.price)
                result = await self._post_order(
                    side=close_side,
                    order_type="LIMIT",
                    qty=qty_r,
                    price=price_r,
                    timeInForce="GTC",
                    reduceOnly="true",
                )
        except RuntimeError as exc:
            log.error("execute_partial %s: failed — %s", fill.event.value, exc)
            return

        price_label = "MKT" if fill.event in _TAKER else f"{fill.price:.1f}"
        log.info("[ORDER] %s %s %.4f BTC @ %s | id=%s | pnl=$%.2f",
                 fill.event.value, close_side, qty_r,
                 price_label, result.order_id, fill.pnl_usd)

        # Track non-terminal limit orders so they can be cancelled if a later event fires first
        if fill.event not in _TERMINAL and result.is_open:
            self._open_legs[leg_key] = _PositionLeg(
                event=fill.event,
                order_id=result.order_id,
                price=fill.price,
                qty=qty_r,
            )

    def _cancel_phantom_position(self) -> None:
        """Undo pm.open() when the entry order was not filled on the exchange.

        Called on GTX expiry (-5022) or any entry rejection.
        asyncio is single-threaded so this is safe to call from execute_entry().
        """
        if self.pm.pos is not None:
            log.warning("[PHANTOM] Entry not filled — cancelling PositionState %s @ %.1f",
                        self.pm.pos.side, self.pm.pos.exec_price)
            self.pm.pos  = None
            self.builder = None
            self._open_legs.clear()

    async def _cancel_open_order(self, order_id: str) -> None:
        """Cancel a resting limit order.

        DELETE /fapi/v1/order?symbol=...&orderId=...
        Best-effort: log failures but do not raise — caller continues regardless.
        """
        try:
            await self._delete_order(order_id)
        except Exception as exc:
            log.warning("_cancel_open_order %s: %s", order_id, exc)

    # ── Auth & HTTP helpers ───────────────────────────────────────────────────

    def _sign(self, params: dict) -> str:
        """Return HMAC-SHA256 hex signature for `params` (with timestamp)."""
        payload = urlencode(params)
        return hmac.new(
            self._api_secret.encode(), payload.encode(), hashlib.sha256
        ).hexdigest()

    def _signed_params(self, params: dict) -> dict:
        """Attach timestamp + signature to `params` and return a new dict."""
        p = {**params, "timestamp": int(time.time() * 1000)}
        p["signature"] = self._sign(p)
        return p

    async def _post_order(
        self,
        side:       str,
        order_type: str,
        qty:        float,
        price:      Optional[float] = None,
        **kwargs,
    ) -> OrderResult:
        """POST /fapi/v1/order — signed, returns normalised OrderResult.

        Raises
        ------
        RuntimeError  if the HTTP response contains a Binance error code.
        """
        params: dict = {
            "symbol":   self._symbol,
            "side":     side,
            "type":     order_type,
            "quantity": f"{qty:.3f}",
        }
        if price is not None:
            params["price"] = f"{price:.1f}"
        params.update(kwargs)
        signed = self._signed_params(params)

        async with self._session.post(
            f"{self._base_url}/fapi/v1/order", data=signed
        ) as resp:
            data = await resp.json()

        code = data.get("code")
        if isinstance(code, int) and code < 0:
            raise RuntimeError(f"Binance {code}: {data.get('msg', '')}")

        return OrderResult(
            order_id=str(data["orderId"]),
            client_id=data.get("clientOrderId", ""),
            status=data["status"],
            side=data["side"],
            price=float(data.get("price") or 0),
            qty=float(data.get("origQty", qty)),
            filled_qty=float(data.get("executedQty", 0)),
        )

    async def _delete_order(self, order_id: str) -> None:
        """DELETE /fapi/v1/order — signed.  Swallows -2011 (unknown order)."""
        signed = self._signed_params({"symbol": self._symbol, "orderId": order_id})
        async with self._session.delete(
            f"{self._base_url}/fapi/v1/order", params=signed
        ) as resp:
            data = await resp.json()

        code = data.get("code")
        if isinstance(code, int) and code < 0 and code != -2011:
            log.warning("_delete_order %s: code=%s msg=%s", order_id, code, data.get("msg"))

    async def _get_order_status(self, order_id: str) -> OrderResult:
        """GET /fapi/v1/order — returns current OrderResult.  Raises RuntimeError on error."""
        signed = self._signed_params({"symbol": self._symbol, "orderId": order_id})
        async with self._session.get(
            f"{self._base_url}/fapi/v1/order", params=signed
        ) as resp:
            data = await resp.json()

        code = data.get("code")
        if isinstance(code, int) and code < 0:
            raise RuntimeError(f"Binance {code}: {data.get('msg', '')}")

        return OrderResult(
            order_id=str(data["orderId"]),
            client_id=data.get("clientOrderId", ""),
            status=data["status"],
            side=data["side"],
            price=float(data.get("price") or 0),
            qty=float(data.get("origQty", 0)),
            filled_qty=float(data.get("executedQty", 0)),
        )

    # ── Reconciliation helper (used by H3 crash recovery) ────────────────────

    async def fetch_open_position(self) -> Optional[dict]:
        """GET /fapi/v2/positionRisk for self._symbol.

        Returns a normalised dict if qty >= _MIN_QTY, else None:
          { side, qty, entry_price, mark_price, upnl }
        """
        signed = self._signed_params({"symbol": self._symbol})
        async with self._session.get(
            f"{self._base_url}/fapi/v2/positionRisk", params=signed
        ) as resp:
            data = await resp.json()

        if isinstance(data, dict) and isinstance(data.get("code"), int) and data["code"] < 0:
            log.warning("fetch_open_position: %s", data.get("msg"))
            return None

        for pos in (data if isinstance(data, list) else []):
            if pos.get("symbol") == self._symbol:
                amt = float(pos.get("positionAmt", 0))
                if abs(amt) >= _MIN_QTY:
                    return {
                        "side":        "LONG" if amt > 0 else "SHORT",
                        "qty":         abs(amt),
                        "entry_price": float(pos.get("entryPrice", 0)),
                        "mark_price":  float(pos.get("markPrice", 0)),
                        "upnl":        float(pos.get("unRealizedProfit", 0)),
                    }
        return None

    async def reconcile_on_start(self) -> None:
        """Compare Binance position vs checkpoint and act.

        Cases
        -----
        A  Binance=flat,  ckpt=flat   -> clean start, nothing to do
        B  Binance=open,  ckpt=flat   -> orphan: reconstruct from entryPrice + config
        C  Binance=flat,  ckpt=open   -> closed during crash: recover PnL from trade history
        D  Binance=open,  ckpt=open   -> normal crash-recovery: reconstruct from checkpoint

        Case C detail
        -------------
        Without PnL recovery the equity stored in the checkpoint is the *pre-trade* value.
        If the position closed at a loss while the process was dead, RiskEngine.current_equity
        stays inflated and the next trade is sized against phantom equity.

        Fix: query /fapi/v1/userTrades for closing fills since entry_ts and apply the net
        realized PnL via risk.update_equity / overlay.update_equity.  If the API call fails
        or returns no trades we log a WARNING with the max-possible equity overstatement and
        continue — never block startup on a best-effort recovery.
        """
        live_pos  = await self.fetch_open_position()
        ckpt      = self.state_mgr.load() or {}
        ckpt_open = ckpt.get("open_position")

        if live_pos is None and ckpt_open is None:
            log.info("[H3-A] Clean start — no open position.")
            return

        if live_pos is None and ckpt_open is not None:
            await self._handle_crash_close(ckpt_open)
            return

        if live_pos is not None:
            tag = "Orphan (B)" if ckpt_open is None else "Crash-recovery (D)"
            log.warning("[H3-%s] %s %.4f BTC @ %.1f (uPnL $%.2f) — restoring.",
                        "B" if ckpt_open is None else "D",
                        live_pos["side"], live_pos["qty"],
                        live_pos["entry_price"], live_pos["upnl"])
            self._restore_from_live(live_pos)

    # ── Case C: position closed on exchange while bot was dead ───────────────

    async def _handle_crash_close(self, ckpt_open: dict) -> None:
        """Apply realized PnL for a position that closed during a crash (H3-C).

        Strategy
        --------
        1. Query /fapi/v1/userTrades for fills since entry_ts.
        2. Sum realizedPnl on closing fills (reduceOnly, or opposite-side fills).
        3. Apply via risk.update_equity + risk.record_result + overlay.update_equity.
        4. Write a clean (no open_position) checkpoint so the ghost is gone.
        5. If anything fails: log WARNING with max-loss estimate, continue.

        The bot must never be blocked from starting by a failed history query.
        """
        side     = ckpt_open.get("side", "")
        entry_ts = float(ckpt_open.get("entry_ts") or 0)
        qty      = float(ckpt_open.get("initial_qty") or ckpt_open.get("remaining_qty") or 0)
        price    = float(ckpt_open.get("exec_price") or 0)

        # Max possible loss (used in fallback warning)
        max_loss_usd = qty * price * self.cfg.stop_loss_pct / 100 if qty and price else 0.0

        realized_pnl = await self._fetch_crash_close_pnl(side, entry_ts)

        if realized_pnl is not None:
            log.warning(
                "[H3-C] Position closed during crash. Recovered PnL=$%.2f — applying to equity.",
                realized_pnl,
            )
            self.risk.update_equity(realized_pnl)
            self.risk.record_result(realized_pnl)
            self.overlay.update_equity(realized_pnl, time.time())
        else:
            log.warning(
                "[H3-C] Position closed during crash. Could not recover PnL from trade history "
                "(API unavailable or no fills found). "
                "Equity may be overstated by up to $%.2f. "
                "Recommend manual reconciliation before continuing.",
                max_loss_usd,
            )

        # Clear the stale open_position from the checkpoint regardless.
        self.state_mgr.save(self.state_mgr.build_checkpoint(
            self.risk.current_equity, time.time(),
            self.risk.consecutive_losses, None, None,
        ))
        log.info("[H3-C] Checkpoint written with equity=$%.2f (open_position cleared).",
                 self.risk.current_equity)

    async def _fetch_crash_close_pnl(self, side: str, entry_ts: float) -> Optional[float]:
        """Query /fapi/v1/userTrades for closing fills since entry_ts.

        Returns net realized PnL in USD, or None if the query fails or
        no closing trades are found.

        Closing fill definition: reduceOnly=true  OR  opposite side to entry
        (LONG entry -> SELL fills, SHORT entry -> BUY fills).
        """
        if not entry_ts:
            log.debug("[H3-C] No entry_ts in checkpoint — cannot query trade history.")
            return None

        # Binance requires ms timestamps; add 1 s buffer to avoid missing the entry fill
        start_ms = int((entry_ts - 1.0) * 1000)
        try:
            signed = self._signed_params({
                "symbol":    self._symbol,
                "startTime": start_ms,
                "limit":     100,
            })
            async with self._session.get(
                f"{self._base_url}/fapi/v1/userTrades", params=signed
            ) as resp:
                trades = await resp.json()
        except Exception as exc:
            log.warning("[H3-C] userTrades query failed: %s", exc)
            return None

        if isinstance(trades, dict) and trades.get("code"):
            log.warning("[H3-C] userTrades API error %s: %s",
                        trades.get("code"), trades.get("msg"))
            return None
        if not isinstance(trades, list):
            log.warning("[H3-C] Unexpected userTrades response: %r", trades)
            return None

        close_side = "SELL" if side == "LONG" else "BUY"
        closing = [
            t for t in trades
            if t.get("symbol") == self._symbol
            and (t.get("reduceOnly") or t.get("side") == close_side)
        ]

        if not closing:
            log.info("[H3-C] No closing fills found after entry_ts — position may have "
                     "closed before trade history window.")
            return None

        net_pnl = sum(float(t.get("realizedPnl", 0)) for t in closing)
        log.info("[H3-C] %d closing fill(s) found — net PnL: $%.2f", len(closing), net_pnl)
        return net_pnl

    def _restore_from_live(self, live_pos: dict) -> None:
        """Reconstruct PositionManager state from a live Binance position.

        Prefers checkpoint data (has SL/TP/tp1_done/remaining_qty).
        Falls back to config-derived SL/TP when checkpoint has no position detail.
        """
        ckpt     = self.state_mgr.load() or {}
        ckpt_pos = ckpt.get("open_position") or {}

        side  = live_pos["side"]
        price = live_pos["entry_price"]
        qty   = live_pos["qty"]
        cfg   = self.cfg

        # Prefer checkpoint SL/TP (exact); fall back to config-derived estimates
        if ckpt_pos.get("sl_price") and ckpt_pos.get("side") == side:
            sl            = float(ckpt_pos["sl_price"])
            tp1           = float(ckpt_pos["tp1_price"])
            tp2           = float(ckpt_pos["tp2_price"])
            remaining_qty = float(ckpt_pos.get("remaining_qty", qty))
            tp1_done      = bool(ckpt_pos.get("tp1_done", False))
            tp2_done      = bool(ckpt_pos.get("tp2_done", False))
            trail         = float(ckpt_pos.get("trail_price", sl))
            regime_str    = ckpt_pos.get("regime", Regime.TREND.value)
            confidence    = float(ckpt_pos.get("confidence", 0.90))
            entry_ts      = float(ckpt_pos.get("entry_ts", time.time()))
            source        = "checkpoint"
        else:
            sl_d  = price * cfg.stop_loss_pct / 100
            tp1_d = price * cfg.tp1_pct / 100
            tp2_d = price * cfg.tp2_pct / 100
            if side == "LONG":
                sl, tp1, tp2 = price - sl_d, price + tp1_d, price + tp2_d
            else:
                sl, tp1, tp2 = price + sl_d, price - tp1_d, price - tp2_d
            remaining_qty = qty
            tp1_done      = False
            tp2_done      = False
            trail         = sl
            regime_str    = Regime.TREND.value
            confidence    = 0.90
            entry_ts      = time.time()
            source        = "config-derived"
            # Case B (orphan): tp1_done defaults to False — if TP1 already fired on exchange
            # (qty < initial_qty * tp1_ratio) this will cause a double-partial on next TP1 hit.
            # We can detect it: if the live qty is materially less than what a fresh entry
            # would produce, warn loudly.  Exact tp1_ratio is in cfg.tp1_ratio.
            tp1_threshold = qty / max(1.0 - getattr(cfg, "tp1_ratio", 0.5), 0.01)
            if qty < tp1_threshold * 0.85:
                log.warning(
                    "[H3-B] Orphan qty=%.4f looks smaller than a fresh position — "
                    "TP1 may have already fired.  tp1_done forced True to be safe.",
                    qty,
                )
                tp1_done = True

        try:
            regime  = Regime(regime_str)
        except ValueError:
            regime  = Regime.TREND
        profile = REGIME_PROFILES[regime]

        self.pm.pos = PositionState(
            side=side, exec_price=price, entry_ts=entry_ts,
            initial_qty=qty, remaining_qty=remaining_qty,
            score=cfg.threshold,
            sl_price=sl, tp1_price=tp1, tp2_price=tp2,
            peak_price=price, trough_price=price,
            regime=regime.value, confidence=confidence,
            profile=profile, trail_price=trail,
            tp1_done=tp1_done, tp2_done=tp2_done,
        )
        log.info("[H3] Restored (%s): %s %.4f/%.4f BTC @ %.1f | SL=%.1f TP1=%.1f TP2=%.1f | tp1_done=%s",
                 source, side, remaining_qty, qty, price, sl, tp1, tp2, tp1_done)

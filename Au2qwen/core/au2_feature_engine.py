#!/usr/bin/env python3
"""AU2 Feature Engine V2 — Microstructure feature extraction from aggTrade stream.

Maintains rolling windows and computes ~16 features for the ML signal model.
Stateful: call on_trade() for each aggTrade, then compute() to get feature vector.
Stateless alternative: from_row() for backtest mode (signals table).
"""
from __future__ import annotations
import numpy as np
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

FEATURE_NAMES = [
    # Group 1: existing (keep)
    "cvd_5s",
    "trend_5s_bps",
    "realized_vol_5s_bps",
    "range_30s_bps",
    # Group 2: volume microstructure
    "buy_vol_ratio_5s",
    "trade_intensity_5s",
    "avg_size_imbalance",
    "large_trade_imbalance_5s",
    "cvd_acceleration",
    # Group 3: price dynamics
    "trend_30s_bps",
    "trend_ratio_5s_30s",
    "vol_ratio_5s_30s",
    "return_skew_30s",
    "price_position_in_range",
    # Group 4: cost context
    "spread_bps",
    "vol_over_cost",
]

N_FEATURES = len(FEATURE_NAMES)
_EPS = 1e-9


class FeatureEngine:
    """Stateful feature extractor — receives raw aggTrade ticks, computes features."""

    def __init__(self, window_short: float = 5.0, window_long: float = 30.0,
                 taker_fee_bps: float = 0.5, maker_fee_bps: float = 0.2) -> None:
        self._w_short = window_short
        self._w_long = window_long
        self._taker_fee_bps = taker_fee_bps
        self._maker_fee_bps = maker_fee_bps

        # Rolling tick windows
        self._trades_short: Deque[Tuple[float, float, float, bool]] = deque()  # (ts, price, qty, is_buyer_maker)
        self._trades_long: Deque[Tuple[float, float, float, bool]] = deque()
        self._trades_prev_short: Deque[Tuple[float, float, float, bool]] = deque()  # 5-10s ago for acceleration

        # 1-second sampled prices for skewness
        self._prices_1s: Deque[Tuple[float, float]] = deque()
        self._last_1s_sample: float = 0.0

        self._last_spread_bps: float = 0.5

    def on_trade(self, ts: float, price: float, qty: float, is_buyer_maker: bool) -> None:
        """Feed a raw aggTrade tick."""
        trade = (ts, price, qty, is_buyer_maker)
        self._trades_short.append(trade)
        self._trades_long.append(trade)
        self._trades_prev_short.append(trade)

        # Prune windows
        cutoff_short = ts - self._w_short
        cutoff_long = ts - self._w_long
        cutoff_prev = ts - 2.0 * self._w_short

        while self._trades_short and self._trades_short[0][0] < cutoff_short:
            self._trades_short.popleft()
        while self._trades_long and self._trades_long[0][0] < cutoff_long:
            self._trades_long.popleft()
        while self._trades_prev_short and self._trades_prev_short[0][0] < cutoff_prev:
            self._trades_prev_short.popleft()

        # 1-second price sampling for skewness
        if ts - self._last_1s_sample >= 1.0:
            self._prices_1s.append((ts, price))
            self._last_1s_sample = ts
            while self._prices_1s and self._prices_1s[0][0] < cutoff_long:
                self._prices_1s.popleft()

    def set_spread(self, spread_bps: float) -> None:
        """Update spread from external source (markPrice, orderbook, etc.)."""
        self._last_spread_bps = max(spread_bps, 0.0)

    def compute(self, state: Optional[Dict] = None) -> np.ndarray:
        """Compute full feature vector from internal state.

        Args:
            state: optional dict with overrides (e.g., from MarketState.build())

        Returns:
            np.ndarray of shape (N_FEATURES,)
        """
        features = np.zeros(N_FEATURES, dtype=np.float64)

        # ── Short-window metrics ──
        short_prices = [p for _, p, _, _ in self._trades_short]
        short_qtys = [q for _, _, q, _ in self._trades_short]
        short_makers = [m for _, _, _, m in self._trades_short]
        n_short = len(self._trades_short)

        if not short_prices:
            return features

        current_price = short_prices[-1]

        # Feature 0: CVD 5s
        buy_vol = sum(q for q, m in zip(short_qtys, short_makers) if not m)
        sell_vol = sum(q for q, m in zip(short_qtys, short_makers) if m)
        cvd_5s = buy_vol - sell_vol
        features[0] = cvd_5s

        # Feature 1: Trend 5s (bps)
        if len(short_prices) >= 2 and short_prices[0] > 0:
            features[1] = (short_prices[-1] - short_prices[0]) / short_prices[0] * 10_000

        # Feature 2: Realized vol 5s (bps)
        hi = max(short_prices)
        lo = min(short_prices)
        if current_price > 0:
            features[2] = (hi - lo) / current_price * 10_000

        # ── Long-window metrics ──
        long_prices = [p for _, p, _, _ in self._trades_long]

        # Feature 3: Range 30s (bps)
        if len(long_prices) >= 2 and current_price > 0:
            hi_30 = max(long_prices)
            lo_30 = min(long_prices)
            features[3] = (hi_30 - lo_30) / current_price * 10_000

        # ── Volume microstructure ──

        # Feature 4: Buy volume ratio 5s
        total_vol = buy_vol + sell_vol
        features[4] = buy_vol / max(total_vol, _EPS)

        # Feature 5: Trade intensity 5s (trades/sec)
        if n_short >= 2:
            dt = self._trades_short[-1][0] - self._trades_short[0][0]
            features[5] = n_short / max(dt, _EPS)

        # Feature 6: Average trade size imbalance (buy/sell)
        n_buys = sum(1 for m in short_makers if not m)
        n_sells = sum(1 for m in short_makers if m)
        avg_buy = buy_vol / max(n_buys, 1)
        avg_sell = sell_vol / max(n_sells, 1)
        features[6] = avg_buy / max(avg_sell, _EPS)

        # Feature 7: Large trade imbalance 5s
        if short_qtys:
            median_qty = np.median(short_qtys)
            large_threshold = median_qty * 2.0
            large_buy = sum(q for q, m in zip(short_qtys, short_makers) if not m and q > large_threshold)
            large_sell = sum(q for q, m in zip(short_qtys, short_makers) if m and q > large_threshold)
            total_large = large_buy + large_sell
            features[7] = (large_buy - large_sell) / max(total_large, _EPS)

        # Feature 8: CVD acceleration (cvd_5s minus cvd from 5-10s ago)
        cutoff_ts = self._trades_short[0][0] if self._trades_short else 0.0
        prev_trades = [(t, p, q, m) for t, p, q, m in self._trades_prev_short if t < cutoff_ts]
        if prev_trades:
            prev_buy = sum(q for _, _, q, m in prev_trades if not m)
            prev_sell = sum(q for _, _, q, m in prev_trades if m)
            cvd_prev = prev_buy - prev_sell
            features[8] = cvd_5s - cvd_prev

        # ── Price dynamics ──

        # Feature 9: Trend 30s (bps)
        if len(long_prices) >= 2 and long_prices[0] > 0:
            features[9] = (long_prices[-1] - long_prices[0]) / long_prices[0] * 10_000

        # Feature 10: Trend ratio 5s/30s
        trend_5s = features[1]
        trend_30s = features[9]
        features[10] = trend_5s / max(abs(trend_30s), 0.1)

        # Feature 11: Vol ratio 5s/30s
        vol_5s = features[2]
        vol_30s = features[3]
        features[11] = vol_5s / max(vol_30s, 0.1)

        # Feature 12: Return skewness 30s (from 1s sampled prices)
        if len(self._prices_1s) >= 4:
            p_arr = [p for _, p in self._prices_1s]
            rets = [(p_arr[i+1] - p_arr[i]) / max(p_arr[i], _EPS) * 10_000
                    for i in range(len(p_arr)-1)]
            if len(rets) >= 3:
                mu = np.mean(rets)
                std = np.std(rets)
                if std > _EPS:
                    features[12] = np.mean(((np.array(rets) - mu) / std) ** 3)

        # Feature 13: Price position in range (0=bottom, 1=top)
        if len(long_prices) >= 2:
            hi_30 = max(long_prices)
            lo_30 = min(long_prices)
            rng = hi_30 - lo_30
            if rng > _EPS:
                features[13] = (current_price - lo_30) / rng

        # ── Cost context ──

        # Feature 14: Spread (bps)
        spread = self._last_spread_bps
        if state and "spread_bps" in state:
            spread = float(state.get("spread_bps", 0) or 0)
        features[14] = spread

        # Feature 15: Vol-over-cost ratio
        cost_bps = self._maker_fee_bps + self._taker_fee_bps + spread
        features[15] = vol_5s / max(cost_bps, _EPS)

        return features

    @staticmethod
    def from_row(row: dict, taker_fee_bps: float = 0.5, maker_fee_bps: float = 0.2) -> np.ndarray:
        """Compute features from a signals table row (backtest mode).

        Features that require raw trade data (5-9, 12) are set to 0 (unavailable).
        The model should be trained with feature dropout to handle this.
        """
        features = np.zeros(N_FEATURES, dtype=np.float64)

        cvd = float(row.get("cvd_delta_5s", 0) or 0)
        trend = float(row.get("trend_bps", 0) or 0)
        vol = float(row.get("realized_vol_bps", 0) or 0)
        range_30s = float(row.get("range_30s_bps", 0) or 0)
        spread = float(row.get("spread_bps", 0) or 0)

        # Group 1
        features[0] = cvd
        features[1] = trend
        features[2] = vol
        features[3] = range_30s

        # Group 2 (from DB if available, else 0)
        features[4] = float(row.get("buy_vol_ratio", 0) or 0)
        features[5] = float(row.get("trade_intensity", 0) or 0)
        features[6] = float(row.get("avg_size_imbalance", 0) or 0)
        features[7] = float(row.get("large_trade_imbalance", 0) or 0)
        features[8] = float(row.get("cvd_accel", 0) or 0)

        # Group 3 (derive what we can)
        trend_30s = float(row.get("trend_30s_bps", 0) or 0)
        features[9] = trend_30s
        features[10] = trend / max(abs(trend_30s), 0.1)  # trend ratio
        features[11] = vol / max(range_30s, 0.1)  # vol ratio (approx)
        features[12] = float(row.get("return_skew_30s", 0) or 0)
        features[13] = float(row.get("price_pos_in_range", 0) or 0)

        # Group 4
        features[14] = spread
        cost_bps = maker_fee_bps + taker_fee_bps + spread
        features[15] = vol / max(cost_bps, _EPS)

        return features

#!/usr/bin/env python3
"""AU2 Regime-Adaptive Signal Layer.

Empirical finding on 6h BTCUSDT data (Apr 2026):
  - Macro downtrend  + momentum  → +0.79 bps/trade @ 30s (maker-both 0.4 bps cost)
  - Ranging market   + mean-rev  → +0.79 bps/trade @ 30s (maker-both 0.4 bps cost)
  - Both strategies break even at 82% maker TP fill rate with mixed costs (2.1 bps avg)

Logic:
  1. Compute 5-min macro trend (rolling price return over 300s window)
  2. Classify regime: TREND_DOWN, TREND_UP, RANGING
  3. In trend: follow momentum (trend direction = trade direction)
  4. In range: fade local 30s trend (mean reversion)
  5. Confidence scales with alignment between macro and micro signals

Designed to:
  - Run on top of existing PositionManager / TradeGate
  - Plug into SignalProcessor.score() as V3 layer
  - Use maker-both order mode for cost efficiency
"""
from __future__ import annotations
from dataclasses import dataclass, field
from collections import deque
from typing import Optional, Tuple
import numpy as np


# ── Regime thresholds ──────────────────────────────────────────────────────────
TREND_THRESHOLD_BPS = 5.0    # 5-min return must exceed this to classify as trending (lowered from 10.0)
MACRO_WINDOW_S = 300.0       # 5-minute macro trend window
MIN_VOL_BPS = 5.0            # skip signals in extremely low-vol environments


@dataclass
class RegimeSignal:
    """Output of the regime-adaptive signal layer."""
    direction: str          # "LONG" | "SHORT" | "FLAT"
    regime: str             # "TREND_DOWN" | "TREND_UP" | "RANGING" | "UNKNOWN"
    macro_trend_bps: float  # 5-min rolling return (bps)
    local_trend_bps: float  # 30s local trend (bps)
    confidence: float       # [0, 1]
    score: float            # backward-compat score [-10, 10]
    vol_bps: float          # realized vol estimate


class RegimeAdaptiveSignal:
    """Stateful regime-adaptive signal detector.

    Feed price ticks via on_tick(), then call compute() at each signal evaluation.
    """

    def __init__(self,
                 trend_threshold_bps: float = TREND_THRESHOLD_BPS,
                 macro_window_s: float = MACRO_WINDOW_S,
                 min_vol_bps: float = MIN_VOL_BPS):
        self.trend_threshold_bps = trend_threshold_bps
        self.macro_window_s = macro_window_s
        self.min_vol_bps = min_vol_bps

        # Circular price/time buffer (keep last 15 minutes worth at ~1 tick/sec)
        self._ts_buf: deque = deque(maxlen=900)
        self._px_buf: deque = deque(maxlen=900)

    def on_tick(self, ts: float, price: float) -> None:
        """Feed a price observation (call on every trade or bar)."""
        self._ts_buf.append(ts)
        self._px_buf.append(price)

    def _lookup_price_ago(self, now_ts: float, seconds_ago: float) -> Optional[float]:
        """Find price closest to (now_ts - seconds_ago) in the buffer."""
        target_ts = now_ts - seconds_ago
        buf_ts = list(self._ts_buf)
        buf_px = list(self._px_buf)
        if not buf_ts:
            return None
        # Binary search for target timestamp
        lo, hi = 0, len(buf_ts) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if buf_ts[mid] < target_ts:
                lo = mid + 1
            else:
                hi = mid
        return buf_px[lo]

    def compute(self, ts: float, price: float, trend30_bps: float,
                range30_bps: float) -> RegimeSignal:
        """Compute regime-adaptive signal.

        Args:
            ts: current timestamp (Unix seconds)
            price: current mid price
            trend30_bps: 30s local trend (bps), pre-computed from FeatureEngine
            range30_bps: 30s price range (bps), proxy for realized vol

        Returns:
            RegimeSignal with direction, regime, confidence, score
        """
        # Always feed tick
        self.on_tick(ts, price)

        # Skip if vol is too low
        vol_bps = range30_bps
        if vol_bps < self.min_vol_bps:
            return RegimeSignal("FLAT", "UNKNOWN", 0.0, trend30_bps, 0.0, 0.0, vol_bps)

        # Macro trend: 5-min rolling return
        px_5m_ago = self._lookup_price_ago(ts, self.macro_window_s)
        if px_5m_ago is None or px_5m_ago <= 0:
            return RegimeSignal("FLAT", "UNKNOWN", 0.0, trend30_bps, 0.0, 0.0, vol_bps)

        macro_trend = (price - px_5m_ago) / px_5m_ago * 10_000  # bps

        # Classify regime
        if macro_trend < -self.trend_threshold_bps:
            regime = "TREND_DOWN"
        elif macro_trend > self.trend_threshold_bps:
            regime = "TREND_UP"
        else:
            regime = "RANGING"

        # Select strategy
        if regime == "TREND_DOWN":
            # Follow momentum: price falling → go SHORT
            direction = "SHORT" if trend30_bps < 0 else "LONG" if trend30_bps > 0 else "FLAT"
            # Actually, momentum in a downtrend: align with the macro direction
            # If local 30s trend also confirms the down move, high confidence SHORT
            if trend30_bps < 0:
                direction = "SHORT"  # local trend confirms macro downtrend
                confidence = min(1.0, (abs(macro_trend) / self.trend_threshold_bps) * 0.5
                                 + (abs(trend30_bps) / 5.0) * 0.5)
            elif trend30_bps > 2.0:
                direction = "FLAT"   # local counter-trend, skip
                confidence = 0.0
            else:
                direction = "SHORT"  # weak local, follow macro
                confidence = min(0.6, abs(macro_trend) / self.trend_threshold_bps * 0.5)

        elif regime == "TREND_UP":
            # Follow momentum: price rising → go LONG
            if trend30_bps > 0:
                direction = "LONG"   # local confirms macro uptrend
                confidence = min(1.0, (abs(macro_trend) / self.trend_threshold_bps) * 0.5
                                 + (abs(trend30_bps) / 5.0) * 0.5)
            elif trend30_bps < -2.0:
                direction = "FLAT"   # local counter-trend, skip
                confidence = 0.0
            else:
                direction = "LONG"   # weak local, follow macro
                confidence = min(0.6, abs(macro_trend) / self.trend_threshold_bps * 0.5)

        else:  # RANGING
            # Mean reversion: fade the local move
            if abs(trend30_bps) < 1.0:
                direction = "FLAT"   # too quiet to trade
                confidence = 0.0
            elif trend30_bps > 0:
                direction = "SHORT"  # local push up → expect revert
                confidence = min(1.0, abs(trend30_bps) / 5.0 * 0.7)
            else:
                direction = "LONG"   # local push down → expect revert
                confidence = min(1.0, abs(trend30_bps) / 5.0 * 0.7)

        # Backward-compat score: [-10, 10], >0 = SHORT, <0 = LONG
        score_mag = confidence * 10.0
        if direction == "SHORT":
            score = score_mag
        elif direction == "LONG":
            score = -score_mag
        else:
            score = 0.0

        return RegimeSignal(
            direction=direction,
            regime=regime,
            macro_trend_bps=macro_trend,
            local_trend_bps=trend30_bps,
            confidence=confidence,
            score=score,
            vol_bps=vol_bps,
        )

    @classmethod
    def from_features(cls, features_dict: dict) -> "RegimeAdaptiveSignal":
        """Create instance and inject one pre-computed row (for backtest use)."""
        inst = cls()
        return inst


def score_from_regime_signal(sig: RegimeSignal,
                              v2_result: Optional[dict] = None,
                              blend_alpha: float = 0.5) -> float:
    """Blend regime score with V2 ML score (if available).

    Args:
        sig: RegimeAdaptiveSignal output
        v2_result: dict from SignalModelV2.predict() (optional)
        blend_alpha: weight given to regime signal (0=pure V2, 1=pure regime)

    Returns:
        Blended score in [-10, 10]
    """
    regime_score = sig.score

    if v2_result is None or v2_result.get("direction") == "FLAT":
        return regime_score

    v2_score = v2_result.get("score", 0.0)
    # If V2 and regime disagree, be cautious (reduce to smaller magnitude)
    if np.sign(regime_score) != np.sign(v2_score) and abs(regime_score) > 1 and abs(v2_score) > 1:
        return 0.0  # conflicting signals → flat

    return blend_alpha * regime_score + (1.0 - blend_alpha) * v2_score

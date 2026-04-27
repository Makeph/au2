#!/usr/bin/env python3
"""AU2 CORE v6.2 — Patched, Stabilized & Signal V1 Ready
Corrections applied:
C1  : Regime enum cohérent partout
C2  : Classifier unique (suppression duplication)
C3  : Score clamp élargi [-10,10] + injection V1 probabiliste
H2  : ts=0 ne casse plus loss_pause
H6  : RR gate pondéré sur structure complète (TP1+TP2+Runner)
M2  : Fallback spread_bps → cfg.assume_spread_bps
H4/H5: Overlay toujours actif en backtest si fourni
"""
from __future__ import annotations
import math
import time
import sqlite3
import logging
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Deque, Dict, List, Optional, Tuple
log = logging.getLogger("au2_core")

# FeatureEngine import (module-level — must NOT be inside the hot loop)
try:
    from au2_feature_engine import FeatureEngine
except ImportError:
    FeatureEngine = None

# ─────────────────────────────────────────────────────────────────────
# SIGNAL V2 INTEGRATION (Cost-aware ML model)
# ─────────────────────────────────────────────────────────────────────
try:
    from au2_signal_v2 import SignalModelV2
    _signal_v2 = SignalModelV2()
    _HAS_V2 = True
    log.info("Signal V2 model loaded successfully")
except Exception:
    _signal_v2 = None
    _HAS_V2 = False

# SIGNAL V3 INTEGRATION (Regime-adaptive rule-based signal — preferred when confident)
# ─────────────────────────────────────────────────────────────────────
# ARCH CONSTRAINT — ONE BOT PER PROCESS
# _signal_v3 is a module-level singleton.  Its internal price buffer (_ts_buf,
# _px_buf) is mutated on every SignalProcessor.score() call via on_tick().
# If two LiveExecutor instances coexist in the same Python process they share
# this object and will corrupt each other's macro-trend history.
#
# Supported deployment: one bot process per Python interpreter.
# Au2Backtest.run() resets this global (see line ~527) before each backtest
# run so sequential backtests in the same process stay isolated.
# Live executors never reset it — intentionally, so history accumulates across
# the entire session.
try:
    from au2_signal_regime import RegimeAdaptiveSignal, score_from_regime_signal
    _signal_v3 = RegimeAdaptiveSignal()
    _HAS_V3 = True
    log.info("Signal V3 (regime-adaptive) loaded successfully")
except Exception:
    _signal_v3 = None
    _HAS_V3 = False

# ─────────────────────────────────────────────────────────────────────
# SIGNAL V1 INTEGRATION (Fallback)
# ─────────────────────────────────────────────────────────────────────
try:
    from au2_signal_v1 import predict_proba as _v1_predict
    _HAS_ML = True
except Exception:
    _v1_predict = None
    _HAS_ML = False

# ─────────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────────
def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


class MacroFilter:
    """Time-decay EMA trend filter — blocks counter-trend entries.

    LONG  blocked when current price < EMA (macro downtrend).
    SHORT blocked when current price > EMA (macro uptrend).

    Uses continuous-time EMA: alpha = 1 - exp(-dt / tau).
    Warmup period = one full tau before filtering activates.
    """

    def __init__(self, ema_seconds: float = 1800.0) -> None:
        self._tau        = max(ema_seconds, 1.0)
        self._ema: Optional[float] = None
        self._last_price: float    = 0.0
        self._last_ts:    float    = 0.0
        self._start_ts:   float    = 0.0

    def update(self, ts: float, price: float) -> None:
        self._last_price = price
        if self._ema is None:
            self._ema     = price
            self._last_ts = ts
            self._start_ts = ts
            return
        dt = max(ts - self._last_ts, 0.0)
        if dt > 0:
            alpha     = 1.0 - math.exp(-dt / self._tau)
            self._ema = alpha * price + (1.0 - alpha) * self._ema
            self._last_ts = ts

    @property
    def warmed_up(self) -> bool:
        return self._ema is not None and (self._last_ts - self._start_ts) >= self._tau

    def blocks(self, signal: str) -> bool:
        """True if macro trend contradicts the proposed entry direction."""
        if not self.warmed_up or self._ema is None:
            return False
        if signal == "LONG"  and self._last_price < self._ema:
            return True
        if signal == "SHORT" and self._last_price > self._ema:
            return True
        return False

# ─────────────────────────────────────────────────────────────────────
# ENUMS & DATA
# ─────────────────────────────────────────────────────────────────────
class Regime(str, Enum):
    TREND="TREND"; FLOW="FLOW"; MEAN_REVERT="MEAN_REVERT"; LIQUIDATION="LIQUIDATION"; CHOP="CHOP"

class RiskState(str, Enum):
    GREEN="GREEN"; AMBER="AMBER"; RED="RED"

class PositionEvent(str, Enum):
    OPEN="OPEN"; TP1_HIT="TP1_HIT"; TP2_HIT="TP2_HIT"; SL_UPDATED_BE="SL_UPDATED_BE"
    TRAIL_UPDATED="TRAIL_UPDATED"; EXIT_SL="EXIT_SL"; EXIT_TIME="EXIT_TIME"; EXIT_BE_FALLBACK="EXIT_BE_FALLBACK"

@dataclass
class SignalQuality:
    signal: str="FLAT"; score: float=0.0; confidence: float=0.0; regime_quality: float=1.0
    eff: float=0.0; should_trade: bool=False; acc_ok: bool=True; coh_ok: bool=True

@dataclass
class PositionFill:
    ts: float; price: float; qty: float; side: str; event: PositionEvent
    pnl_usd: float=0.0; pnl_per_unit: float=0.0; remaining_qty: float=0.0; slippage_applied_bps: float=0.0

@dataclass
class PositionState:
    side: str; exec_price: float; entry_ts: float; initial_qty: float; remaining_qty: float
    score: float; sl_price: float; tp1_price: float; tp2_price: float; peak_price: float; trough_price: float
    regime: str; confidence: float; profile: object; trail_price: float
    tp1_done: bool=False; tp2_done: bool=False; runner_active: bool=False

@dataclass
class TradeResult:
    signal: str; entry_price: float; entry_ts: float; exit_price: float; exit_ts: float
    pnl_usd: float; hold_seconds: float; exit_reason: str; entry_score: float
    confidence: float; regime: str; regime_quality: float=1.0; qty: float=0.0

@dataclass
class BacktestMetrics:
    total_trades: int=0; wins: int=0; losses: int=0; win_rate: float=0.0
    total_pnl: float=0.0; avg_pnl: float=0.0; profit_factor: float=0.0
    max_dd_usd: float=0.0; max_dd_pct: float=0.0; avg_hold_seconds: float=0.0; expectancy: float=0.0
    by_exit_reason: dict=field(default_factory=dict); by_confidence: dict=field(default_factory=dict)
    by_regime: dict=field(default_factory=dict); filtered_vs_taken: int=0; total_evaluated: int=0
    rejection_counts: dict=field(default_factory=dict); avg_effective_score: float=0.0

@dataclass
class AdvancedScore:
    setup_score: float=0.0; context_score: float=0.0; execution_score: float=0.0
    prop_score: float=1.0; final_score: float=0.0; reason: str=""

@dataclass
class TradeDecision:
    approved: bool=False; reason: str=""; log: dict=field(default_factory=dict)

@dataclass(frozen=True)
class PropProfile:
    name: str="PERSONAL"; max_daily_dd_pct: float=5.0; max_total_dd_pct: float=10.0
    daily_trade_cap: int=10; min_rr: float=1.0; max_risk_pct: float=1.0

PROP_FTMO_SAFE       = PropProfile("FTMO_SAFE", max_daily_dd_pct=4.0, max_total_dd_pct=8.0, daily_trade_cap=6, min_rr=1.5, max_risk_pct=0.5)
PROP_FUTURES_CUSHION = PropProfile("FUTURES_CUSHION", max_daily_dd_pct=3.5, max_total_dd_pct=7.0, daily_trade_cap=8, min_rr=1.2, max_risk_pct=0.75)
PROP_PERSONAL        = PropProfile("PERSONAL", max_daily_dd_pct=5.0, max_total_dd_pct=10.0, daily_trade_cap=10, min_rr=1.0, max_risk_pct=1.0)

# ─────────────────────────────────────────────────────────────────────
# ENGINES
# ─────────────────────────────────────────────────────────────────────
class SlippageModel:
    @staticmethod
    def apply(price: float, side: str, spread: float, cfg) -> Tuple[float, float]:
        spread_component = spread * cfg.slippage_spread_ratio
        total_bps = cfg.slippage_fixed_bps + spread_component
        slip_amt = price * total_bps / 10_000
        exec_px = price + slip_amt if side == "LONG" else price - slip_amt
        return exec_px, total_bps

class RiskEngine:
    def __init__(self, cfg, start_equity: float = 10000.0):
        self.cfg = cfg; self.total_start_equity = start_equity; self.current_equity = start_equity
        self.day_start_equity = start_equity; self.last_day: str = ""
        self.consecutive_losses: int = 0; self.consecutive_wins: int = 0
        self._daily_trades: int = 0; self._loss_pause_until: float = 0.0; self._results: List[float] = []

    def evaluate(self, ts: float = 0.0) -> Tuple[RiskState, float, float]:
        total_dd = max((self.total_start_equity - self.current_equity) / max(self.total_start_equity, 1) * 100, 0.0)
        day_dd = max((self.day_start_equity - self.current_equity) / max(self.day_start_equity, 1) * 100, 0.0)
        if total_dd >= self.cfg.max_total_dd_pct or day_dd >= self.cfg.max_daily_dd_pct: return RiskState.RED, 0.0, 1.0
        if total_dd >= self.cfg.total_dd_amber_pct or day_dd >= self.cfg.daily_dd_amber_pct: return RiskState.AMBER, 0.70, 1.05
        return RiskState.GREEN, 1.0, 1.0

    def can_trade(self, ts: float) -> bool:
        # ✅ H2: ts=0 ne bypass plus la pause
        if self._loss_pause_until > 0:
            check_ts = ts if ts > 0 else time.time()
            if check_ts < self._loss_pause_until:
                return False
        return self._daily_trades < self.cfg.max_daily_trades

    def update_equity(self, pnl: float) -> None: self.current_equity += pnl

    def record_result(self, pnl: float, ts: float = 0.0) -> None:
        if pnl < 0: self.consecutive_losses += 1; self.consecutive_wins = 0
        else: self.consecutive_wins += 1
        if self.consecutive_wins >= 2: self.consecutive_losses = 0
        self._results.append(pnl)
        if len(self._results) > 20: self._results.pop(0)

    def record_trade(self, ts: float) -> None: self._daily_trades += 1

    def recent_wr(self) -> float:
        window = self._results[-10:]
        return sum(1 for r in window if r > 0) / len(window) if window else 0.5

    def reset_day(self, day: str) -> None: self.last_day = day; self.day_start_equity = self.current_equity; self._daily_trades = 0

    def trigger_loss_pause(self, ts: float) -> None:
        # ✅ H2: ts=0 → fallback time.time()
        if self.consecutive_losses >= self.cfg.max_consecutive_losses:
            safe_ts = ts if ts > 0 else time.time()
            self._loss_pause_until = safe_ts + self.cfg.loss_pause_seconds

    def allowed_risk(self, base_risk_usd: float, prop: Optional[PropProfile] = None) -> float:
        if prop: return max(min(base_risk_usd, self.current_equity * prop.max_risk_pct / 100.0), 0.0)
        return clamp(base_risk_usd, self.cfg.min_risk_usd, self.cfg.max_risk_usd)

    def should_block_trade(self, ts: float, prop: Optional[PropProfile] = None) -> Tuple[bool, str]:
        state, _, _ = self.evaluate(ts)
        if state == RiskState.RED: return True, "blocked_by_core_risk"
        if not self.can_trade(ts): return True, "blocked_by_core_trade_limits"
        if prop:
            day_dd = max((self.day_start_equity - self.current_equity) / max(self.day_start_equity, 1.0) * 100.0, 0.0)
            if day_dd >= prop.max_daily_dd_pct: return True, "blocked_by_prop_daily_dd"
            total_dd = max((self.total_start_equity - self.current_equity) / max(self.total_start_equity, 1.0) * 100.0, 0.0)
            if total_dd >= prop.max_total_dd_pct: return True, "blocked_by_prop_total_dd"
            if self._daily_trades >= prop.daily_trade_cap: return True, "blocked_by_prop_trade_cap"
        return False, ""

class TradeBuilder:
    def __init__(self, signal: str, entry_price: float, entry_ts: float,
                 score: float, regime, qty: float, confidence: float, regime_quality: float = 1.0):
        self.signal = signal; self.entry_price = entry_price; self.entry_ts = entry_ts
        self.score = score; self.regime = regime.value if isinstance(regime, Regime) else str(regime)
        self.qty = qty; self.confidence = confidence; self.regime_quality = regime_quality
        self._pnl = 0.0; self._exit_price = entry_price; self._exit_ts = entry_ts

    def add_fill(self, fill: PositionFill) -> None:
        if fill.qty > 0: self._pnl += fill.pnl_usd; self._exit_price = fill.price; self._exit_ts = fill.ts

    @property
    def pnl_usd(self) -> float: return self._pnl

    def build(self, ts: float, exit_reason: str) -> TradeResult:
        return TradeResult(self.signal, self.entry_price, self.entry_ts, self._exit_price, ts,
                           self._pnl, ts - self.entry_ts, exit_reason, self.score, self.confidence,
                           self.regime, self.regime_quality, self.qty)

class SizingEngine:
    """Risk-adjusted position sizing used by the live executor."""
    @staticmethod
    def compute_risk_usd(equity: float, cfg: "CoreConfig", confidence: float,
                         regime_quality: float, r_mult: float,
                         consecutive_losses: int) -> float:
        base = equity * cfg.risk_per_trade_pct / 100
        adj  = base * r_mult * clamp(regime_quality, 0.5, 1.2)
        if consecutive_losses >= 2:
            adj *= cfg.loss_penalty_risk_mult
        return clamp(adj, cfg.min_risk_usd, cfg.max_risk_usd)

@dataclass(frozen=True)
class CoreConfig:
    threshold: float = 4.00
    regime_multiplier: Dict[Regime, float] = field(default_factory=lambda: {Regime.TREND: 0.9, Regime.FLOW: 0.95, Regime.MEAN_REVERT: 1.1, Regime.LIQUIDATION: 0.85, Regime.CHOP: 1.5})
    regime_quality_multiplier: Dict[Regime, float] = field(default_factory=lambda: {Regime.TREND: 1.0, Regime.FLOW: 1.0, Regime.MEAN_REVERT: 0.9, Regime.LIQUIDATION: 1.05, Regime.CHOP: 0.8})
    risk_per_trade_pct: float = 1.0; max_risk_usd: float = 350.0; min_risk_usd: float = 15.0
    stop_loss_pct: float = 0.20; tp1_pct: float = 0.20; tp2_pct: float = 0.45
    tp1_ratio: float = 0.55; tp2_ratio: float = 0.25; runner_ratio: float = 0.20
    trailing_pct: float = 0.10; breakeven_trigger_bps: float = 4.0; breakeven_buffer_bps: float = 1.5
    max_hold_seconds: int = 900; cooldown_seconds: int = 30; confirmation_cycles: int = 2
    max_daily_trades: int = 8; min_vol_bps: float = 3.0; max_spread_bps: float = 4.0
    assume_spread_bps: float = 0.0
    min_confidence_threshold: float = 0.90; min_setup_quality: float = 0.50
    min_score_acceleration: float = 0.65
    cluster_window_s: int = 300; cluster_max_per_side: int = 3
    loss_pause_seconds: int = 600; max_consecutive_losses: int = 3; loss_penalty_risk_mult: float = 0.5
    equity_feedback_window: float = 0.03
    taker_fee_bps: float = 0.5; maker_fee_bps: float = 0.2; entry_fee_mode: str = "maker"
    slippage_fixed_bps: float = 0.5; slippage_spread_ratio: float = 0.35
    max_total_dd_pct: float = 10.0; max_daily_dd_pct: float = 5.0
    daily_dd_amber_pct: float = 3.5; daily_dd_red_pct: float = 4.5
    total_dd_amber_pct: float = 7.0; total_dd_red_pct: float = 9.0
    min_adv_score: float = 2.5
    macro_ema_seconds: int = 0  # 0 = disabled; 1800 = 30-min EMA macro trend filter
    def __post_init__(self): assert self.tp1_ratio + self.tp2_ratio + self.runner_ratio <= 1.01

@dataclass(frozen=True)
class RegimeExitProfile:
    trailing_pct: float = 0.10; max_hold_seconds: int = 120; runner_enabled: bool = True
    be_trigger_bps: float = 4.0; confidence_boost_trail: float = 1.0; confidence_penalize_trail: float = 0.8
    runner_conf_threshold: float = 0.90

REGIME_PROFILES: Dict[Regime, RegimeExitProfile] = {
    Regime.TREND:       RegimeExitProfile(0.14, 150, True,  5.0, 1.15, 0.85, runner_conf_threshold=0.92),
    Regime.FLOW:        RegimeExitProfile(0.12, 130, True,  4.0, 1.05, 0.90, runner_conf_threshold=0.93),  # be_trigger 3.0->4.0 (2026-04-27 rollback: FLOW PF=0.52 in apr20-26 with 3.0)
    Regime.MEAN_REVERT: RegimeExitProfile(0.08, 100, True,  4.0, 1.0,  0.80, runner_conf_threshold=0.91),
    Regime.LIQUIDATION: RegimeExitProfile(0.16, 110, False, 6.0, 1.0,  0.75),
    Regime.CHOP:        RegimeExitProfile(0.06,  80, False, 2.5, 1.0,  0.70),  # be_trigger 3.0->2.5 (2026-04-22 live tuning)
}

class SignalProcessor:
    # ✅ C1/C2: retourne Enum Regime, source unique de vérité
    @staticmethod
    def classify_regime(vol: float, trend: float, cvd: float) -> Regime:
        abs_trend = abs(trend)
        if vol > 20.0: return Regime.LIQUIDATION
        if vol < 2.0: return Regime.CHOP
        if abs_trend > 8.0 and vol > 5.0: return Regime.TREND
        if abs_trend > 4.0: return Regime.FLOW
        if abs_trend < 2.0 and vol > 4.0: return Regime.MEAN_REVERT
        return Regime.CHOP

    # ✅ C3 + V3/V2/V1: Regime-adaptive → Cost-aware ML → V1 → linear fallback
    @staticmethod
    def score(cvd: float, trend: float, vol: float, regime: Regime, cfg: CoreConfig,
              features=None, _v2_cache: dict = None,
              trend30_bps: float = 0.0, range30_bps: float = 0.0,
              ts: float = 0.0, price: float = 0.0) -> float:
        """Compute signal score. Cascade: V3 regime-adaptive → V2 ML → V1 → linear.
        _v2_cache: if provided, V2/V3 result dict is stored here for use by evaluate_quality.
        """
        # V3: regime-adaptive rule-based signal (most robust, no overfitting risk)
        if _HAS_V3 and _signal_v3 is not None and ts > 0 and price > 0 and range30_bps > 0:
            try:
                v3_sig = _signal_v3.compute(ts, price, trend30_bps, range30_bps)
                if v3_sig.direction != "FLAT" and v3_sig.confidence >= 0.5:
                    if _v2_cache is not None:
                        _v2_cache["direction"] = v3_sig.direction
                        _v2_cache["confidence"] = v3_sig.confidence
                        _v2_cache["expected_edge_bps"] = v3_sig.confidence * 1.2  # empirical ~1.2 bps gross edge
                        _v2_cache["regime_v3"] = v3_sig.regime
                    # Also blend with V2 if available
                    v2_score = 0.0
                    v2_result = None
                    if _HAS_V2 and _signal_v2 is not None and features is not None:
                        try:
                            v2_result = _signal_v2.predict(features)
                            v2_score = v2_result.get("score", 0.0)
                        except Exception:
                            pass
                    blended = score_from_regime_signal(v3_sig, v2_result, blend_alpha=0.7)
                    return blended
                elif v3_sig.regime in ("TREND_DOWN", "TREND_UP"):
                    # Strong macro trend detected but V3 is FLAT (local counter-move).
                    # Block V2/linear to avoid counter-trend entries.
                    return 0.0
            except Exception:
                pass

        # V2: cost-aware GBM (fallback)
        if _HAS_V2 and _signal_v2 is not None and features is not None:
            try:
                result = _signal_v2.predict(features)
                if _v2_cache is not None:
                    _v2_cache.update(result)
                return result["score"]
            except Exception:
                pass

        # V1: logistic regression fallback
        if _HAS_ML and _v1_predict is not None:
            try:
                state = {
                    "cvd_delta_5s": cvd,
                    "trend_bps": trend,
                    "realized_vol_bps": vol,
                    "range_30s_bps": 0.0
                }
                p = _v1_predict(state)
                return (p - 0.5) * 20.0 * cfg.regime_multiplier.get(regime, 1.0)
            except Exception:
                pass

        # Linear fallback
        return clamp(2.0 * (cvd/6.0) + 1.5 * (trend/5.0) + ((vol-5.0)/10.0), -10.0, 10.0) * cfg.regime_multiplier.get(regime, 1.0)

    @staticmethod
    def evaluate_quality(score: float, threshold: float, regime: Regime, cvd: float, trend: float, vol: float,
                         cfg: CoreConfig, last_score: float, v2_result: dict = None) -> Tuple[SignalQuality, Dict[str, bool]]:
        """Evaluate signal quality. If v2_result provided, direction is data-driven."""
        # V2: data-driven direction + confidence
        if v2_result and v2_result.get("direction", "FLAT") != "FLAT":
            sig = v2_result["direction"]
            conf = min(v2_result.get("confidence", 0.0) + 0.5, 1.5)  # shift: conf 0.5 = edge equals cost
            rq = cfg.regime_quality_multiplier.get(regime, 1.0)
            eff = abs(score) * conf * rq
            # V2 already gates on cost; simplify quality checks
            acc_ok = True  # V2 handles this internally
            coh_ok = v2_result.get("expected_edge_bps", 0) > 0
            profile = REGIME_PROFILES.get(regime, REGIME_PROFILES[Regime.FLOW])
            runner_gate = conf >= profile.runner_conf_threshold if profile.runner_enabled else True
            trade_ok = coh_ok and conf >= cfg.min_confidence_threshold and rq >= 0.75 and runner_gate
            return SignalQuality(sig, score, conf, rq, eff, trade_ok, acc_ok, coh_ok), {"v2_flat": False}

        # Legacy path: threshold-based direction
        abs_score = abs(score); sig = "SHORT" if score >= threshold else ("LONG" if score <= -threshold else "FLAT")
        if sig == "FLAT": return SignalQuality("FLAT", score, 0.0, 1.0, 0.0, False, False, False), {"no_signal": True}
        conf = min(abs_score / threshold, 1.5); rq = cfg.regime_quality_multiplier.get(regime, 1.0); eff = abs_score * conf * rq
        acc_ok = abs(score) >= abs(last_score) * cfg.min_score_acceleration if abs(last_score) > 2.0 else True
        coh_ok = abs(trend) > 1.0 or vol > 4.0
        profile = REGIME_PROFILES.get(regime, REGIME_PROFILES[Regime.FLOW])
        runner_gate = conf >= profile.runner_conf_threshold if profile.runner_enabled else True
        trade_ok = eff >= cfg.min_setup_quality and conf >= cfg.min_confidence_threshold and rq >= 0.75 and acc_ok and coh_ok and runner_gate
        return SignalQuality(sig, score, conf, rq, eff, trade_ok, acc_ok, coh_ok), {"acc_fail": not acc_ok, "coh_fail": not coh_ok, "runner_fail": not runner_gate, "regime_weak": rq < 0.75}

    @staticmethod
    def determine_signal(score: float, threshold: float) -> str:
        if score >= threshold:  return "SHORT"
        if score <= -threshold: return "LONG"
        return "FLAT"

    @staticmethod
    def compute_advanced_score(score: float, threshold: float, regime: Regime, cvd: float, trend: float, vol: float, cfg: CoreConfig, last_score: float, prop: Optional[PropProfile] = None) -> AdvancedScore:
        abs_score = abs(score)
        if abs_score < threshold * 0.5: return AdvancedScore(reason="score_too_low")
        setup = clamp(abs_score / max(threshold, 1e-9) * 5.0, 0.0, 10.0)
        rq = cfg.regime_quality_multiplier.get(regime, 1.0); coh = 1.0 if (abs(trend) > 1.0 or vol > 4.0) else 0.5
        context = clamp(rq * coh, 0.0, 1.0)
        execution = clamp((abs_score / max(abs(last_score), 1e-9)) / max(cfg.min_score_acceleration, 1e-9), 0.0, 1.0) if abs(last_score) > 2.0 else 1.0
        prop_mult = 0.70 if (regime == Regime.LIQUIDATION and prop and prop.max_risk_pct < 0.75) else (0.60 if regime == Regime.CHOP else 1.0)
        final = setup * context * execution * prop_mult
        reason = "context_weak" if context < 0.50 else ("execution_weak" if execution < cfg.min_score_acceleration else ("prop_constraint" if prop_mult < 0.80 else ""))
        return AdvancedScore(setup, context, execution, prop_mult, final, reason)

class SelectivityEngine:
    def __init__(self, cfg: CoreConfig): self.cfg = cfg; self.recent_entries: Deque[Tuple[float, str]] = deque()
    def _prune_entries(self, ts: float):
        cutoff = ts - self.cfg.cluster_window_s
        while self.recent_entries and self.recent_entries[0][0] < cutoff: self.recent_entries.popleft()
    def record_entry(self, ts: float, side: str): self._prune_entries(ts); self.recent_entries.append((ts, side))
    def is_clustered(self, ts: float, side: str) -> bool:
        self._prune_entries(ts)
        return sum(1 for _, s in self.recent_entries if s == side) >= self.cfg.cluster_max_per_side
    def compute_dynamic_multiplier(self, equity: float, day_start_equity: float, recent_wr: float, daily_dd_pct: float) -> Tuple[float, float]:
        if day_start_equity <= 0: return 1.0, 1.0
        eq_slope = (equity - day_start_equity) / day_start_equity
        thr_adj = 1.0 - clamp(eq_slope * 5.0, -0.15, 0.15); risk_adj = 1.0 + clamp(eq_slope * 4.0, -0.20, 0.20)
        if daily_dd_pct > 2.0: thr_adj *= (1.0 + clamp((daily_dd_pct - 2.0) / 3.0, 0.0, 1.0) * 0.25); risk_adj *= (1.0 - clamp((daily_dd_pct - 2.0) / 3.0, 0.0, 1.0) * 0.20)
        if recent_wr < 0.40: thr_adj *= 1.08; risk_adj *= 0.90
        return clamp(thr_adj, 0.85, 1.20), clamp(risk_adj, 0.70, 1.15)

class TradeGate:
    def __init__(self, cfg: CoreConfig, prop: Optional[PropProfile] = None): self.cfg = cfg; self.prop = prop
    def evaluate(self, q: SignalQuality, adv: AdvancedScore, risk_mult: float, spread: float, vol: float, signal_count: int, cluster: bool, ts: float, last_trade_ts: float) -> TradeDecision:
        if q.signal == "FLAT": return TradeDecision(False, "flat_signal")
        if spread > self.cfg.max_spread_bps: return TradeDecision(False, "spread_too_wide")
        if vol < self.cfg.min_vol_bps: return TradeDecision(False, "vol_too_low")
        if ts - last_trade_ts < self.cfg.cooldown_seconds: return TradeDecision(False, "cooldown")
        if cluster: return TradeDecision(False, "clustered")
        if signal_count < self.cfg.confirmation_cycles: return TradeDecision(False, "confirmation_pending")
        if not q.should_trade: return TradeDecision(False, "acc_fail" if not q.acc_ok else ("coh_fail" if not q.coh_ok else "quality_fail"))
        if adv.final_score < self.cfg.min_adv_score: return TradeDecision(False, "adv_score_low")
        
        # ✅ H6: RR pondéré sur la structure complète (TP1 + TP2 + Runner)
        if self.prop:
            avg_tp_pct = (self.cfg.tp1_pct * self.cfg.tp1_ratio) + (self.cfg.tp2_pct * (self.cfg.tp2_ratio + self.cfg.runner_ratio))
            expected_rr = avg_tp_pct / max(self.cfg.stop_loss_pct, 1e-9)
            if expected_rr < self.prop.min_rr:
                return TradeDecision(False, "prop_rr_fail")
                
        return TradeDecision(True, "approved")

class PositionManager:
    def __init__(self, cfg: CoreConfig): self.cfg = cfg; self.pos: Optional[PositionState] = None; self.logs: List[PositionFill] = []
    def open(self, ts: float, price: float, score: float, qty: float, side: str, spread: float, regime: Regime, confidence: float):
        exec_px, slip = SlippageModel.apply(price, side, spread, self.cfg)
        p = REGIME_PROFILES.get(regime, REGIME_PROFILES[Regime.FLOW])
        sl = exec_px * (1 - self.cfg.stop_loss_pct/100) if side=="LONG" else exec_px * (1 + self.cfg.stop_loss_pct/100)
        tp1 = exec_px * (1 + self.cfg.tp1_pct/100) if side=="LONG" else exec_px * (1 - self.cfg.tp1_pct/100)
        tp2 = exec_px * (1 + self.cfg.tp2_pct/100) if side=="LONG" else exec_px * (1 - self.cfg.tp2_pct/100)
        trail_mult = p.confidence_boost_trail if confidence > 1.05 else p.confidence_penalize_trail
        trail = exec_px * (1 - p.trailing_pct*trail_mult/100) if side=="LONG" else exec_px * (1 + p.trailing_pct*trail_mult/100)
        self.pos = PositionState(side, exec_px, ts, qty, qty, score, sl, tp1, tp2, exec_px, exec_px, regime.value, confidence, p, trail)
        self.logs.append(PositionFill(ts, exec_px, qty, side, PositionEvent.OPEN, remaining_qty=qty, slippage_applied_bps=slip))
        return self.pos, exec_px

    def update(self, ts: float, price: float, spread: float = 0.0):
        if not self.pos: return []
        p = self.pos; hits = []
        hold_limit = clamp(p.profile.max_hold_seconds * (0.7 if p.confidence < 0.85 else (1.25 if p.confidence > 1.15 else 1.0)), 40, 300)
        if p.side == "LONG":
            p.peak_price = max(p.peak_price, price)
            tp1_ok, tp2_ok, sl_ok = price>=p.tp1_price, price>=p.tp2_price, price<=p.sl_price
            trail = p.peak_price * (1 - (p.profile.trailing_pct * (1.1 if p.confidence > 1.05 else 1.0)) / 100)
        else:
            p.trough_price = min(p.trough_price, price)
            tp1_ok, tp2_ok, sl_ok = price<=p.tp1_price, price<=p.tp2_price, price>=p.sl_price
            trail = p.trough_price * (1 + (p.profile.trailing_pct * (1.1 if p.confidence > 1.05 else 1.0)) / 100)

        if not p.tp1_done and tp1_ok:
            q = p.initial_qty * self.cfg.tp1_ratio; pnl = self._pnl(p.side, p.exec_price, price, q, spread, exit_is_tp=True)
            hits.append(PositionFill(ts, price, q, p.side, PositionEvent.TP1_HIT, pnl, pnl/q, p.remaining_qty-q))
            p.tp1_done = True; p.remaining_qty = max(p.remaining_qty-q, 0.0)
            be_buf = self.cfg.breakeven_buffer_bps * 1.5 if p.confidence < 0.9 else self.cfg.breakeven_buffer_bps
            p.sl_price = p.exec_price + p.exec_price*be_buf/10000 if p.side=="LONG" else p.exec_price - p.exec_price*be_buf/10000
            hits.append(PositionFill(ts, p.sl_price, 0, p.side, PositionEvent.SL_UPDATED_BE))

        if p.tp1_done and not p.tp2_done and tp2_ok:
            q = p.initial_qty * self.cfg.tp2_ratio; pnl = self._pnl(p.side, p.exec_price, price, q, spread, exit_is_tp=True)
            hits.append(PositionFill(ts, price, q, p.side, PositionEvent.TP2_HIT, pnl, pnl/q, p.remaining_qty-q))
            p.tp2_done = True; p.remaining_qty = max(p.remaining_qty-q, 0.0)
            p.runner_active = p.profile.runner_enabled and p.confidence > 0.85
            p.trail_price = trail if p.runner_active else (p.exec_price * 1.0002 if p.side=="LONG" else p.exec_price * 0.9998)
            if p.runner_active and ((p.side=="LONG" and trail > p.sl_price) or (p.side=="SHORT" and trail < p.sl_price)): p.sl_price = p.trail_price = trail

        reason = ""
        if sl_ok: reason = "EXIT_SL"
        elif (p.side=="LONG" and (p.peak_price-p.exec_price)/p.exec_price*10000 >= p.profile.be_trigger_bps and 0 < (price-p.exec_price)/p.exec_price*10000 <= self.cfg.breakeven_buffer_bps) or \
             (p.side=="SHORT" and (p.exec_price-p.trough_price)/p.exec_price*10000 >= p.profile.be_trigger_bps and 0 < (p.exec_price-price)/p.exec_price*10000 <= self.cfg.breakeven_buffer_bps): reason="EXIT_BE_FALLBACK"
        if ts - p.entry_ts >= hold_limit: reason = "EXIT_TIME"

        if reason:
            # EXIT_TIME and EXIT_BE_FALLBACK use maker limit close (post limit at mid),
            # SL always taker (market order for guaranteed fill)
            is_tp_like = reason in ("EXIT_TIME", "EXIT_BE_FALLBACK")
            pnl = self._pnl(p.side, p.exec_price, price, p.remaining_qty, spread, exit_is_tp=is_tp_like)
            hits.append(PositionFill(ts, price, p.remaining_qty, p.side, PositionEvent[reason], pnl, pnl/p.remaining_qty if p.remaining_qty>0 else 0, 0))
            self.pos = None; return hits

        self.logs.extend(hits); return hits

    def _pnl(self, side, entry, exit, qty, spread, exit_is_tp: bool = False):
        entry_fee = self.cfg.maker_fee_bps if self.cfg.entry_fee_mode == "maker" else self.cfg.taker_fee_bps
        # TP exits use maker limit orders; SL/time exits use taker market orders
        if self.cfg.entry_fee_mode == "maker" and exit_is_tp:
            exit_fee = self.cfg.maker_fee_bps
        else:
            exit_fee = self.cfg.taker_fee_bps
        fee_cost = entry*qty * entry_fee / 10000 + exit*qty * exit_fee / 10000
        return (exit-entry)*qty - fee_cost if side=="LONG" else (entry-exit)*qty - fee_cost

# ─────────────────────────────────────────────────────────────────────
# BACKTEST ENGINE
# ─────────────────────────────────────────────────────────────────────
class Au2Backtest:
    def __init__(self, cfg: CoreConfig, overlay: Optional[object] = None, prop: Optional[PropProfile] = None):
        self.cfg = cfg; self.overlay = overlay; self._prop = prop
        self.risk = RiskEngine(cfg); self.pm = PositionManager(cfg); self.sel = SelectivityEngine(cfg)
        self.trades: List[TradeResult] = []; self.events: List[PositionFill] = []
        self.last_trade_ts = 0.0; self.signal_side = ""; self.signal_count = 0; self.signal_ts = 0.0
        self.builder: Optional[TradeBuilder] = None; self._last_score = 0.0; self._eval_count = 0; self._take_count = 0
        self._macro: Optional[MacroFilter] = (
            MacroFilter(cfg.macro_ema_seconds) if cfg.macro_ema_seconds > 0 else None
        )
        self._rejections: Dict[str, int] = defaultdict(int)
        self._gate = TradeGate(cfg, prop); self._prop = prop; self._trade_log: List[dict] = []

    def run(self, db_path: str) -> Tuple[List[TradeResult], List[PositionFill], BacktestMetrics]:
        if self._prop:
            rr = (self.cfg.tp1_pct / 100.0) / max(self.cfg.stop_loss_pct / 100.0, 1e-9)
            if rr < self._prop.min_rr: raise ValueError(f"Incompatible config: RR {rr:.2f} < prop.min_rr {self._prop.min_rr}")

        # Reset module-level V3 signal state so sequential runs don't contaminate each other
        global _signal_v3, _HAS_V3
        if _HAS_V3:
            try:
                _signal_v3 = RegimeAdaptiveSignal()
            except Exception:
                pass

        conn = sqlite3.connect(db_path); conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute("SELECT * FROM signals ORDER BY ts ASC").fetchall()
        except sqlite3.OperationalError:
            log.warning("SELECT * failed, falling back to minimal columns.")
            rows = conn.execute("SELECT ts, price, cvd_delta_5s, trend_bps, realized_vol_bps FROM signals ORDER BY ts ASC").fetchall()
        conn.close()

        peak_eq = self.risk.total_start_equity; max_dd = 0.0
        self._gate.prop = self._prop
        for row in rows:
            ts = float(row["ts"]); price = float(row["price"])
            if not price or price<=0: continue
            if self._macro: self._macro.update(ts, price)
            day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
            if day != self.risk.last_day:
                self.risk.reset_day(day)
                if self.overlay and hasattr(self.overlay, "reset_day"): self.overlay.reset_day()

            r_state, r_mult, _ = self.risk.evaluate(ts)

            # ── Position management MUST run regardless of trade-limit checks ──
            # Open trades need to update/exit even when new entries are blocked.
            if self.pm.pos:
                # ✅ M2: fallback explicite sur cfg.assume_spread_bps
                try:
                    spread = float(row["spread_bps"] or self.cfg.assume_spread_bps)
                except (IndexError, KeyError):
                    spread = self.cfg.assume_spread_bps
                hits = self.pm.update(ts, price, spread)
                for h in hits:
                    self.events.append(h)
                    if h.qty > 0:
                        self.risk.update_equity(h.pnl_usd)
                        if self.risk.current_equity > peak_eq: peak_eq = self.risk.current_equity
                        dd = peak_eq - self.risk.current_equity
                        if dd > max_dd: max_dd = dd
                        if self.builder: self.builder.add_fill(h)

                if not self.pm.pos:
                    reason = self.events[-1].event.name if self.events else "EXIT_TIME"
                    pnl = 0.0
                    if self.builder:
                        trade = self.builder.build(ts, reason)
                        self.trades.append(trade)
                        pnl = trade.pnl_usd
                    self.risk.record_result(pnl, ts)
                    self.risk.trigger_loss_pause(ts)
                    if self.overlay and hasattr(self.overlay, "update_equity"): self.overlay.update_equity(pnl, ts)
                    self.last_trade_ts = ts; self.builder = None
                continue

            # ── New-entry checks (only reached when no position is open) ──
            if not self.risk.can_trade(ts): self._rejections["blocked_by_core_trade_limits"] += 1; continue
            if r_state == RiskState.RED: self._rejections["blocked_by_core_risk"] += 1; continue
            if self.overlay and hasattr(self.overlay, "should_block"):
                ov_blocked, ov_reason = self.overlay.should_block(ts)
                if ov_blocked: self._rejections[ov_reason] += 1; continue

            day_dd_pct = max((self.risk.day_start_equity - self.risk.current_equity) / max(self.risk.day_start_equity, 1), 0.0) * 100.0
            dyn_t, dyn_r = self.sel.compute_dynamic_multiplier(self.risk.current_equity, self.risk.day_start_equity, self.risk.recent_wr(), day_dd_pct)
            eff_thr = self.cfg.threshold * dyn_t

            cvd, trend, vol = float(row["cvd_delta_5s"] or 0), float(row["trend_bps"] or 0), float(row["realized_vol_bps"] or 0)
            # ✅ M2
            try:
                spread = float(row["spread_bps"] or self.cfg.assume_spread_bps)
            except (IndexError, KeyError):
                spread = self.cfg.assume_spread_bps
            # ✅ C1/C2: classification unifiée via SignalProcessor (retourne Enum Regime)
            regime = SignalProcessor.classify_regime(vol, trend, cvd)
            # ✅ V2: cost-aware ML if model available
            _v2_cache = {}
            if _HAS_V2 and FeatureEngine is not None:
                try:
                    features = FeatureEngine.from_row(dict(row))
                except Exception:
                    features = None
            else:
                features = None
            # Extract V3 params from row
            try:
                trend30 = float(row["trend_30s_bps"] or 0)
                range30 = float(row["range_30s_bps"] or 0)
                row_ts = float(row["ts"] or 0)
                row_price = float(row["price"] or 0)
            except (IndexError, KeyError):
                trend30 = trend; range30 = vol; row_ts = 0.0; row_price = 0.0
            raw_score = SignalProcessor.score(cvd, trend, vol, regime, self.cfg, features=features, _v2_cache=_v2_cache,
                                              trend30_bps=trend30, range30_bps=range30, ts=row_ts, price=row_price)
            q, rej = SignalProcessor.evaluate_quality(raw_score, eff_thr, regime, cvd, trend, vol, self.cfg, self._last_score,
                                                       v2_result=_v2_cache if _v2_cache else None)
            adv = SignalProcessor.compute_advanced_score(raw_score, eff_thr, regime, cvd, trend, vol, self.cfg, self._last_score, self._prop)
            self._last_score = raw_score; self._eval_count += 1

            for k, v in rej.items():
                if v: self._rejections[k] += 1
            if q.signal == "FLAT": self.signal_side = ""; self.signal_count = 0; self.signal_ts = 0.0; continue

            if self._macro and self._macro.blocks(q.signal):
                self._rejections["macro_trend_block"] += 1; continue

            if q.signal == self.signal_side:
                if (ts - self.signal_ts) <= 3.0: self.signal_count += 1
                else: self.signal_count = 1; self.signal_ts = ts
            else: self.signal_side = q.signal; self.signal_count = 1; self.signal_ts = ts

            cluster = self.sel.is_clustered(ts, q.signal)
            if cluster: self._rejections["cluster"] += 1

            decision = self._gate.evaluate(q, adv, r_mult, spread, vol, self.signal_count, cluster, ts, self.last_trade_ts)
            if not decision.approved: self._rejections[decision.reason] += 1; continue

            blocked, block_reason = self.risk.should_block_trade(ts, self._prop)
            if blocked: self._rejections["blocked_by_core_loss_pause" if "loss_pause" in block_reason else "blocked_by_core_risk"] += 1; continue

            self._take_count += 1
            risk_adj = r_mult * dyn_r
            sl_dist = self.cfg.stop_loss_pct / 100.0
            entry_fee = self.cfg.maker_fee_bps if self.cfg.entry_fee_mode == "maker" else self.cfg.taker_fee_bps
            fee_rt = (entry_fee + self.cfg.taker_fee_bps) / 10_000.0
            base_risk = self.risk.current_equity * self.cfg.risk_per_trade_pct / 100.0 * risk_adj
            risk_usd = self.risk.allowed_risk(base_risk, self._prop)
            if risk_usd < self.cfg.min_risk_usd: self._rejections["risk_too_small"] += 1; continue

            qty = risk_usd / max(price * (sl_dist + fee_rt), 1e-9)
            pos, exec_px = self.pm.open(ts, price, raw_score, qty, q.signal, spread, regime, q.confidence)
            self.builder = TradeBuilder(q.signal, exec_px, ts, raw_score, regime, qty, q.confidence, q.regime_quality)
            self.sel.record_entry(ts, q.signal); self.risk.record_trade(ts)
            self.signal_side, self.signal_count = "", 0; self.last_trade_ts = ts

        return self.trades, self.events, self._build_metrics(peak_eq, max_dd)

    def _build_metrics(self, peak, max_dd):
        if not self.trades: return BacktestMetrics(rejection_counts=dict(self._rejections), total_evaluated=self._eval_count)
        w = [t for t in self.trades if t.pnl_usd>0]; l = [t for t in self.trades if t.pnl_usd<=0]
        total = sum(t.pnl_usd for t in self.trades); wr = len(w)/len(self.trades)
        pf = abs(sum(t.pnl_usd for t in w) / sum(t.pnl_usd for t in l)) if l and sum(t.pnl_usd for t in l)!=0 else float('inf')
        br = {}; rs = {}; cb = {}; eff_scores = []
        for t in self.trades:
            eff_scores.append(t.entry_score * t.confidence * t.regime_quality)
            br.setdefault(t.exit_reason, {"count":0, "pnl":0.0, "wins":0}); br[t.exit_reason]["count"]+=1; br[t.exit_reason]["pnl"]+=t.pnl_usd
            if t.pnl_usd>0: br[t.exit_reason]["wins"]+=1
            rs.setdefault(t.regime, {"count":0, "pnl":0.0, "wins":0}); rs[t.regime]["count"]+=1; rs[t.regime]["pnl"]+=t.pnl_usd
            if t.pnl_usd>0: rs[t.regime]["wins"]+=1
            bucket = "low" if t.confidence<0.95 else "med" if t.confidence<1.1 else "high"
            cb.setdefault(bucket, {"count":0, "pnl":0.0}); cb[bucket]["count"]+=1; cb[bucket]["pnl"]+=t.pnl_usd
        for k in rs: rs[k]["wr"] = rs[k]["wins"]/max(rs[k]["count"],1)
        exp = wr * (sum(t.pnl_usd for t in w)/len(w) if w else 0) - (1-wr) * abs(sum(t.pnl_usd for t in l)/len(l) if l else 0)
        return BacktestMetrics(len(self.trades), len(w), len(l), wr, total, total/len(self.trades), pf, max_dd, max_dd/peak*100, sum(t.hold_seconds for t in self.trades)/len(self.trades), exp, br, cb, rs, self._eval_count - self._take_count, self._eval_count, dict(self._rejections), sum(eff_scores)/len(eff_scores))
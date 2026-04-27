#!/usr/bin/env python3
"""AU2QWEN — Centralized trade decision logic.

Single entry point: build_trade_decision()
==========================================
  from au2_decision import build_trade_decision, TradeDecisionLog

  score = SignalProcessor.score(cvd, trend, vol, regime, cfg,
                                trend30_bps=t30, range30_bps=r30,
                                ts=ts, price=price, _v2_cache=v2c)
  dlog = build_trade_decision(score=score, eff_thr=eff_thr, ..., v2_result=v2c)
  if dlog.approved:
      # open position using dlog.signal, dlog.confidence, dlog.score

Why score is computed by the caller, not here
---------------------------------------------
RegimeAdaptiveSignal (V3) maintains internal price history via on_tick().
Computing the score inside build_trade_decision() would mean the caller
must NOT have computed it already — which breaks signal-direction peek
needed for confirmation counting.  Keeping score computation in the caller
avoids a double V3 state mutation.

What the caller is responsible for
-----------------------------------
  score          : SignalProcessor.score() — call exactly once per tick
  eff_thr        : cfg.threshold * dyn_t  (SelectivityEngine)
  r_mult         : RiskEngine.evaluate()[1]
  signal_count   : consecutive same-direction counter (update before calling)
  clustered      : SelectivityEngine.is_clustered()
  last_trade_ts  : timestamp of last closed trade
  last_score     : dlog.score from the previous flat tick (init to 0.0)
  v2_result      : the _v2_cache dict populated during SignalProcessor.score()

After every call (approved or not):
  last_score = dlog.score
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from au2_core import (
    CoreConfig, Regime, SignalProcessor, TradeGate, PropProfile,
)


# ---------------------------------------------------------------------------
# TradeDecisionLog
# ---------------------------------------------------------------------------

@dataclass
class TradeDecisionLog:
    """Serialisable record of every field that determined a tick's outcome.

    Use dlog.to_dict() + json.dumps() for structured logging / parity diffs.
    Fields are ordered from context → inputs → quality → gate → verdict.
    """
    # ── Tick context ──────────────────────────────────────────────────────────
    ts: float = 0.0
    price: float = 0.0
    regime: str = "CHOP"

    # ── Score ─────────────────────────────────────────────────────────────────
    score: float = 0.0
    eff_threshold: float = 0.0      # cfg.threshold * dyn_t

    # ── Signal quality (evaluate_quality) ────────────────────────────────────
    signal: str = "FLAT"            # "LONG" | "SHORT" | "FLAT"
    confidence: float = 0.0
    regime_quality: float = 1.0
    eff_score: float = 0.0          # abs(score) * confidence * regime_quality
    should_trade: bool = False
    acc_ok: bool = True             # score-acceleration gate
    coh_ok: bool = True             # trend/vol coherence gate

    # ── Advanced score (compute_advanced_score) ───────────────────────────────
    adv_setup: float = 0.0
    adv_context: float = 0.0
    adv_execution: float = 0.0
    adv_final: float = 0.0
    adv_reason: str = ""

    # ── Gate inputs ───────────────────────────────────────────────────────────
    spread: float = 0.0
    vol: float = 0.0
    signal_count: int = 0
    clustered: bool = False

    # ── Verdict ───────────────────────────────────────────────────────────────
    approved: bool = False
    rejection_reason: str = ""      # "" when approved

    # ── Derived flag ─────────────────────────────────────────────────────────
    near_miss: bool = False         # abs(score) in [0.7 * eff_threshold, eff_threshold)

    def to_dict(self) -> dict:
        import dataclasses
        return dataclasses.asdict(self)


# ---------------------------------------------------------------------------
# build_trade_decision — the one function
# ---------------------------------------------------------------------------

def build_trade_decision(
    *,
    # --- pre-computed score (caller called SignalProcessor.score once) -----
    score: float,
    # --- tick features -------------------------------------------------------
    ts: float,
    price: float,
    cvd: float,
    trend: float,
    vol: float,
    spread: float,
    regime: Regime,
    # --- executor state (caller manages, function is pure) -------------------
    eff_thr: float,             # cfg.threshold * dyn_t
    r_mult: float,              # from RiskEngine.evaluate()
    signal_count: int,          # consecutive confirmation count
    clustered: bool,            # from SelectivityEngine.is_clustered()
    last_trade_ts: float,
    last_score: float,          # dlog.score from the previous flat tick
    # --- shared, stateless evaluation objects --------------------------------
    cfg: CoreConfig,
    gate: TradeGate,
    # --- optional V2 ML cache (populated by SignalProcessor.score) -----------
    v2_result: Optional[dict] = None,
    # --- optional prop constraints -------------------------------------------
    prop: Optional[PropProfile] = None,
) -> TradeDecisionLog:
    """Evaluate a pre-scored tick through the full quality + gate pipeline.

    Sequence (identical to the backtest loop in Au2Backtest.run()):
      1. SignalProcessor.evaluate_quality()      — direction, confidence, acc, coh
      2. SignalProcessor.compute_advanced_score()— composite quality gate
      3. TradeGate.evaluate()                    — spread, vol, cooldown,
                                                   cluster, confirmation, adv

    Returns a TradeDecisionLog populated with every field that influenced
    the outcome.  The function has NO side effects.

    Parameters
    ----------
    score          : Raw signal score — call SignalProcessor.score() ONCE
                     before this function, then pass the result here.
    ts, price      : Tick timestamp and mid-price
    cvd            : 5-second cumulative volume delta
    trend          : 5-second trend in bps
    vol            : Realised volatility in bps (5-second window)
    spread         : Bid/ask spread in bps (0 if unavailable)
    regime         : Classified regime — from SignalProcessor.classify_regime()
    eff_thr        : Effective threshold = cfg.threshold * dyn_t
    r_mult         : Risk multiplier from RiskEngine (0.7 AMBER, 1.0 GREEN)
    signal_count   : How many consecutive ticks with the same direction
    clustered      : True if SelectivityEngine detected cluster saturation
    last_trade_ts  : Timestamp of the most recently completed trade
    last_score     : score from the previous evaluated flat tick (acc check)
    cfg            : Must be GOAT_VALIDATED_CFG — never pass an unvalidated config
    gate           : Stateless TradeGate (shared instance, safe to reuse)
    v2_result      : _v2_cache dict populated by SignalProcessor.score()
                     — pass {} if none available
    prop           : Optional PropProfile for prop-firm RR constraints
    """
    # 1. Signal quality -------------------------------------------------------
    q, _rej = SignalProcessor.evaluate_quality(
        score, eff_thr, regime, cvd, trend, vol, cfg, last_score,
        v2_result=v2_result if v2_result else None,
    )

    # 2. Advanced score -------------------------------------------------------
    adv = SignalProcessor.compute_advanced_score(
        score, eff_thr, regime, cvd, trend, vol, cfg, last_score, prop,
    )

    # 3. Gate -----------------------------------------------------------------
    decision = gate.evaluate(
        q, adv, r_mult, spread, vol,
        signal_count, clustered, ts, last_trade_ts,
    )

    # 4. Assemble log ---------------------------------------------------------
    abs_score = abs(score)
    return TradeDecisionLog(
        ts=ts,
        price=price,
        regime=regime.value if isinstance(regime, Regime) else str(regime),
        score=score,
        eff_threshold=eff_thr,
        signal=q.signal,
        confidence=q.confidence,
        regime_quality=q.regime_quality,
        eff_score=q.eff,
        should_trade=q.should_trade,
        acc_ok=q.acc_ok,
        coh_ok=q.coh_ok,
        adv_setup=adv.setup_score,
        adv_context=adv.context_score,
        adv_execution=adv.execution_score,
        adv_final=adv.final_score,
        adv_reason=adv.reason,
        spread=spread,
        vol=vol,
        signal_count=signal_count,
        clustered=clustered,
        approved=decision.approved,
        rejection_reason=decision.reason if not decision.approved else "",
        near_miss=(abs_score >= 0.70 * eff_thr) and (abs_score < eff_thr),
    )

#!/usr/bin/env python3
"""AU2FP Runtime — Prop-firm specialized entrypoint.
Imports AU2 core components but overrides decision pipeline with FP guards.
Zero modifications to au2_core.py or au2_v22_5.py."""
from __future__ import annotations
import asyncio, time, logging
from datetime import datetime, timezone
from typing import Optional

from au2_core import CoreConfig, SignalProcessor, RiskEngine, PositionManager, TradeBuilder, SlippageModel, clamp
from au2_v22_5 import BotConfig, FlowEngine, FlowState, Trader, Journal, BinanceREST, LocalBook

from au2fp_config import FUNDINGPIPS_CLASSIC_V1, AU2FPConfig
from au2fp_prop_score import PropScoreCalculator
from au2fp_news_guard import PropNewsGuard, NewsEvent
from au2fp_risk_manager import AU2FPRiskManager
from au2fp_trade_gate import AU2FPTradeGate, BANNED_SETUPS

log = logging.getLogger("au2fp")

class AU2FPDecisionEngine:
    def __init__(self, cfg_fp: AU2FPConfig):
        self.cfg_fp = cfg_fp
        self.news_guard = PropNewsGuard(block_before_min=20, block_after_min=20, force_flat_before_min=10)
        self.risk_mgr = AU2FPRiskManager(cfg_fp)
        self.gate = AU2FPTradeGate(cfg_fp)

    def inject_news(self, events: list[NewsEvent]):
        self.news_guard.inject_events(events)

    def decide(self, state: FlowState, alpha_score: float, alpha_conf: float, rr: float, setup_name: str) -> dict:
        now_ts = time.time()
        now_utc = datetime.fromtimestamp(now_ts, tz=timezone.utc)

        # 1. Session Filter
        session_ok, sess_name = self.risk_mgr.check_session_filter(now_ts)
        if not session_ok:
            return {"approved": False, "reason": f"outside_session_{sess_name}"}

        # 2. News Guard
        news_state = self.news_guard.evaluate(now_utc)
        if news_state["disable_entries"]:
            return {"approved": False, "reason": "news_lock"}

        # 3. Risk & Meta Control (uses daily_dd from core or external feed)
        daily_dd = max((self.risk_mgr.daily_pnl) / 10000.0, 0.0) * 100 # Simplified proxy
        risk_ok, risk_reason = self.risk_mgr.can_enter(now_ts, daily_dd)
        if not risk_ok:
            return {"approved": False, "reason": risk_reason}

        # 4. Prop Score
        prop_metrics = PropScoreCalculator.from_state(
            daily_dd=daily_dd,
            max_open_risk_pct=self.cfg_fp.max_open_risk_pct,
            current_risk=0.18, # FP fixed
            news_locked=news_state["state"]=="LOCKED",
            recent_slippage=state.spread_bps * 0.35,
            consec_losses=self.risk_mgr.consec_losses
        )

        # 5. FP Trade Gate
        gate_ok, gate_reason = self.gate.evaluate(
            final_score=alpha_score,
            context_score=alpha_conf * 100,
            execution_score=max(0, 100 - state.spread_bps * 20),
            prop_score=prop_metrics["prop_score"],
            rr=rr,
            setup_name=setup_name.lower()
        )
        if not gate_ok:
            return {"approved": False, "reason": gate_reason}

        # 6. Force Flat before news
        if news_state["force_flat"]:
            return {"approved": False, "reason": "force_flat_before_news"}

        return {"approved": True, "reason": "fp_approved", "prop_metrics": prop_metrics}

# ──────────────────────────────────────────────────────────────
# Runtime Wrapper (Drop-in replacement for strategy_loop logic)
# ──────────────────────────────────────────────────────────────
async def run_au2fp_state(state: FlowState, decision_engine: AU2FPDecisionEngine, trader: Trader, journal: Journal):
    """Called from your existing WS pipeline instead of trader.on_state(state)."""
    if not state.mid or state.signal == "FLAT":
        return

    # Derive FP inputs from existing flow
    rr = state.take_profit_1_pct / state.stop_loss_pct if state.stop_loss_pct > 0 else 0.0
    setup = state.setup_name or "unknown"
    alpha_score = abs(state.calibrated_score) * 10.0 # Scale to 0-100
    alpha_conf = clamp(abs(state.calibrated_score) / 4.0, 0, 1) * 100 # Scale to 0-100

    decision = decision_engine.decide(state, alpha_score, alpha_conf, rr, setup)

    if not decision["approved"]:
        log.info(f"AU2FP REJECT | {decision['reason']} | signal={state.signal} setup={setup}")
        trader.reset_entry_signal()
        return

    log.info(f"AU2FP APPROVED | {state.signal} | RR={rr:.2f} | prop_score={decision['prop_metrics']['prop_score']:.1f}")
    # Delegate to original trader.open_position() but force FP sizing/brackets
    await trader.open_position(state.signal, state.mid, state, f"fp_{decision['reason']}")
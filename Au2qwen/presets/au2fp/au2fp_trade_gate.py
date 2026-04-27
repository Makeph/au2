from __future__ import annotations
from au2fp_config import AU2FPConfig

BANNED_SETUPS = {"late_breakout", "news_reaction", "impulse_chase", "revenge_trade", "tight_scalp", "low_liquidity"}

class AU2FPTradeGate:
    def __init__(self, cfg: AU2FPConfig):
        self.cfg = cfg

    def evaluate(self, final_score: float, context_score: float, execution_score: float,
                 prop_score: float, rr: float, setup_name: str) -> tuple[bool, str]:
        if final_score < self.cfg.min_final_score: return False, "low_final_score"
        if context_score < self.cfg.min_context_score: return False, "weak_context"
        if execution_score < self.cfg.min_execution_score: return False, "bad_execution"
        if prop_score < self.cfg.min_prop_score: return False, "low_prop_score"
        if rr < self.cfg.min_rr: return False, "insufficient_rr"
        if setup_name in BANNED_SETUPS: return False, "banned_setup"
        if self.cfg.require_trend_alignment and context_score < 78.0:
            return False, "trend_misalignment"
        return True, "gate_approved"
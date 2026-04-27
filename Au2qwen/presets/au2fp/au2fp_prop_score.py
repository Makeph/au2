from __future__ import annotations

class PropScoreCalculator:
    """Explicit prop-firm safety score (0-100). Separated from alpha scoring."""
    @staticmethod
    def compute(drawdown_safety: float, news_safety: float, execution_cleanliness: float,
                consistency_pattern: float, risk_stacking: float) -> float:
        # Weights reflect prop survival priorities
        score = (
            drawdown_safety * 0.35 +
            news_safety * 0.25 +
            execution_cleanliness * 0.20 +
            consistency_pattern * 0.10 +
            risk_stacking * 0.10
        )
        return max(0.0, min(100.0, score))

    @staticmethod
    def from_state(daily_dd: float, max_open_risk_pct: float, current_risk: float,
                   news_locked: bool, recent_slippage: float, consec_losses: int) -> dict:
        # Drawdown safety: 100 if safe, drops linearly to 0 at hard limit
        dd_safe = max(0.0, 100.0 * (1.0 - daily_dd / 2.5))
        # News safety: binary
        news_safe = 0.0 if news_locked else 100.0
        # Execution cleanliness: inverse of slippage/latency
        exec_clean = max(0.0, 100.0 - recent_slippage * 15.0)
        # Consistency: penalize streaks
        consistency = max(0.0, 100.0 - (consec_losses * 20.0))
        # Risk stacking: how close to max_open_risk_pct
        risk_stack = max(0.0, 100.0 * (1.0 - (current_risk / max_open_risk_pct)))

        final = PropScoreCalculator.compute(
            dd_safe, news_safe, exec_clean, consistency, risk_stack
        )
        return {
            "prop_score": final,
            "drawdown_safety": dd_safe,
            "news_safety": news_safe,
            "execution_cleanliness": exec_clean,
            "consistency_pattern": consistency,
            "risk_stacking": risk_stack
        }
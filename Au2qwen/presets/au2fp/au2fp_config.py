from __future__ import annotations
from dataclasses import dataclass, field
from typing import List

@dataclass(frozen=True)
class AU2FPConfig:
    # Risk
    risk_per_trade_pct: float = 0.18
    max_open_risk_pct: float = 0.35
    min_risk_usd: float = 5.0

    # DD Control
    soft_daily_stop_pct: float = 1.6
    hard_daily_stop_pct: float = 2.2
    soft_weekly_stop_pct: float = 3.8
    hard_weekly_stop_pct: float = 5.5

    # Frequency & Timing
    max_trades_per_day: int = 3
    max_trades_per_session: int = 2
    max_consecutive_losses: int = 2
    entry_cooldown_sec: int = 45
    same_dir_reentry_sec: int = 120

    # Quality Filters (0-100 scale)
    min_final_score: float = 80.0
    min_context_score: float = 72.0
    min_execution_score: float = 75.0
    min_prop_score: float = 85.0

    # Structure & RR
    one_symbol: bool = True
    require_trend_alignment: bool = True
    require_liquidity_ok: bool = True
    min_rr: float = 1.9

    # Position Management (Partial Secure)
    tp1_rr: float = 1.0
    tp1_close_pct: float = 0.5
    move_sl_to_be_after_tp1: bool = True
    tp2_rr: float = 2.2
    runner_enabled: bool = False

    # Kill Switch
    daily_profit_stop_pct: float = 1.8
    daily_loss_stop_pct: float = -1.6

    # Session Filter (UTC)
    allowed_sessions: List[str] = field(default_factory=lambda: ["london", "ny_open"])

    # Meta Control
    meta_loss_days_threshold: int = 3
    meta_risk_reduction: float = 0.70
    meta_consec_loss_cooldown_sec: int = 43200  # 12h

# Preset: FundingPips Classic V1
FUNDINGPIPS_CLASSIC_V1 = AU2FPConfig()
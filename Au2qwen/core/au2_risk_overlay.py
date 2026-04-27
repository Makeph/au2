#!/usr/bin/env python3
"""AU2 RISK OVERLAY — Daily Profit Cap ONLY. Loss pause explicitly configurable."""
import logging
from dataclasses import dataclass
from typing import Tuple

log = logging.getLogger("au2_overlay")

@dataclass
class RiskOverlayConfig:
    daily_profit_cap_pct: float = 0.80
    post_loss_pause_trades: int = 2
    pause_duration_seconds: float = 600.0
    enable_post_loss_pause: bool = False  # ✅ Patch 7: Explicit disable flag

class RiskOverlay:
    def __init__(self, start_equity: float = 10000.0, cfg: RiskOverlayConfig | None = None):
        self.start_equity = start_equity; self.day_start_equity = start_equity; self.current_equity = start_equity
        self.cfg = cfg or RiskOverlayConfig(); self.daily_pnl = 0.0; self.consecutive_losses = 0; self.pause_until = 0.0

    def reset_day(self) -> None:
        self.day_start_equity = self.current_equity; self.daily_pnl = 0.0; self.consecutive_losses = 0; self.pause_until = 0.0
        log.info("[OVERLAY] overlay_daily_reset | daily_pnl_cleared")

    def update_equity(self, pnl: float, ts: float = 0.0) -> None:
        self.current_equity += pnl; self.daily_pnl += pnl
        self.consecutive_losses = self.consecutive_losses + 1 if pnl < 0 else 0

    def should_block(self, ts: float = 0.0) -> Tuple[bool, str]:
        daily_gain_pct = (self.daily_pnl / self.day_start_equity * 100.0) if self.day_start_equity > 0 else 0.0
        if daily_gain_pct >= self.cfg.daily_profit_cap_pct:
            log.info("[OVERLAY] blocked_by_daily_profit_cap | gain=%.2f%%", daily_gain_pct)
            return True, "blocked_by_daily_profit_cap"
        if self.cfg.enable_post_loss_pause and self.consecutive_losses >= self.cfg.post_loss_pause_trades:
            self.pause_until = ts + self.cfg.pause_duration_seconds; self.consecutive_losses = 0
            log.info("[OVERLAY] blocked_by_post_loss_pause | triggered %.0fs pause", self.cfg.pause_duration_seconds)
            return True, "blocked_by_post_loss_pause"
        return False, ""
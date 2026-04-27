from __future__ import annotations
import time
from datetime import datetime, timezone
from au2fp_config import AU2FPConfig

class AU2FPRiskManager:
    def __init__(self, cfg: AU2FPConfig):
        self.cfg = cfg
        self.daily_pnl = 0.0
        self.day_trades = 0
        self.session_trades = 0
        self.consec_losses = 0
        self.last_entry_ts = 0.0
        self.meta_pause_until = 0.0
        self.daily_dd_pct = 0.0

    def check_session_filter(self, now_ts: float) -> tuple[bool, str]:
        dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        # London: 07:00-10:30, NY Open: 13:00-16:30 UTC
        is_london = 7.0 <= dt.hour + dt.minute/60.0 < 10.5
        is_ny = 13.0 <= dt.hour + dt.minute/60.0 < 16.5
        session = "london" if is_london else ("ny_open" if is_ny else "off")
        return session in self.cfg.allowed_sessions, session

    def apply_meta_control(self, now_ts: float):
        if self.consec_losses >= self.cfg.max_consecutive_losses:
            self.meta_pause_until = max(self.meta_pause_until, now_ts + self.cfg.meta_consec_loss_cooldown_sec)

    def check_daily_stops(self) -> tuple[bool, str]:
        if self.daily_dd_pct >= self.cfg.hard_daily_stop_pct: return True, "hard_daily_stop"
        if self.daily_dd_pct >= self.cfg.soft_daily_stop_pct: return True, "soft_daily_stop"
        if self.daily_pnl <= self.cfg.daily_loss_stop_pct: return True, "protect_account"
        if self.daily_pnl >= self.cfg.daily_profit_stop_pct: return True, "secure_profit"
        return False, ""

    def can_enter(self, now_ts: float, daily_dd: float) -> tuple[bool, str]:
        self.daily_dd_pct = daily_dd
        ok, reason = self.check_daily_stops()
        if not ok: return False, reason
        if now_ts < self.meta_pause_until: return False, "meta_loss_cooldown"
        if now_ts - self.last_entry_ts < self.cfg.entry_cooldown_sec: return False, "entry_cooldown"
        if self.day_trades >= self.cfg.max_trades_per_day: return False, "daily_cap"
        if self.session_trades >= self.cfg.max_trades_per_session: return False, "session_cap"
        return True, "risk_ok"

    def record_trade(self, pnl: float, now_ts: float):
        self.daily_pnl += pnl
        self.day_trades += 1
        self.session_trades += 1
        self.last_entry_ts = now_ts
        if pnl < 0:
            self.consec_losses += 1
            self.apply_meta_control(now_ts)
        else:
            self.consec_losses = 0

    def reset_session(self):
        self.session_trades = 0
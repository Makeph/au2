"""AU2QWEN — GOAT Pay Later Consistency Guard.

Enforces prop-firm consistency rules independently of core signal logic.
The core (au2_core.py, au2_decision.py) has zero knowledge of these rules.

Usage
-----
    guard = ConsistencyGuard(ConsistencyGuardConfig())
    guard.load(saved.get("paylater_state", {}))

    # On each trade close:
    guard.record_day_pnl(today_date, daily_pnl, equity)

    # Before each entry:
    blocked, reason = guard.should_block(today_date, today_pnl, equity)

    # For Telegram / CLI report:
    report = guard.payout_readiness(equity)
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

log = logging.getLogger("au2_consistency")


# ── Config ─────────────────────────────────────────────────────────────────────

@dataclass
class ConsistencyGuardConfig:
    """Parameters for GOAT Pay Later consistency enforcement."""

    # Internal safety margin below the 20% prop-firm rule
    max_best_day_share:      float = 0.18   # block if today would exceed 18% of period profit
    # A day must clear this profit threshold to count as "valid"
    valid_day_min_profit_pct: float = 0.50  # % of start equity
    # Stop taking new entries after a valid day (reset at midnight UTC)
    stop_after_valid_day:    bool  = True
    # Minimum valid days before payout is considered reachable
    min_valid_days:          int   = 3
    # Daily profit target (informational — used in readiness report)
    daily_target_pct:        float = 0.55
    # Whether the guard is active (set False for ALPHA mode)
    enabled:                 bool  = True


# ── Guard ──────────────────────────────────────────────────────────────────────

class ConsistencyGuard:
    """Tracks payout-period day profits and enforces consistency rules.

    State is persisted via load()/dump() so restarts don't lose period history.
    """

    def __init__(self, cfg: Optional[ConsistencyGuardConfig] = None) -> None:
        self.cfg = cfg or ConsistencyGuardConfig()
        # { "YYYY-MM-DD": pnl_usd }  — only closed days
        self._period_day_profits: Dict[str, float] = {}
        self._period_start_ts:    float = time.time()
        self._today_valid:        bool  = False   # did today already hit valid threshold?

    # ── Persistence ──────────────────────────────────────────────────────────

    def load(self, saved: dict) -> None:
        """Restore from checkpoint dict (saved["paylater_state"])."""
        self._period_day_profits = saved.get("period_day_profits", {})
        self._period_start_ts    = float(saved.get("period_start_ts", time.time()))
        self._today_valid        = bool(saved.get("today_valid", False))

    def dump(self) -> dict:
        """Serialize for checkpoint."""
        return {
            "period_day_profits": self._period_day_profits,
            "period_start_ts":    self._period_start_ts,
            "today_valid":        self._today_valid,
        }

    def reset_period(self) -> None:
        """Start a new payout period (call after successful payout)."""
        self._period_day_profits = {}
        self._period_start_ts    = time.time()
        self._today_valid        = False
        log.info("[PAYLATER] Payout period reset.")

    # ── Daily accounting ─────────────────────────────────────────────────────

    def on_day_reset(self, date_str: str, closed_day_pnl: float, equity: float) -> None:
        """Call at UTC midnight when the previous day closes.

        Parameters
        ----------
        date_str      : 'YYYY-MM-DD' of the day that just closed
        closed_day_pnl: net PnL for that day in USD
        equity        : equity at day close (used for % calculation)
        """
        if closed_day_pnl != 0.0:
            self._period_day_profits[date_str] = closed_day_pnl

        valid_threshold = equity * self.cfg.valid_day_min_profit_pct / 100.0
        if closed_day_pnl >= valid_threshold:
            log.info("[PAYLATER] Valid day recorded: %s PnL=$%.2f (%.2f%%)",
                     date_str, closed_day_pnl,
                     closed_day_pnl / equity * 100 if equity else 0)
        self._today_valid = False   # reset for the new day

    def mark_today_valid(self, equity: float, today_pnl: float) -> None:
        """Mark current day as valid (called intra-day when threshold crossed)."""
        pct = today_pnl / equity * 100 if equity else 0
        if not self._today_valid and pct >= self.cfg.valid_day_min_profit_pct:
            self._today_valid = True
            log.info("[PAYLATER] Today reached valid-day threshold (%.2f%% >= %.2f%%)",
                     pct, self.cfg.valid_day_min_profit_pct)

    # ── Entry gate ───────────────────────────────────────────────────────────

    def should_block(self, today_date: str, today_pnl_usd: float,
                     equity: float) -> Tuple[bool, str]:
        """Return (blocked, reason) for a prospective new entry.

        Checks, in order:
        1. Guard disabled → always allow
        2. stop_after_valid_day → block if today already hit valid threshold
        3. Consistency share → block if today_pnl would push share > max_best_day_share
        """
        if not self.cfg.enabled:
            return False, ""

        # Refresh valid-day flag intra-day
        self.mark_today_valid(equity, today_pnl_usd)

        # Rule 1: stop after valid day
        if self.cfg.stop_after_valid_day and self._today_valid:
            return True, "paylater_valid_day_reached"

        # Rule 2: consistency share check
        # Only block if TODAY would become the best day at >= max_best_day_share.
        # If another past day is already the best, adding profit TODAY dilutes it —
        # we should ALLOW trading to restore balance, not block it.
        if today_pnl_usd > 0:
            projected = dict(self._period_day_profits)
            today_total = projected.get(today_date, 0.0) + today_pnl_usd
            projected[today_date] = today_total
            positive = [v for v in projected.values() if v > 0]
            if positive and today_total >= max(positive):
                # Today is the best day — check if its share exceeds cap
                period_total = sum(positive)
                share = today_total / period_total
                if share >= self.cfg.max_best_day_share:
                    return True, f"paylater_consistency_cap_{share:.0%}"

        return False, ""

    # ── Readiness report ─────────────────────────────────────────────────────

    def payout_readiness(self, equity: float,
                         today_date: Optional[str] = None,
                         today_pnl_usd: float = 0.0) -> "PayoutReadinessReport":
        """Build a snapshot of current payout eligibility."""
        today_date = today_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Merge closed days + today
        snapshot: Dict[str, float] = dict(self._period_day_profits)
        if today_pnl_usd != 0.0:
            snapshot[today_date] = snapshot.get(today_date, 0.0) + today_pnl_usd

        positive_days = {d: v for d, v in snapshot.items() if v > 0}
        all_days      = snapshot

        # Valid days: profit >= valid_day_min_profit_pct of equity
        valid_threshold = equity * self.cfg.valid_day_min_profit_pct / 100.0
        valid_days = [d for d, v in positive_days.items() if v >= valid_threshold]

        period_profit = sum(v for v in all_days.values())
        period_profit_pos = sum(positive_days.values()) or 1e-9

        best_day     = max(positive_days.values()) if positive_days else 0.0
        best_day_share = best_day / period_profit_pos

        eligible = (
            len(valid_days) >= self.cfg.min_valid_days
            and best_day_share <= self.cfg.max_best_day_share
            and period_profit > 0
        )

        # Next action
        if not eligible:
            if len(valid_days) < self.cfg.min_valid_days:
                next_action = f"Need {self.cfg.min_valid_days - len(valid_days)} more valid day(s)"
            elif best_day_share > self.cfg.max_best_day_share:
                next_action = f"Best day share {best_day_share:.0%} > {self.cfg.max_best_day_share:.0%} — need other days to grow"
            else:
                next_action = "Period profit negative — keep trading"
        else:
            next_action = "✅ Eligible for payout request"

        # Breach risks
        breach_risks = []
        if best_day_share > 0.15:
            breach_risks.append(f"Best day at {best_day_share:.0%} — close to {self.cfg.max_best_day_share:.0%} cap")
        if len(valid_days) < self.cfg.min_valid_days:
            breach_risks.append(f"Only {len(valid_days)}/{self.cfg.min_valid_days} valid days")

        return PayoutReadinessReport(
            period_profit_usd  = period_profit,
            valid_day_count    = len(valid_days),
            min_valid_days     = self.cfg.min_valid_days,
            best_day_usd       = best_day,
            best_day_share     = best_day_share,
            max_best_day_share = self.cfg.max_best_day_share,
            eligible           = eligible,
            next_action        = next_action,
            breach_risks       = breach_risks,
            day_profits        = dict(snapshot),
            period_start_ts    = self._period_start_ts,
        )


# ── Report dataclass ──────────────────────────────────────────────────────────

@dataclass
class PayoutReadinessReport:
    period_profit_usd:   float
    valid_day_count:     int
    min_valid_days:      int
    best_day_usd:        float
    best_day_share:      float
    max_best_day_share:  float
    eligible:            bool
    next_action:         str
    breach_risks:        list
    day_profits:         Dict[str, float]
    period_start_ts:     float

    def format_telegram(self) -> str:
        """Format for Telegram Markdown reply."""
        em = "✅" if self.eligible else "⏳"
        share_em = "⚠️" if self.best_day_share > 0.15 else "✅"
        valid_em = "✅" if self.valid_day_count >= self.min_valid_days else "❌"

        days_str = "\n".join(
            f"  `{d}`: `{'+'if v>=0 else ''}{v:.2f}$`"
            for d, v in sorted(self.day_profits.items())
        ) or "  _aucune donnée_"

        risks_str = ("\n".join(f"  ⚠️ {r}" for r in self.breach_risks)
                     if self.breach_risks else "  ✅ Aucun risque identifié")

        return (
            f"{em} *GOAT Pay Later — Payout Readiness*\n\n"
            f"💰 Profit période: `${self.period_profit_usd:.2f}`\n"
            f"{valid_em} Jours valides: `{self.valid_day_count}/{self.min_valid_days}`\n"
            f"{share_em} Best day share: `{self.best_day_share:.0%}` (cap: `{self.max_best_day_share:.0%}`)\n\n"
            f"📅 *Jours:*\n{days_str}\n\n"
            f"🎯 *Prochaine action:* {self.next_action}\n\n"
            f"*Risques:*\n{risks_str}"
        )

    def format_cli(self) -> str:
        """Format for terminal / log output."""
        return (
            f"PayoutReadiness | eligible={self.eligible} | "
            f"profit=${self.period_profit_usd:.2f} | "
            f"valid_days={self.valid_day_count}/{self.min_valid_days} | "
            f"best_day_share={self.best_day_share:.1%} | "
            f"next={self.next_action}"
        )

from __future__ import annotations
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import List

@dataclass
class NewsEvent:
    timestamp: datetime
    impact: str  # "HIGH"

class PropNewsGuard:
    def __init__(self, block_before_min: int = 20, block_after_min: int = 20, force_flat_before_min: int = 10):
        self.block_before = timedelta(minutes=block_before_min)
        self.block_after = timedelta(minutes=block_after_min)
        self.force_flat_before = timedelta(minutes=force_flat_before_min)
        self._events: List[NewsEvent] = []

    def inject_events(self, events: List[NewsEvent]):
        self._events = sorted([e for e in events if e.impact == "HIGH"], key=lambda x: x.timestamp)

    def evaluate(self, now_utc: datetime | None = None) -> dict:
        now = now_utc or datetime.now(timezone.utc)
        locked = False
        flat_zone = False

        for evt in self._events:
            diff = evt.timestamp - now
            if -self.block_after <= diff <= self.block_before:
                locked = True
                break
            if -self.force_flat_before <= diff < 0:
                flat_zone = True

        return {
            "state": "LOCKED" if locked else ("FLAT_ZONE" if flat_zone else "SAFE"),
            "disable_entries": locked,
            "force_flat": flat_zone and not locked,
            "disable_tp_sl_moves": locked,
            "reason": "news_lock" if locked else ("news_flat_zone" if flat_zone else "clear")
        }
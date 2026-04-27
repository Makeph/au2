#!/usr/bin/env python3
"""AU2 STATE MANAGER — Atomic JSON persistence for live crash recovery."""
from __future__ import annotations
import json, os, tempfile, time
from typing import Dict, Any, Optional

class StatePersistence:
    def __init__(self, path: str = "au2_live_state.json"):
        self.path = path

    def save(self, state: Dict[str, Any]):
        try:
            dir_name = os.path.dirname(os.path.abspath(self.path))
            fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
            with os.fdopen(fd, 'w') as f:
                json.dump(state, f, indent=2, default=str)
            os.replace(tmp_path, self.path)
        except Exception as e:
            print(f"[State] Save failed: {e}")

    def load(self) -> Optional[Dict[str, Any]]:
        if not os.path.exists(self.path): return None
        try:
            with open(self.path, 'r') as f: return json.load(f)
        except Exception: return None

    def build_checkpoint(self, equity: float, last_ts: float, loss_streak: int, 
                         open_pos: Optional[Dict], builder: Optional[Dict]) -> Dict[str, Any]:
        return {
            "ts": time.time(),
            "equity": equity,
            "last_trade_ts": last_ts,
            "loss_streak": loss_streak,
            "open_position": open_pos,
            "builder_state": builder,
            "risk_state": "GREEN"  # placeholder, recalculated on load
        }
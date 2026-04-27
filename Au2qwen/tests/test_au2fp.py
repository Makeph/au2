#!/usr/bin/env python3
import sys, pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime", _ROOT / "presets" / "au2fp"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

import time, unittest
from datetime import datetime, timedelta, timezone
from au2fp_config import AU2FPConfig, FUNDINGPIPS_CLASSIC_V1
from au2fp_news_guard import PropNewsGuard, NewsEvent
from au2fp_risk_manager import AU2FPRiskManager
from au2fp_trade_gate import AU2FPTradeGate

def clamp(v, lo, hi): return max(lo, min(hi, v))

class TestAU2FP(unittest.TestCase):
    def setUp(self):
        self.cfg = FUNDINGPIPS_CLASSIC_V1
        self.risk = AU2FPRiskManager(self.cfg)
        self.news = PropNewsGuard()
        self.gate = AU2FPTradeGate(self.cfg)

    def test_au2_config_integrity(self):
        self.assertEqual(self.cfg.risk_per_trade_pct, 0.18)
        self.assertFalse(self.cfg.runner_enabled)
        self.assertEqual(self.cfg.min_rr, 1.9)

    def test_news_lock_blocking(self):
        now = datetime.now(timezone.utc)
        evt = NewsEvent(timestamp=now + timedelta(minutes=10), impact="HIGH")
        self.news.inject_events([evt])
        state = self.news.evaluate(now)
        self.assertTrue(state["disable_entries"])
        self.assertEqual(state["reason"], "news_lock")

    def test_daily_kill_switch(self):
        self.risk.daily_pnl = -1.7
        ok, reason = self.risk.check_daily_stops()
        self.assertFalse(ok)
        self.assertIn("protect_account", reason)

    def test_meta_pause_after_2_losses(self):
        now = time.time()
        self.risk.record_trade(-10.0, now)
        self.risk.record_trade(-10.0, now + 60)
        ok, _ = self.risk.can_enter(now + 300, 1.0)
        self.assertFalse(ok) # 12h cooldown activated

    def test_gate_rejects_banned_setups(self):
        ok, reason = self.gate.evaluate(90, 85, 80, 95, 2.0, "late_breakout")
        self.assertFalse(ok)
        self.assertEqual(reason, "banned_setup")

    def test_session_filter_blocks_asia(self):
        # Asia ~ 02:00 UTC
        ts = datetime(2024, 1, 1, 2, 0, tzinfo=timezone.utc).timestamp()
        ok, sess = self.risk.check_session_filter(ts)
        self.assertFalse(ok)

if __name__ == "__main__":
    unittest.main()
    print("✅ AU2FP Tests Passed")
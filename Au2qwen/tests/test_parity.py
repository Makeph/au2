#!/usr/bin/env python3
"""AU2QWEN — Minimal parity and correctness tests.

Run with:
  python test_parity.py
  python -m pytest test_parity.py -v

Tests cover:
  1. GOAT_VALIDATED_CFG loads correctly (all validated params).
  2. Fee model: round-trip = maker_fee + taker_fee.
  3. V3 activates when range30 > 0 and ts > 0; degrades gracefully when range30=0.
  4. build_trade_decision() approves / rejects deterministically.
  5. Rejection reasons are correct for each gate.
  6. TradeDecisionLog is fully serialisable (JSON).
  7. MarketState builds trend_30s_bps (was missing — broke V3 in live).
"""
from __future__ import annotations

import sys, pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

import time
import unittest

from au2_core import (
    Regime, SignalProcessor, TradeGate,
)
from au2_config import GOAT_VALIDATED_CFG, build_goat_config
from au2_decision import build_trade_decision, TradeDecisionLog


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CFG = GOAT_VALIDATED_CFG


def _gate():
    return TradeGate(CFG)


def _score_once(cvd=0.0, trend=0.0, vol=5.0, regime=Regime.TREND,
                trend30=0.0, range30=0.0, ts=0.0, price=0.0) -> float:
    """Call SignalProcessor.score exactly once (as the live executor does)."""
    return SignalProcessor.score(
        cvd, trend, vol, regime, CFG,
        trend30_bps=trend30, range30_bps=range30, ts=ts, price=price,
    )


def _decide(score=10.0, eff_thr=8.0, cvd=5.0, trend=6.0, vol=5.0,
            spread=1.0, signal_count=1, clustered=False,
            ts=1_000_000_000.0, last_ts=0.0, last_score=0.0,
            r_mult=1.0, regime=Regime.TREND) -> TradeDecisionLog:
    """Build a decision with sensible defaults."""
    return build_trade_decision(
        score=score,
        ts=ts, price=75_000.0,
        cvd=cvd, trend=trend, vol=vol, spread=spread,
        regime=regime, eff_thr=eff_thr, r_mult=r_mult,
        signal_count=signal_count, clustered=clustered,
        last_trade_ts=last_ts, last_score=last_score,
        cfg=CFG, gate=_gate(),
    )


# ---------------------------------------------------------------------------
# 1. Config correctness
# ---------------------------------------------------------------------------

class TestGoatValidatedCfg(unittest.TestCase):

    def test_name_constant_is_validated(self):
        """GOAT_VALIDATED_CFG is the canonical constant — must exist."""
        self.assertIsNotNone(GOAT_VALIDATED_CFG)

    def test_threshold(self):          self.assertAlmostEqual(CFG.threshold, 8.0)
    def test_stop_loss(self):          self.assertAlmostEqual(CFG.stop_loss_pct, 0.18)
    def test_tp1(self):                self.assertAlmostEqual(CFG.tp1_pct, 0.23)
    def test_tp2(self):                self.assertAlmostEqual(CFG.tp2_pct, 0.35)
    def test_cooldown(self):           self.assertEqual(CFG.cooldown_seconds, 15)
    def test_confirmation_cycles(self):self.assertEqual(CFG.confirmation_cycles, 1)
    def test_max_daily_trades(self):   self.assertEqual(CFG.max_daily_trades, 50)
    def test_entry_fee_mode(self):     self.assertEqual(CFG.entry_fee_mode, "maker")
    def test_taker_fee(self):          self.assertAlmostEqual(CFG.taker_fee_bps, 0.5)
    def test_maker_fee(self):          self.assertAlmostEqual(CFG.maker_fee_bps, 0.2)

    def test_build_goat_config_matches_constant(self):
        fresh = build_goat_config()
        self.assertAlmostEqual(fresh.threshold,       CFG.threshold)
        self.assertAlmostEqual(fresh.stop_loss_pct,   CFG.stop_loss_pct)
        self.assertAlmostEqual(fresh.tp1_pct,         CFG.tp1_pct)
        self.assertEqual(fresh.cooldown_seconds,      CFG.cooldown_seconds)
        self.assertEqual(fresh.confirmation_cycles,   CFG.confirmation_cycles)
        self.assertEqual(fresh.max_daily_trades,      CFG.max_daily_trades)


# ---------------------------------------------------------------------------
# 2. Fee model
# ---------------------------------------------------------------------------

class TestFeeModel(unittest.TestCase):

    def test_maker_round_trip_bps(self):
        """Round-trip = 0.2 (maker entry) + 0.5 (taker SL) = 0.7 bps."""
        entry = CFG.maker_fee_bps if CFG.entry_fee_mode == "maker" else CFG.taker_fee_bps
        fee_rt = (entry + CFG.taker_fee_bps) / 10_000.0
        self.assertAlmostEqual(fee_rt, 0.7 / 10_000.0, places=8)

    def test_maker_fee_lower_than_taker(self):
        self.assertLess(CFG.maker_fee_bps, CFG.taker_fee_bps)


# ---------------------------------------------------------------------------
# 3. V3 signal activation
# ---------------------------------------------------------------------------

class TestV3Activation(unittest.TestCase):

    def test_score_is_float(self):
        s = _score_once(cvd=5.0, trend=6.0, vol=5.0,
                        trend30=5.0, range30=50.0,
                        ts=1_745_000_000, price=75_000.0)
        self.assertIsInstance(s, float)

    def test_score_in_plausible_range(self):
        for r30 in (0.0, 20.0, 60.0):
            s = _score_once(cvd=4.0, trend=5.0, vol=5.0,
                            trend30=4.0, range30=r30,
                            ts=1_745_000_000, price=75_000.0)
            self.assertLessEqual(abs(s), 200.0,
                                 f"Score out of range for range30={r30}: {s}")

    def test_zero_range30_is_stable(self):
        """range30=0 must not crash — V3 should short-circuit."""
        s = _score_once(cvd=5.0, trend=6.0, vol=5.0, range30=0.0, ts=0.0, price=0.0)
        self.assertIsInstance(s, float)
        self.assertFalse(abs(s) > 100.0)


# ---------------------------------------------------------------------------
# 4 & 5. build_trade_decision — gate correctness + determinism
# ---------------------------------------------------------------------------

class TestBuildTradeDecision(unittest.TestCase):

    # -- Basic API -----------------------------------------------------------

    def test_returns_trade_decision_log(self):
        dlog = _decide()
        self.assertIsInstance(dlog, TradeDecisionLog)

    def test_score_field_populated(self):
        dlog = _decide(score=9.0)
        self.assertAlmostEqual(dlog.score, 9.0)

    def test_regime_field_is_string(self):
        dlog = _decide()
        self.assertIsInstance(dlog.regime, str)
        self.assertIn(dlog.regime, ("TREND", "FLOW", "MEAN_REVERT",
                                    "LIQUIDATION", "CHOP"))

    # -- Signal direction ----------------------------------------------------

    def test_flat_score_gives_flat_signal(self):
        dlog = _decide(score=0.0)
        self.assertEqual(dlog.signal, "FLAT")
        self.assertFalse(dlog.approved)

    def test_positive_score_gives_short(self):
        dlog = _decide(score=10.0)
        self.assertEqual(dlog.signal, "SHORT")

    def test_negative_score_gives_long(self):
        dlog = _decide(score=-10.0)
        self.assertEqual(dlog.signal, "LONG")

    # -- Gate rejections -----------------------------------------------------

    def test_flat_signal_not_approved(self):
        dlog = _decide(score=0.0)
        self.assertFalse(dlog.approved)
        self.assertIn(dlog.rejection_reason,
                      ("flat_signal", "no_signal", "", "quality_fail",
                       "acc_fail", "coh_fail"))

    def test_strong_signal_approved(self):
        dlog = _decide(score=10.0, trend=7.0, vol=7.0)
        self.assertTrue(dlog.approved,
                        msg=f"Expected approved; got: {dlog.rejection_reason}")

    def test_spread_blocks(self):
        dlog = _decide(score=10.0, spread=99.0)
        self.assertFalse(dlog.approved)
        self.assertEqual(dlog.rejection_reason, "spread_too_wide")

    def test_vol_too_low_blocks(self):
        dlog = _decide(score=10.0, vol=0.5)   # below min_vol_bps default
        self.assertFalse(dlog.approved)
        self.assertEqual(dlog.rejection_reason, "vol_too_low")

    def test_cooldown_blocks(self):
        ts = 1_000_000_000.0
        dlog = _decide(score=10.0, ts=ts, last_ts=ts - 5.0)  # 5s < 15s cooldown
        self.assertFalse(dlog.approved)
        self.assertEqual(dlog.rejection_reason, "cooldown")

    def test_cluster_blocks(self):
        dlog = _decide(score=10.0, clustered=True)
        self.assertFalse(dlog.approved)
        self.assertEqual(dlog.rejection_reason, "clustered")

    def test_confirmation_pending_blocks(self):
        # confirmation_cycles=1, so signal_count=0 should block
        dlog = _decide(score=10.0, signal_count=0)
        self.assertFalse(dlog.approved)
        self.assertEqual(dlog.rejection_reason, "confirmation_pending")

    # -- Near-miss flag ------------------------------------------------------

    def test_near_miss_flag_set_below_threshold(self):
        # score=7.0, eff_thr=8.0 → 7.0 >= 0.7*8.0=5.6 and 7.0 < 8.0
        dlog = _decide(score=7.0, eff_thr=8.0)
        self.assertTrue(dlog.near_miss)

    def test_near_miss_flag_not_set_above_threshold(self):
        dlog = _decide(score=10.0, eff_thr=8.0)
        self.assertFalse(dlog.near_miss)

    def test_near_miss_flag_not_set_far_below_threshold(self):
        dlog = _decide(score=1.0, eff_thr=8.0)   # 1.0 < 0.7*8.0=5.6
        self.assertFalse(dlog.near_miss)

    # -- Determinism ---------------------------------------------------------

    def test_same_inputs_same_outputs(self):
        kwargs = dict(score=9.5, trend=6.0, vol=5.5, ts=1_000_000_000.0)
        d1 = _decide(**kwargs)
        d2 = _decide(**kwargs)
        self.assertEqual(d1.approved,          d2.approved)
        self.assertEqual(d1.rejection_reason,  d2.rejection_reason)
        self.assertEqual(d1.signal,            d2.signal)
        self.assertAlmostEqual(d1.confidence,  d2.confidence)
        self.assertAlmostEqual(d1.adv_final,   d2.adv_final)

    # -- Approved log fields are complete ------------------------------------

    def test_approved_log_fields(self):
        dlog = _decide(score=10.0, trend=7.0, vol=7.0)
        self.assertTrue(dlog.approved)
        self.assertGreater(dlog.confidence, 0.0)
        self.assertGreater(dlog.adv_final,  0.0)
        self.assertEqual(dlog.rejection_reason, "")

    # -- JSON serialisable ---------------------------------------------------

    def test_to_dict_json_serialisable(self):
        import json
        dlog = _decide(score=10.0)
        d = dlog.to_dict()
        payload = json.dumps(d)          # must not raise
        self.assertIn("score", payload)
        self.assertIn("approved", payload)


# ---------------------------------------------------------------------------
# 5. Structural coherence invariants
# ---------------------------------------------------------------------------

class TestDecisionLogCoherence(unittest.TestCase):
    """Invariants that must hold for every TradeDecisionLog, regardless of inputs.

    These tests catch regressions where the approval flag and the supporting
    fields fall out of sync — e.g. approved=True with a non-empty rejection
    reason, or a FLAT signal reaching approved=True.
    """

    # ── Approval invariants ─────────────────────────────────────────────────

    def test_approved_implies_empty_rejection_reason(self):
        dlog = _decide(score=10.0, trend=7.0, vol=7.0)
        self.assertTrue(dlog.approved)
        self.assertEqual(dlog.rejection_reason, "",
                         "approved=True must have rejection_reason=''")

    def test_rejected_implies_nonempty_rejection_reason(self):
        dlog = _decide(score=0.0)   # flat → rejected
        self.assertFalse(dlog.approved)
        # rejection_reason may be empty for FLAT (gate returns "flat_signal" or "")
        # but must never be empty for a non-flat rejected signal
        dlog2 = _decide(score=10.0, spread=99.0)
        self.assertFalse(dlog2.approved)
        self.assertNotEqual(dlog2.rejection_reason, "",
                            "non-flat rejected signal must have a rejection_reason")

    def test_approved_implies_signal_not_flat(self):
        dlog = _decide(score=10.0, trend=7.0, vol=7.0)
        self.assertTrue(dlog.approved)
        self.assertNotEqual(dlog.signal, "FLAT",
                            "approved=True must have signal != FLAT")

    def test_flat_signal_never_approved(self):
        dlog = _decide(score=0.0)
        self.assertEqual(dlog.signal, "FLAT")
        self.assertFalse(dlog.approved,
                         "FLAT signal must never reach approved=True")

    # ── near_miss invariants ────────────────────────────────────────────────

    def test_near_miss_and_approved_are_mutually_exclusive(self):
        """A near-miss is below threshold, so it cannot also be approved."""
        dlog = _decide(score=7.0, eff_thr=8.0)   # below threshold
        if dlog.near_miss:
            self.assertFalse(dlog.approved,
                             "near_miss=True implies below threshold -> cannot be approved")

    def test_near_miss_iff_score_in_range(self):
        eff_thr = 8.0
        for score, expected in [
            (5.5, False),   # 5.5 < 0.7*8=5.6 → not near miss
            (5.7, True),    # 5.6 <= 5.7 < 8.0 → near miss
            (7.9, True),    # just under threshold
            (8.0, False),   # at threshold → signal, not near miss
            (10.0, False),  # above threshold
        ]:
            dlog = _decide(score=score, eff_thr=eff_thr)
            self.assertEqual(
                dlog.near_miss, expected,
                f"score={score} eff_thr={eff_thr}: expected near_miss={expected}, got {dlog.near_miss}",
            )

    # ── Score field coherence ───────────────────────────────────────────────

    def test_score_matches_signal_direction(self):
        pos = _decide(score=10.0)
        self.assertEqual(pos.signal, "SHORT")
        neg = _decide(score=-10.0)
        self.assertEqual(neg.signal, "LONG")
        flat = _decide(score=0.0)
        self.assertEqual(flat.signal, "FLAT")

    def test_eff_threshold_stored_correctly(self):
        dlog = _decide(score=10.0, eff_thr=9.5)
        self.assertAlmostEqual(dlog.eff_threshold, 9.5)

    def test_signal_count_stored_correctly(self):
        for cnt in (0, 1, 3, 10):
            dlog = _decide(score=10.0, signal_count=cnt)
            self.assertEqual(dlog.signal_count, cnt)

    def test_clustered_stored_correctly(self):
        d_true  = _decide(score=10.0, clustered=True)
        d_false = _decide(score=10.0, clustered=False)
        self.assertTrue(d_true.clustered)
        self.assertFalse(d_false.clustered)

    # ── Sequential _last_score propagation ─────────────────────────────────

    def test_last_score_influences_acc_ok(self):
        """acc_ok=False when score drops sharply from a high previous score.
        Concretely: last_score=13.0, score=3.0 → abs(3/13)=0.23 < min_score_acceleration=0.65
        → acc_ok=False → should_trade=False.
        """
        # Large previous score, small current score
        dlog = _decide(
            score=3.0, eff_thr=8.0,
            last_score=13.0,   # previous tick was very strong
            trend=6.0, vol=5.0,
        )
        # With such a sharp drop the acceleration check should fire.
        # The score 3.0 < 8.0 so signal is FLAT anyway — we verify acc_ok
        # is False when we force a non-flat scenario by lowering eff_thr.
        dlog2 = _decide(
            score=3.5, eff_thr=3.0,     # score above threshold
            last_score=13.0,
            trend=6.0, vol=5.0,
        )
        # abs(3.5) / abs(13.0) = 0.27 < 0.65 → acc_ok should be False
        self.assertFalse(dlog2.acc_ok,
                         "Sharp score drop should set acc_ok=False")

    def test_last_score_zero_skips_acc_check(self):
        """When last_score=0 (session start) the acc check is bypassed."""
        dlog = _decide(score=9.0, eff_thr=8.0, last_score=0.0,
                       trend=6.0, vol=5.0)
        self.assertTrue(dlog.acc_ok,
                        "acc check should pass when last_score=0")

    def test_sequential_decisions_update_last_score(self):
        """Simulate two consecutive ticks; verify last_score feeds correctly."""
        gate = TradeGate(CFG)

        # Tick 1 — low score
        dlog1 = build_trade_decision(
            score=2.0, ts=1_000_000_000.0, price=75_000.0,
            cvd=1.0, trend=2.0, vol=5.0, spread=1.0,
            regime=Regime.TREND, eff_thr=8.0, r_mult=1.0,
            signal_count=0, clustered=False,
            last_trade_ts=0.0, last_score=0.0,
            cfg=CFG, gate=gate,
        )
        last_score_after_t1 = dlog1.score   # caller must do: self._last_score = dlog.score

        # Tick 2 — high score; last_score is from tick 1 (not 0)
        dlog2 = build_trade_decision(
            score=10.0, ts=1_000_000_001.0, price=75_000.0,
            cvd=5.0, trend=7.0, vol=6.0, spread=1.0,
            regime=Regime.TREND, eff_thr=8.0, r_mult=1.0,
            signal_count=1, clustered=False,
            last_trade_ts=0.0, last_score=last_score_after_t1,
            cfg=CFG, gate=gate,
        )
        self.assertAlmostEqual(dlog2.score, 10.0)
        # With last_score=2.0 and score=10.0: 10/2=5.0 >> 0.65 → acc_ok=True
        self.assertTrue(dlog2.acc_ok)

    # ── DecisionLogger coherence ────────────────────────────────────────────

    def test_decision_logger_writes_approved(self):
        import tempfile, os
        from au2_decision_logger import DecisionLogger
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            dl = DecisionLogger(path, filter_policy="approved_and_near_miss")
            approved = _decide(score=10.0, trend=7.0, vol=7.0)
            rejected = _decide(score=0.5)
            dl.log(approved)
            dl.log(rejected)
            dl.close()
            with open(path) as fh:
                lines = fh.readlines()
            self.assertEqual(len(lines), 1, "Only approved should be written")
            import json
            d = json.loads(lines[0])
            self.assertTrue(d["approved"])
            self.assertIn("_logged_at", d)
        finally:
            os.unlink(path)

    def test_decision_logger_near_miss_written(self):
        import tempfile, os
        from au2_decision_logger import DecisionLogger
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            dl = DecisionLogger(path, filter_policy="approved_and_near_miss")
            near_miss = _decide(score=7.0, eff_thr=8.0)   # near_miss=True
            self.assertTrue(near_miss.near_miss)
            dl.log(near_miss)
            dl.close()
            with open(path) as fh:
                lines = fh.readlines()
            self.assertEqual(len(lines), 1, "Near-miss should be written")
        finally:
            os.unlink(path)

    def test_decision_logger_summary_counts(self):
        from au2_decision_logger import DecisionLogger
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            dl = DecisionLogger(path, filter_policy="all")
            dl.log(_decide(score=10.0, trend=7.0, vol=7.0))  # approved
            dl.log(_decide(score=10.0, spread=99.0))          # rejected (spread)
            dl.log(_decide(score=0.0))                         # flat
            dl.close()
            s = dl.summary()
            self.assertEqual(s["approved"], 1)
            self.assertGreaterEqual(s["rejected"], 1)
        finally:
            os.unlink(path)

    def test_decision_logger_read_summary(self):
        import tempfile, os
        from au2_decision_logger import DecisionLogger
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            dl = DecisionLogger(path, filter_policy="all")
            for _ in range(3):
                dl.log(_decide(score=10.0, trend=7.0, vol=7.0))   # approved
            dl.log(_decide(score=10.0, spread=99.0))               # rejected
            dl.close()
            s = DecisionLogger.read_summary(path)
            self.assertEqual(s["approved"], 3)
            self.assertIn("spread_too_wide", s["rejections"])
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# 6. MarketState produces trend_30s_bps
# ---------------------------------------------------------------------------

class TestMarketStateTrend30(unittest.TestCase):

    def _market_state(self):
        try:
            from au2_bot_live import MarketState
            return MarketState(window_s=5.0)
        except ImportError:
            self.skipTest("au2_bot_live not importable (websockets missing?)")

    def test_trend_30s_bps_key_present(self):
        ms = self._market_state()
        base = time.time() - 35.0
        for i in range(10):
            ms.on_agg_trade(75_000.0 + i * 10, 0.1, False, base + i * 3.5)
        state = ms.build()
        self.assertIn("trend_30s_bps", state,
                      "trend_30s_bps must be in MarketState.build() output")

    def test_trend_30s_bps_nonzero_on_movement(self):
        ms = self._market_state()
        base = time.time() - 35.0
        for i in range(10):
            ms.on_agg_trade(75_000.0 + i * 40.0, 0.1, False, base + i * 3.5)
        state = ms.build()
        self.assertNotEqual(state["trend_30s_bps"], 0.0,
                            "trend_30s_bps should reflect price movement")

    def test_trend_30s_bps_sign_correct(self):
        """Rising price over 30s → positive trend_30s_bps."""
        ms = self._market_state()
        base = time.time() - 35.0
        for i in range(10):
            ms.on_agg_trade(74_000.0 + i * 50.0, 0.1, False, base + i * 3.5)
        state = ms.build()
        self.assertGreater(state["trend_30s_bps"], 0.0,
                           "Rising prices should give positive trend_30s_bps")


if __name__ == "__main__":
    unittest.main(verbosity=2)

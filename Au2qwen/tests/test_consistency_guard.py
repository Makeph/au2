"""Tests for au2_consistency_guard.py — GOAT Pay Later consistency rules."""
from __future__ import annotations
import sys, pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core",):
    if str(_d) not in sys.path:
        sys.path.insert(0, str(_d))

import pytest
from au2_consistency_guard import ConsistencyGuard, ConsistencyGuardConfig


@pytest.fixture
def cfg():
    return ConsistencyGuardConfig(
        max_best_day_share       = 0.18,
        valid_day_min_profit_pct = 0.50,
        stop_after_valid_day     = True,
        min_valid_days           = 3,
        enabled                  = True,
    )


@pytest.fixture
def guard(cfg):
    return ConsistencyGuard(cfg)


EQUITY = 10_000.0


# ── Consistency share cap ─────────────────────────────────────────────────────

def test_consistency_cap_blocks_when_share_exceeds_18pct(guard):
    """If today would be the best day AND share > 18%, block new entries.
    Period = {$20}, today = $25 → today IS best day, share = $25/$45 = 55% > 18%.
    $25 < $50 valid-day threshold → valid-day rule skipped, only cap fires.
    """
    guard._period_day_profits = {"2026-01-01": 20.0}
    blocked, reason = guard.should_block("2026-01-04", 25.0, EQUITY)
    assert blocked, "Should block when today is best day with share > 18%"
    assert "paylater_consistency_cap" in reason


def test_consistency_cap_allows_when_share_below_18pct(guard):
    """If today is NOT the best day, adding profit today dilutes existing best → allow."""
    guard._period_day_profits = {"2026-01-01": 200.0, "2026-01-02": 200.0, "2026-01-03": 200.0}
    # Today: $40 < $200 (best day) → today is not best day → allow
    blocked, reason = guard.should_block("2026-01-04", 40.0, EQUITY)
    assert not blocked, f"Should allow when today is not best day, got: {reason}"


def test_no_block_when_today_pnl_zero_or_negative(guard):
    """No block when today has no profit (consistency rule doesn't apply)."""
    guard._period_day_profits = {"2026-01-01": 100.0}
    blocked, reason = guard.should_block("2026-01-02", 0.0, EQUITY)
    assert not blocked
    blocked, reason = guard.should_block("2026-01-02", -50.0, EQUITY)
    assert not blocked


# ── Stop after valid day ──────────────────────────────────────────────────────

def test_stop_after_valid_day_blocks(guard):
    """Once a valid day is reached, block further entries."""
    # valid_day_min_profit_pct = 0.5% of $10k = $50
    blocked, reason = guard.should_block("2026-01-04", 60.0, EQUITY)
    assert blocked, "Should block after valid day threshold reached"
    assert reason == "paylater_valid_day_reached"


def test_stop_after_valid_day_allows_below_threshold(guard):
    """If daily profit hasn't hit valid threshold AND today is not best day → allow."""
    guard._period_day_profits = {"2026-01-01": 200.0}
    # $30 < $50 valid-day threshold AND $30 < $200 (best day) → both rules skip
    blocked, reason = guard.should_block("2026-01-02", 30.0, EQUITY)
    assert not blocked, f"Should allow below valid-day threshold, got: {reason}"


def test_stop_after_valid_day_disabled(cfg):
    """When stop_after_valid_day=False, don't block even after valid day.
    Use large period so consistency cap doesn't fire either.
    """
    cfg.stop_after_valid_day = False
    g = ConsistencyGuard(cfg)
    # 10 days × $200 = $2000, today $60 → share = $60/$2060 = 2.9% < 18%
    g._period_day_profits = {f"2026-01-{i:02d}": 200.0 for i in range(1, 11)}
    # $60 > $50 valid-day threshold, but stop_after_valid_day=False → no block
    blocked, reason = g.should_block("2026-01-11", 60.0, EQUITY)
    assert not blocked, f"Should allow when stop_after_valid_day=False, got: {reason}"


# ── Valid day counting ────────────────────────────────────────────────────────

def test_valid_day_counting(guard):
    """Days with profit >= 0.5% of equity are counted as valid."""
    report = guard.payout_readiness(EQUITY)
    assert report.valid_day_count == 0

    # Add 2 valid days ($50+ each) and 1 invalid day ($20)
    guard._period_day_profits = {
        "2026-01-01": 55.0,   # 0.55% → valid
        "2026-01-02": 20.0,   # 0.20% → invalid
        "2026-01-03": 60.0,   # 0.60% → valid
    }
    report = guard.payout_readiness(EQUITY)
    assert report.valid_day_count == 2


def test_min_valid_days_eligibility(guard):
    """Payout not eligible until min_valid_days (3) reached."""
    guard._period_day_profits = {
        "2026-01-01": 55.0,
        "2026-01-02": 55.0,
    }
    report = guard.payout_readiness(EQUITY)
    assert not report.eligible

    guard._period_day_profits["2026-01-03"] = 55.0
    # Now 3 valid days — check share too
    report = guard.payout_readiness(EQUITY)
    # 3 days × $55 = $165 total, best_day = $55 = 33% > 18% → not eligible by share
    # (equal days: $55/$165 = 33.3%)
    assert not report.eligible   # best_day_share > max_best_day_share


def test_payout_eligible_with_distributed_profits(guard):
    """Eligible when 3+ valid days AND best_day_share <= 18%."""
    # 5 valid days, roughly equal → best_day_share ~20% → borderline
    # Use spread days to stay below 18%
    guard._period_day_profits = {
        "2026-01-01": 50.0,
        "2026-01-02": 55.0,
        "2026-01-03": 52.0,
        "2026-01-04": 60.0,
        "2026-01-05": 58.0,   # best: $60 / $275 = 21.8% → not eligible
    }
    report = guard.payout_readiness(EQUITY)
    # best = 60, total = 275, share = 21.8% > 18% → not eligible
    assert not report.eligible

    # Add more days to dilute
    guard._period_day_profits["2026-01-06"] = 55.0
    guard._period_day_profits["2026-01-07"] = 58.0
    guard._period_day_profits["2026-01-08"] = 54.0
    # total = 275 + 167 = 442, best = 60, share = 13.6% < 18% → eligible
    report = guard.payout_readiness(EQUITY)
    assert report.eligible


# ── Guard disabled ────────────────────────────────────────────────────────────

def test_guard_disabled_never_blocks(cfg):
    """When enabled=False, guard never blocks regardless of state."""
    cfg.enabled = False
    g = ConsistencyGuard(cfg)
    g._today_valid = True
    blocked, reason = g.should_block("2026-01-04", 9999.0, EQUITY)
    assert not blocked


# ── Persistence ──────────────────────────────────────────────────────────────

def test_dump_load_roundtrip(guard):
    """State survives a dump/load cycle."""
    guard._period_day_profits = {"2026-01-01": 100.0, "2026-01-02": 80.0}
    guard._today_valid = True

    snapshot = guard.dump()
    g2 = ConsistencyGuard(guard.cfg)
    g2.load(snapshot)

    assert g2._period_day_profits == guard._period_day_profits
    assert g2._today_valid == True


# ── GOAT vs ALPHA behavior from same conditions ───────────────────────────────

def test_goat_blocks_alpha_allows_same_signal(cfg):
    """GOAT guard (enabled) blocks after valid day; ALPHA guard (disabled) allows."""
    goat = ConsistencyGuard(cfg)          # enabled=True
    alpha_cfg = ConsistencyGuardConfig(enabled=False)
    alpha = ConsistencyGuard(alpha_cfg)

    # Both see the same conditions: today_pnl = $60 (valid day threshold crossed)
    goat_blocked, _ = goat.should_block("2026-01-04", 60.0, EQUITY)
    alpha_blocked, _ = alpha.should_block("2026-01-04", 60.0, EQUITY)

    assert goat_blocked,  "GOAT should block after valid day"
    assert not alpha_blocked, "ALPHA should always allow (guard disabled)"

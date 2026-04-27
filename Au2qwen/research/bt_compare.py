#!/usr/bin/env python3
"""
Backtest comparison: FTMO 10k vs GOAT Cash — same database, same signals.

Uses the DB's pre-computed `score` column (original v22_5 formula, range ≈ ±8)
so both presets see identical raw signals and are only differentiated by their
threshold, filters, position parameters and risk management.

Usage:
    python bt_compare.py [db_path]
    default db: ../au2_v22_5_live_fresh.db
"""
import sys, math, datetime, collections, sqlite3, logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional

# Silence INFO logs during run
logging.basicConfig(level=logging.WARNING)

import sys as _sys
_sys.path.insert(0, ".")

from au2_core import (
    CoreConfig, RiskEngine, PositionManager, SizingEngine,
    SignalProcessor, PropProfile, RiskState, Regime,
    TradeResult, PositionFill, PositionEvent,
    TradeGate, SignalQuality, SelectivityEngine,
)
from au2_risk_overlay import RiskOverlay, RiskOverlayConfig
from au2_preset_ftmo_10k     import CFG as FTMO_CFG, OVERLAY_CFG as FTMO_OV, PROP_FTMO_SAFE
from au2_goat_cash_preset    import CFG as GOAT_CFG, OVERLAY_CFG as GOAT_OV, GOAT_CASH_START_EQUITY

DB = sys.argv[1] if len(sys.argv) > 1 else "../au2_v22_5_live_fresh.db"

# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class RunMetrics:
    trades:         List[TradeResult] = field(default_factory=list)
    rejections:     Dict[str, int]    = field(default_factory=lambda: collections.defaultdict(int))
    start_equity:   float = 10000.0

# ─────────────────────────────────────────────────────────────────────────────
def run_backtest(
    cfg: CoreConfig,
    overlay_cfg: RiskOverlayConfig,
    prop: Optional[PropProfile],
    start_equity: float,
    rows: list,
) -> RunMetrics:
    """
    Minimal faithful backtest that drives PositionManager + RiskEngine
    with the DB's pre-computed score (bypasses local SignalProcessor.score()
    whose formula currently produces values that cannot exceed the thresholds).
    """
    risk    = RiskEngine(cfg, start_equity)
    pm      = PositionManager(cfg)
    overlay = RiskOverlay(start_equity=start_equity, cfg=overlay_cfg)
    gate    = TradeGate(cfg, prop)
    sel     = SelectivityEngine(cfg)

    result  = RunMetrics(start_equity=start_equity)
    rej     = result.rejections

    last_trade_ts  = 0.0
    signal_side    = ""
    signal_count   = 0
    signal_ts      = 0.0
    last_score     = 0.0
    current_builder_side  = ""
    current_builder_entry = 0.0
    current_builder_score = 0.0
    current_builder_qty   = 0.0
    current_builder_regime = ""
    current_builder_ts    = 0.0
    current_builder_conf  = 0.0
    partial_fills: List[PositionFill] = []

    peak_eq = start_equity

    for row in rows:
        ts    = float(row["ts"])
        price = float(row["price"])
        if not price or price <= 0:
            continue

        # Daily reset
        day = datetime.datetime.fromtimestamp(ts, datetime.UTC).strftime("%Y-%m-%d")
        if day != risk.last_day:
            risk.reset_day(day)
            if hasattr(overlay, "reset_day"):
                overlay.reset_day()

        # ── Risk / overlay gates ──────────────────────────────────────────────
        r_state, r_mult, _ = risk.evaluate(ts)
        if not risk.can_trade(ts):
            rej["blocked_by_core_trade_limits"] += 1
            # still update position if open
        if r_state == RiskState.RED:
            rej["blocked_by_core_risk"] += 1

        ov_blocked, ov_reason = overlay.should_block(ts)
        if ov_blocked:
            rej[ov_reason] += 1

        # ── Position management (always runs) ────────────────────────────────
        if pm.pos:
            spread = float(row["spread_bps"] or 0) or cfg.assume_spread_bps
            hits = pm.update(ts, price, spread)
            for h in hits:
                partial_fills.append(h)
                if h.qty > 0:
                    risk.update_equity(h.pnl_usd)
                    if risk.current_equity > peak_eq:
                        peak_eq = risk.current_equity
            if not pm.pos and partial_fills:
                # Build TradeResult from partial fills
                total_pnl   = sum(f.pnl_usd for f in partial_fills if f.qty > 0)
                exit_reason = partial_fills[-1].event.name
                entry_fill  = partial_fills[0]
                exit_fill   = partial_fills[-1]
                hold_s      = (exit_fill.ts - current_builder_ts) if current_builder_ts else 0.0
                result.trades.append(TradeResult(
                    signal      = current_builder_side,
                    entry_price = current_builder_entry,
                    entry_ts    = current_builder_ts,
                    exit_price  = exit_fill.price,
                    exit_ts     = exit_fill.ts,
                    pnl_usd     = total_pnl,
                    hold_seconds= hold_s,
                    exit_reason = exit_reason,
                    entry_score = current_builder_score,
                    confidence  = current_builder_conf,
                    regime      = current_builder_regime,
                    qty         = current_builder_qty,
                ))
                risk.record_result(total_pnl, ts)
                risk.trigger_loss_pause(ts)
                overlay.update_equity(total_pnl, ts)
                last_trade_ts = ts
                partial_fills = []
            continue  # skip entry logic while in position

        # ── Entry gate ───────────────────────────────────────────────────────
        if r_state == RiskState.RED or ov_blocked or not risk.can_trade(ts):
            continue

        # Use DB score directly (same signals for both presets)
        db_score = float(row["score"] or 0)
        cvd  = float(row["cvd_delta_5s"] or 0)
        trend = float(row["trend_bps"] or 0)
        vol   = float(row["realized_vol_bps"] or 0)
        spread = float(row["spread_bps"] or 0)
        regime_str = SignalProcessor.classify_regime(vol, trend, cvd)
        regime = Regime[regime_str] if regime_str in Regime.__members__ else Regime.CHOP

        # Dynamic threshold adjustment
        day_dd_pct = max(
            (risk.day_start_equity - risk.current_equity) / max(risk.day_start_equity, 1) * 100.0, 0.0
        )
        dyn_t, dyn_r = sel.compute_dynamic_multiplier(
            risk.current_equity, risk.day_start_equity, risk.recent_wr(), day_dd_pct
        )
        eff_thr = cfg.threshold * dyn_t

        # Signal direction from DB score
        abs_score = abs(db_score)
        if db_score >= eff_thr:
            signal = "SHORT"
        elif db_score <= -eff_thr:
            signal = "LONG"
        else:
            signal_side = ""; signal_count = 0
            continue

        # Confidence & quality
        conf = min(abs_score / max(eff_thr, 1e-9), 1.5)
        rq   = cfg.regime_quality_multiplier.get(regime, 1.0)
        eff  = abs_score * conf * rq

        trade_ok = (
            eff >= cfg.min_setup_quality
            and conf >= cfg.min_confidence_threshold
            and rq >= 0.75
        )
        coh_ok = (abs(trend) > 1.0 or vol > 4.0)
        q = SignalQuality(
            signal         = signal,
            score          = db_score,
            confidence     = conf,
            regime_quality = rq,
            eff            = eff,
            should_trade   = trade_ok,
            acc_ok         = True,
            coh_ok         = coh_ok,
        )

        if not q.should_trade:
            reason = (
                "conf_low"   if conf < cfg.min_confidence_threshold else
                "regime_weak" if rq < 0.75 else
                "setup_weak"
            )
            rej[reason] += 1
            continue

        # Vol / spread / cooldown / confirmation / cluster
        if vol < cfg.min_vol_bps:
            rej["vol_too_low"] += 1; continue
        if spread > cfg.max_spread_bps:
            rej["spread_too_wide"] += 1; continue
        if ts - last_trade_ts < cfg.cooldown_seconds:
            rej["cooldown"] += 1; continue
        if sel.is_clustered(ts, signal):
            rej["cluster"] += 1; continue

        # Confirmation cycles
        if signal == signal_side and (ts - signal_ts) <= 3.0:
            signal_count += 1
        else:
            signal_side, signal_count, signal_ts = signal, 1, ts

        if signal_count < cfg.confirmation_cycles:
            rej["confirmation_pending"] += 1
            continue

        # Prop extra gates
        if prop:
            if risk._daily_trades >= prop.daily_trade_cap:
                rej["blocked_by_prop_trade_cap"] += 1; continue
            prop_day_dd = max(
                (risk.day_start_equity - risk.current_equity) / max(risk.day_start_equity, 1) * 100, 0.0
            )
            if prop_day_dd >= prop.max_daily_dd_pct:
                rej["blocked_by_prop_daily_dd"] += 1; continue

        # ── Entry ─────────────────────────────────────────────────────────────
        risk_usd = SizingEngine.compute_risk_usd(
            risk.current_equity, cfg, conf, rq, r_mult * dyn_r, risk.consecutive_losses
        )
        sl_dist = cfg.stop_loss_pct / 100.0
        fee_rt  = 2.0 * cfg.taker_fee_bps / 10_000.0
        qty = risk_usd / max(price * (sl_dist + fee_rt), 1e-9)

        pos, exec_px = pm.open(ts, price, db_score, qty, signal, spread, regime, conf)
        partial_fills = []
        current_builder_side   = signal
        current_builder_entry  = exec_px
        current_builder_ts     = ts
        current_builder_score  = db_score
        current_builder_qty    = qty
        current_builder_regime = regime_str
        current_builder_conf   = conf
        last_score = db_score
        sel.record_entry(ts, signal)
        risk.record_trade(ts)
        signal_side, signal_count = "", 0

    return result


# ─────────────────────────────────────────────────────────────────────────────
print(f"\nLoading signals from: {DB}")
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
try:
    rows = conn.execute(
        "SELECT ts, price, score, cvd_delta_5s, trend_bps, realized_vol_bps, "
        "COALESCE(spread_bps, 0.0) as spread_bps FROM signals ORDER BY ts ASC"
    ).fetchall()
except sqlite3.OperationalError:
    rows = conn.execute(
        "SELECT ts, price, score, cvd_delta_5s, trend_bps, realized_vol_bps, "
        "0.0 as spread_bps FROM signals ORDER BY ts ASC"
    ).fetchall()
conn.close()

# Date span
ts_min = min(float(r["ts"]) for r in rows)
ts_max = max(float(r["ts"]) for r in rows)
db_days = (ts_max - ts_min) / 86400.0
print(f"  {len(rows):,} ticks  |  "
      f"{datetime.datetime.fromtimestamp(ts_min, datetime.UTC).strftime('%Y-%m-%d')} → "
      f"{datetime.datetime.fromtimestamp(ts_max, datetime.UTC).strftime('%Y-%m-%d')}  "
      f"({db_days:.1f} days)")

# Score stats
abs_scores = sorted(abs(float(r["score"] or 0)) for r in rows)
n = len(abs_scores)
for thr, name in [(GOAT_CFG.threshold, "GOAT"), (FTMO_CFG.threshold, "FTMO")]:
    cnt = sum(1 for v in abs_scores if v >= thr)
    print(f"  Score >= {thr} ({name} thr): {cnt:,} ticks ({cnt/n*100:.2f}%)")

print()

# ─────────────────────────────────────────────────────────────────────────────
# Run both presets
# ─────────────────────────────────────────────────────────────────────────────
configs = [
    ("FTMO 10k",  FTMO_CFG, FTMO_OV,  PROP_FTMO_SAFE,        10000.0),
    ("GOAT Cash", GOAT_CFG, GOAT_OV,  None,                   GOAT_CASH_START_EQUITY),
]

results = {}
for name, cfg, ov_cfg, prop, eq in configs:
    print(f"  Running {name}...", end=" ", flush=True)
    rm = run_backtest(cfg, ov_cfg, prop, eq, rows)
    results[name] = rm
    print(f"done — {len(rm.trades)} trades")

# ─────────────────────────────────────────────────────────────────────────────
# Analytics helpers
# ─────────────────────────────────────────────────────────────────────────────
def active_days(trades: List[TradeResult]) -> int:
    days = {datetime.datetime.fromtimestamp(t.entry_ts, datetime.UTC).strftime("%Y-%m-%d")
            for t in trades}
    return max(len(days), 1)

def per_day_stats(trades: List[TradeResult]):
    daily: Dict[str, float] = collections.defaultdict(float)
    for t in trades:
        d = datetime.datetime.fromtimestamp(t.entry_ts, datetime.UTC).strftime("%Y-%m-%d")
        daily[d] += t.pnl_usd
    vals = list(daily.values()) or [0.0]
    return sum(vals) / len(vals), max(vals), min(vals), len(vals)

def max_drawdown(trades: List[TradeResult], start_eq: float):
    eq = start_eq; peak = eq; max_dd_pct = 0.0
    for t in sorted(trades, key=lambda x: x.exit_ts):
        eq += t.pnl_usd
        if eq > peak: peak = eq
        dd_pct = (peak - eq) / max(peak, 1e-9) * 100.0
        if dd_pct > max_dd_pct: max_dd_pct = dd_pct
    return max_dd_pct, eq - start_eq

def profit_factor(trades: List[TradeResult]) -> float:
    gross_w = sum(t.pnl_usd for t in trades if t.pnl_usd > 0)
    gross_l = sum(-t.pnl_usd for t in trades if t.pnl_usd < 0)
    return gross_w / max(gross_l, 1e-9)

def exit_bd(trades: List[TradeResult]):
    c = collections.Counter(t.exit_reason for t in trades)
    total = len(trades) or 1
    return {k: (v, v / total * 100) for k, v in c.most_common()}

def regime_bd(trades: List[TradeResult]):
    by_pnl = collections.defaultdict(float)
    by_cnt = collections.Counter(t.regime for t in trades)
    for t in trades: by_pnl[t.regime] += t.pnl_usd
    return {r: (by_cnt[r], by_pnl[r]) for r in sorted(by_cnt, key=lambda x: -by_cnt[x])}

# Build row data
stats = {}
for name in ["FTMO 10k", "GOAT Cash"]:
    rm = results[name]
    trades = rm.trades
    eq = rm.start_equity
    n_active = active_days(trades)
    avg_dpnl, best_d, worst_d, n_days = per_day_stats(trades)
    dd_pct, final_pnl = max_drawdown(trades, eq)
    pf = profit_factor(trades)
    wins   = sum(1 for t in trades if t.pnl_usd > 0)
    losses = sum(1 for t in trades if t.pnl_usd <= 0)
    total  = len(trades)
    wr     = wins / max(total, 1)
    avg_pnl = sum(t.pnl_usd for t in trades) / max(total, 1)
    expectancy = avg_pnl
    avg_hold = sum(t.hold_seconds for t in trades) / max(total, 1)
    total_pnl = sum(t.pnl_usd for t in trades)
    ret_pct = total_pnl / eq * 100
    sharpe = avg_dpnl / (abs(worst_d) + 1e-9) if worst_d < 0 else float("inf")
    trades_pd = total / n_active
    stats[name] = dict(
        total=total, wins=wins, losses=losses, wr=wr,
        total_pnl=total_pnl, ret_pct=ret_pct,
        avg_pnl=avg_pnl, expectancy=expectancy,
        pf=pf, dd_pct=dd_pct,
        avg_hold=avg_hold, n_active=n_active,
        trades_pd=trades_pd,
        avg_dpnl=avg_dpnl, best_d=best_d, worst_d=worst_d,
        sharpe=sharpe,
        rejections=dict(rm.rejections),
        exit_bd=exit_bd(trades),
        regime_bd=regime_bd(trades),
    )

# ─────────────────────────────────────────────────────────────────────────────
# Print comparison table
# ─────────────────────────────────────────────────────────────────────────────
W = 70
print("\n" + "=" * W)
print(f"{'METRIC':<32} {'FTMO 10k':>16} {'GOAT Cash':>16}")
print("=" * W)

def prow(label, key, fmt, suffix="", lower_is_better=False):
    f = stats["FTMO 10k"][key]
    g = stats["GOAT Cash"][key]
    try:
        fv = format(f, fmt) + suffix
        gv = format(g, fmt) + suffix
        if lower_is_better:
            mark = "<" if f < g else (">" if g < f else "=")
        else:
            mark = ">" if f > g else ("<" if g > f else "=")
        winner = ("F" if (mark == ">" and not lower_is_better) or (mark == "<" and lower_is_better)
                  else ("G" if (mark == "<" and not lower_is_better) or (mark == ">" and lower_is_better)
                  else " "))
    except:
        fv, gv, winner = str(f), str(g), " "
    print(f"  {label:<30} {fv:>16} {gv:>16}   {winner}")

prow("Total trades",         "total",      "d")
prow("Active trading days",  "n_active",   "d")
prow("Trades / active day",  "trades_pd",  ".2f")
prow("Win rate",             "wr",         ".1%")
prow("  Wins",               "wins",       "d")
prow("  Losses",             "losses",     "d",  lower_is_better=True)
print("  " + "-" * (W - 2))
prow("Total PnL",            "total_pnl",  ".2f",  "$")
prow("Return on equity",     "ret_pct",    ".2f",  "%")
prow("PnL / active day",     "avg_dpnl",   ".2f",  "$")
prow("Best day",             "best_d",     ".2f",  "$")
prow("Worst day (best=high)","worst_d",    ".2f",  "$")
print("  " + "-" * (W - 2))
prow("Profit factor",        "pf",         ".3f",  "x")
prow("Expectancy / trade",   "expectancy", ".2f",  "$")
prow("Avg PnL / trade",      "avg_pnl",    ".2f",  "$")
prow("Avg hold (s)",         "avg_hold",   ".0f",  "s")
print("  " + "-" * (W - 2))
prow("Max DD %",             "dd_pct",     ".2f",  "%",  lower_is_better=True)
prow("Sharpe proxy",         "sharpe",     ".2f")
print("=" * W)

# ─────────────────────────────────────────────────────────────────────────────
# Verdict
# ─────────────────────────────────────────────────────────────────────────────
print("\n-- VERDICT " + "-" * (W - 11))
f, g = stats["FTMO 10k"], stats["GOAT Cash"]
score_g = 0; score_f = 0; verdict_lines = []

def chk(label, goat_wins, reason):
    global score_g, score_f
    tag = "[GOAT]" if goat_wins else "[FTMO]"
    mark = "+" if goat_wins else "-"
    if goat_wins: score_g += 1
    else:         score_f += 1
    verdict_lines.append(f"  {mark} {tag} {label}: {reason}")

chk("PnL/jour",
    g["avg_dpnl"] > f["avg_dpnl"],
    f"GOAT ${g['avg_dpnl']:.2f}/j  vs  FTMO ${f['avg_dpnl']:.2f}/j")
chk("Total PnL",
    g["total_pnl"] > f["total_pnl"],
    f"GOAT ${g['total_pnl']:.2f}  vs  FTMO ${f['total_pnl']:.2f}")
chk("Profit factor",
    g["pf"] > f["pf"],
    f"GOAT {g['pf']:.3f}x  vs  FTMO {f['pf']:.3f}x")
chk("Expectancy",
    g["expectancy"] > f["expectancy"],
    f"GOAT ${g['expectancy']:.2f}  vs  FTMO ${f['expectancy']:.2f}")
chk("Win rate",
    g["wr"] > f["wr"],
    f"GOAT {g['wr']:.1%}  vs  FTMO {f['wr']:.1%}")
chk("Max DD (lower is better)",
    g["dd_pct"] < f["dd_pct"],
    f"GOAT {g['dd_pct']:.2f}%  vs  FTMO {f['dd_pct']:.2f}%")
if math.isfinite(g["sharpe"]) and math.isfinite(f["sharpe"]):
    chk("Sharpe proxy",
        g["sharpe"] > f["sharpe"],
        f"GOAT {g['sharpe']:.2f}  vs  FTMO {f['sharpe']:.2f}")

for line in verdict_lines:
    print(line)

print()
total_criteria = score_g + score_f
if score_g > score_f:
    print(f"  ==> GOAT genuinely better: {score_g}/{total_criteria} criteria")
elif score_g == score_f:
    print(f"  ==> TIE {score_g}/{total_criteria} — not clearly better; inspect DD vs return trade-off")
else:
    print(f"  ==> FTMO wins {score_f}/{total_criteria} — GOAT more permissive but NOT more productive")
    print("      Review: GOAT signal quality, threshold calibration, DD window.")

# ─────────────────────────────────────────────────────────────────────────────
# Exit reason breakdown
# ─────────────────────────────────────────────────────────────────────────────
print("\n-- EXIT REASONS " + "-" * (W - 16))
print(f"  {'Exit reason':<24} {'FTMO n(%)':>14}  {'GOAT n(%)':>14}")
all_exits = sorted(set(list(f["exit_bd"]) + list(g["exit_bd"])))
for ex in all_exits:
    fn, fp = f["exit_bd"].get(ex, (0, 0.0))
    gn, gp = g["exit_bd"].get(ex, (0, 0.0))
    print(f"  {ex:<24} {fn:>5} ({fp:>5.1f}%)   {gn:>5} ({gp:>5.1f}%)")

# ─────────────────────────────────────────────────────────────────────────────
# Regime breakdown
# ─────────────────────────────────────────────────────────────────────────────
print("\n-- BY REGIME " + "-" * (W - 13))
print(f"  {'Regime':<14} {'FTMO n':>7} {'FTMO PnL':>11}  {'GOAT n':>7} {'GOAT PnL':>11}")
all_reg = sorted(set(list(f["regime_bd"]) + list(g["regime_bd"])))
for r in all_reg:
    fn, fp = f["regime_bd"].get(r, (0, 0.0))
    gn, gp = g["regime_bd"].get(r, (0, 0.0))
    print(f"  {r:<14} {fn:>7} {fp:>+11.2f}  {gn:>7} {gp:>+11.2f}")

# ─────────────────────────────────────────────────────────────────────────────
# Top rejection reasons
# ─────────────────────────────────────────────────────────────────────────────
print("\n-- TOP REJECTIONS (signals blocked before entry) " + "-" * (W - 49))
all_rj = sorted(
    set(list(f["rejections"]) + list(g["rejections"])),
    key=lambda k: -(f["rejections"].get(k, 0) + g["rejections"].get(k, 0))
)
print(f"  {'Reason':<32} {'FTMO':>10} {'GOAT':>10}")
for r in all_rj[:15]:
    fv = f["rejections"].get(r, 0)
    gv = g["rejections"].get(r, 0)
    print(f"  {r:<32} {fv:>10,} {gv:>10,}")

print()

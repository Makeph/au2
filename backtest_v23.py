#!/usr/bin/env python3
"""AU2 V23 Phase 1 — Backtest on historical signals.

Replays 1.2M+ signals from the live database, recomputes V23 scores
(CVD + Trend + Volume only), simulates entries/exits, and reports
expected performance.

Usage:
    python backtest_v23.py [--db au2_v22_5_live_fresh.db] [--threshold 4.00]
"""
from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


# ─── V23 Scoring Formula ─────────────────────────────────────────
def v23_score(cvd_delta_5s: float, trend_bps: float, realized_vol_bps: float) -> float:
    """Exact replica of V23 Phase 1 scoring: 3 components only."""
    cvd_score = clamp(cvd_delta_5s / 6.0, -3.0, 3.0)
    trend_score = clamp(trend_bps / 5.0, -2.0, 2.0)
    vol_score = clamp((realized_vol_bps - 5.0) / 10.0, -0.5, 0.5)
    return 2.00 * cvd_score + 1.50 * trend_score + vol_score


# ─── Config ──────────────────────────────────────────────────────
@dataclass
class BacktestConfig:
    threshold: float = 4.00
    stop_loss_pct: float = 0.20
    tp1_pct: float = 0.20
    tp2_pct: float = 0.45
    trailing_pct: float = 0.10
    max_hold_seconds: int = 120
    cooldown_seconds: int = 30
    confirmation_cycles: int = 2
    max_daily_trades: int = 10
    # Execution guard thresholds
    min_vol_bps: float = 3.0
    max_trend_adverse_bps: float = 2.2
    # Position sizing (simplified for backtest)
    notional_usd: float = 350.0
    taker_fee_bps: float = 4.5  # one-way
    # TP split
    tp1_ratio: float = 0.55
    tp2_ratio: float = 0.25
    runner_ratio: float = 0.20


@dataclass
class SimPosition:
    side: str  # LONG / SHORT
    entry_price: float
    entry_ts: int
    qty: float
    remaining_qty: float
    peak_price: float
    trough_price: float
    tp1_done: bool = False
    tp2_done: bool = False
    runner_active: bool = False
    sl_price: float = 0.0
    tp1_price: float = 0.0
    tp2_price: float = 0.0


@dataclass
class TradeResult:
    side: str
    entry_price: float
    exit_price: float
    entry_ts: int
    exit_ts: int
    pnl_usd: float
    pnl_bps: float
    hold_seconds: int
    exit_reason: str
    v23_score_at_entry: float
    cvd_at_entry: float
    trend_at_entry: float


def run_backtest(db_path: str, cfg: BacktestConfig) -> List[TradeResult]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Load all signals chronologically
    print("Loading signals...")
    rows = conn.execute(
        "SELECT ts, price, cvd_delta_5s, trend_bps, realized_vol_bps, "
        "regime_ok, trend_ok, time_ok, dangerous_time_ok, absorption "
        "FROM signals ORDER BY ts ASC"
    ).fetchall()
    print("Loaded %d signals" % len(rows))

    trades: List[TradeResult] = []
    position: Optional[SimPosition] = None
    last_trade_ts = 0
    signal_count = 0
    signal_side = ""
    signal_ts = 0.0
    daily_trades = {}  # date_str -> count

    for i, row in enumerate(rows):
        ts = row["ts"]
        price = row["price"]
        if not price or price <= 0:
            continue

        cvd = row["cvd_delta_5s"] or 0.0
        trend = row["trend_bps"] or 0.0
        vol = row["realized_vol_bps"] or 0.0
        regime_ok = bool(row["regime_ok"])
        trend_ok = bool(row["trend_ok"])
        time_ok = bool(row["time_ok"])
        dangerous_ok = bool(row["dangerous_time_ok"])
        absorption = bool(row["absorption"])

        score = v23_score(cvd, trend, vol)

        # ── Position management ──
        if position is not None:
            age = ts - position.entry_ts

            if position.side == "LONG":
                pnl_bps = (price - position.entry_price) / position.entry_price * 10000.0
                position.peak_price = max(position.peak_price, price)

                # SL hit
                if price <= position.sl_price:
                    exit_pnl = _calc_pnl(position, position.sl_price, cfg)
                    trades.append(TradeResult(
                        position.side, position.entry_price, position.sl_price,
                        position.entry_ts, ts, exit_pnl,
                        (position.sl_price - position.entry_price) / position.entry_price * 10000,
                        age, "SL", score, cvd, trend))
                    position = None
                    last_trade_ts = ts
                    continue

                # TP1
                if not position.tp1_done and price >= position.tp1_price:
                    position.tp1_done = True
                    # Move SL to breakeven + buffer
                    position.sl_price = position.entry_price + position.entry_price * 1.5 / 10000

                # TP2
                if position.tp1_done and not position.tp2_done and price >= position.tp2_price:
                    position.tp2_done = True
                    position.runner_active = True

                # Trailing stop for runner
                if position.runner_active:
                    trail = position.peak_price * (1 - cfg.trailing_pct / 100.0)
                    if price <= trail:
                        exit_pnl = _calc_pnl(position, price, cfg)
                        trades.append(TradeResult(
                            position.side, position.entry_price, price,
                            position.entry_ts, ts, exit_pnl, pnl_bps,
                            age, "runner trail", score, cvd, trend))
                        position = None
                        last_trade_ts = ts
                        continue

                # Early exit: 40% hold + losing -5bps + score reversed
                if age >= cfg.max_hold_seconds * 0.40 and not position.tp1_done and pnl_bps < -5.0:
                    if score < -1.0:  # score reversed for LONG
                        exit_pnl = _calc_pnl(position, price, cfg)
                        trades.append(TradeResult(
                            position.side, position.entry_price, price,
                            position.entry_ts, ts, exit_pnl, pnl_bps,
                            age, "early exit", score, cvd, trend))
                        position = None
                        last_trade_ts = ts
                        continue

                # Fast fail: signal completely reversed + losing
                if age <= 20 and pnl_bps <= -5.0 and score <= -3.0:
                    exit_pnl = _calc_pnl(position, price, cfg)
                    trades.append(TradeResult(
                        position.side, position.entry_price, price,
                        position.entry_ts, ts, exit_pnl, pnl_bps,
                        age, "fast fail", score, cvd, trend))
                    position = None
                    last_trade_ts = ts
                    continue

            else:  # SHORT
                pnl_bps = (position.entry_price - price) / position.entry_price * 10000.0
                position.trough_price = min(position.trough_price, price)

                # SL hit
                if price >= position.sl_price:
                    exit_pnl = _calc_pnl(position, position.sl_price, cfg)
                    trades.append(TradeResult(
                        position.side, position.entry_price, position.sl_price,
                        position.entry_ts, ts, exit_pnl,
                        (position.entry_price - position.sl_price) / position.entry_price * 10000,
                        age, "SL", score, cvd, trend))
                    position = None
                    last_trade_ts = ts
                    continue

                # TP1
                if not position.tp1_done and price <= position.tp1_price:
                    position.tp1_done = True
                    position.sl_price = position.entry_price - position.entry_price * 1.5 / 10000

                # TP2
                if position.tp1_done and not position.tp2_done and price <= position.tp2_price:
                    position.tp2_done = True
                    position.runner_active = True

                # Trailing stop for runner
                if position.runner_active:
                    trail = position.trough_price * (1 + cfg.trailing_pct / 100.0)
                    if price >= trail:
                        exit_pnl = _calc_pnl(position, price, cfg)
                        trades.append(TradeResult(
                            position.side, position.entry_price, price,
                            position.entry_ts, ts, exit_pnl, pnl_bps,
                            age, "runner trail", score, cvd, trend))
                        position = None
                        last_trade_ts = ts
                        continue

                # Early exit
                if age >= cfg.max_hold_seconds * 0.40 and not position.tp1_done and pnl_bps < -5.0:
                    if score > 1.0:
                        exit_pnl = _calc_pnl(position, price, cfg)
                        trades.append(TradeResult(
                            position.side, position.entry_price, price,
                            position.entry_ts, ts, exit_pnl, pnl_bps,
                            age, "early exit", score, cvd, trend))
                        position = None
                        last_trade_ts = ts
                        continue

                # Fast fail
                if age <= 20 and pnl_bps <= -5.0 and score >= 3.0:
                    exit_pnl = _calc_pnl(position, price, cfg)
                    trades.append(TradeResult(
                        position.side, position.entry_price, price,
                        position.entry_ts, ts, exit_pnl, pnl_bps,
                        age, "fast fail", score, cvd, trend))
                    position = None
                    last_trade_ts = ts
                    continue

            # Time stop
            if age >= cfg.max_hold_seconds:
                exit_pnl = _calc_pnl(position, price, cfg)
                trades.append(TradeResult(
                    position.side, position.entry_price, price,
                    position.entry_ts, ts, exit_pnl, pnl_bps,
                    age, "time stop", score, cvd, trend))
                position = None
                last_trade_ts = ts
            continue

        # ── Entry logic (no position) ──
        # Cooldown
        if ts - last_trade_ts < cfg.cooldown_seconds:
            continue

        # Daily limit
        day_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        if daily_trades.get(day_str, 0) >= cfg.max_daily_trades:
            continue

        # Determine signal
        sig = "FLAT"
        if not absorption:
            if score >= cfg.threshold:
                sig = "LONG"
            elif score <= -cfg.threshold:
                sig = "SHORT"

        if sig == "FLAT":
            signal_count = 0
            signal_side = ""
            continue

        # Filters (simplified — use stored regime_ok, trend_ok, time_ok)
        if not (regime_ok and trend_ok and time_ok and dangerous_ok):
            signal_count = 0
            signal_side = ""
            continue

        # Execution guard (simplified)
        if vol < cfg.min_vol_bps:
            continue
        # Trend adverse
        if sig == "LONG" and trend < -cfg.max_trend_adverse_bps:
            continue
        if sig == "SHORT" and trend > cfg.max_trend_adverse_bps:
            continue

        # Confirmation cycles
        if sig == signal_side and ts - signal_ts <= 3.0:
            signal_count += 1
        else:
            signal_side = sig
            signal_count = 1
        signal_ts = ts

        if signal_count < cfg.confirmation_cycles:
            continue

        # ── ENTRY ──
        signal_count = 0
        signal_side = ""

        qty = cfg.notional_usd / price
        if sig == "LONG":
            sl = price * (1 - cfg.stop_loss_pct / 100.0)
            tp1 = price * (1 + cfg.tp1_pct / 100.0)
            tp2 = price * (1 + cfg.tp2_pct / 100.0)
        else:
            sl = price * (1 + cfg.stop_loss_pct / 100.0)
            tp1 = price * (1 - cfg.tp1_pct / 100.0)
            tp2 = price * (1 - cfg.tp2_pct / 100.0)

        position = SimPosition(
            side=sig, entry_price=price, entry_ts=ts, qty=qty,
            remaining_qty=qty, peak_price=price, trough_price=price,
            sl_price=sl, tp1_price=tp1, tp2_price=tp2,
        )
        daily_trades[day_str] = daily_trades.get(day_str, 0) + 1

    # Close any remaining position at last price
    if position is not None and rows:
        last_price = rows[-1]["price"]
        age = rows[-1]["ts"] - position.entry_ts
        pnl_bps = ((last_price - position.entry_price) / position.entry_price * 10000.0) if position.side == "LONG" else ((position.entry_price - last_price) / position.entry_price * 10000.0)
        exit_pnl = _calc_pnl(position, last_price, cfg)
        trades.append(TradeResult(
            position.side, position.entry_price, last_price,
            position.entry_ts, rows[-1]["ts"], exit_pnl, pnl_bps,
            age, "session end", 0, 0, 0))

    conn.close()
    return trades


def _calc_pnl(pos: SimPosition, exit_price: float, cfg: BacktestConfig) -> float:
    """Calculate PnL in USD including fees."""
    if pos.side == "LONG":
        gross_pnl = (exit_price - pos.entry_price) * pos.qty
    else:
        gross_pnl = (pos.entry_price - exit_price) * pos.qty
    # Round-trip fees
    notional = pos.entry_price * pos.qty
    fees = notional * cfg.taker_fee_bps * 2 / 10000.0
    return gross_pnl - fees


def print_report(trades: List[TradeResult], cfg: BacktestConfig):
    if not trades:
        print("\n*** NO TRADES GENERATED ***")
        print("Threshold %.2f may be too high, or filters too strict." % cfg.threshold)
        return

    total = len(trades)
    winners = [t for t in trades if t.pnl_usd > 0]
    losers = [t for t in trades if t.pnl_usd <= 0]
    win_count = len(winners)
    loss_count = len(losers)
    win_rate = win_count / total * 100.0

    total_pnl = sum(t.pnl_usd for t in trades)
    avg_pnl = total_pnl / total
    avg_win = sum(t.pnl_usd for t in winners) / win_count if winners else 0
    avg_loss = sum(t.pnl_usd for t in losers) / loss_count if losers else 0
    best = max(t.pnl_usd for t in trades)
    worst = min(t.pnl_usd for t in trades)
    avg_hold = sum(t.hold_seconds for t in trades) / total
    profit_factor = abs(sum(t.pnl_usd for t in winners) / sum(t.pnl_usd for t in losers)) if losers and sum(t.pnl_usd for t in losers) != 0 else float('inf')

    # Time range
    first_dt = datetime.fromtimestamp(trades[0].entry_ts, tz=timezone.utc)
    last_dt = datetime.fromtimestamp(trades[-1].entry_ts, tz=timezone.utc)
    days = max((trades[-1].entry_ts - trades[0].entry_ts) / 86400.0, 1.0)

    print("\n" + "=" * 70)
    print("AU2 V23 PHASE 1 BACKTEST RESULTS")
    print("=" * 70)
    print("Period: %s to %s (%.1f days)" % (
        first_dt.strftime("%Y-%m-%d %H:%M"),
        last_dt.strftime("%Y-%m-%d %H:%M"),
        days))
    print("Threshold: %.2f | Hold: %ds | SL: %.2f%% | TP1: %.2f%% | TP2: %.2f%%" % (
        cfg.threshold, cfg.max_hold_seconds, cfg.stop_loss_pct, cfg.tp1_pct, cfg.tp2_pct))
    print("-" * 70)
    print("Total Trades:    %d (%.1f/day)" % (total, total / days))
    print("Win Rate:        %.1f%% (%d W / %d L)" % (win_rate, win_count, loss_count))
    print("Total PnL:       $%.2f" % total_pnl)
    print("Avg PnL/trade:   $%.4f" % avg_pnl)
    print("Avg Win:         $%.4f" % avg_win)
    print("Avg Loss:        $%.4f" % avg_loss)
    print("Best Trade:      $%.4f" % best)
    print("Worst Trade:     $%.4f" % worst)
    print("Profit Factor:   %.2f" % profit_factor)
    print("Avg Hold:        %.0fs" % avg_hold)
    print("R:R (avg W/L):   %.2f" % (abs(avg_win / avg_loss) if avg_loss != 0 else 0))

    # By exit reason
    print("\nEXIT REASONS:")
    print("-" * 70)
    reasons = {}
    for t in trades:
        if t.exit_reason not in reasons:
            reasons[t.exit_reason] = {"count": 0, "pnl": 0.0, "wins": 0}
        reasons[t.exit_reason]["count"] += 1
        reasons[t.exit_reason]["pnl"] += t.pnl_usd
        if t.pnl_usd > 0:
            reasons[t.exit_reason]["wins"] += 1
    for reason, stats in sorted(reasons.items(), key=lambda x: -x[1]["count"]):
        wr = stats["wins"] / stats["count"] * 100 if stats["count"] else 0
        print("  %-18s: %3d trades | Win%%: %5.1f%% | PnL: $%7.2f | Avg: $%.4f" % (
            reason, stats["count"], wr, stats["pnl"], stats["pnl"] / stats["count"]))

    # By side
    print("\nBY SIDE:")
    for side in ("LONG", "SHORT"):
        side_trades = [t for t in trades if t.side == side]
        if not side_trades:
            continue
        sw = sum(1 for t in side_trades if t.pnl_usd > 0)
        sp = sum(t.pnl_usd for t in side_trades)
        print("  %-5s: %3d trades | Win%%: %5.1f%% | PnL: $%7.2f" % (
            side, len(side_trades), sw / len(side_trades) * 100, sp))

    # By day
    print("\nBY DAY:")
    day_stats = {}
    for t in trades:
        day = datetime.fromtimestamp(t.entry_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        if day not in day_stats:
            day_stats[day] = {"count": 0, "pnl": 0.0, "wins": 0}
        day_stats[day]["count"] += 1
        day_stats[day]["pnl"] += t.pnl_usd
        if t.pnl_usd > 0:
            day_stats[day]["wins"] += 1
    for day in sorted(day_stats.keys()):
        s = day_stats[day]
        wr = s["wins"] / s["count"] * 100 if s["count"] else 0
        print("  %s: %3d trades | Win%%: %5.1f%% | PnL: $%7.2f" % (
            day, s["count"], wr, s["pnl"]))

    # Cumulative PnL curve (min/max)
    cum = 0.0
    peak_cum = 0.0
    max_dd = 0.0
    for t in trades:
        cum += t.pnl_usd
        peak_cum = max(peak_cum, cum)
        dd = peak_cum - cum
        max_dd = max(max_dd, dd)
    print("\nMax Drawdown: $%.2f" % max_dd)
    print("Final Cum PnL: $%.2f" % cum)

    # Score distribution at entry
    scores = [abs(t.v23_score_at_entry) for t in trades if t.v23_score_at_entry != 0]
    if scores:
        print("\nSCORE AT ENTRY:")
        print("  Min: %.2f | Avg: %.2f | Max: %.2f" % (
            min(scores), sum(scores)/len(scores), max(scores)))

    # Print last 10 trades
    print("\nLAST 10 TRADES:")
    print("-" * 70)
    for t in trades[-10:]:
        dt = datetime.fromtimestamp(t.entry_ts, tz=timezone.utc)
        symbol = "W" if t.pnl_usd > 0 else "L"
        print("  %s %s | %5s | $%.2f->$%.2f | PnL: $%7.4f | %3ds | %s | cvd=%.1f trend=%.1f" % (
            symbol, dt.strftime("%m-%d %H:%M"), t.side,
            t.entry_price, t.exit_price, t.pnl_usd,
            t.hold_seconds, t.exit_reason,
            t.cvd_at_entry, t.trend_at_entry))


def run_sweep(db_path: str):
    """Run backtest across multiple thresholds to find optimal."""
    print("=" * 70)
    print("THRESHOLD SWEEP")
    print("=" * 70)
    print("%-8s | %5s | %6s | %8s | %8s | %6s | %5s" % (
        "Thr", "Trades", "WR%", "TotalPnL", "AvgPnL", "PF", "AvgHold"))
    print("-" * 70)

    for thr in [2.50, 3.00, 3.50, 4.00, 4.50, 5.00, 5.50, 6.00]:
        cfg = BacktestConfig(threshold=thr)
        trades = run_backtest(db_path, cfg)
        if not trades:
            print("%-8.2f | %5d | %6s | %8s | %8s | %6s | %5s" % (
                thr, 0, "N/A", "N/A", "N/A", "N/A", "N/A"))
            continue
        total = len(trades)
        wins = sum(1 for t in trades if t.pnl_usd > 0)
        wr = wins / total * 100
        pnl = sum(t.pnl_usd for t in trades)
        avg = pnl / total
        win_pnl = sum(t.pnl_usd for t in trades if t.pnl_usd > 0)
        loss_pnl = sum(t.pnl_usd for t in trades if t.pnl_usd <= 0)
        pf = abs(win_pnl / loss_pnl) if loss_pnl != 0 else float('inf')
        avg_hold = sum(t.hold_seconds for t in trades) / total
        print("%-8.2f | %5d | %5.1f%% | $%7.2f | $%7.4f | %5.2f | %4.0fs" % (
            thr, total, wr, pnl, avg, pf, avg_hold))


def main():
    ap = argparse.ArgumentParser(description="AU2 V23 Phase 1 Backtest")
    ap.add_argument("--db", default="au2_v22_5_live_fresh.db", help="Path to live DB")
    ap.add_argument("--threshold", type=float, default=4.00, help="Score threshold")
    ap.add_argument("--sweep", action="store_true", help="Run threshold sweep")
    ap.add_argument("--hold", type=int, default=120, help="Max hold seconds")
    ap.add_argument("--tp1", type=float, default=0.20, help="TP1 percent")
    ap.add_argument("--tp2", type=float, default=0.45, help="TP2 percent")
    ap.add_argument("--sl", type=float, default=0.20, help="SL percent")
    args = ap.parse_args()

    if args.sweep:
        run_sweep(args.db)
        return

    cfg = BacktestConfig(
        threshold=args.threshold,
        max_hold_seconds=args.hold,
        tp1_pct=args.tp1,
        tp2_pct=args.tp2,
        stop_loss_pct=args.sl,
    )
    trades = run_backtest(args.db, cfg)
    print_report(trades, cfg)


if __name__ == "__main__":
    main()

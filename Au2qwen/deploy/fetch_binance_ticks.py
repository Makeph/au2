#!/usr/bin/env python3
"""Fetch real Binance aggTrade data via REST API (no API key needed).

Downloads BTCUSDT aggTrades in chunks, feeds them through FeatureEngine,
and populates the signals table with real microstructure data.
"""
import sys, pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

import time
import sqlite3
import requests
import numpy as np
from au2_core import CoreConfig, SignalProcessor, Regime
from au2_feature_engine import FeatureEngine

SYMBOL = "BTCUSDT"
BASE_URL = "https://api.binance.com"  # spot (not geo-blocked)
AGG_TRADES_EP = f"{BASE_URL}/api/v3/aggTrades"
CHUNK_SIZE = 1000  # max per request
RATE_LIMIT_S = 0.15  # 150ms between requests to stay under limits

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS signals (
    ts REAL PRIMARY KEY,
    price REAL,
    cvd_delta_5s REAL,
    trend_bps REAL,
    realized_vol_bps REAL,
    range_30s_bps REAL,
    spread_bps REAL,
    score REAL,
    regime TEXT,
    buy_vol_ratio REAL,
    trade_intensity REAL,
    avg_size_imbalance REAL,
    large_trade_imbalance REAL,
    cvd_accel REAL,
    trend_30s_bps REAL,
    return_skew_30s REAL,
    price_pos_in_range REAL
)
"""

INSERT_SQL = """INSERT OR REPLACE INTO signals VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)"""


def fetch_agg_trades(start_time_ms: int, end_time_ms: int) -> list:
    """Fetch aggTrades from Binance futures API."""
    all_trades = []
    current_start = start_time_ms

    while current_start < end_time_ms:
        params = {
            "symbol": SYMBOL,
            "startTime": current_start,
            "endTime": min(current_start + 3_600_000, end_time_ms),  # 1h chunks
            "limit": CHUNK_SIZE,
        }
        try:
            resp = requests.get(AGG_TRADES_EP, params=params, timeout=10)
            resp.raise_for_status()
            trades = resp.json()
        except Exception as e:
            print(f"  API error: {e}, retrying in 2s...")
            time.sleep(2)
            continue

        if not trades:
            current_start += 3_600_000
            continue

        for t in trades:
            ts_s = t["T"] / 1000.0  # ms → seconds
            price = float(t["p"])
            qty = float(t["q"])
            is_buyer_maker = t["m"]  # True = seller is maker = buyer aggressed
            all_trades.append((ts_s, price, qty, is_buyer_maker))

        last_ts_ms = trades[-1]["T"]
        current_start = last_ts_ms + 1

        n = len(all_trades)
        if n % 10000 < CHUNK_SIZE:
            elapsed_h = (all_trades[-1][0] - all_trades[0][0]) / 3600
            print(f"  {n:>8,} trades fetched ({elapsed_h:.1f}h of data)")

        time.sleep(RATE_LIMIT_S)

    return all_trades


def populate_from_trades(db_path: str, trades: list, cfg: CoreConfig = None):
    """Run FeatureEngine on real trades and populate signals table."""
    if cfg is None:
        cfg = CoreConfig(threshold=4.0)

    conn = sqlite3.connect(db_path)
    conn.execute("DROP TABLE IF EXISTS signals")
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()

    engine = FeatureEngine()
    batch = []
    sample_step = 10  # sample every 10th tick (aggTrades are very dense)

    for idx, (ts, price, qty, is_buyer_maker) in enumerate(trades):
        engine.on_trade(ts, price, qty, is_buyer_maker)

        if idx < 500:  # warmup
            continue
        if idx % sample_step != 0:
            continue

        features = engine.compute()
        cvd = features[0]
        trend = features[1]
        vol = features[2]
        regime_enum = SignalProcessor.classify_regime(vol, trend, cvd)

        try:
            score = SignalProcessor.score(cvd, trend, vol, regime_enum, cfg)
        except Exception:
            score = 0.0

        row = (
            ts, price,
            features[0], features[1], features[2], features[3],
            0.5,  # spread estimate
            score, regime_enum.name,
            features[4], features[5], features[6], features[7],
            features[8], features[9], features[12], features[13],
        )
        batch.append(row)

        if len(batch) >= 5000:
            conn.executemany(INSERT_SQL, batch)
            conn.commit()
            pct = idx / len(trades) * 100
            print(f"  {pct:.0f}% signals written ({len(batch) * (idx // (len(batch) * sample_step) + 1):,})")
            batch = []

    if batch:
        conn.executemany(INSERT_SQL, batch)
        conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]

    # Stats
    row = conn.execute("SELECT MIN(ts), MAX(ts), AVG(price) FROM signals").fetchone()
    duration_h = (row[1] - row[0]) / 3600
    avg_price = row[2]
    conn.close()

    print(f"  {count:,} signals | {duration_h:.1f}h | avg price ${avg_price:,.0f}")
    return count


def stream_fetch_and_populate(db_path: str, start_ms: int, end_ms: int,
                               cfg: CoreConfig = None, sample_step: int = 10):
    """Fetch trades and populate DB in streaming fashion (no full buffer in memory)."""
    if cfg is None:
        cfg = CoreConfig(threshold=4.0)

    conn = sqlite3.connect(db_path)
    conn.execute("DROP TABLE IF EXISTS signals")
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()

    engine = FeatureEngine()
    batch = []
    total_trades = 0
    total_signals = 0
    current_start = start_ms

    while current_start < end_ms:
        params = {
            "symbol": SYMBOL,
            "startTime": current_start,
            "endTime": min(current_start + 3_600_000, end_ms),
            "limit": CHUNK_SIZE,
        }
        try:
            resp = requests.get(AGG_TRADES_EP, params=params, timeout=10)
            resp.raise_for_status()
            trades = resp.json()
        except Exception as e:
            print(f"  API error: {e}, retrying...")
            time.sleep(2)
            continue

        if not trades:
            current_start += 3_600_000
            continue

        for t in trades:
            ts_s = t["T"] / 1000.0
            price = float(t["p"])
            qty = float(t["q"])
            is_buyer_maker = t["m"]

            engine.on_trade(ts_s, price, qty, is_buyer_maker)
            total_trades += 1

            if total_trades < 500 or total_trades % sample_step != 0:
                continue

            features = engine.compute()
            cvd, trend, vol = features[0], features[1], features[2]
            regime_enum = SignalProcessor.classify_regime(vol, trend, cvd)
            try:
                score = SignalProcessor.score(cvd, trend, vol, regime_enum, cfg)
            except Exception:
                score = 0.0

            batch.append((
                ts_s, price, features[0], features[1], features[2], features[3],
                0.5, score, regime_enum.name,
                features[4], features[5], features[6], features[7],
                features[8], features[9], features[12], features[13],
            ))

            if len(batch) >= 2000:
                conn.executemany(INSERT_SQL, batch)
                conn.commit()
                total_signals += len(batch)
                batch = []

        last_ts_ms = trades[-1]["T"]
        current_start = last_ts_ms + 1
        elapsed_h = (last_ts_ms / 1000.0 - start_ms / 1000.0) / 3600
        target_h = (end_ms - start_ms) / 3_600_000
        print(f"  {total_trades:>8,} trades | {total_signals:>6,} signals | {elapsed_h:.1f}/{target_h:.0f}h")

        time.sleep(RATE_LIMIT_S)

    if batch:
        conn.executemany(INSERT_SQL, batch)
        conn.commit()
        total_signals += len(batch)

    row = conn.execute("SELECT MIN(ts), MAX(ts), AVG(price), COUNT(*) FROM signals").fetchone()
    conn.close()
    duration_h = (row[1] - row[0]) / 3600 if row[0] and row[1] else 0
    avg_price = row[2] or 0
    count = row[3]
    print(f"\n  {count:,} signals | {duration_h:.1f}h | avg ${avg_price:,.0f} | {total_trades:,} raw trades")
    return count


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else "au2_real.db"
    hours = float(sys.argv[2]) if len(sys.argv) > 2 else 6.0

    end_ms = int(time.time() * 1000)
    start_ms = end_ms - int(hours * 3_600_000)

    print(f"Streaming {hours:.0f}h of {SYMBOL} aggTrades -> {db_path}")
    print(f"  {time.strftime('%Y-%m-%d %H:%M', time.gmtime(start_ms/1000))} -> {time.strftime('%Y-%m-%d %H:%M', time.gmtime(end_ms/1000))} UTC")

    n = stream_fetch_and_populate(db_path, start_ms, end_ms)

    if n < 1000:
        print("WARNING: < 1000 signals. May not be enough for training.")

    print(f"\nDone. Next: python train_signal_v2.py {db_path}")


if __name__ == "__main__":
    main()

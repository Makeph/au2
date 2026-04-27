#!/usr/bin/env python3
"""Build a recent signals DB from data.binance.vision daily aggTrade files.

Much faster than REST pagination: downloads pre-packaged daily CSVs (~10 MB/day
compressed) and processes through FeatureEngine at identical sample_step to the
existing validated DBs (sample_step=10 → one signal per 10 raw trades).

Usage
-----
  python deploy/build_recent_db.py                        # Apr 20-26, output to data/validated/au2_apr20_26.db
  python deploy/build_recent_db.py --start 2026-04-20 --end 2026-04-26 --out my.db
  python deploy/build_recent_db.py --start 2026-04-20 --end 2026-04-23   # 4 days

Output
------
  signals table identical schema to existing validated DBs — plug into Au2Backtest.
"""
from __future__ import annotations
import sys, pathlib, argparse, io, csv, time, zipfile, sqlite3, tempfile, os
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core",):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

import requests
from au2_core import CoreConfig, SignalProcessor
from au2_feature_engine import FeatureEngine

SYMBOL  = "BTCUSDT"
BASE_URL = "https://data.binance.vision/data/spot/daily/aggTrades"
SAMPLE_STEP = 10   # identical to existing validated DBs
WARMUP_TRADES = 500

CREATE_SQL = """
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


def _date_range(start: str, end: str):
    """Yield YYYY-MM-DD strings from start to end inclusive."""
    from datetime import date, timedelta
    d = date.fromisoformat(start)
    e = date.fromisoformat(end)
    while d <= e:
        yield d.isoformat()
        d += timedelta(days=1)


def _download_day(date_str: str) -> bytes | None:
    url = f"{BASE_URL}/{SYMBOL}/{SYMBOL}-aggTrades-{date_str}.zip"
    try:
        r = requests.get(url, timeout=60)
        if r.status_code == 404:
            print(f"  {date_str}: not available yet (404) — skipping")
            return None
        r.raise_for_status()
        return r.content
    except Exception as e:
        print(f"  {date_str}: download error — {e}")
        return None


def _process_zip(zip_bytes: bytes, engine: FeatureEngine, cfg: CoreConfig,
                 conn: sqlite3.Connection, trade_counter: list, date_str: str) -> int:
    """Extract CSV from zip, feed to FeatureEngine, write signals to DB."""
    batch = []
    signals_written = 0

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            reader = csv.reader(io.TextIOWrapper(f, "utf-8"))
            for row in reader:
                # Format: agg_trade_id, price, quantity, first_id, last_id, timestamp_ms, is_buyer_maker
                if len(row) < 7:
                    continue
                try:
                    # data.binance.vision CSV timestamps are in microseconds
                    ts_s = int(row[5]) / 1_000_000.0
                    price = float(row[1])
                    qty   = float(row[2])
                    # is_buyer_maker: "True"/"False" or "1"/"0"
                    ibm   = row[6].strip().lower() in ("true", "1")
                except (ValueError, IndexError):
                    continue

                engine.on_trade(ts_s, price, qty, ibm)
                trade_counter[0] += 1

                if trade_counter[0] < WARMUP_TRADES:
                    continue
                if trade_counter[0] % SAMPLE_STEP != 0:
                    continue

                features = engine.compute()
                cvd, trend, vol = features[0], features[1], features[2]
                regime_enum = SignalProcessor.classify_regime(vol, trend, cvd)
                try:
                    score = SignalProcessor.score(cvd, trend, vol, regime_enum, cfg)
                except Exception:
                    score = 0.0

                batch.append((
                    ts_s, price,
                    features[0], features[1], features[2], features[3],
                    0.5, score, regime_enum.name,
                    features[4], features[5], features[6], features[7],
                    features[8], features[9], features[12], features[13],
                ))

                if len(batch) >= 5000:
                    conn.executemany(INSERT_SQL, batch)
                    conn.commit()
                    signals_written += len(batch)
                    batch = []

    if batch:
        conn.executemany(INSERT_SQL, batch)
        conn.commit()
        signals_written += len(batch)

    return signals_written


def build(start: str, end: str, out_path: str) -> None:
    cfg = CoreConfig(threshold=4.0)
    engine = FeatureEngine()
    trade_counter = [0]

    conn = sqlite3.connect(out_path)
    conn.execute("DROP TABLE IF EXISTS signals")
    conn.execute(CREATE_SQL)
    conn.commit()

    total_signals = 0
    days_ok = 0

    dates = list(_date_range(start, end))
    print(f"Building {out_path}")
    print(f"  Period : {start} to {end}  ({len(dates)} days)")
    print(f"  Sample : every {SAMPLE_STEP} raw trades")
    print()

    for date_str in dates:
        t0 = time.time()
        print(f"  [{date_str}] downloading...", end="", flush=True)
        data = _download_day(date_str)
        if data is None:
            continue
        dl_s = time.time() - t0
        print(f" {len(data)/1e6:.1f} MB in {dl_s:.1f}s | processing...", end="", flush=True)

        t1 = time.time()
        n = _process_zip(data, engine, cfg, conn, trade_counter, date_str)
        proc_s = time.time() - t1
        total_signals += n
        days_ok += 1
        print(f" {n:,} signals in {proc_s:.1f}s")

    # Final stats
    row = conn.execute("SELECT MIN(ts), MAX(ts), AVG(price), COUNT(*) FROM signals").fetchone()
    conn.close()

    if row[0]:
        import datetime
        d0 = datetime.datetime.utcfromtimestamp(row[0]).strftime('%Y-%m-%d %H:%M')
        d1 = datetime.datetime.utcfromtimestamp(row[1]).strftime('%Y-%m-%d %H:%M')
        print(f"\nDone: {row[3]:,} signals | {d0} to {d1} UTC | avg ${row[2]:,.0f}")
        print(f"  {days_ok}/{len(dates)} days fetched | {trade_counter[0]:,} raw trades processed")
    else:
        print("\nWarning: no signals written — check dates and connectivity.")


def _parse_args():
    p = argparse.ArgumentParser(description="Build recent BT signals DB from data.binance.vision")
    p.add_argument("--start", default="2026-04-20", help="Start date YYYY-MM-DD (inclusive)")
    p.add_argument("--end",   default="2026-04-26", help="End date YYYY-MM-DD (inclusive)")
    p.add_argument("--out",   default="data/validated/au2_apr20_26.db", help="Output DB path")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    build(args.start, args.end, args.out)

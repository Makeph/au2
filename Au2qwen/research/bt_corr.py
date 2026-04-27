#!/usr/bin/env python3
"""
Raw correlation test: does DB score predict forward price movement?

If corr(score, forward_return) ≈ 0, no preset can extract alpha.
If corr is meaningful (|corr| > 0.02 at short horizons), the issue
is execution (filters, entry timing, hold window), not signal.

Tests multiple horizons: 30s, 60s, 120s, 300s, 600s.
Also tests conditional on |score| >= threshold buckets.
"""
import sys, sqlite3, statistics, math

DB = sys.argv[1] if len(sys.argv) > 1 else "../au2_v22_5_live_fresh.db"
HORIZONS = [30, 60, 120, 300, 600]

print(f"Loading {DB}...")
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
rows = conn.execute("SELECT ts, price, score FROM signals ORDER BY ts ASC").fetchall()
conn.close()
print(f"  {len(rows):,} ticks\n")

# Build arrays
ts_arr    = [float(r["ts"])    for r in rows]
price_arr = [float(r["price"]) for r in rows]
score_arr = [float(r["score"] or 0) for r in rows]
n = len(rows)

# Build a ts→index for fast forward lookup
def pearson(xs, ys):
    if len(xs) < 2: return 0.0
    mx = sum(xs)/len(xs); my = sum(ys)/len(ys)
    num = sum((x-mx)*(y-my) for x, y in zip(xs, ys))
    dx  = math.sqrt(sum((x-mx)**2 for x in xs))
    dy  = math.sqrt(sum((y-my)**2 for y in ys))
    return num / (dx*dy) if dx*dy > 0 else 0.0

# For each horizon, compute forward return and correlation with score
# Use a pointer walk for efficiency
def forward_return(i, horizon_s):
    target_ts = ts_arr[i] + horizon_s
    # linear search forward from i (prices are time-ordered)
    j = i
    while j < n and ts_arr[j] < target_ts:
        j += 1
    if j >= n: return None
    return (price_arr[j] - price_arr[i]) / price_arr[i] * 10_000  # in bps

print(f"{'Horizon':>8} {'Corr(all)':>12} {'Corr(|s|>3)':>14} {'Corr(|s|>4.8)':>16} {'Corr(|s|>5.5)':>16} {'N(|s|>4.8)':>12}")
print("-" * 80)

# Sample every 20th tick to keep it fast but statistically meaningful
SAMPLE_STEP = 20

for h in HORIZONS:
    s_all, r_all = [], []
    s_3,   r_3   = [], []
    s_48,  r_48  = [], []
    s_55,  r_55  = [], []

    for i in range(0, n, SAMPLE_STEP):
        fr = forward_return(i, h)
        if fr is None: break
        sc = score_arr[i]
        s_all.append(sc); r_all.append(fr)
        if abs(sc) >= 3.0: s_3.append(sc);  r_3.append(fr)
        if abs(sc) >= 4.8: s_48.append(sc); r_48.append(fr)
        if abs(sc) >= 5.5: s_55.append(sc); r_55.append(fr)

    c_all = pearson(s_all, r_all)
    c_3   = pearson(s_3,   r_3)
    c_48  = pearson(s_48,  r_48)
    c_55  = pearson(s_55,  r_55)
    print(f"{h:>6}s  {c_all:>+12.4f} {c_3:>+14.4f} {c_48:>+16.4f} {c_55:>+16.4f} {len(s_48):>12,}")

# Also: mean forward return conditional on sign(score) for high-conviction signals
print("\n── Directional check: mean forward return given |score| >= 4.8 ──")
print(f"{'Horizon':>8} {'N(pos)':>8} {'mean_fr(pos)':>14} {'N(neg)':>8} {'mean_fr(neg)':>14} {'edge_bps':>12}")
print("-" * 70)
for h in HORIZONS:
    pos_rets, neg_rets = [], []
    for i in range(0, n, SAMPLE_STEP):
        sc = score_arr[i]
        if abs(sc) < 4.8: continue
        fr = forward_return(i, h)
        if fr is None: break
        if sc > 0: pos_rets.append(fr)
        else:      neg_rets.append(fr)
    if pos_rets and neg_rets:
        mp = statistics.mean(pos_rets)
        mn = statistics.mean(neg_rets)
        # If score > 0 predicts SHORT (mean-revert), pos should go DOWN (negative fr)
        # edge is (pos should be low, neg should be high): mean_revert_edge = mn - mp
        # If score > 0 predicts LONG (momentum), pos should go UP (positive fr)
        # momentum_edge = mp - mn
        mr_edge = mn - mp
        mo_edge = mp - mn
        best = "MEAN-REVERT" if mr_edge > mo_edge else "MOMENTUM"
        edge_bps = max(mr_edge, mo_edge)
        print(f"{h:>6}s  {len(pos_rets):>8,}  {mp:>+12.2f}bp  {len(neg_rets):>8,}  {mn:>+12.2f}bp  {edge_bps:>+8.2f}bp ({best})")

# Round-trip cost for reference
print("\n── Round-trip cost (for reference) ──")
print(f"  Fees: 4.5 bps × 2 = 9.0 bps")
print(f"  Slippage: ~0.5 bps × 2 = 1.0 bps")
print(f"  Total: ~10 bps per trade")
print(f"\n  ==> Any edge must exceed ~10 bps (0.10%) to be profitable before SL/TP structure.")

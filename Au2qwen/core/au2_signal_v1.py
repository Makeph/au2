#!/usr/bin/env python3
"""AU2 Signal V1 — Probabilistic Calibrated Classifier (Logistic Regression)
Features: cvd, trend, abs_trend, vol, range_30s
Label: forward return @ 60s, thresholded at X bps (neutral zone ignored)
Output: p(up) ∈ [0,1]
"""
import os
import sqlite3
import pickle
import numpy as np
from sklearn.linear_model import LogisticRegression
from typing import Tuple, Dict

DEFAULT_MODEL_PATH = "au2_signal_v1.pkl"
FEATURE_NAMES = ["cvd", "trend", "abs_trend", "vol", "range_30s"]

def build_dataset(db_path: str, horizon_s: float = 60.0, threshold_bps: float = 4.0) -> Tuple[np.ndarray, np.ndarray, Dict]:
    conn = sqlite3.connect(db_path)
    cols = [c[1] for c in conn.execute("PRAGMA table_info(signals)").fetchall()]
    has_range = "range_30s_bps" in cols
    sql = f"SELECT ts, price, cvd_delta_5s, trend_bps, realized_vol_bps, {'range_30s_bps,' if has_range else ''} spread_bps FROM signals ORDER BY ts ASC"
    rows = conn.execute(sql).fetchall()
    conn.close()

    ts    = np.array([r[0] for r in rows], dtype=np.float64)
    price = np.array([r[1] for r in rows], dtype=np.float64)
    cvd   = np.array([r[2] or 0.0 for r in rows], dtype=np.float64)
    trend = np.array([r[3] or 0.0 for r in rows], dtype=np.float64)
    vol   = np.array([r[4] or 0.0 for r in rows], dtype=np.float64)
    rng   = np.array([r[5] or 0.0 for r in rows], dtype=np.float64) if has_range else np.zeros_like(vol)

    # Forward return @ horizon_s
    target_ts = ts + horizon_s
    idx_future = np.searchsorted(ts, target_ts)
    valid = idx_future < len(ts)

    future_prices = price[np.minimum(idx_future, len(ts)-1)]
    ret_bps = (future_prices - price) / price * 10000.0
    ret_bps[~valid] = np.nan

    # Label: 1 if > +X, 0 if < -X, else ignore (-1)
    y = np.full(len(ts), -1.0)
    valid_labeled = valid & ~np.isnan(ret_bps)
    y[valid_labeled & (ret_bps > threshold_bps)] = 1.0
    y[valid_labeled & (ret_bps < -threshold_bps)] = 0.0

    # Keep only labeled samples
    keep = y != -1.0
    X_raw = np.column_stack([cvd[keep], trend[keep], np.abs(trend[keep]), vol[keep], rng[keep]])
    y_clean = y[keep]

    # Z-score normalization
    mu = X_raw.mean(axis=0)
    sigma = X_raw.std(axis=0)
    sigma[sigma == 0] = 1.0
    X_norm = (X_raw - mu) / sigma

    return X_norm, y_clean, {"mu": mu, "sigma": sigma}

def train_model(X: np.ndarray, y: np.ndarray, model_path: str = DEFAULT_MODEL_PATH) -> LogisticRegression:
    model = LogisticRegression(random_state=42, max_iter=1000, class_weight="balanced")
    model.fit(X, y)

    artifact = {
        "model": model,
        "params": {"mu": X.mean(axis=0), "sigma": X.std(axis=0)},
        "features": FEATURE_NAMES
    }
    with open(model_path, "wb") as f:
        pickle.dump(artifact, f)

    acc = model.score(X, y)
    print(f"✅ Model saved to {model_path}")
    print(f"   Train Acc: {acc:.3f}")
    print(f"   Coeffs: {dict(zip(FEATURE_NAMES, model.coef_[0]))}")
    return model

def predict_proba(state: dict, model_path: str = DEFAULT_MODEL_PATH) -> float:
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model not found: {model_path}. Run training first.")
    with open(model_path, "rb") as f:
        artifact = pickle.load(f)

    model: LogisticRegression = artifact["model"]
    mu = artifact["params"]["mu"]
    sigma = artifact["params"]["sigma"]

    feat = np.array([
        float(state.get("cvd_delta_5s", 0.0)),
        float(state.get("trend_bps", 0.0)),
        abs(float(state.get("trend_bps", 0.0))),
        float(state.get("realized_vol_bps", 0.0)),
        float(state.get("range_30s_bps", 0.0))
    ])

    feat_norm = (feat - mu) / sigma
    return model.predict_proba(feat_norm.reshape(1, -1))[0, 1]
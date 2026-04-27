#!/usr/bin/env python3
"""AU2 Signal V2 — Cost-Aware ML Signal Model.

Two-stage approach:
  Stage 1: GradientBoostingClassifier (3-class: UP/DOWN/NEUTRAL)
  Stage 2: Cost-aware edge calculation → direction + confidence

Output: direction (LONG/SHORT/FLAT), expected_edge_bps, confidence, backward-compat score.
Direction is DATA-DRIVEN — no hardcoded score→direction mapping.
"""
from __future__ import annotations
import os
import pickle
import numpy as np
from typing import Dict, Optional
from au2_feature_engine import FEATURE_NAMES, N_FEATURES

DEFAULT_MODEL_PATH = "au2_signal_v2.pkl"

# Label constants
LABEL_DOWN = 0
LABEL_NEUTRAL = 1
LABEL_UP = 2


class SignalModelV2:
    """Cost-aware ML signal model. Loads a trained artifact and predicts."""

    def __init__(self, model_path: str = DEFAULT_MODEL_PATH) -> None:
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"V2 model not found: {model_path}. Run train_signal_v2.py first.")
        with open(model_path, "rb") as f:
            artifact = pickle.load(f)

        self.model = artifact["model"]
        self.mu: np.ndarray = artifact["scaler"]["mu"]
        self.sigma: np.ndarray = artifact["scaler"]["sigma"]
        self.cost_bps: float = artifact.get("cost_bps", 10.0)
        self.feature_names: list = artifact.get("feature_names", FEATURE_NAMES)
        self._k: float = artifact.get("move_calibration_k", 0.6)

    def predict(self, features: np.ndarray) -> Dict:
        """Predict direction, edge, and confidence from feature vector.

        Args:
            features: np.ndarray of shape (N_FEATURES,) — raw (unnormalized)

        Returns:
            dict with keys:
                direction: "LONG" | "SHORT" | "FLAT"
                p_up: float       — P(price goes up > threshold)
                p_down: float     — P(price goes down > threshold)
                p_neutral: float  — P(neutral zone)
                expected_edge_bps: float — expected profit after cost
                confidence: float — edge / cost ratio (>1.0 = profitable)
                score: float      — backward-compat score in [-10, 10]
        """
        # Normalize
        x = (features - self.mu) / np.maximum(self.sigma, 1e-9)
        x = x.reshape(1, -1)

        # Stage 1: classifier probabilities
        proba = self.model.predict_proba(x)[0]

        # Map to our label order (model may order differently)
        classes = list(self.model.classes_)
        p_down = proba[classes.index(LABEL_DOWN)] if LABEL_DOWN in classes else 0.0
        p_neutral = proba[classes.index(LABEL_NEUTRAL)] if LABEL_NEUTRAL in classes else 0.0
        p_up = proba[classes.index(LABEL_UP)] if LABEL_UP in classes else 0.0

        # Stage 2: cost-aware edge calculation
        # Use realized vol from features as expected move magnitude
        vol_bps = max(features[2], 1.0)  # realized_vol_5s_bps, floor at 1 bps
        expected_move = vol_bps * self._k

        edge_long = p_up * expected_move - p_down * expected_move - self.cost_bps
        edge_short = p_down * expected_move - p_up * expected_move - self.cost_bps

        best_edge = max(edge_long, edge_short)
        if best_edge <= 0:
            direction = "FLAT"
            confidence = 0.0
        else:
            direction = "LONG" if edge_long > edge_short else "SHORT"
            confidence = best_edge / max(self.cost_bps, 1e-9)

        # Backward-compat score: [-10, 10]
        # score > 0 → SHORT, score < 0 → LONG (legacy convention)
        score_mag = min(abs(best_edge) / max(self.cost_bps, 1e-9) * 5.0, 10.0)
        if direction == "SHORT":
            score = score_mag
        elif direction == "LONG":
            score = -score_mag
        else:
            score = 0.0

        return {
            "direction": direction,
            "p_up": p_up,
            "p_down": p_down,
            "p_neutral": p_neutral,
            "expected_edge_bps": best_edge,
            "confidence": confidence,
            "score": score,
        }

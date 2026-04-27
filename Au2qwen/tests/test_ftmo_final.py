#!/usr/bin/env python3
"""Vérifications de cohérence finale avant live"""
import sys, pathlib
_ROOT = pathlib.Path(__file__).resolve().parent.parent
for _d in (_ROOT / "core", _ROOT / "runtime", _ROOT / "presets"):
    if str(_d) not in sys.path: sys.path.insert(0, str(_d))

import logging
logging.basicConfig(level=logging.INFO)
from au2_preset_ftmo_10k import CFG, OVERLAY_CFG, PROP_FTMO_SAFE
from au2_risk_overlay import RiskOverlay

def test_rr_compatibility():
    rr = CFG.tp1_pct / CFG.stop_loss_pct
    assert rr >= PROP_FTMO_SAFE.min_rr, f"RR {rr:.2f} < Prop min_rr {PROP_FTMO_SAFE.min_rr}"
    print(f"✅ Test 1 OK: RR brut = {rr:.2f}x ≥ {PROP_FTMO_SAFE.min_rr} (FTMO compatible)")

def test_sl_tp_ratio():
    # Vérifie que le SL élargi ne casse pas la structure de sortie
    assert CFG.tp1_pct > CFG.stop_loss_pct, "TP1 doit > SL pour expectancy positive"
    assert CFG.tp2_pct > CFG.tp1_pct * 1.5, "TP2 doit conserver un gradient cohérent"
    print("✅ Test 2 OK: Structure TP/SL cohérente")

def test_cluster_discipline():
    # 180s + cooldown 120s + max 3 trades/jour → suffisant pour FTMO
    assert CFG.cluster_window_s >= CFG.cooldown_seconds, "Cluster window doit ≥ cooldown"
    assert CFG.max_daily_trades <= 4, "Cap journalier conservateur"
    print("✅ Test 3 OK: Discipline cluster/jour respectée")

def test_overlay_no_conflict():
    ov = RiskOverlay(cfg=OVERLAY_CFG)
    assert ov.cfg.enable_post_loss_pause is False, "Overlay loss pause doit être désactivé (enable_post_loss_pause=False)"
    assert ov.cfg.daily_profit_cap_pct == 0.80, "Cap journalier réaliste FTMO"
    print("✅ Test 4 OK: Overlay propre, aucun conflit Core")

if __name__ == "__main__":
    test_rr_compatibility()
    test_sl_tp_ratio()
    test_cluster_discipline()
    test_overlay_no_conflict()
    print("\n🟢 Tous les tests de cohérence finale sont PASSÉS. Preset prêt FTMO.")
#!/usr/bin/env python3
"""AU2 LAUNCHER — Safe Execution Wrapper"""
import sys
import logging
from pathlib import Path
from au2_preset_ftmo_10k import CFG, run_ftmo_safe

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger("au2_launcher")

def main():
    db = sys.argv[1] if len(sys.argv) > 1 else "au2_v22_5_live_fresh.db"
    if not Path(db).exists():
        log.error(f"DB introuvable: {db}")
        sys.exit(1)

    log.info("Lancement backtest FTMO avec overlay actif...")
    try:
        _, _, metrics = run_ftmo_safe(db)
    except Exception as exc:
        log.error(f"Execution échouée: {exc}", exc_info=True)
        sys.exit(1) # ✅ Patch A: Proper exit code

    # ✅ Patch 5: Align threshold with CFG
    if metrics.max_dd_pct > CFG.daily_dd_red_pct:
        log.warning("⚠️ DD > %.1f%% (CFG daily_dd_red_pct) — Ajuster threshold/risk.", CFG.daily_dd_red_pct)
        sys.exit(2)
    log.info("✅ Preset FTMO 10k validé. Overlay logs actifs.")

if __name__ == "__main__":
    main()
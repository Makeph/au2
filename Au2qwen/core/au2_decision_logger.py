#!/usr/bin/env python3
"""AU2QWEN — Structured decision logger.

Writes TradeDecisionLog entries as JSONL (one JSON object per line) so
every tick decision can be replayed, audited, and diffed against backtest.

Usage in LiveExecutor
---------------------
  from au2_decision_logger import DecisionLogger
  logger = DecisionLogger("au2_decisions.jsonl")

  dlog = build_trade_decision(...)
  logger.log(dlog)               # writes if approved or near_miss
  logger.log(dlog, force=True)   # always writes

  print(logger.summary())        # live stats

Usage as standalone audit tool
------------------------------
  from au2_decision_logger import DecisionLogger
  summary = DecisionLogger.read_summary("au2_decisions.jsonl")
  for dlog in DecisionLogger.iter_file("au2_decisions.jsonl"):
      ...

File format
-----------
Two record types coexist in the same JSONL file, distinguished by "_type".

Entry decisions (TradeDecisionLog):
  {"_type": "decision", "ts": 1745123456.0, "price": 75000.0, "regime": "CHOP",
   "score": 9.4, "eff_threshold": 8.0, "signal": "SHORT", "confidence": 1.17,
   "approved": true, "rejection_reason": "", "near_miss": false,
   ..., "_logged_at": "2026-04-18T12:34:56Z"}

Trade results (TradeResult — written on position close):
  {"_type": "trade_result", "ts": 1745123530.0, "entry_ts": 1745123456.0,
   "entry_price": 75000.0, "exit_price": 74865.0, "pnl_usd": -14.32,
   "hold_seconds": 74.0, "exit_reason": "EXIT_SL", "signal": "SHORT",
   "entry_score": 9.4, "confidence": 1.17, "regime": "CHOP",
   "regime_quality": 0.8, "qty": 0.0026, "_logged_at": "2026-04-18T12:35:30Z"}

Readers must check "_type" and route accordingly.  Legacy files without "_type"
contain only decision records.

Filter policy
-------------
By default only approved trades and near-misses are written (to keep the
file small during long live sessions).  Pass filter_policy="all" to write
every scored tick, or "approved" to write only entries.
"""
from __future__ import annotations

import json
import os
import threading
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Literal, Optional

from au2_decision import TradeDecisionLog


FilterPolicy = Literal["approved_and_near_miss", "approved", "all"]


class DecisionLogger:
    """Thread-safe JSONL writer for TradeDecisionLog objects.

    Parameters
    ----------
    path           : File path.  Directory is created if it does not exist.
    filter_policy  : Which decisions to persist.
                     "approved_and_near_miss" (default) — entries + near-misses
                     "approved"                         — entries only
                     "all"                              — every scored tick
    max_bytes      : Rotate file when it exceeds this size (0 = never rotate).
                     Rotated files get a timestamp suffix: decisions.jsonl.20260418T120000
    """

    def __init__(
        self,
        path: str = "au2_decisions.jsonl",
        filter_policy: FilterPolicy = "approved_and_near_miss",
        max_bytes: int = 50 * 1024 * 1024,  # 50 MB
    ) -> None:
        self._path        = Path(path)
        self._policy      = filter_policy
        self._max_bytes   = max_bytes
        self._lock        = threading.Lock()
        self._fh          = None

        # In-memory stats (not persisted — reset on restart)
        self._n_written:   int            = 0
        self._n_approved:  int            = 0
        self._n_near_miss: int            = 0
        self._n_rejected:  int            = 0
        self._rejections:  Counter        = Counter()
        self._by_regime:   Counter        = Counter()

        # Trade result stats (populated by log_result)
        self._n_results:     int   = 0
        self._n_wins:        int   = 0
        self._gross_profit:  float = 0.0
        self._gross_loss:    float = 0.0   # stored as positive number
        self._exit_reasons:  Counter = Counter()

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._open()

    # ── File management ───────────────────────────────────────────────────────

    def _open(self) -> None:
        self._fh = open(self._path, "a", encoding="utf-8", buffering=1)

    def _should_rotate(self) -> bool:
        if self._max_bytes <= 0:
            return False
        try:
            return os.path.getsize(self._path) >= self._max_bytes
        except OSError:
            return False

    def _rotate(self) -> None:
        if self._fh:
            self._fh.close()
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        rotated = self._path.with_suffix(f".{ts}.jsonl")
        self._path.rename(rotated)
        self._open()

    def close(self) -> None:
        with self._lock:
            if self._fh:
                self._fh.close()
                self._fh = None

    # ── Core write ────────────────────────────────────────────────────────────

    def _should_write(self, dlog: TradeDecisionLog) -> bool:
        if self._policy == "all":
            return True
        if self._policy == "approved":
            return dlog.approved
        # default: approved_and_near_miss
        return dlog.approved or dlog.near_miss

    def log(self, dlog: TradeDecisionLog, *, force: bool = False) -> None:
        """Persist a decision if it passes the filter policy.

        Parameters
        ----------
        dlog  : Decision to log.
        force : If True, write regardless of filter policy.
        """
        # Update in-memory stats (always, even if not written)
        if dlog.approved:
            self._n_approved += 1
        elif dlog.signal != "FLAT":
            self._n_rejected += 1
            self._rejections[dlog.rejection_reason or "unknown"] += 1
        if dlog.near_miss:
            self._n_near_miss += 1
        if dlog.approved:
            self._by_regime[dlog.regime] += 1

        if not force and not self._should_write(dlog):
            return

        record = dlog.to_dict()
        record["_type"]       = "decision"
        record["_logged_at"]  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        with self._lock:
            if self._should_rotate():
                self._rotate()
            try:
                self._fh.write(json.dumps(record, separators=(",", ":")) + "\n")
                self._n_written += 1
            except Exception:
                pass   # never crash the executor due to logging

    def log_result(self, result) -> None:
        """Persist a TradeResult (exit event) to the JSONL file.

        Called by LiveExecutor immediately after a position closes.
        The ``result`` argument is a TradeResult dataclass instance or any
        object with the same fields (duck-typing — no hard import of au2_core).

        Updates in-memory WR / PF counters so summary() always reflects
        the current session's exit statistics.
        """
        pnl: float = float(getattr(result, "pnl_usd", 0.0))

        # Update in-memory stats
        self._n_results += 1
        if pnl > 0:
            self._n_wins       += 1
            self._gross_profit += pnl
        else:
            self._gross_loss   += abs(pnl)
        exit_reason: str = str(getattr(result, "exit_reason", ""))
        self._exit_reasons[exit_reason] += 1

        record = {
            "_type":          "trade_result",
            "ts":             float(getattr(result, "exit_ts",    0.0)),
            "entry_ts":       float(getattr(result, "entry_ts",   0.0)),
            "entry_price":    float(getattr(result, "entry_price",0.0)),
            "exit_price":     float(getattr(result, "exit_price", 0.0)),
            "pnl_usd":        round(pnl, 6),
            "hold_seconds":   float(getattr(result, "hold_seconds", 0.0)),
            "exit_reason":    exit_reason,
            "signal":         str(getattr(result, "signal",       "")),
            "entry_score":    float(getattr(result, "entry_score",0.0)),
            "confidence":     float(getattr(result, "confidence", 0.0)),
            "regime":         str(getattr(result, "regime",       "")),
            "regime_quality": float(getattr(result, "regime_quality", 1.0)),
            "qty":            float(getattr(result, "qty",        0.0)),
            "_logged_at":     datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        with self._lock:
            if self._should_rotate():
                self._rotate()
            try:
                self._fh.write(json.dumps(record, separators=(",", ":")) + "\n")
                self._n_written += 1
            except Exception:
                pass

    # ── Stats ─────────────────────────────────────────────────────────────────

    def summary(self) -> dict:
        """Return in-memory stats as a plain dict.

        Includes trade result stats (WR, PF) when at least one result has
        been logged via log_result() in this session.
        """
        total_signals = self._n_approved + self._n_rejected
        pf = (self._gross_profit / self._gross_loss
              if self._gross_loss > 0 else None)
        wr = (self._n_wins / self._n_results
              if self._n_results > 0 else None)
        return {
            "approved":      self._n_approved,
            "rejected":      self._n_rejected,
            "near_misses":   self._n_near_miss,
            "approval_rate": (self._n_approved / max(total_signals, 1)),
            "written":       self._n_written,
            "rejections":    dict(self._rejections.most_common()),
            "by_regime":     dict(self._by_regime.most_common()),
            # Exit stats (None when no results logged yet)
            "trades_closed": self._n_results,
            "win_rate":      wr,
            "profit_factor": pf,
            "gross_profit":  round(self._gross_profit, 4),
            "gross_loss":    round(self._gross_loss,   4),
            "exit_reasons":  dict(self._exit_reasons.most_common()),
        }

    def log_summary(self, logger_instance) -> None:
        """Write a summary line via the supplied Python logger (e.g. logging.getLogger)."""
        s = self.summary()
        logger_instance.info(
            "DecisionLogger | approved=%d rejected=%d near_miss=%d "
            "approval_rate=%.1f%% written=%d",
            s["approved"], s["rejected"], s["near_misses"],
            s["approval_rate"] * 100, s["written"],
        )

    # ── Offline readers ───────────────────────────────────────────────────────

    @staticmethod
    def iter_file(path: str) -> Iterator[TradeDecisionLog]:
        """Yield TradeDecisionLog objects from a JSONL file (offline use).

        Silently skips ``_type=trade_result`` lines so callers that only
        care about entry decisions are unaffected by the new record type.
        """
        import dataclasses
        fields = {f.name for f in dataclasses.fields(TradeDecisionLog)}
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    if d.get("_type") == "trade_result":
                        continue   # skip exit records
                    kwargs = {k: v for k, v in d.items() if k in fields}
                    yield TradeDecisionLog(**kwargs)
                except Exception:
                    continue

    @staticmethod
    def iter_results(path: str) -> Iterator[dict]:
        """Yield trade result dicts (``_type=trade_result``) from a JSONL file.

        Each yielded dict has keys: ts, entry_ts, entry_price, exit_price,
        pnl_usd, hold_seconds, exit_reason, signal, entry_score, confidence,
        regime, regime_quality, qty, _logged_at.
        """
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    if d.get("_type") == "trade_result":
                        yield d
                except Exception:
                    continue

    @staticmethod
    def read_summary(path: str) -> dict:
        """Aggregate stats from a JSONL file without loading it all into RAM.

        Includes exit stats (WR, PF) when trade_result records are present.
        """
        n_approved = n_rejected = n_near = 0
        rejections: Counter = Counter()
        regimes:    Counter = Counter()
        # Entry stats
        for dlog in DecisionLogger.iter_file(path):
            if dlog.approved:
                n_approved += 1
                regimes[dlog.regime] += 1
            elif dlog.signal != "FLAT":
                n_rejected += 1
                rejections[dlog.rejection_reason or "unknown"] += 1
            if dlog.near_miss:
                n_near += 1
        # Exit stats
        n_results = n_wins = 0
        gross_profit = gross_loss = 0.0
        exit_reasons: Counter = Counter()
        for r in DecisionLogger.iter_results(path):
            n_results += 1
            pnl = float(r.get("pnl_usd", 0.0))
            if pnl > 0:
                n_wins       += 1
                gross_profit += pnl
            else:
                gross_loss   += abs(pnl)
            exit_reasons[r.get("exit_reason", "")] += 1

        total = n_approved + n_rejected
        return {
            "approved":      n_approved,
            "rejected":      n_rejected,
            "near_misses":   n_near,
            "approval_rate": n_approved / max(total, 1),
            "rejections":    dict(rejections.most_common()),
            "by_regime":     dict(regimes.most_common()),
            # Exit stats (None when no trade_result records in file)
            "trades_closed": n_results,
            "win_rate":      n_wins / n_results if n_results else None,
            "profit_factor": gross_profit / gross_loss if gross_loss > 0 else None,
            "gross_profit":  round(gross_profit, 4),
            "gross_loss":    round(gross_loss,   4),
            "exit_reasons":  dict(exit_reasons.most_common()),
        }

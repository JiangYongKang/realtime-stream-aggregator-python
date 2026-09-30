"""Durable local state: append-only WAL + periodic snapshot (fsync, atomic rename).

Layout (all local files, no external services)::

    <data_dir>/wal.jsonl        # one JSON record per line, monotonic "seq"
    <data_dir>/snapshot.json    # last consistent snapshot + seq it covers

Recovery rule: load snapshot, then replay WAL records whose ``seq`` is greater
than the snapshot's ``seq``. A torn trailing line (partial flush on crash) is
ignored deterministically.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

from streamagg.config import Config

logger = logging.getLogger("streamagg.storage")


class WalStorage:
    def __init__(self, config: Config) -> None:
        self._dir = Path(config.data_dir)
        self._enabled = config.write_wal
        self._dir.mkdir(parents=True, exist_ok=True)
        self._wal_path = self._dir / "wal.jsonl"
        self._snap_path = self._dir / "snapshot.json"
        self._tmp_suffix = ".tmp"
        self._lock = threading.Lock()
        self._seq = 0
        if self._enabled and self._wal_path.exists():
            # Recover the highest good sequence on startup.
            for rec in self._iter_records():
                self._seq = rec["seq"]

    # ------------------------------------------------------------------ WAL
    def append_record(self, record: dict[str, Any]) -> int:
        """Append one record; assigns monotonic ``seq``. Durable when enabled."""
        with self._lock:
            self._seq += 1
            record = {"seq": self._seq, **record}
            if self._enabled:
                line = json.dumps(record, ensure_ascii=False, sort_keys=True)
                with self._wal_path.open("a", encoding="utf-8") as f:
                    f.write(line + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            return self._seq

    def append_event(self, event_dict: dict[str, Any]) -> int:
        return self.append_record({"type": "event", "event": event_dict})

    def read_records(self) -> list[dict[str, Any]]:
        if not self._enabled:
            return []
        return list(self._iter_records())

    def _iter_records(self):
        if not self._wal_path.exists():
            return
        with self._wal_path.open("rb") as f:
            for raw in f:
                try:
                    rec = json.loads(raw.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Torn tail from a crash: stop replay here deterministically.
                    logger.warning("ignore malformed WAL tail: %r", raw[:80])
                    break
                if not isinstance(rec, dict) or "seq" not in rec:
                    logger.warning("ignore invalid WAL record: %r", raw[:80])
                    break
                yield rec

    # -------------------------------------------------------------- snapshot
    def save_snapshot(self, state: dict[str, Any]) -> None:
        """Atomically persist snapshot covering every seq <= state['seq']."""
        if not self._enabled:
            return
        tmp = self._snap_path.with_suffix(self._tmp_suffix)
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._snap_path)
        logger.info("snapshot saved at seq=%s", state.get("seq"))

    def load_snapshot(self) -> dict[str, Any] | None:
        if not self._enabled or not self._snap_path.exists():
            return None
        try:
            with self._snap_path.open(encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            logger.warning("snapshot unreadable, full WAL replay will run")
            return None

    def truncate(self, seq: int) -> None:
        """Drop WAL records with seq <= ``seq`` after they are snapshotted."""
        if not self._enabled or not self._wal_path.exists():
            return
        keep = [r for r in self._iter_records() if r["seq"] > seq]
        tmp = self._wal_path.with_suffix(self._tmp_suffix)
        with tmp.open("w", encoding="utf-8") as f:
            for r in keep:
                f.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._wal_path)

    def close(self) -> None:
        return None

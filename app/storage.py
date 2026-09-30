from __future__ import annotations

import sqlite3
import threading

from app.models import Event, WindowAggregate, WindowKey

PartitionKey = tuple[str, str, int]


class Storage:
    """SQLite-backed durable state.

    Tables
    ------
    events      : raw inbox + global dedup set (event_id PRIMARY KEY).
                  processed=0 -> still pending replay; 1 -> aggregated.
    windows     : one materialized row per (tenant, stream, partition, window).
    watermarks  : current event-time watermark per partition.
    offsets     : per consumer-group committed offsets.

    A single guarded connection is used intentionally: all state is local,
    and a global lock gives us clear, testable ordering semantics.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        if path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    # ------------------------------------------------------------------ #
    def _create_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS events (
                    event_id      TEXT PRIMARY KEY,
                    tenant        TEXT NOT NULL,
                    stream        TEXT NOT NULL,
                    partition_id  INTEGER NOT NULL,
                    event_time_ms INTEGER NOT NULL,
                    value         REAL NOT NULL,
                    processed     INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS ix_events_pending
                    ON events(tenant, stream, partition_id, processed);

                CREATE TABLE IF NOT EXISTS windows (
                    tenant       TEXT NOT NULL,
                    stream       TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    start_ms     INTEGER NOT NULL,
                    end_ms       INTEGER NOT NULL,
                    count        INTEGER NOT NULL,
                    total        REAL NOT NULL,
                    min_value    REAL,
                    max_value    REAL,
                    watermark_ms INTEGER NOT NULL,
                    finalized    INTEGER NOT NULL,
                    amended      INTEGER NOT NULL,
                    PRIMARY KEY (tenant, stream, partition_id, start_ms)
                );

                CREATE TABLE IF NOT EXISTS watermarks (
                    tenant       TEXT NOT NULL,
                    stream       TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    wm_ms        INTEGER NOT NULL,
                    PRIMARY KEY (tenant, stream, partition_id)
                );

                CREATE TABLE IF NOT EXISTS offsets (
                    tenant       TEXT NOT NULL,
                    stream       TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    grp          TEXT NOT NULL,
                    offset       INTEGER NOT NULL,
                    PRIMARY KEY (tenant, stream, partition_id, grp)
                );
                """
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ #
    # dedup / inbox
    # ------------------------------------------------------------------ #
    def seen_event_ids(self, event_ids: list[str]) -> set[str]:
        if not event_ids:
            return set()
        marks = ",".join("?" for _ in event_ids)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT event_id FROM events WHERE event_id IN ({marks})", event_ids
            ).fetchall()
        return {r["event_id"] for r in rows}

    def enqueue_inbox(self, event: Event) -> bool:
        """Durably insert a raw event. False => event_id already exists."""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO events"
                "(event_id, tenant, stream, partition_id, event_time_ms, value, processed)"
                " VALUES (?,?,?,?,?,?,0)",
                (
                    event.event_id, event.tenant, event.stream,
                    event.partition, event.event_time_ms, event.value,
                ),
            )
            return cur.rowcount == 1

    def inbox_pending(self, key: PartitionKey, limit: int = 256) -> list[Event]:
        tenant, stream, partition = key
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE processed=0 AND tenant=? AND stream=?"
                " AND partition_id=? ORDER BY rowid LIMIT ?",
                (tenant, stream, partition, limit),
            ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def inbox_pending_partitions(self) -> list[PartitionKey]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT tenant, stream, partition_id FROM events WHERE processed=0"
            ).fetchall()
        return [(r["tenant"], r["stream"], r["partition_id"]) for r in rows]

    def ack_inbox(self, event_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("UPDATE events SET processed=1 WHERE event_id=?", (event_id,))

    @staticmethod
    def _row_to_event(r: sqlite3.Row) -> Event:
        return Event(
            event_id=r["event_id"],
            tenant=r["tenant"],
            stream=r["stream"],
            partition=r["partition_id"],
            event_time_ms=r["event_time_ms"],
            value=r["value"],
        )

    # ------------------------------------------------------------------ #
    # windows
    # ------------------------------------------------------------------ #
    def get_window(self, key: WindowKey) -> WindowAggregate | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM windows WHERE tenant=? AND stream=?"
                " AND partition_id=? AND start_ms=?",
                (key.tenant, key.stream, key.partition, key.start_ms),
            ).fetchone()
        return self._row_to_window(r) if r else None

    def put_window(self, key: WindowKey, agg: WindowAggregate, size_ms: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO windows(tenant, stream, partition_id, start_ms, end_ms,"
                " count, total, min_value, max_value, watermark_ms, finalized, amended)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(tenant, stream, partition_id, start_ms) DO UPDATE SET"
                " end_ms=excluded.end_ms, count=excluded.count, total=excluded.total,"
                " min_value=excluded.min_value, max_value=excluded.max_value,"
                " watermark_ms=excluded.watermark_ms, finalized=excluded.finalized,"
                " amended=excluded.amended",
                (
                    key.tenant, key.stream, key.partition, key.start_ms,
                    key.start_ms + size_ms, agg.count, agg.total,
                    agg.minimum, agg.maximum, agg.watermark_ms,
                    1 if agg.finalized else 0, 1 if agg.amended else 0,
                ),
            )

    def list_windows(self, key: PartitionKey) -> list[WindowAggregate]:
        tenant, stream, partition = key
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM windows WHERE tenant=? AND stream=? AND partition_id=?"
                " ORDER BY start_ms",
                (tenant, stream, partition),
            ).fetchall()
        return [self._row_to_window(r) for r in rows]

    @staticmethod
    def _row_to_window(r: sqlite3.Row) -> WindowAggregate:
        return WindowAggregate(
            start_ms=r["start_ms"],
            end_ms=r["end_ms"],
            count=r["count"],
            total=r["total"],
            minimum=r["min_value"],
            maximum=r["max_value"],
            distinct_event_ids=r["count"],
            watermark_ms=r["watermark_ms"],
            finalized=bool(r["finalized"]),
            amended=bool(r["amended"]),
        )

    # ------------------------------------------------------------------ #
    # watermark
    # ------------------------------------------------------------------ #
    def get_watermark(self, key: PartitionKey) -> int:
        with self._lock:
            r = self._conn.execute(
                "SELECT wm_ms FROM watermarks WHERE tenant=? AND stream=? AND partition_id=?",
                key,
            ).fetchone()
        return int(r["wm_ms"]) if r else 0

    def put_watermark(self, key: PartitionKey, wm_ms: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO watermarks(tenant, stream, partition_id, wm_ms) VALUES (?,?,?,?)"
                " ON CONFLICT(tenant, stream, partition_id) DO UPDATE SET wm_ms=excluded.wm_ms",
                (key[0], key[1], key[2], wm_ms),
            )

    # ------------------------------------------------------------------ #
    # offsets
    # ------------------------------------------------------------------ #
    def get_offset(self, tenant: str, stream: str, partition: int, group: str) -> int | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT offset FROM offsets WHERE tenant=? AND stream=?"
                " AND partition_id=? AND grp=?",
                (tenant, stream, partition, group),
            ).fetchone()
        return int(r["offset"]) if r else None

    def commit_offset(
        self, tenant: str, stream: str, partition: int, group: str, offset: int
    ) -> str:
        """Idempotent monotonic offset commit.

        Returns OK / DUPLICATE (same value resubmitted) /
        REJECTED_REGRESSION (smaller than stored).
        """
        with self._lock, self._conn:
            r = self._conn.execute(
                "SELECT offset FROM offsets WHERE tenant=? AND stream=?"
                " AND partition_id=? AND grp=?",
                (tenant, stream, partition, group),
            ).fetchone()
            if r is None:
                self._conn.execute(
                    "INSERT INTO offsets(tenant, stream, partition_id, grp, offset)"
                    " VALUES (?,?,?,?,?)",
                    (tenant, stream, partition, group, offset),
                )
                return "OK"
            current = int(r["offset"])
            if offset == current:
                return "DUPLICATE"
            if offset < current:
                return "REJECTED_REGRESSION"
            self._conn.execute(
                "UPDATE offsets SET offset=? WHERE tenant=? AND stream=?"
                " AND partition_id=? AND grp=?",
                (offset, tenant, stream, partition, group),
            )
            return "OK"

    # ------------------------------------------------------------------ #
    def list_streams(self) -> list[PartitionKey]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT tenant, stream, partition_id FROM windows"
            ).fetchall()
        return [(r["tenant"], r["stream"], r["partition_id"]) for r in rows]

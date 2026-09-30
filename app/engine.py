from __future__ import annotations

import threading

from app.aggregator import Aggregator
from app.models import (
    BacklogInfo,
    Event,
    IngestResult,
    IngestStatus,
    StreamConfig,
)

PartitionKey = tuple[str, str, int]


class _Partition:
    """All state and serialization for one (tenant, stream, partition)."""

    __slots__ = (
        "pkey", "lock", "buffered", "capacity", "in_flight",
        "rejected_overload", "last_watermark_ms",
    )

    def __init__(self, pkey: PartitionKey, capacity: int) -> None:
        self.pkey = pkey
        self.lock = threading.Lock()   # serializes processing within a partition
        self.buffered = 0              # events durable but not yet processed
        self.capacity = capacity
        self.in_flight = 0
        self.rejected_overload = 0
        self.last_watermark_ms = 0


class StreamEngine:
    """Bounded, per-partition serialized ingestion with synchronous classification.

    Concurrency model
    -----------------
    * Each partition has a lock, so window/watermark updates are strictly
      serialized and deterministic even with many concurrent clients.
    * Every accepted event is FIRST written to the durable SQLite inbox;
      the deterministic classification (ACCEPTED / AMENDED / DROPPED /
      REJECTED_FUTURE) is then computed synchronously and returned.
    * Backpressure is bounded two ways:
        - ``buffered >= queue_capacity``  -> 503 (durable spool backlog cap)
        - ``in_flight >= max_inflight``   -> 503 (processing saturation)
      Rejected events stay un-acked in the inbox and are replayed after a
      restart, so overload never silently loses data.
    """

    def __init__(self, storage, config, clock, gate: threading.Event | None = None) -> None:
        self.storage = storage
        self.config = config
        self.clock = clock
        self._gate = gate
        self._lock = threading.Lock()
        self._partitions: dict[PartitionKey, _Partition] = {}
        self._aggregators: dict[tuple[str, str], Aggregator] = {}

    # ------------------------------------------------------------------ #
    def _aggregator_for(self, tenant: str, stream: str) -> Aggregator:
        skey = (tenant, stream)
        agg = self._aggregators.get(skey)
        if agg is None:
            with self._lock:
                agg = self._aggregators.get(skey)
                if agg is None:
                    agg = Aggregator(self.storage, self.config.default_stream, self.clock)
                    self._aggregators[skey] = agg
        return agg

    def _partition_for(self, pkey: PartitionKey) -> _Partition:
        part = self._partitions.get(pkey)
        if part is None:
            with self._lock:
                part = self._partitions.get(pkey)
                if part is None:
                    part = _Partition(pkey, self.config.default_stream.queue_capacity)
                    self._partitions[pkey] = part
        return part

    # ------------------------------------------------------------------ #
    def submit(self, event: Event) -> IngestResult:
        cfg: StreamConfig = self.config.default_stream
        pkey = (event.tenant, event.stream, event.partition)
        part = self._partition_for(pkey)

        # ---- fast overload gate: saturation is checked without holding the
        #      partition lock across (possibly slow) processing ----
        with part.lock:
            if part.in_flight >= cfg.max_inflight or part.buffered >= part.capacity:
                part.rejected_overload += 1
                return self._overloaded(event, pkey, part, cfg)
            part.in_flight += 1

        claimed = True
        try:
            # 1) durable inbox first — also the global dedup table
            inserted = self.storage.enqueue_inbox(event)
            if not inserted:
                return IngestResult(
                    status=IngestStatus.DUPLICATE,
                    event_id=event.event_id,
                    window_start_ms=Aggregator.window_start(
                        event.event_time_ms, cfg.window_size_ms
                    ),
                    watermark_ms=self.storage.get_watermark(pkey),
                    detail="event_id already recorded; duplicate submit is ignored",
                )

            with part.lock:
                part.buffered += 1

            # allow tests (or a slow consumer) to pause processing. The
            # partition lock is NOT held while waiting, so overload checks
            # keep working and backpressure remains observable.
            if self._gate is not None:
                self._gate.wait()

            # While parked, the buffer may have filled; reject explicitly
            # instead of growing memory. The durable inbox row survives and
            # is replayed on recover()/restart.
            with part.lock:
                saturated = part.buffered > part.capacity
            if saturated:
                with part.lock:
                    part.rejected_overload += 1
                return self._overloaded(event, pkey, part, cfg)

            # 2) deterministic synchronous classification + aggregation;
            #    a short critical section keeps per-partition ordering.
            with part.lock:
                result = self._aggregator_for(event.tenant, event.stream).apply(event)
                part.buffered -= 1
                part.last_watermark_ms = result.watermark_ms
            return result
        finally:
            if claimed:
                with part.lock:
                    part.in_flight -= 1

    @staticmethod
    def _overloaded(event: Event, pkey: PartitionKey, part: "_Partition",
                    cfg: StreamConfig) -> IngestResult:
        return IngestResult(
            status=IngestStatus.REJECTED_OVERLOAD,
            event_id=event.event_id,
            detail=(
                f"partition {pkey} overloaded"
                f" (buffered={part.buffered}/{part.capacity},"
                f" in_flight={part.in_flight}/{cfg.max_inflight});"
                " request explicitly rejected; event remains durable in the"
                " spool if it had been stored, otherwise nothing was buffered"
            ),
        )

    # ------------------------------------------------------------------ #
    def backlog(self, tenant: str, stream: str) -> list[BacklogInfo]:
        with self._lock:
            items = sorted(
                (p for p in self._partitions.values()
                 if p.pkey[0] == tenant and p.pkey[1] == stream),
                key=lambda p: p.pkey[2],
            )
        out: list[BacklogInfo] = []
        for part in items:
            ratio = (part.buffered + part.in_flight) / max(part.capacity, 1)
            out.append(
                BacklogInfo(
                    tenant=part.pkey[0],
                    stream=part.pkey[1],
                    partition=part.pkey[2],
                    queued=part.buffered,
                    in_flight=part.in_flight,
                    queue_capacity=part.capacity,
                    backpressure_ratio=round(ratio, 4),
                    rejected_overload=part.rejected_overload,
                    last_watermark_ms=part.last_watermark_ms,
                )
            )
        return out

    # ------------------------------------------------------------------ #
    def recover(self) -> dict[PartitionKey, int]:
        """Replay un-acked inbox events from disk through their aggregators."""
        report: dict[PartitionKey, int] = {}
        for pkey in self.storage.inbox_pending_partitions():
            tenant, stream, _partition = pkey
            agg = self._aggregator_for(tenant, stream)
            part = self._partition_for(pkey)
            with part.lock:
                report[pkey] = agg.drain_pending_inbox(pkey)
                part.last_watermark_ms = self.storage.get_watermark(pkey)
        return report

    # ------------------------------------------------------------------ #
    def wait_idle(self, timeout: float | None = None) -> bool:
        """Synchronous engine is idle once every partition lock is free."""
        import time as _time

        deadline = None if timeout is None else _time.monotonic() + timeout
        while True:
            with self._lock:
                parts = list(self._partitions.values())
            if all(p.in_flight == 0 and p.buffered == 0 for p in parts):
                return True
            if deadline is not None and _time.monotonic() >= deadline:
                return False
            _time.sleep(0.005)

    def shutdown(self, timeout: float = 5.0) -> None:
        self.wait_idle(timeout)

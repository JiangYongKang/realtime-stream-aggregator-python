"""Bounded in-memory queue with explicit overload rejection.

Memory is bounded hard: ``put_nowait`` raises :class:`OverloadError` once the
queue is full (callers never block waiting for room). Every enqueued item is
tagged with its stream so per-stream backlog is queryable. ``in_flight`` counts
items the worker has dequeued but not yet finished, giving the true pending
work as ``queue_depth + in_flight``.
"""

from __future__ import annotations

import queue
import threading

from streamagg.config import Config
from streamagg.errors import OverloadError
from streamagg.models import BacklogMetrics


class BoundedQueue:
    def __init__(self, config: Config) -> None:
        self._q: queue.Queue = queue.Queue(maxsize=config.queue_capacity)
        self._capacity = config.queue_capacity
        self._in_flight = 0
        self._enqueued = 0
        self._rejected = 0
        self._backlog: dict[str, int] = {}
        self._lock = threading.Lock()

    def put_nowait(self, item: tuple, stream: str) -> None:
        """Enqueue or reject immediately with current backlog in the error."""
        try:
            self._q.put_nowait((item, stream))
        except queue.Full:
            with self._lock:
                self._rejected += 1
                depth = self._q.qsize()
            raise OverloadError(
                "ingest queue is full; request rejected under overload policy",
                details={
                    "queue_depth": depth,
                    "queue_capacity": self._capacity,
                    "in_flight": self._in_flight,
                },
            )
        with self._lock:
            self._enqueued += 1
            self._backlog[stream] = self._backlog.get(stream, 0) + 1

    def get(self, timeout: float | None = 0.1) -> tuple | None:
        try:
            packed = self._q.get(timeout=timeout)
        except queue.Empty:
            return None
        item, stream = packed
        with self._lock:
            self._in_flight += 1
            self._backlog[stream] -= 1
            if self._backlog[stream] <= 0:
                self._backlog.pop(stream, None)
        return item

    def task_done(self, stream: str) -> None:
        with self._lock:
            self._in_flight = max(0, self._in_flight - 1)

    def metrics(self) -> BacklogMetrics:
        with self._lock:
            return BacklogMetrics(
                queue_depth=self._q.qsize(),
                queue_capacity=self._capacity,
                in_flight=self._in_flight,
                total_enqueued=self._enqueued,
                total_rejected_overload=self._rejected,
                per_stream=dict(self._backlog),
            )

    def depth(self) -> int:
        return self._q.qsize()

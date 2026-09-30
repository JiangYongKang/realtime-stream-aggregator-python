from __future__ import annotations

import time
from threading import Lock


class Clock:
    """Injectable clock so tests can control wall-clock (future-event checks)."""

    __slots__ = ("_lock", "_override_ms")

    def __init__(self) -> None:
        self._lock = Lock()
        self._override_ms: int | None = None

    def now_ms(self) -> int:
        with self._lock:
            if self._override_ms is not None:
                return self._override_ms
        return default_now_ms()

    def set(self, epoch_ms: int) -> None:
        with self._lock:
            self._override_ms = epoch_ms

    def reset(self) -> None:
        with self._lock:
            self._override_ms = None


# A monotonic anchor avoids system-clock jumps inside a single run.
_wall_origin_ns = time.perf_counter_ns()
_wall_origin_ms = int(time.time() * 1000)


def default_now_ms() -> int:
    return _wall_origin_ms + (time.perf_counter_ns() - _wall_origin_ns) // 1_000_000

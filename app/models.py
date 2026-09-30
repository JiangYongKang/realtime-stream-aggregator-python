from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Enumerations / result codes
# ---------------------------------------------------------------------------
class IngestStatus(str, enum.Enum):
    """Deterministic outcome of an ingest request."""

    ACCEPTED = "ACCEPTED"        # counted in a currently-open window
    DUPLICATE = "DUPLICATE"      # event_id already seen, not counted twice
    AMENDED = "AMENDED"          # late but within allowed lateness -> window corrected
    DROPPED_LATE = "DROPPED"     # too late: window already finalized past retention
    REJECTED_FUTURE = "REJECTED_FUTURE"  # event_time ahead of wall clock tolerance
    REJECTED_OVERLOAD = "REJECTED_OVERLOAD"  # bounded queue / inflight full
    REJECTED_AUTH = "REJECTED_AUTH"
    REJECTED_ARG = "REJECTED_ARG"


class CommitStatus(str, enum.Enum):
    OK = "OK"
    DUPLICATE = "DUPLICATE"            # same offset committed again -> idempotent
    REJECTED_REGRESSION = "REJECTED_REGRESSION"
    REJECTED_AUTH = "REJECTED_AUTH"


@dataclass(frozen=True, slots=True)
class Event:
    event_id: str
    tenant: str
    stream: str
    partition: int
    event_time_ms: int
    value: float

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex


@dataclass(slots=True)
class WindowKey:
    tenant: str
    stream: str
    partition: int
    start_ms: int

    def end_ms(self, size_ms: int) -> int:
        return self.start_ms + size_ms


@dataclass(slots=True)
class WindowAggregate:
    start_ms: int
    end_ms: int
    count: int = 0
    total: float = 0.0
    minimum: float | None = None
    maximum: float | None = None
    distinct_event_ids: int = 0
    watermark_ms: int = 0
    finalized: bool = False
    amended: bool = False

    def as_dict(self) -> dict:
        return {
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "count": self.count,
            "sum": round(self.total, 9),
            "min": self.minimum,
            "max": self.maximum,
            "distinct_event_ids": self.distinct_event_ids,
            "watermark_ms": self.watermark_ms,
            "finalized": self.finalized,
            "amended": self.amended,
        }


@dataclass(slots=True)
class IngestResult:
    status: IngestStatus
    event_id: str
    window_start_ms: int | None = None
    watermark_ms: int = 0
    detail: str = ""

    def as_dict(self) -> dict:
        return {
            "status": self.status.value,
            "event_id": self.event_id,
            "window_start_ms": self.window_start_ms,
            "watermark_ms": self.watermark_ms,
            "detail": self.detail,
        }


@dataclass(slots=True)
class BacklogInfo:
    tenant: str
    stream: str
    partition: int
    queued: int
    in_flight: int
    queue_capacity: int
    backpressure_ratio: float
    rejected_overload: int
    last_watermark_ms: int


@dataclass(slots=True)
class StreamConfig:
    window_size_ms: int = 1000
    allowed_lateness_ms: int = 500
    queue_capacity: int = 256
    max_inflight: int = 64
    future_tolerance_ms: int = 60_000


@dataclass(slots=True)
class PendingEvent:
    event: Event
    enqueued_at: float = field(default_factory=time.time)

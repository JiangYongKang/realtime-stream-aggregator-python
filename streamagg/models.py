"""Domain types: events, outcomes, window aggregates, backlog metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class EventOutcome(str, Enum):
    """Deterministic classification of an ingest attempt."""

    ACCEPTED = "ACCEPTED"          # 在窗口关闭前正常计入
    LATE_ACCEPTED = "LATE_ACCEPTED"  # 迟到但未超过宽限期，补算进已输出窗口
    DUPLICATE = "DUPLICATE"        # event_id 重复，幂等忽略
    DROPPED = "DROPPED"            # 已接入但超过迟到宽限，明确丢弃
    REJECTED = "REJECTED"          # 接入前拒绝（鉴权/过载/非法/已清理）


@dataclass(frozen=True)
class Event:
    event_id: str
    tenant: str
    stream: str
    timestamp_ms: int
    partition_key: str = ""
    payload: dict = field(default_factory=dict)


@dataclass
class IngestResult:
    outcome: EventOutcome
    reason: str | None = None
    window_start: int | None = None
    window_end: int | None = None
    watermark_ms: int | None = None
    event_count: int | None = None      # 窗口内当前计数
    backlog: int = 0


@dataclass
class WindowAgg:
    window_start: int
    window_end: int
    count: int
    version: int = 0  # 补算一次递增一次，可区分稳定结果与补算结果


@dataclass
class BacklogMetrics:
    queue_depth: int
    queue_capacity: int
    in_flight: int
    total_enqueued: int
    total_rejected_overload: int
    per_stream: dict[str, int] = field(default_factory=dict)

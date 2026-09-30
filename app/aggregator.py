from __future__ import annotations

from app.models import (
    Event,
    IngestResult,
    IngestStatus,
    StreamConfig,
    WindowAggregate,
    WindowKey,
)

PartitionKey = tuple[str, str, int]


class Aggregator:
    """Tumbling, event-time window aggregation with bounded lateness.

    Rules (deterministic, no wall-clock dependency for window membership):

    * Window size ``W`` is epoch aligned: ``start = (t // W) * W``.
    * Watermark: ``WM = max(event_time seen on partition) - allowed_lateness``.
    * A window ``[s, s+W)`` stays *open* while ``s+W > WM`` and becomes
      *finalized* once ``s+W <= WM``.
    * Event classification:
        - future event (``t > now + future_tolerance``) -> REJECTED_FUTURE;
        - ``t < WM`` and its window finalized          -> DROPPED_LATE;
        - ``t < WM`` but window still open (out of order) -> AMENDED (counted,
          window flagged ``amended``);
        - otherwise                                    -> ACCEPTED.
    * ``event_id`` is the dedup key; duplicate ids are never counted twice.
    """

    __slots__ = ("storage", "cfg", "clock")

    def __init__(self, storage, cfg: StreamConfig, clock) -> None:
        self.storage = storage
        self.cfg = cfg
        self.clock = clock

    @staticmethod
    def window_start(event_time_ms: int, size_ms: int) -> int:
        return (event_time_ms // size_ms) * size_ms

    # ------------------------------------------------------------------ #
    def apply(self, event: Event) -> IngestResult:
        cfg = self.cfg
        key = WindowKey(event.tenant, event.stream, event.partition, 0)  # start filled below
        pkey: PartitionKey = (event.tenant, event.stream, event.partition)

        # 1) future events are rejected outright (never move the watermark)
        now = self.clock.now_ms()
        if event.event_time_ms > now + cfg.future_tolerance_ms:
            self.storage.ack_inbox(event.event_id)
            return IngestResult(
                status=IngestStatus.REJECTED_FUTURE,
                event_id=event.event_id,
                detail=(
                    f"event_time_ms={event.event_time_ms} is more than "
                    f"{cfg.future_tolerance_ms}ms ahead of wall clock {now}"
                ),
            )

        # 2) advance watermark
        wm_before = self.storage.get_watermark(pkey)
        wm_after = max(wm_before, event.event_time_ms - cfg.allowed_lateness_ms)
        if wm_after != wm_before:
            self.storage.put_watermark(pkey, wm_after)

        start = self.window_start(event.event_time_ms, cfg.window_size_ms)
        end = start + cfg.window_size_ms
        key.start_ms = start
        late = wm_before > 0 and event.event_time_ms < wm_before

        # 3) window already finalized by the watermark -> terminal drop
        if end <= wm_after:
            self.storage.ack_inbox(event.event_id)
            return IngestResult(
                status=IngestStatus.DROPPED_LATE,
                event_id=event.event_id,
                window_start_ms=start,
                watermark_ms=wm_after,
                detail=(
                    f"window [{start},{end}) closed by watermark={wm_after}; "
                    "event is past allowed lateness and is dropped (not counted)"
                ),
            )

        # 4) open window: aggregate (late => amendment)
        agg = self.storage.get_window(key)
        if agg is None:
            agg = WindowAggregate(start_ms=start, end_ms=end)
        agg.count += 1
        agg.total += event.value
        agg.minimum = event.value if agg.minimum is None else min(agg.minimum, event.value)
        agg.maximum = event.value if agg.maximum is None else max(agg.maximum, event.value)
        agg.distinct_event_ids = agg.count
        agg.watermark_ms = wm_after
        if late:
            agg.amended = True
        self.storage.put_window(key, agg, cfg.window_size_ms)

        # 5) finalize every older open window the watermark has now passed
        self._finalize_due_windows(pkey, wm_after)

        self.storage.ack_inbox(event.event_id)
        status = IngestStatus.AMENDED if late else IngestStatus.ACCEPTED
        return IngestResult(
            status=status,
            event_id=event.event_id,
            window_start_ms=start,
            watermark_ms=wm_after,
            detail=(
                "late event within allowed lateness, window aggregate corrected"
                if late
                else "event counted in its event-time window"
            ),
        )

    # ------------------------------------------------------------------ #
    def _finalize_due_windows(self, pkey: PartitionKey, wm_ms: int) -> None:
        for agg in self.storage.list_windows(pkey):
            if not agg.finalized and agg.end_ms <= wm_ms:
                agg.finalized = True
                agg.watermark_ms = wm_ms
                key = WindowKey(pkey[0], pkey[1], pkey[2], agg.start_ms)
                self.storage.put_window(key, agg, self.cfg.window_size_ms)
            elif not agg.finalized and agg.watermark_ms != wm_ms:
                # keep watermark annotation fresh on still-open windows
                agg.watermark_ms = wm_ms
                key = WindowKey(pkey[0], pkey[1], pkey[2], agg.start_ms)
                self.storage.put_window(key, agg, self.cfg.window_size_ms)

    # ------------------------------------------------------------------ #
    def drain_pending_inbox(self, pkey: PartitionKey) -> int:
        """Replay un-acked inbox rows for one partition in arrival order."""
        processed = 0
        while True:
            pending = self.storage.inbox_pending(pkey, limit=256)
            if not pending:
                break
            for event in pending:
                self.apply(event)
                processed += 1
        return processed

"""Tumbling event-time windows with deterministic late/out-of-order handling.

Rules (per ``tenant/stream``; partitions are carried in the event and share the
stream timeline):

* Windows are fixed tumbling buckets ``[k*W, (k+1)*W)`` keyed by event time.
* The watermark equals the highest event timestamp seen so far (strict
  monotonic progress). A window is *finalized* (emitted) when
  ``watermark >= window_end``.
* Out-of-order events whose window is still open are counted normally:
  ``ACCEPTED``.
* A finalized window remains mutable during the allowed-lateness period
  ``watermark <= window_end + L``: such events are counted as ``LATE_ACCEPTED``
  and the window ``version`` is incremented (a supplement/补算).
* Events arriving with ``watermark > window_end + L`` are ``DROPPED`` with
  reason ``beyond_allowed_lateness`` (the window is retained for queries but
  the event is explicitly not counted; never silent).
* After retention, finalized windows are evicted (the R most recently expired
  windows are kept). Events for evicted windows are ``REJECTED`` with reason
  ``window_retired``.
* ``event_id`` dedup is checked first within the retained window range, so
  replayed duplicates always return ``DUPLICATE`` there. Once a window ages out
  of retention its event ids are retired with it; a later replay is then
  ``REJECTED/window_retired`` (still never counted) -- deterministic either way.
"""

from __future__ import annotations

import logging

from streamagg.config import Config
from streamagg.models import Event, EventOutcome, WindowAgg

logger = logging.getLogger("streamagg.aggregator")


class StreamState:
    def __init__(self, window_ms: int, allowed_lateness_ms: int,
                 retention_windows: int) -> None:
        self.W = window_ms
        self.L = allowed_lateness_ms
        self.R = retention_windows
        self.max_ts: int | None = None
        # window_start -> mutable window row
        self.windows: dict[int, dict] = {}
        # finalization order: list of (ordinal, window_start)
        self.finalized: list[tuple[int, int]] = []
        self._next_ordinal = 1
        # event_id -> window_start (ids of evicted windows are forgotten)
        self.seen: dict[str, int] = {}
        # any window starting before this boundary has been evicted (retired)
        self.retired_before: int = 0

    # ------------------------------------------------------------- internals
    def _window_row(self, ws: int) -> WindowAgg:
        w = self.windows[ws]
        return WindowAgg(window_start=ws, window_end=ws + self.W,
                         count=w["count"], version=w["version"])

    def _advance(self, wm: int) -> None:
        """Finalize windows past the watermark, then evict beyond retention."""
        for ws in sorted(self.windows):
            w = self.windows[ws]
            if not w["finalized"] and ws + self.W <= wm:
                w["finalized"] = True
                w["ordinal"] = self._next_ordinal
                self._next_ordinal += 1
                self.finalized.append((w["ordinal"], ws))
                logger.info("window finalized: [%s,%s) ordinal=%s count=%s",
                            ws, ws + self.W, w["ordinal"], w["count"])
        # Expired slots are window ends e with e + L < wm. Retain the R most
        # recent expired slots; older *slots* (including empty gaps) are retired
        # so retention is time-based and independent of whether gaps had data.
        newest_expired_end = ((wm - 1 - self.L) // self.W) * self.W
        cutoff = newest_expired_end - self.R * self.W  # retire ws < cutoff
        for ws in [s for s, w in self.windows.items()
                   if w["finalized"] and s < cutoff]:
            self._evict(ws, reason="retention")

    def _evict(self, ws: int, reason: str) -> None:
        self.windows.pop(ws, None)
        self.finalized = [(o, s) for (o, s) in self.finalized if s != ws]
        gone = [eid for eid, s in self.seen.items() if s == ws]
        for eid in gone:
            self.seen.pop(eid, None)
        self.retired_before = max(self.retired_before, ws + self.W)
        logger.info("window evicted (%s): start=%s, events=%s",
                    reason, ws, len(gone))

    # --------------------------------------------------------------- apply
    def apply(self, event: Event) -> tuple[EventOutcome, str | None, WindowAgg | None]:
        # 1) idempotency first: duplicate replays must be stable forever.
        if event.event_id in self.seen:
            ws = self.seen[event.event_id]
            row = self._window_row(ws) if ws in self.windows else None
            logger.info("duplicate event_id=%s window=%s", event.event_id, ws)
            return EventOutcome.DUPLICATE, "duplicate_event_id", row

        ts = event.timestamp_ms
        if ts < 0:
            return EventOutcome.REJECTED, "invalid_timestamp", None

        # 2) advance watermark using this event's time as well (watermark=max ts).
        self.max_ts = ts if self.max_ts is None else max(self.max_ts, ts)
        wm = self.max_ts
        self._advance(wm)

        ws = (ts // self.W) * self.W
        end = ws + self.W

        # 3) classify against the (possibly advanced) watermark.
        # 3) retired by retention? (state evicted -> distinct from a mere drop)
        if ws < self.retired_before:
            return EventOutcome.REJECTED, "window_retired", None

        w = self.windows.get(ws)
        if w is None:
            if end + self.L < wm:
                # Window never had data and is already beyond lateness:
                # admitted but explicitly not counted.
                return EventOutcome.DROPPED, "beyond_allowed_lateness", None
            w = {"count": 0, "version": 0, "finalized": False, "ordinal": None}
            self.windows[ws] = w
            # The watermark may already be past this window's end (first event
            # of a window arrives late): finalize before classification.
            self._advance(wm)

        if end + self.L < wm:
            return EventOutcome.DROPPED, "beyond_allowed_lateness", self._window_row(ws)

        w["count"] += 1
        w["version"] += 1
        self.seen[event.event_id] = ws

        if w["finalized"]:
            outcome = EventOutcome.LATE_ACCEPTED
            reason = "late_within_allowed_lateness"
            logger.info("LATE_ACCEPTED event=%s window=[%s,%s) count=%s v%s",
                        event.event_id, ws, end, w["count"], w["version"])
        else:
            outcome = EventOutcome.ACCEPTED
            reason = None
        return outcome, reason, self._window_row(ws)

    # ------------------------------------------------------------- queries
    def watermark(self) -> int | None:
        return self.max_ts

    def list_windows(self, include_open: bool = True) -> list[WindowAgg]:
        rows = [self._window_row(ws) for ws in sorted(self.windows)]
        if not include_open:
            rows = [r for r in rows if self.windows[r.window_start]["finalized"]]
        return rows

    def windows_since(self, ordinal: int) -> list[dict]:
        out = []
        for o, ws in self.finalized:
            if o > ordinal:
                row = self._window_row(ws)
                out.append({"ordinal": o, **row.__dict__})
        return out

    def snapshot(self) -> dict:
        return {
            "W": self.W,
            "L": self.L,
            "R": self.R,
            "max_ts": self.max_ts,
            "retired_before": self.retired_before,
            "next_ordinal": self._next_ordinal,
            "windows": [
                {"start": ws, "count": w["count"], "version": w["version"],
                 "finalized": w["finalized"], "ordinal": w["ordinal"]}
                for ws, w in sorted(self.windows.items())
            ],
            "seen": [{"id": k, "ws": v} for k, v in self.seen.items()],
        }

    def restore(self, st: dict) -> None:
        self.W = st["W"]
        self.L = st["L"]
        self.R = st["R"]
        self.max_ts = st["max_ts"]
        self.retired_before = st.get("retired_before", 0)
        self._next_ordinal = st["next_ordinal"]
        self.windows = {}
        for row in st["windows"]:
            self.windows[row["start"]] = {
                "count": row["count"], "version": row["version"],
                "finalized": row["finalized"], "ordinal": row["ordinal"],
            }
        self.finalized = sorted(
            ((w["ordinal"], ws) for ws, w in self.windows.items()
             if w["finalized"] and w["ordinal"] is not None),
        )
        self.seen = {e["id"]: e["ws"] for e in st["seen"]}


class Aggregator:
    def __init__(self, config: Config) -> None:
        self._cfg = config
        self._streams: dict[str, StreamState] = {}

    @staticmethod
    def key(tenant: str, stream: str) -> str:
        return f"{tenant}/{stream}"

    def get_or_create(self, key: str) -> StreamState:
        st = self._streams.get(key)
        if st is None:
            st = StreamState(self._cfg.window_ms,
                             self._cfg.allowed_lateness_ms,
                             self._cfg.retention_windows)
            self._streams[key] = st
        return st

    def apply(self, event: Event) -> tuple[EventOutcome, str | None, WindowAgg | None]:
        return self.get_or_create(self.key(event.tenant, event.stream)).apply(event)

    def snapshot(self) -> dict:
        return {k: v.snapshot() for k, v in self._streams.items()}

    def restore(self, state: dict) -> None:
        for k, st in (state or {}).items():
            s = self.get_or_create(k)
            s.restore(st)

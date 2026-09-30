"""Single worker draining the bounded queue.

All serialization happens here: WAL append -> aggregator apply -> response.
Because there is exactly one worker thread, the event order seen by the
aggregator is the accepted (enqueued) order, which makes results deterministic
and concurrent-writer tests stable. Each request carries a ``Future`` so the
HTTP call waits for its own result (ingest is synchronous from the caller's
point of view) while overload is rejected at the queue boundary.

Durability: every event is appended with fsync before it is counted; a snapshot
(aggregator + offsets) is written periodically and the WAL truncated. On
startup, state is rebuilt as snapshot + WAL suffix replay, and replayed events
keep their original semantic outcome without re-serving duplicates.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import Future
from dataclasses import dataclass

from streamagg.aggregator import Aggregator
from streamagg.config import Config
from streamagg.models import Event, IngestResult
from streamagg.offsets import OffsetStore
from streamagg.queue import BoundedQueue
from streamagg.storage import WalStorage

logger = logging.getLogger("streamagg.worker")


@dataclass
class _Job:
    event: Event
    future: Future


class IngestWorker:
    def __init__(self, config: Config, queue: BoundedQueue,
                 aggregator: Aggregator, storage: WalStorage,
                 offsets: OffsetStore | None = None,
                 snapshot_every: int = 200) -> None:
        self._cfg = config
        self._q = queue
        self._agg = aggregator
        self._store = storage
        self._offsets = offsets or OffsetStore()
        self._snapshot_every = snapshot_every
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._processed = 0
        self._last_snap_seq = 0

    # --------------------------------------------------------------- lifecycle
    def start(self) -> None:
        self.restore()
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="streamagg-worker", daemon=True)
        self._thread.start()
        logger.info("worker started")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        self.snapshot_now()
        logger.info("worker stopped, final snapshot taken")

    def restore(self) -> dict:
        """Rebuild aggregator/offsets from snapshot + WAL suffix."""
        snap = self._store.load_snapshot()
        base_seq = 0
        if snap:
            base_seq = snap.get("seq", 0)
            self._agg.restore(snap.get("aggregator", {}))
            self._offsets.restore(snap.get("offsets", {}))
            logger.info("restored snapshot seq=%s streams=%d offsets=%d",
                        base_seq, len(snap.get("aggregator", {})),
                        len(snap.get("offsets", {})))
        replayed = 0
        for rec in self._store.read_records():
            if rec["seq"] <= base_seq:
                continue
            if rec.get("type") == "event":
                ev = Event(**rec["event"])
                self._agg.apply(ev)
                replayed += 1
        logger.info("WAL replay after seq=%s: %d events", base_seq, replayed)
        return {"snapshot_seq": base_seq, "replayed": replayed}

    def snapshot_now(self) -> int:
        seq = self._store.append_record({"type": "snapshot_marker"})
        self._store.save_snapshot({
            "seq": seq,
            "aggregator": self._agg.snapshot(),
            "offsets": self._offsets.snapshot(),
        })
        self._store.truncate(seq)
        self._last_snap_seq = seq
        return seq

    # ------------------------------------------------------------------- submit
    def submit_future(self, event: Event) -> Future:
        """Enqueue only; overload is raised here, processing happens later."""
        fut: Future = Future()
        self._q.put_nowait(_Job(event, fut),
                           Aggregator.key(event.tenant, event.stream))
        return fut

    def submit(self, event: Event, timeout: float = 5.0) -> IngestResult:
        return self.submit_future(event).result(timeout=timeout)

    def drain(self, timeout: float = 10.0) -> None:
        """Wait until the accepted queue is empty (for deterministic tests)."""
        import time
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            m = self._q.metrics()
            if m.queue_depth == 0 and m.in_flight == 0:
                return
            time.sleep(0.005)
        raise TimeoutError("queue did not drain in time")

    @property
    def offsets(self) -> OffsetStore:
        return self._offsets

    # ----------------------------------------------------------------- run loop
    def _run(self) -> None:
        while not self._stop.is_set():
            item = self._q.get(timeout=0.1)
            if item is None:
                continue
            job: _Job = item
            stream = Aggregator.key(job.event.tenant, job.event.stream)
            try:
                result = self._process(job.event)
                job.future.set_result(result)
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("worker processing failed")
                job.future.set_exception(exc)
            finally:
                self._q.task_done(stream)
        # graceful drain of already-accepted work before thread exits
        while True:
            item = self._q.get(timeout=0.01)
            if item is None:
                break
            job = item
            stream = Aggregator.key(job.event.tenant, job.event.stream)
            try:
                job.future.set_result(self._process(job.event))
            except Exception as exc:  # pragma: no cover
                job.future.set_exception(exc)
            finally:
                self._q.task_done(stream)

    def _process(self, event: Event) -> IngestResult:
        if self._cfg.process_delay_s > 0:
            import time
            time.sleep(self._cfg.process_delay_s)
        seq = self._store.append_event(event.__dict__)
        outcome, reason, win = self._agg.apply(event)
        self._processed += 1
        if self._processed % self._snapshot_every == 0:
            self.snapshot_now()
        st = self._agg.get_or_create(
            Aggregator.key(event.tenant, event.stream))
        return IngestResult(
            outcome=outcome,
            reason=reason,
            window_start=win.window_start if win else None,
            window_end=win.window_end if win else None,
            watermark_ms=st.watermark(),
            event_count=win.count if win else None,
            backlog=self._q.depth(),
        )

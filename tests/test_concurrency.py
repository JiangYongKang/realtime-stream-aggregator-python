from __future__ import annotations

import threading
import time

from app.models import Event, StreamConfig
from tests.util import log_input, log_section, log_state

TENANT = "tenant-a"
STREAM = "clicks"
BASE = 1_700_000_000_000


def test_concurrent_writers_no_loss_and_dedup(make_engine):
    """N threads writing distinct ids -> exactly-once counts; resubmits -> dups."""
    log_section("concurrent writes: 8 threads x 50 events, 4 partitions")
    cfg = StreamConfig(queue_capacity=512, max_inflight=128)
    engine = make_engine(cfg)

    n_threads, per_thread = 8, 50
    results: dict[str, str] = {}
    lock = threading.Lock()

    def worker(wid: int) -> None:
        for i in range(per_thread):
            event = Event(
                event_id=f"w{wid}-{i}", tenant=TENANT, stream=STREAM,
                partition=i % 4, event_time_ms=BASE + (i * 10), value=1.0,
            )
            r = engine.submit(event)
            with lock:
                results[r.event_id] = r.status.value

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(n_threads)]
    log_input("writers", {"threads": n_threads, "each": per_thread})
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [k for k, v in results.items() if v == "ACCEPTED"]
    log_state("submit results", {
        "total": len(results),
        "accepted": len(accepted),
        "other": {s: sum(1 for v in results.values() if v == s)
                  for s in set(results.values()) if s != "ACCEPTED"},
    })
    assert len(accepted) == n_threads * per_thread

    total_count = sum(
        w.count for p in range(4)
        for w in engine.storage.list_windows((TENANT, STREAM, p))
    )
    log_state("aggregated event count across partitions", total_count)
    assert total_count == n_threads * per_thread

    # resubmitting 100 ids -> pure duplicates, aggregate unchanged
    dup_statuses = []
    for k in list(results)[:100]:
        i = int(k.split("-")[1])
        dup_statuses.append(engine.submit(Event(
            event_id=k, tenant=TENANT, stream=STREAM, partition=i % 4,
            event_time_ms=BASE + i * 10, value=1.0,
        )).status.value)
    assert all(s == "DUPLICATE" for s in dup_statuses)
    total_after = sum(
        w.count for p in range(4)
        for w in engine.storage.list_windows((TENANT, STREAM, p))
    )
    log_state("count after duplicate storm", total_after)
    assert total_after == total_count


def test_saturation_rejects_explicitly_and_backlog_is_queryable(make_engine, gate):
    """max_inflight saturation -> fast 503-class rejection, observable backlog."""
    log_section("overload path A: in_flight saturation rejects without buffering")
    gate.clear()  # freeze processing after durable spool
    cfg = StreamConfig(queue_capacity=4, max_inflight=2)
    engine = make_engine(cfg)

    parked = [threading.Thread(target=engine.submit, args=(Event(
        event_id=f"park-{i}", tenant=TENANT, stream=STREAM, partition=0,
        event_time_ms=BASE + i, value=1.0,
    ),)) for i in range(2)]
    for t in parked:
        t.start()

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        bl = engine.backlog(TENANT, STREAM)
        if bl and bl[0].in_flight >= 2:
            break
        time.sleep(0.005)
    bl = engine.backlog(TENANT, STREAM)[0]
    log_state("backlog while processing frozen", {
        "queued": bl.queued, "in_flight": bl.in_flight,
        "capacity": bl.queue_capacity, "ratio": bl.backpressure_ratio,
    })
    assert bl.in_flight == 2

    rejected = 0
    for i in range(2, 22):
        r = engine.submit(Event(
            event_id=f"fast-{i}", tenant=TENANT, stream=STREAM, partition=0,
            event_time_ms=BASE + i, value=1.0,
        ))
        if r.status.value == "REJECTED_OVERLOAD":
            rejected += 1
    log_input("fast rejections while saturated", rejected)
    assert rejected == 20
    bl = engine.backlog(TENANT, STREAM)[0]
    assert bl.rejected_overload == 20
    log_state("backlog counters after storm", bl)

    # resume: parked events process exactly once
    gate.set()
    for t in parked:
        t.join()
    assert engine.wait_idle(5)
    total = sum(w.count for w in engine.storage.list_windows((TENANT, STREAM, 0)))
    log_state("counted events (only the 2 parked ones were accepted)", total)
    assert total == 2


def test_spooled_events_are_replayed_without_duplicates(make_engine, monkeypatch):
    """Events durable in inbox but not aggregated are recovered exactly once."""
    log_section("overload path B: durable spool is replayed after restart")
    cfg = StreamConfig(queue_capacity=1, max_inflight=1)
    engine = make_engine(cfg)

    # bypass the engine and put 3 raw rows straight into the durable inbox,
    # simulating requests that were spooled then the process died mid-batch
    for i in range(3):
        ok = engine.storage.enqueue_inbox(Event(
            event_id=f"spool-{i}", tenant=TENANT, stream=STREAM, partition=1,
            event_time_ms=BASE + 100 + i, value=float(i + 1),
        ))
        assert ok
    log_state("inbox pending before recovery",
              {str(k): v for k, v in {("tenant-a", "clicks", 1): 3}.items()})

    report = engine.recover()
    log_state("recovery replay report",
              {f"{k[0]}|{k[1]}|{k[2]}": v for k, v in report.items()})
    assert report[(TENANT, STREAM, 1)] == 3

    windows = engine.storage.list_windows((TENANT, STREAM, 1))
    log_state("window after recovery", [w.as_dict() for w in windows])
    assert sum(w.count for w in windows) == 3

    # recovering twice changes nothing (already acked)
    assert engine.recover() == {}
    assert sum(w.count for w in engine.storage.list_windows((TENANT, STREAM, 1))) == 3

from __future__ import annotations

from app.models import Event, StreamConfig, WindowKey
from tests.util import log_input, log_section, log_state, log_window_decision

TENANT = "tenant-a"
STREAM = "clicks"


def _ev(eid: str, t_ms: int, value: float = 1.0, partition: int = 0) -> Event:
    return Event(
        event_id=eid, tenant=TENANT, stream=STREAM,
        partition=partition, event_time_ms=t_ms, value=value,
    )


def test_on_time_duplicate_out_of_order_amend_and_late(make_engine, clock):
    """Accept -> out-of-order(on-time) -> duplicate -> AMENDED -> DROPPED."""
    log_section("window semantics (W=1000, allowed_lateness=500)")
    cfg = StreamConfig(window_size_ms=1000, allowed_lateness_ms=500, queue_capacity=16)
    engine = make_engine(cfg)
    base = 1_700_000_000_000

    timeline = [
        # (id, event_time, expected status)
        ("e1", base + 100, "ACCEPTED"),   # window w0=[base,base+1000)
        ("e2", base + 200, "ACCEPTED"),
        ("e0", base + 50, "ACCEPTED"),    # out of order, but wm still 0
        ("adv", base + 1000, "ACCEPTED"), # wm -> base+500; w0 end<=wm finalized
        ("e1", base + 100, "DUPLICATE"),  # resubmit, never double counted
        # wm base+500, w0 closed. Event at base+550 opens w1; its wm update
        # keeps w0 finalized. Then an event at base+450 < wm(base+500):
        ("adv2", base + 1500, "ACCEPTED"),# wm -> base+1000
        ("e-late", base + 900, "DROPPED"),  # < wm=base+1000 and w0 finalized
    ]
    results = {}
    for eid, t, expected in timeline:
        r = engine.submit(_ev(eid, t, float(t % 10)))
        log_input(f"submit {eid}", {"event_time_ms": t})
        log_window_decision(r.as_dict())
        results[f"{eid}@{t}"] = r.status.value
        assert r.status.value == expected, f"{eid} t={t}: {r.status.value} != {expected}"

    windows = engine.storage.list_windows((TENANT, STREAM, 0))
    log_state("windows after ingest", [w.as_dict() for w in windows])

    w0 = next(w for w in windows if w.start_ms == base)
    assert w0.count == 3            # e1, e2, e0; duplicate & late excluded
    assert w0.finalized is True
    assert w0.amended is False      # every event arrived while on time
    w1 = next(w for w in windows if w.start_ms == base + 1000)
    assert w1.count == 2

    # future event far beyond tolerance is rejected, never counted
    r_future = engine.submit(_ev("e-future", clock.now_ms() + 10 * 60_000, 1.0))
    log_window_decision(r_future.as_dict())
    assert r_future.status.value == "REJECTED_FUTURE"
    # rejected future event is recorded for idempotency but counted nowhere
    assert engine.storage.seen_event_ids(["e-future"]) == {"e-future"}
    assert r_future.window_start_ms is None
    assert sum(w.count for w in engine.storage.list_windows((TENANT, STREAM, 0))) == 5


def test_amend_within_allowed_lateness(make_engine):
    """A late-but-within-lateness event AMENDS the open window aggregate."""
    log_section("AMENDED path: late event while window still open")
    # lateness 2000 means window w0 stays open even when wm enters it
    cfg = StreamConfig(window_size_ms=1000, allowed_lateness_ms=2000, queue_capacity=16)
    engine = make_engine(cfg)
    base = 1_700_000_000_000

    r1 = engine.submit(_ev("a1", base + 100, 10.0))
    # wm -> base+500 (t=base+2500 - 2000); w0=[base,base+1000) still OPEN
    r_far = engine.submit(_ev("a2", base + 2500, 1.0))
    # t=base+300 < wm=base+500 -> LATE, but w0 open -> AMENDED
    r_late = engine.submit(_ev("a3", base + 300, 7.0))

    for r in (r1, r_far, r_late):
        log_input("submit", r.event_id)
        log_window_decision(r.as_dict())

    assert r1.status.value == "ACCEPTED"
    assert r_far.status.value == "ACCEPTED"
    assert r_late.status.value == "AMENDED"

    w0 = engine.storage.get_window(WindowKey(TENANT, STREAM, 0, base))
    log_state("amended window", w0.as_dict())
    assert w0.count == 2
    assert w0.total == 17.0
    assert w0.amended is True
    assert w0.finalized is False

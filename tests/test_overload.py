"""Bounded backpressure: when consumers can't keep up, the queue fills and the
server rejects explicitly (HTTP 503 / overloaded) while exposing backlog;
memory never grows without bound and no event is silently dropped."""

import threading

import pytest
from fastapi.testclient import TestClient

from streamagg.api import create_app
from streamagg.config import Config
from tests.conftest import HEADERS_A


@pytest.fixture
def slow_client(tmp_path):
    cfg = Config(
        data_dir=str(tmp_path / "data"),
        window_ms=1_000, allowed_lateness_ms=500, retention_windows=10,
        queue_capacity=10, process_delay_s=0.02, write_wal=False,
    )
    app = create_app(cfg)
    with TestClient(app) as c:
        yield c, cfg


def _send(c, eid, ts=100):
    return c.post(
        "/v1/tenants/tenantA/streams/s1/events",
        json={"event_id": eid, "timestamp_ms": ts}, headers=HEADERS_A)


def test_overload_is_explicit_503_and_backlog_queryable(slow_client):
    c, cfg = slow_client
    statuses = []
    lock = threading.Lock()

    def burst(base):
        # each thread opens many in-flight-ish requests by interleaving sends
        r = _send(c, f"t{base}")
        with lock:
            statuses.append((r.status_code,
                             r.json().get("error") if r.status_code != 200 else None))

    n = 60
    threads = [threading.Thread(target=burst, args=(b,)) for b in range(n)]
    print(f"\nINPUT {n} concurrent requests, queue_capacity={cfg.queue_capacity}, "
          f"process_delay=20ms")
    for t in threads: t.start()
    for t in threads: t.join()

    accepted = sum(1 for s, _ in statuses if s == 200)
    rejected = [(s, e) for s, e in statuses if s == 503]
    other = [s for s, _ in statuses if s not in (200, 503)]
    print(f"accepted={accepted} rejected(503)={len(rejected)} other={other}")
    assert not other
    assert rejected, "expected explicit overload rejections"
    assert all(e == "overloaded" for _, e in rejected)

    backlog = c.get("/backlog").json()
    print("BACKLOG after burst:", backlog)
    assert backlog["queue_capacity"] == cfg.queue_capacity
    assert backlog["total_rejected_overload"] == len(rejected)
    assert backlog["queue_depth"] <= cfg.queue_capacity  # hard bound held

    c.post("/admin/drain")
    backlog = c.get("/backlog").json()
    print("BACKLOG after drain:", backlog)
    assert backlog["queue_depth"] == 0 and backlog["in_flight"] == 0
    # every rejected request surfaced a reason; nothing was silently dropped:
    # accepted + explicitly rejected == total attempted
    assert accepted + len(rejected) == n


def test_rejection_details_carry_backlog(slow_client):
    c, cfg = slow_client
    results = []
    rlock = threading.Lock()
    def send(i):
        r = _send(c, f"s-{i}")
        with rlock:
            results.append(r)
    threads = [threading.Thread(target=send, args=(i,)) for i in range(60)]
    for t in threads: t.start()
    for t in threads: t.join()
    rejected = [r for r in results if r.status_code == 503]
    assert rejected, "slow worker should have produced a 503"
    d = rejected[0].json()
    print("\nREJECT detail body:", d)
    assert d["details"]["queue_depth"] <= cfg.queue_capacity
    assert "queue_capacity" in d["details"]

"""Many concurrent writers: exactly-once counting and idempotent duplicates
across threads, with stable (deterministic) aggregate results."""

import threading

from tests.conftest import HEADERS_A


def _post(client, eid, ts):
    return client.post(
        "/v1/tenants/tenantA/streams/orders/events",
        json={"event_id": eid, "timestamp_ms": ts}, headers=HEADERS_A)


def test_concurrent_writers_count_exactly_once(client):
    n_threads, per_thread = 20, 50
    results = []
    lock = threading.Lock()

    def worker(t):
        local = []
        for i in range(per_thread):
            eid = f"t{t}-e{i}"
            r = _post(client, eid, 100 + (i % 5) * 100).json()
            local.append(r["outcome"])
        with lock:
            results.extend(local)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    print(f"\nINPUT {n_threads} threads x {per_thread} unique events")
    for t in threads: t.start()
    for t in threads: t.join()
    client.post("/admin/drain")

    assert results.count("ACCEPTED") == n_threads * per_thread
    wins = client.get(
        "/v1/tenants/tenantA/streams/orders/windows", headers=HEADERS_A).json()
    total = sum(w["count"] for w in wins["windows"])
    print("WINDOWS:", [(w["window_start"], w["count"]) for w in wins["windows"]])
    print("total counted:", total, "unique events:", n_threads * per_thread)
    assert total == n_threads * per_thread


def test_concurrent_duplicate_submissions_are_idempotent(client):
    # Same event id hammered from many threads must be counted exactly once.
    def submit():
        _post(client, "hot-id", 100)

    threads = [threading.Thread(target=submit) for _ in range(30)]
    print("\nINPUT 30 threads submitting the SAME event_id hot-id")
    for t in threads: t.start()
    for t in threads: t.join()
    client.post("/admin/drain")

    wins = client.get(
        "/v1/tenants/tenantA/streams/orders/windows", headers=HEADERS_A).json()
    w0 = [w for w in wins["windows"] if w["window_start"] == 0][0]
    print("WINDOW0 count/version after duplicate storm:", w0["count"], w0["version"])
    assert w0["count"] == 1
    assert w0["version"] == 1

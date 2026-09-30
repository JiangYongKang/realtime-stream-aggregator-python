from __future__ import annotations

from tests.util import log_input, log_section, log_state

BASE = 1_700_000_000_000
TENANT, STREAM, HDR = "tenant-a", "clicks", {"X-Principal": "a-key"}


def test_full_http_flow_validation_and_backlog(make_client):
    log_section("HTTP end-to-end: ingest -> backlog -> windows -> offsets")
    client = make_client()

    # validation errors: bad partition / missing tenant / negative time
    bad = [
        {"event_id": "v1", "tenant": TENANT, "stream": STREAM,
         "partition": -1, "event_time_ms": BASE, "value": 1.0},
        {"event_id": "v2", "stream": STREAM,
         "partition": 0, "event_time_ms": BASE, "value": 1.0},
        {"event_id": "v3", "tenant": TENANT, "stream": STREAM,
         "partition": 0, "event_time_ms": -1, "value": 1.0},
    ]
    for b in bad:
        r = client.post("/v1/events", json=b, headers=HDR)
        log_input("invalid payload rejected", r.status_code)
        assert r.status_code == 422

    # happy path ingests across partitions
    for p in range(2):
        for i in range(3):
            r = client.post("/v1/events", json={
                "event_id": f"e-{p}-{i}", "tenant": TENANT, "stream": STREAM,
                "partition": p, "event_time_ms": BASE + i * 100,
                "value": float(i + 1),
            }, headers=HDR)
            assert r.status_code == 202
            body = r.json()
            assert body["status"] == "ACCEPTED"

    # duplicate via HTTP
    r = client.post("/v1/events", json={
        "event_id": "e-0-0", "tenant": TENANT, "stream": STREAM,
        "partition": 0, "event_time_ms": BASE, "value": 1.0,
    }, headers=HDR)
    log_input("duplicate HTTP submit", r.json())
    assert r.json()["status"] == "DUPLICATE"

    # backlog endpoint shows two partitions and bounded capacity fields
    bl = client.get(f"/v1/backlog/{TENANT}/{STREAM}", headers=HDR).json()
    log_state("backlog", bl)
    assert {p["partition"] for p in bl["partitions"]} == {0, 1}
    for p in bl["partitions"]:
        assert p["queue_capacity"] == 256
        assert 0.0 <= p["backpressure_ratio"] <= 1.0
        assert p["queued"] == 0

    # windows: sum/min/max deterministically aggregated
    wins = client.get(f"/v1/windows/{TENANT}/{STREAM}/0", headers=HDR).json()
    log_state("windows partition 0", wins)
    w0 = wins["windows"][0]
    assert w0["count"] == 3
    assert w0["sum"] == 6.0
    assert w0["min"] == 1.0 and w0["max"] == 3.0

    # finalized_only filter returns nothing yet (watermark still early)
    fo = client.get(
        f"/v1/windows/{TENANT}/{STREAM}/0?finalized_only=true", headers=HDR
    ).json()
    assert fo["windows"] == []

    # health
    assert client.get("/healthz").json() == {"status": "ok"}

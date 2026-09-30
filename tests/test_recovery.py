from __future__ import annotations

from tests.util import log_input, log_section, log_state

TENANT, STREAM = "tenant-a", "clicks"
BASE = 1_700_000_000_000


def test_offset_commit_idempotent_and_rejects_rollback(make_client):
    log_section("offset commit: idempotent, regression and gap rejection")
    client = make_client()
    url = "/v1/offset/commit"
    hdr = {"X-Principal": "a-key"}
    body = {"tenant": TENANT, "stream": STREAM, "partition": 0, "group": "g1"}

    r = client.post(url, json={**body, "offset": 5}, headers=hdr)
    log_input("commit offset 5", r.json())
    assert r.status_code == 200 and r.json()["status"] == "OK"

    # duplicate commit of the same offset is idempotent, not an error
    r = client.post(url, json={**body, "offset": 5}, headers=hdr)
    log_input("commit offset 5 again", r.json())
    assert r.status_code == 200 and r.json()["status"] == "DUPLICATE"

    # rollback is refused with explicit reason + stored/requested offsets
    r = client.post(url, json={**body, "offset": 3}, headers=hdr)
    log_input("rollback to 3", r.json())
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["code"] == "REJECTED_REGRESSION"
    assert d["stored_offset"] == 5 and d["requested_offset"] == 3

    # jumping beyond known events is refused when max_offset is supplied
    r = client.post(url, json={**body, "offset": 99, "max_offset": 10}, headers=hdr)
    log_input("gap commit 99 (max 10)", r.json())
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "REJECTED_OFFSET_GAP"

    # monotonic advance works
    r = client.post(url, json={**body, "offset": 6}, headers=hdr)
    assert r.status_code == 200 and r.json()["status"] == "OK"

    # stored offset is queryable
    r = client.get(f"/v1/offset/{TENANT}/{STREAM}/0?group=g1", headers=hdr)
    log_state("stored offset", r.json())
    assert r.json()["offset"] == 6

    # cross-tenant offset access is forbidden
    r = client.post(url, json={**body, "tenant": "tenant-b", "offset": 1},
                    headers={"X-Principal": "a-key"})
    assert r.status_code == 403


def test_restart_preserves_windows_watermark_and_offsets(make_client, tmp_db):
    """Simulate process restart: a brand new app over the same db file."""
    log_section("restart recovery of windows/watermark/offsets")
    client1 = make_client()
    hdr = {"X-Principal": "a-key"}

    # ingest across two windows and push watermark to finalize the first.
    # w0=[B,B+1000) closes once watermark>=B+1000 => max t reaches B+1500.
    for eid, t in [
        ("r-1", BASE + 100), ("r-2", BASE + 200),
        ("r-3", BASE + 1000), ("r-4", BASE + 1100),
        ("r-5", BASE + 1500),
    ]:
        r = client1.post("/v1/events", json={
            "event_id": eid, "tenant": TENANT, "stream": STREAM,
            "partition": 0, "event_time_ms": t, "value": 2.0,
        }, headers=hdr)
        assert r.status_code == 202

    before = client1.get(f"/v1/windows/{TENANT}/{STREAM}/0", headers=hdr).json()
    client1.post("/v1/offset/commit", json={
        "tenant": TENANT, "stream": STREAM, "partition": 0,
        "group": "g1", "offset": 5,
    }, headers=hdr)
    log_state("BEFORE restart", {
        "watermark_ms": before["watermark_ms"],
        "windows": before["windows"],
        "offset": 5,
    })
    assert before["windows"][0]["finalized"] is True
    assert before["windows"][0]["count"] == 2

    # ---- restart: new TestClient/app/engine on the same SQLite file ----
    client2 = make_client()
    after = client2.get(f"/v1/windows/{TENANT}/{STREAM}/0", headers=hdr).json()
    off = client2.get(f"/v1/offset/{TENANT}/{STREAM}/0?group=g1", headers=hdr).json()
    log_state("AFTER restart", {
        "watermark_ms": after["watermark_ms"],
        "windows": after["windows"],
        "offset": off["offset"],
    })

    assert after["watermark_ms"] == before["watermark_ms"]
    assert [w["count"] for w in after["windows"]] == [2, 3]
    assert after["windows"][0]["finalized"] is True
    assert off["offset"] == 5

    # resubmitting after restart must not double-count
    r = client2.post("/v1/events", json={
        "event_id": "r-1", "tenant": TENANT, "stream": STREAM,
        "partition": 0, "event_time_ms": BASE + 100, "value": 2.0,
    }, headers=hdr)
    log_input("resubmit r-1 after restart", r.json())
    assert r.json()["status"] == "DUPLICATE"
    again = client2.get(f"/v1/windows/{TENANT}/{STREAM}/0", headers=hdr).json()
    assert sum(w["count"] for w in again["windows"]) == 5

    # offset rollback after restart is still refused
    r = client2.post("/v1/offset/commit", json={
        "tenant": TENANT, "stream": STREAM, "partition": 0,
        "group": "g1", "offset": 1,
    }, headers=hdr)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "REJECTED_REGRESSION"


def test_unprocessed_spool_replayed_on_startup(make_client):
    """Rows durable in the inbox but never aggregated are replayed at startup."""
    log_section("startup replay of durable spool")
    # first instance: inject spool rows directly, then stop without processing
    client = make_client(recover=False)
    storage = client.app.state.storage
    from app.models import Event
    for i in range(3):
        storage.enqueue_inbox(Event(
            event_id=f"cold-{i}", tenant=TENANT, stream=STREAM, partition=2,
            event_time_ms=BASE + 100 + i, value=1.0,
        ))
    pending_before = storage.inbox_pending((TENANT, STREAM, 2))
    log_state("BEFORE startup recovery (pending inbox)", len(pending_before))
    assert len(pending_before) == 3

    # new instance with startup recovery enabled over same db
    client2 = make_client(recover=True)
    wins = client2.get(f"/v1/windows/{TENANT}/{STREAM}/2",
                       headers={"X-Principal": "a-key"}).json()
    log_state("AFTER startup recovery", wins["windows"])
    assert sum(w["count"] for w in wins["windows"]) == 3

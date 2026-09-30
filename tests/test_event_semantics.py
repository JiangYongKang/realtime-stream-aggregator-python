"""Outcome classification: ACCEPTED / LATE_ACCEPTED / DUPLICATE /
DROPPED / REJECTED must be deterministic and distinguishable."""

from tests.conftest import HEADERS_A

URL = "/v1/tenants/tenantA/streams/s1/events"


def post(client, event_id, ts, headers=HEADERS_A, stream="s1"):
    return client.post(
        f"/v1/tenants/tenantA/streams/{stream}/events",
        json={"event_id": event_id, "timestamp_ms": ts}, headers=headers)


def test_out_of_order_within_open_window_is_accepted(client):
    print("\nINPUT e1@100, e3@2100, e2@1100 (out of order)")
    assert post(client, "e1", 100).json()["outcome"] == "ACCEPTED"
    assert post(client, "e3", 2100).json()["outcome"] == "ACCEPTED"
    r = post(client, "e2", 1100)  # window [1k,2k) finalized by e3, still in L
    body = r.json()
    print("WINDOW DECISION e2 ->", body["outcome"], body["reason"],
          "window", body["window_start"], "count", body["window_event_count"])
    # wm=2100: [1k,2k) finalized (end 2000 <= 2100) and 2000+500 >= 2100
    assert body["outcome"] == "LATE_ACCEPTED"
    assert body["reason"] == "late_within_allowed_lateness"
    assert body["window_event_count"] == 1

    wins = client.get(
        "/v1/tenants/tenantA/streams/s1/windows", headers=HEADERS_A).json()
    counts = {w["window_start"]: (w["count"], w["version"]) for w in wins["windows"]}
    print("WINDOWS:", counts)
    assert counts[0] == (1, 1)
    # window [1k,2k) had no data before; the late event creates+supplements
    # it once -> version 1 (version counts window writes, including first fill)
    assert counts[1000] == (1, 1)
    assert counts[2000] == (1, 1)


def test_late_supplement_bumps_existing_window_version(client):
    post(client, "a", 100)
    post(client, "b", 1200)   # finalizes window0
    r = post(client, "a2", 500)   # supplement existing finalized window
    assert r.json()["outcome"] == "LATE_ACCEPTED"
    wins = client.get(
        "/v1/tenants/tenantA/streams/s1/windows", headers=HEADERS_A).json()
    w0 = [w for w in wins["windows"] if w["window_start"] == 0][0]
    print("\nWINDOW0 after two in-window + one late:", w0["count"], "v"+str(w0["version"]))
    assert (w0["count"], w0["version"]) == (2, 2)


def test_duplicate_events_are_idempotent(client):
    print("\nINPUT same event id submitted 3 times (1st original, then replays)")
    first = post(client, "dup-1", 100)
    second = post(client, "dup-1", 100)
    third = post(client, "dup-1", 999)  # different ts but same id
    print("1st/2nd/3rd:", first.json()["outcome"], second.json()["outcome"],
          third.json()["outcome"])
    assert first.json()["outcome"] == "ACCEPTED"
    assert second.json()["outcome"] == "DUPLICATE"
    assert third.json()["outcome"] == "DUPLICATE"
    wins = client.get(
        "/v1/tenants/tenantA/streams/s1/windows?include_open=true",
        headers=HEADERS_A).json()
    only = [w for w in wins["windows"] if w["window_start"] == 0][0]
    print("WINDOW count after dup replays:", only["count"], only["version"])
    assert only["count"] == 1  # counted exactly once


def test_late_supplement_then_drop_boundary(client):
    print("\nINPUT baseline@100; jump to 1400; probe 800 (late) and 10 (drop)")
    post(client, "base", 100)
    post(client, "jump", 1400)   # wm 1400: window0 finalized, end+L=1500
    late = post(client, "late", 800)   # 1500 >= 1400 -> supplement
    print("WINDOW DECISION late@800 ->", late.json()["outcome"])
    assert late.json()["outcome"] == "LATE_ACCEPTED"

    post(client, "jump2", 2600)   # wm 2600: window0 end+L=1500 < 2600
    dropped = post(client, "very-late", 10)
    print("WINDOW DECISION very-late@10 ->", dropped.json()["outcome"],
          dropped.json()["reason"])
    assert dropped.json()["outcome"] == "DROPPED"
    assert dropped.json()["reason"] == "beyond_allowed_lateness"


def test_retired_window_is_rejected_not_dropped(client, cfg):
    # retention_windows=10 + W=1000,L=500: push watermark far beyond retention
    print("\nINPUT old@100 then far@60000, replay old window")
    post(client, "old", 100)
    post(client, "far", 60_000)
    r = post(client, "old2", 50)
    print("WINDOW DECISION @50 ->", r.json()["outcome"], r.json()["reason"])
    assert r.json()["outcome"] == "REJECTED"
    assert r.json()["reason"] == "window_retired"

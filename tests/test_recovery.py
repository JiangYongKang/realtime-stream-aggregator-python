"""Restart recovery from local WAL + snapshot: aggregates, dedup state and
consumer offsets must survive; replays and offset rollback after restart stay
safe. Pre/post recovery state is printed for inspection."""

from tests.conftest import HEADERS_A


def _send(c, eid, ts, stream="s1"):
    return c.post(f"/v1/tenants/tenantA/streams/{stream}/events",
                  json={"event_id": eid, "timestamp_ms": ts}, headers=HEADERS_A)


def _windows(c, stream="s1"):
    j = c.get(f"/v1/tenants/tenantA/streams/{stream}/windows",
              headers=HEADERS_A).json()
    return {w["window_start"]: (w["count"], w["version"]) for w in j["windows"]}


def test_restart_preserves_aggregates_dedup_and_offsets(make_client, cfg):
    c1 = make_client()
    print("\n--- PRE-RECOVERY INPUT ---")
    for eid, ts in [("e1", 100), ("e2", 300), ("e3", 1200), ("e4", 1300),
                    ("late1", 600)]:
        r = _send(c1, eid, ts)
        print("send", eid, ts, "->", r.json()["outcome"], r.json()["reason"])
    _send(c1, "e1", 100)  # duplicate before restart
    c1.post("/admin/drain")
    c1.post("/admin/snapshot")  # force durable checkpoint + WAL truncation

    pre = _windows(c1)
    # consume finalized windows and commit offset=1
    batch = c1.get(
        "/v1/tenants/tenantA/streams/s1/consume?consumer=c1",
        headers=HEADERS_A).json()
    pre_ordinals = [w["ordinal"] for w in batch["windows"]]
    c1.post("/v1/tenants/tenantA/streams/s1/consumers/c1/offset",
            json={"offset": 1}, headers=HEADERS_A)
    print("PRE-RECOVERY windows:", pre)
    print("PRE-RECOVERY finalized ordinals available:", pre_ordinals)

    # ---- restart over the same data directory ----
    c2 = make_client()
    post = _windows(c2)
    print("POST-RECOVERY windows:", post)
    assert post == pre, f"aggregate drift after restart: {pre} != {post}"

    # dedup survived: the duplicate is still not recounted
    r = _send(c2, "e1", 100)
    print("replay e1 after restart ->", r.json()["outcome"])
    assert r.json()["outcome"] == "DUPLICATE"
    c2.post("/admin/drain")
    assert _windows(c2) == pre

    # a new late event still classifies correctly using recovered watermark
    r = _send(c2, "late-after-restart", 1250)  # window[1k,2k) finalized, within L?
    print("new late event after restart ->", r.json()["outcome"], r.json()["reason"])
    c2.post("/admin/drain")

    # offset survived: consume resumes strictly after committed offset 1
    batch = c2.get(
        "/v1/tenants/tenantA/streams/s1/consume?consumer=c1",
        headers=HEADERS_A).json()
    print("POST-RECOVERY committed_offset:", batch["committed_offset"],
          "new ordinals:", [w["ordinal"] for w in batch["windows"]])
    assert batch["committed_offset"] == 1
    assert all(w["ordinal"] > 1 for w in batch["windows"])

    # rollback attempt after restart is rejected with a clear reason
    r = c2.post("/v1/tenants/tenantA/streams/s1/consumers/c1/offset",
                json={"offset": 0}, headers=HEADERS_A)
    print("offset rollback after restart ->", r.status_code,
          r.json()["details"]["reason"])
    assert r.status_code == 409
    assert r.json()["details"]["reason"] == "offset_rolled_back"
    print("POST-RECOVERY final windows:", _windows(c2))


def test_restart_without_snapshot_replays_full_wal(make_client):
    c1 = make_client()
    for eid, ts in [("a", 10), ("b", 20), ("c", 1500)]:
        _send(c1, eid, ts)
    c1.post("/admin/drain")
    pre = _windows(c1)
    print("\nPRE-RECOVERY (WAL only, no explicit snapshot):", pre)
    # note: stop() takes a final snapshot automatically on context exit
    c2 = make_client()
    post = _windows(c2)
    print("POST-RECOVERY:", post)
    assert post == pre

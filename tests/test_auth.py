from __future__ import annotations

from tests.util import log_input, log_section, log_state

BASE = 1_700_000_000_000


def _event_payload(tenant: str, stream: str = "clicks", eid: str = "x-1") -> dict:
    return {
        "event_id": eid,
        "tenant": tenant,
        "stream": stream,
        "partition": 0,
        "event_time_ms": BASE,
        "value": 1.0,
    }


def test_tenant_isolation_write_and_read(make_client):
    """Tenant A key cannot write/read tenant B, and data never crosses tenants."""
    log_section("tenant isolation")
    client = make_client()

    # no principal -> rejected
    r = client.post("/v1/events", json=_event_payload("tenant-a", eid="noauth"))
    log_input("POST without principal", r.status_code)
    assert r.status_code == 403
    assert r.json()["detail"]["code"] == "MISSING_PRINCIPAL"

    # unknown principal
    r = client.post("/v1/events", json=_event_payload("tenant-a", eid="unk"),
                    headers={"X-Principal": "nobody"})
    assert r.status_code == 403
    assert r.json()["detail"]["code"] == "UNKNOWN_PRINCIPAL"

    # A writes to its own tenant: ok
    r = client.post("/v1/events", json=_event_payload("tenant-a", eid="a-1"),
                    headers={"X-Principal": "a-key"})
    log_input("A writes tenant-a", r.status_code)
    assert r.status_code == 202

    # A tries to write tenant B: forbidden
    r = client.post("/v1/events", json=_event_payload("tenant-b", eid="b-cross"),
                    headers={"X-Principal": "a-key"})
    log_input("A attempts tenant-b write", r.json())
    assert r.status_code == 403
    assert r.json()["detail"]["code"] == "TENANT_FORBIDDEN"

    # B writes its own data
    r = client.post("/v1/events", json=_event_payload("tenant-b", eid="b-1"),
                    headers={"X-Principal": "b-key"})
    assert r.status_code == 202

    # read isolation
    ra = client.get("/v1/windows/tenant-a/clicks/0", headers={"X-Principal": "a-key"})
    rb = client.get("/v1/windows/tenant-b/clicks/0", headers={"X-Principal": "b-key"})
    rcross = client.get("/v1/windows/tenant-b/clicks/0", headers={"X-Principal": "a-key"})
    log_state("tenant-a view", ra.json()["windows"])
    log_state("tenant-b view", rb.json()["windows"])
    assert ra.status_code == 200 and rb.status_code == 200
    assert rcross.status_code == 403
    a_events = {e for w in ra.json()["windows"] for e in [w["count"]]}
    assert sum(a_events) == 1
    assert sum(w["count"] for w in rb.json()["windows"]) == 1

    # backlog is tenant scoped too
    rbl = client.get("/v1/backlog/tenant-b/clicks", headers={"X-Principal": "a-key"})
    assert rbl.status_code == 403


def test_admin_endpoint_requires_admin(make_client):
    log_section("admin endpoint authorization")
    client = make_client()
    r = client.post("/admin/recover", headers={"X-Principal": "a-key"})
    assert r.status_code == 403
    assert r.json()["detail"]["code"] == "ADMIN_FORBIDDEN"
    r = client.post("/admin/recover", headers={"X-Principal": "admin"})
    assert r.status_code == 200

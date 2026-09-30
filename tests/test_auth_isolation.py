"""Tenant isolation: 401 without valid token, 403 across tenants, and a tenant
must never see another tenant's stream data or offsets."""

from tests.conftest import HEADERS_A, HEADERS_B


def post(client, tenant, eid, ts, headers, stream="s1"):
    return client.post(f"/v1/tenants/{tenant}/streams/{stream}/events",
                       json={"event_id": eid, "timestamp_ms": ts}, headers=headers)


def test_missing_and_bad_token_are_401(client):
    r1 = client.post("/v1/tenants/tenantA/streams/s1/events",
                     json={"event_id": "x", "timestamp_ms": 1})
    r2 = post(client, "tenantA", "x", 1, {"Authorization": "Bearer nope"})
    r3 = post(client, "tenantA", "x", 1, {"Authorization": "Basic abc"})
    print("\nno/bad/malformed token status:", r1.status_code, r2.status_code, r3.status_code)
    assert r1.status_code == 401 and r2.status_code == 401 and r3.status_code == 401
    assert all(r.json()["error"] == "unauthorized" for r in (r1, r2, r3))


def test_cross_tenant_is_403_and_isolated(client):
    assert post(client, "tenantA", "a1", 100, HEADERS_A).status_code == 200
    r = post(client, "tenantA", "a2", 100, HEADERS_B)  # B touching A's tenant
    print("\ncross-tenant write:", r.status_code, r.json()["details"])
    assert r.status_code == 403
    assert r.json()["details"]["reason"] == "tenant_mismatch"

    # B writes to its own tenant stream of the same name; counts stay separate
    assert post(client, "tenantB", "b1", 100, HEADERS_B).status_code == 200
    wa = client.get("/v1/tenants/tenantA/streams/s1/windows", headers=HEADERS_A).json()
    wb = client.get("/v1/tenants/tenantB/streams/s1/windows", headers=HEADERS_B).json()
    print("tenantA windows:", [(w["window_start"], w["count"]) for w in wa["windows"]])
    print("tenantB windows:", [(w["window_start"], w["count"]) for w in wb["windows"]])
    assert wa["windows"][0]["count"] == 1
    assert wb["windows"][0]["count"] == 1

    # B cannot read A's stream
    assert client.get("/v1/tenants/tenantA/streams/s1/windows",
                      headers=HEADERS_B).status_code == 403

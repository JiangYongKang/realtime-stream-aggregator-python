from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.auth import AuthError, Authz
from app.config import AppConfig
from app.engine import StreamEngine
from app.models import Event
from app.storage import Storage
from app.clock import Clock

# HTTP status mapping for the distinct ingest outcomes
_INGEST_HTTP_STATUS = {
    "ACCEPTED": 202,
    "DUPLICATE": 200,
    "AMENDED": 200,
    "DROPPED": 200,                 # explicitly classified, not a 5xx
    "REJECTED_FUTURE": 422,
    "REJECTED_OVERLOAD": 503,
    "REJECTED_AUTH": 403,
    "REJECTED_ARG": 400,
}


# --------------------------------------------------------------------------- #
# request / response schemas
# --------------------------------------------------------------------------- #
class EventIn(BaseModel):
    event_id: str | None = Field(default=None, min_length=1, max_length=128)
    tenant: str = Field(min_length=1, max_length=128)
    stream: str = Field(min_length=1, max_length=128)
    partition: int = Field(ge=0, le=4095)
    event_time_ms: int = Field(ge=0)
    value: float


class CommitIn(BaseModel):
    tenant: str = Field(min_length=1, max_length=128)
    stream: str = Field(min_length=1, max_length=128)
    partition: int = Field(ge=0, le=4095)
    group: str = Field(default="default", min_length=1, max_length=128)
    offset: int = Field(ge=0)
    # if provided, offset must not exceed this many aggregated events
    max_offset: int | None = Field(default=None, ge=0)


# --------------------------------------------------------------------------- #
def create_app(config: AppConfig | None = None) -> FastAPI:
    config = config or AppConfig()
    clock = Clock()
    storage = Storage(config.db_path)
    engine = StreamEngine(storage, config, clock, gate=None)
    authz = Authz(config.tenants)

    app = FastAPI(title="Local Realtime Stream Aggregator", version="0.1.0")
    app.state.storage = storage
    app.state.engine = engine
    app.state.clock = clock
    app.state.config = config

    # ------------------------------------------------------------------ #
    def _authorize(principal: str | None, tenant: str) -> str:
        try:
            return authz.check(principal, tenant)
        except AuthError as exc:
            raise HTTPException(status_code=403, detail={"code": exc.code, "reason": exc.detail})

    # ------------------------------------------------------------------ #
    @app.on_event("startup")
    def _startup() -> None:
        if config.recover_on_startup:
            engine.recover()

    @app.on_event("shutdown")
    def _shutdown() -> None:
        engine.shutdown()
        storage.close()

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    # ------------------------------------------------------------------ #
    @app.post("/v1/events")
    def ingest(body: EventIn, x_principal: str | None = Header(default=None)) -> dict[str, Any]:
        _authorize(x_principal, body.tenant)
        event = Event(
            event_id=body.event_id or Event.new_id(),
            tenant=body.tenant,
            stream=body.stream,
            partition=body.partition,
            event_time_ms=body.event_time_ms,
            value=body.value,
        )
        result = engine.submit(event)
        status_code = _INGEST_HTTP_STATUS[result.status.value]
        if status_code >= 400:
            raise HTTPException(
                status_code=status_code,
                detail={"code": result.status.value, "reason": result.detail},
            )
        return JSONResponse(status_code=status_code, content=result.as_dict())

    # ------------------------------------------------------------------ #
    @app.get("/v1/windows/{tenant}/{stream}/{partition}")
    def get_windows(
        tenant: str,
        stream: str,
        partition: int,
        finalized_only: bool = False,
        x_principal: str | None = Header(default=None),
    ) -> dict[str, Any]:
        _authorize(x_principal, tenant)
        pkey = (tenant, stream, partition)
        windows = storage.list_windows(pkey)
        data = [w.as_dict() for w in windows if not finalized_only or w.finalized]
        return {
            "tenant": tenant,
            "stream": stream,
            "partition": partition,
            "watermark_ms": storage.get_watermark(pkey),
            "windows": data,
        }

    @app.get("/v1/backlog/{tenant}/{stream}")
    def get_backlog(
        tenant: str, stream: str, x_principal: str | None = Header(default=None)
    ) -> dict[str, Any]:
        _authorize(x_principal, tenant)
        items = engine.backlog(tenant, stream)
        return {
            "tenant": tenant,
            "stream": stream,
            "partitions": [
                {
                    "partition": i.partition,
                    "queued": i.queued,
                    "in_flight": i.in_flight,
                    "queue_capacity": i.queue_capacity,
                    "backpressure_ratio": i.backpressure_ratio,
                    "rejected_overload": i.rejected_overload,
                    "last_watermark_ms": i.last_watermark_ms,
                }
                for i in items
            ],
        }

    # ------------------------------------------------------------------ #
    @app.get("/v1/offset/{tenant}/{stream}/{partition}")
    def get_offset(
        tenant: str,
        stream: str,
        partition: int,
        group: str = "default",
        x_principal: str | None = Header(default=None),
    ) -> dict[str, Any]:
        _authorize(x_principal, tenant)
        offset = storage.get_offset(tenant, stream, partition, group)
        return {"tenant": tenant, "stream": stream, "partition": partition,
                "group": group, "offset": offset}

    @app.post("/v1/offset/commit")
    def commit_offset(
        body: CommitIn, x_principal: str | None = Header(default=None)
    ) -> dict[str, Any]:
        _authorize(x_principal, body.tenant)

        if body.max_offset is not None and body.offset > body.max_offset:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "REJECTED_OFFSET_GAP",
                    "reason": (
                        f"offset {body.offset} exceeds known aggregated events"
                        f" {body.max_offset}; refusing to skip ahead"
                    ),
                },
            )

        code = storage.commit_offset(
            body.tenant, body.stream, body.partition, body.group, body.offset
        )
        if code == "REJECTED_REGRESSION":
            current = storage.get_offset(body.tenant, body.stream, body.partition, body.group)
            raise HTTPException(
                status_code=409,
                detail={
                    "code": code,
                    "reason": (
                        f"offset rollback refused: stored={current}, requested={body.offset}"
                    ),
                    "stored_offset": current,
                    "requested_offset": body.offset,
                },
            )
        return {
            "status": code,
            "tenant": body.tenant,
            "stream": body.stream,
            "partition": body.partition,
            "group": body.group,
            "offset": body.offset,
        }

    @app.post("/admin/recover")
    def admin_recover(x_principal: str | None = Header(default=None)) -> dict[str, Any]:
        # local-only admin endpoint: principal must be a registered admin
        if x_principal not in config.admin_principals:
            raise HTTPException(
                status_code=403,
                detail={
                    "code": "ADMIN_FORBIDDEN",
                    "reason": "admin principal required for /admin/recover",
                },
            )
        report = engine.recover()
        return {"recovered": {f"{k[0]}|{k[1]}|{k[2]}": v for k, v in report.items()}}

    return app

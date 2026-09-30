"""FastAPI application wiring auth, bounded ingestion, windows and offsets.

Endpoints (all local, bearer token scoped to one tenant):

* ``POST /v1/tenants/{tenant}/streams/{stream}/events`` ingest one event.
* ``GET  /v1/tenants/{tenant}/streams/{stream}/windows`` query window aggs.
* ``GET  /v1/tenants/{tenant}/streams/{stream}/consume`` finalized windows
  after a consumer's committed offset (does not advance the offset).
* ``POST /v1/tenants/{tenant}/streams/{stream}/consumers/{c}/offset`` commit.
* ``GET  /backlog`` queryable backlog / overload counters.
* ``POST /admin/drain`` / ``POST /admin/snapshot`` for deterministic tests.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, Request
from pydantic import BaseModel, Field

from streamagg.aggregator import Aggregator
from streamagg.auth import TenantAuth
from streamagg.config import Config
from streamagg.errors import AppError, NotFoundError
from streamagg.models import Event
from streamagg.offsets import OffsetStore
from streamagg.queue import BoundedQueue
from streamagg.storage import WalStorage
from streamagg.worker import IngestWorker

logger = logging.getLogger("streamagg.api")

DEFAULT_TOKENS = {
    "token-tenant-a": "tenantA",
    "token-tenant-b": "tenantB",
}


class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=200)
    timestamp_ms: int = Field(ge=0)
    partition_key: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)


class OffsetIn(BaseModel):
    offset: int = Field(ge=0)
    expected: int | None = None


class Server:
    """Owned by the app lifespan so tests can construct/destroy it with a dir."""

    def __init__(self, config: Config) -> None:
        self.config = config
        tokens = config.token_tenant if config.token_tenant is not None else DEFAULT_TOKENS
        self.auth = TenantAuth(tokens)
        self.queue = BoundedQueue(config)
        self.aggregator = Aggregator(config)
        self.storage = WalStorage(config)
        self.offsets = OffsetStore()
        self.worker = IngestWorker(config, self.queue, self.aggregator,
                                   self.storage, self.offsets)

    def start(self) -> dict:
        return self.worker.start()

    def stop(self) -> None:
        self.worker.stop()
        self.storage.close()


def create_app(config: Config | None = None) -> FastAPI:
    cfg = config or Config()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        server = Server(cfg)
        info = server.start()
        app.state.server = server
        logger.info("server started data_dir=%s restore=%s", cfg.data_dir, info)
        try:
            yield
        finally:
            server.stop()

    app = FastAPI(title="realtime-stream-aggregator", lifespan=lifespan)

    def server(request: Request) -> Server:
        return request.app.state.server

    def authed_tenant(tenant: str, authorization: str | None,
                      request: Request) -> str:
        srv: Server = request.app.state.server
        token = TenantAuth.parse_bearer(authorization)
        return srv.auth.authorize(token, tenant)

    # --------------------------------------------------------------- errors
    @app.exception_handler(AppError)
    async def app_error_handler(request: Request, exc: AppError):
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": exc.code, "message": exc.message,
                     "details": exc.details},
        )

    # ------------------------------------------------------------------ root
    @app.get("/")
    def root():
        return {"service": "realtime-stream-aggregator", "status": "ok"}

    # ---------------------------------------------------------------- ingest
    @app.post("/v1/tenants/{tenant}/streams/{stream}/events")
    def ingest(tenant: str, stream: str, body: EventIn,
               request: Request, authorization: str | None = Header(default=None)):
        srv = server(request)
        authed_tenant(tenant, authorization, request)
        event = Event(event_id=body.event_id, tenant=tenant, stream=stream,
                      timestamp_ms=body.timestamp_ms,
                      partition_key=body.partition_key, payload=body.payload)
        logger.info("ingest in: tenant=%s stream=%s id=%s ts=%s",
                    tenant, stream, event.event_id, event.timestamp_ms)
        result = srv.worker.submit(event)
        logger.info(
            "window decision: id=%s outcome=%s reason=%s window=[%s,%s) "
            "count=%s watermark=%s",
            event.event_id, result.outcome.value, result.reason,
            result.window_start, result.window_end, result.event_count,
            result.watermark_ms,
        )
        return {
            "event_id": event.event_id,
            "outcome": result.outcome.value,
            "reason": result.reason,
            "window_start": result.window_start,
            "window_end": result.window_end,
            "window_event_count": result.event_count,
            "watermark_ms": result.watermark_ms,
            "backlog": result.backlog,
        }

    # --------------------------------------------------------------- windows
    @app.get("/v1/tenants/{tenant}/streams/{stream}/windows")
    def list_windows(tenant: str, stream: str, request: Request,
                     include_open: bool = True,
                     authorization: str | None = Header(default=None)):
        srv = server(request)
        authed_tenant(tenant, authorization, request)
        key = Aggregator.key(tenant, stream)
        if key not in srv.aggregator._streams:
            raise NotFoundError("stream not found",
                                details={"reason": "unknown_stream"})
        st = srv.aggregator.get_or_create(key)
        rows = st.list_windows(include_open=include_open)
        return {
            "tenant": tenant, "stream": stream,
            "watermark_ms": st.watermark(),
            "windows": [r.__dict__ for r in rows],
        }

    # --------------------------------------------------------------- consume
    @app.get("/v1/tenants/{tenant}/streams/{stream}/consume")
    def consume(tenant: str, stream: str, consumer: str, request: Request,
                max_events: int = 100,
                authorization: str | None = Header(default=None)):
        srv = server(request)
        authed_tenant(tenant, authorization, request)
        key = Aggregator.key(tenant, stream)
        if key not in srv.aggregator._streams:
            raise NotFoundError("stream not found",
                                details={"reason": "unknown_stream"})
        st = srv.aggregator.get_or_create(key)
        off = srv.offsets.get(tenant, stream, consumer)
        batch = st.windows_since(off)[:max_events]
        return {"tenant": tenant, "stream": stream, "consumer": consumer,
                "committed_offset": off, "windows": batch}

    @app.post("/v1/tenants/{tenant}/streams/{stream}/consumers/{consumer}/offset")
    def commit_offset(tenant: str, stream: str, consumer: str, body: OffsetIn,
                      request: Request,
                      authorization: str | None = Header(default=None)):
        srv = server(request)
        authed_tenant(tenant, authorization, request)
        key = Aggregator.key(tenant, stream)
        if key not in srv.aggregator._streams:
            raise NotFoundError("stream not found",
                                details={"reason": "unknown_stream"})
        st = srv.aggregator.get_or_create(key)
        max_ord = max((o for o, _ in st.finalized), default=0)
        new_off = srv.offsets.commit(tenant, stream, consumer, body.offset,
                                     expected=body.expected,
                                     max_ordinal=max_ord)
        seq = srv.worker.snapshot_now()  # make offset durable immediately
        return {"consumer": consumer, "committed_offset": new_off,
                "durable_seq": seq}

    # ---------------------------------------------------------------- backlog
    @app.get("/backlog")
    def backlog(request: Request):
        srv = server(request)
        m = srv.queue.metrics()
        return m.__dict__

    # ----------------------------------------------------------------- admin
    @app.post("/admin/drain")
    def admin_drain(request: Request):
        srv = server(request)
        srv.worker.drain()
        return {"status": "drained", **srv.queue.metrics().__dict__}

    @app.post("/admin/snapshot")
    def admin_snapshot(request: Request):
        srv = server(request)
        seq = srv.worker.snapshot_now()
        return {"status": "snapshot_saved", "seq": seq}

    @app.get("/health")
    def health():
        return {"status": "healthy"}

    return app

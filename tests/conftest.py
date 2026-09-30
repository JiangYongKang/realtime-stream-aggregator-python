from __future__ import annotations

import os
import sys
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.api import create_app  # noqa: E402
from app.clock import Clock  # noqa: E402
from app.config import AppConfig  # noqa: E402
from app.engine import StreamEngine  # noqa: E402
from app.models import StreamConfig  # noqa: E402
from app.storage import Storage  # noqa: E402

T0 = 1_700_000_000_000  # fixed "now" for deterministic future-event checks


@pytest.fixture
def clock() -> Clock:
    c = Clock()
    c.set(T0)
    return c


@pytest.fixture
def tmp_db(tmp_path: Path) -> str:
    return str(tmp_path / "test.db")


@pytest.fixture
def gate() -> threading.Event:
    """Open by default; a test can .clear() it to freeze workers."""
    ev = threading.Event()
    ev.set()
    return ev


@pytest.fixture
def make_engine(tmp_db, clock, gate):
    created: list[StreamEngine] = []

    def _make(cfg: StreamConfig | None = None, *, recover: bool = True) -> StreamEngine:
        config = AppConfig(
            db_path=tmp_db,
            default_stream=cfg or StreamConfig(),
        )
        storage = Storage(config.db_path)
        engine = StreamEngine(storage, config, clock, gate=gate)
        if recover:
            engine.recover()
        created.append(engine)
        return engine

    yield _make

    for engine in created:
        engine.shutdown()
        engine.storage.close()


@pytest.fixture
def make_client(tmp_db, clock):
    clients: list[TestClient] = []

    def _make(cfg: StreamConfig | None = None, *, recover: bool = True) -> TestClient:
        config = AppConfig(
            db_path=tmp_db,
            default_stream=cfg or StreamConfig(),
            tenants={
                "a-key": "tenant-a",
                "b-key": "tenant-b",
                "admin": "_admin",
            },
            admin_principals=frozenset({"admin"}),
            recover_on_startup=recover,
        )
        app = create_app(config)
        app.state.clock.set(T0)
        client = TestClient(app).__enter__()
        clients.append(client)
        return client

    yield _make

    for client in clients:
        client.__exit__(None, None, None)

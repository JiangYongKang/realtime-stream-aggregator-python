"""Shared fixtures + logging so test runs print inputs, window decisions and
pre/post recovery state (as required by the task)."""

from __future__ import annotations

import logging
import sys

import pytest
from fastapi.testclient import TestClient

from streamagg.api import create_app
from streamagg.config import Config

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)


@pytest.fixture
def cfg(tmp_path):
    return Config(
        data_dir=str(tmp_path / "data"),
        window_ms=1_000,
        allowed_lateness_ms=500,
        retention_windows=10,
        queue_capacity=500,
    )


@pytest.fixture
def client(cfg):
    app = create_app(cfg)
    with TestClient(app) as c:
        yield c


@pytest.fixture
def make_client(cfg):
    """Factory that builds a fresh server over the SAME data dir (restart)."""
    created = []

    def _make():
        app = create_app(cfg)
        cm = TestClient(app)
        c = cm.__enter__()
        created.append(cm)
        return c

    yield _make
    for cm in created:
        cm.__exit__(None, None, None)


HEADERS_A = {"Authorization": "Bearer token-tenant-a"}
HEADERS_B = {"Authorization": "Bearer token-tenant-b"}

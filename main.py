from __future__ import annotations

import os

import uvicorn

from app.api import create_app
from app.config import AppConfig
from app.models import StreamConfig

DB_PATH = os.environ.get("APP_DB_PATH", "data/stream.db")
_db_dir = os.path.dirname(DB_PATH)
if _db_dir:
    os.makedirs(_db_dir, exist_ok=True)

config = AppConfig(
    db_path=DB_PATH,
    default_stream=StreamConfig(
        window_size_ms=int(os.environ.get("APP_WINDOW_MS", "1000")),
        allowed_lateness_ms=int(os.environ.get("APP_LATENESS_MS", "500")),
        queue_capacity=int(os.environ.get("APP_QUEUE_CAPACITY", "256")),
        max_inflight=int(os.environ.get("APP_MAX_INFLIGHT", "64")),
    ),
    # demo principals (local only). Add tenants here for multi-tenant runs.
    tenants={
        "tenant-a-key": "tenant-a",
        "tenant-b-key": "tenant-b",
        "admin": "_admin",
    },
    admin_principals=frozenset({"admin"}),
)

app = create_app(config)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("APP_PORT", "8080")))

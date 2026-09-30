from __future__ import annotations

from dataclasses import dataclass, field

from app.models import StreamConfig


@dataclass(slots=True)
class AppConfig:
    db_path: str = ":memory:"
    default_stream: StreamConfig = field(default_factory=StreamConfig)
    # principal -> tenant it may access (one principal belongs to one tenant)
    tenants: dict[str, str] = field(default_factory=dict)
    # principals allowed to call local-only admin endpoints
    admin_principals: frozenset[str] = frozenset({"admin"})
    # replay durable-but-unprocessed inbox rows when the service starts
    recover_on_startup: bool = True

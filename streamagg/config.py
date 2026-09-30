"""Runtime configuration for one server instance."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    data_dir: str = "./data"
    window_ms: int = 10_000
    allowed_lateness_ms: int = 5_000
    retention_windows: int = 6            # 已关闭窗口保留数量，超过则清理并拒绝旧数据
    queue_capacity: int = 1000
    max_inflight: int = 64
    write_wal: bool = True                # 关闭则纯内存（测试方便）
    process_delay_s: float = 0.0         # 测试用：人为放慢消费以制造积压
    token_tenant: dict[str, str] | None = None  # bearer token -> tenant

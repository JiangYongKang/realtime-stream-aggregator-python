# Realtime Stream Aggregator (Python, local-only)

本地可运行的多租户实时事件接入与事件时间窗口聚合服务。支持：

- 按 **租户 / 流 / 分区** 接入，滚动窗口聚合（count/sum/min/max）
- `event_id` 幂等去重；乱序、迟到、未来事件按确定性规则区分为
  **ACCEPTED / DUPLICATE / AMENDED（补算）/ DROPPED（丢弃）/ REJECTED_*（拒绝）**
- **有界背压**：分区级 buffered/in-flight 上限，过载显式返回 503，积压可查询，不静默丢数据
- **权限隔离**：`X-Principal` 静态绑定租户，跨租户一律 403
- **重启恢复**：SQLite 持久化 inbox/窗口/水位/消费位点；启动重放未处理事件；
  位点提交幂等，回退/跳点被明确拒绝

仅依赖 FastAPI 与 Python 标准库（sqlite3/threading），不接入任何外部消息服务或真实凭据。

## 快速开始

```bash
uv sync
uv run pytest -s          # 全量测试，日志含输入 / 窗口判定 / 恢复前后状态
APP_PORT=8080 uv run python main.py
```

演示密钥：`tenant-a-key` → `tenant-a`，`tenant-b-key` → `tenant-b`，`admin` 为管理员。
详细事件语义、窗口与迟到规则、背压策略、权限边界、恢复方式见
[`docs/DESIGN.md`](docs/DESIGN.md)。

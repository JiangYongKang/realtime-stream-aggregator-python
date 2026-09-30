# 本地实时事件流接入与窗口聚合 — 设计与语义说明

一套完全运行在本地的实时接入/聚合服务：多租户、按流分区接入、事件时间滚动窗口聚合、
确定性的乱序/迟到处理、有界背压与过载保护、基于 SQLite 的崩溃恢复与位点管理。
**不依赖任何外部消息服务、外部鉴权或真实凭据**；除 FastAPI（+ 测试用的 pytest/httpx）
外只使用 Python 标准库（`sqlite3`、`threading`、`queue`）。

---

## 1. 数据模型与接入语义

### 1.1 事件

| 字段 | 含义 |
| --- | --- |
| `event_id` | 全局幂等键（客户端提供；缺省由服务端生成 UUID）。相同 id 永远只计一次。 |
| `tenant` / `stream` / `partition` | 租户、流、分区。聚合、水位、队列、位点都按分区隔离。 |
| `event_time_ms` | 事件发生时间（epoch 毫秒），**窗口只由事件时间决定**，与处理时刻无关。 |
| `value` | 参与聚合的数值（count / sum / min / max）。 |

### 1.2 接入结果状态（可区分、可断言）

| 状态 | HTTP | 含义 |
| --- | --- | --- |
| `ACCEPTED` | 202 | 事件落入仍打开的窗口并计数。 |
| `DUPLICATE` | 200 | `event_id` 已存在；幂等返回，**不重复计数、不入内存缓冲**。 |
| `AMENDED` | 200 | 事件时间已落后于当前水位，但目标窗口仍未关闭（迟到但在容忍度内），**补算**进窗口，窗口标记 `amended=true`。 |
| `DROPPED` | 200 | 窗口已被水位关闭且超过迟到容忍度；**明确丢弃、不计数**，响应中给出窗口与水位。 |
| `REJECTED_FUTURE` | 422 | 事件时间比墙上时钟超前超过 `future_tolerance_ms`（默认 60s）；拒绝，且不推进水位。 |
| `REJECTED_OVERLOAD` | 503 | 分区有界缓冲/在制槽位已满；**显式拒绝**，不静默丢数据（见第 3 节）。 |
| `REJECTED_AUTH` | 403 | 主体缺失/未知/跨租户。 |
| 参数校验失败 | 422 | FastAPI/pydantic 校验（分区越界、时间为负等）。 |

> 拒绝（REJECTED_*）与丢弃（DROPPED）的区别：拒绝表示请求不被接受（过载、未来时间、鉴权）；
> 丢弃表示请求语义合法但事件时间太晚、窗口已定稿。补算（AMENDED）则是迟到但仍可纠正结果。

### 1.3 窗口聚合输出

每个 `(tenant, stream, partition, window_start_ms)` 一行物化结果：
`count / sum / min / max / watermark_ms / finalized / amended`。

---

## 2. 窗口、水位、乱序与迟到规则（全部确定性）

配置（`StreamConfig`，可按环境变量覆盖）：

* `window_size_ms`（默认 1000）
* `allowed_lateness_ms`（默认 500）
* `future_tolerance_ms`（默认 60000）

规则：

1. **滚动窗口**，epoch 对齐：`window_start = floor(event_time_ms / W) * W`，
   窗口为左闭右开 `[start, start+W)`。
2. **分区水位**：`WM(p) = max(已见 event_time) − allowed_lateness_ms`，按分区持久化。
3. 窗口 `[s, s+W)` 在 `s+W <= WM` 时**定稿（finalized）**，此前为 open。
4. 事件分类（在分区锁内串行判定，故并发下结果确定）：
   * `event_time > now + future_tolerance_ms` → `REJECTED_FUTURE`；
   * `event_time < WM` 且 `window_end <= WM` → `DROPPED`；
   * `event_time < WM` 但窗口仍 open（乱序/迟到在容忍度内）→ `AMENDED`，
     结果被补算，`amended=true`；
   * 其他（包括“乱序但未迟到”，即水位尚未推进到该事件之前）→ `ACCEPTED`。
5. **去重**：`event_id` 为主键。写入先进持久收件箱（inbox），重复 id 在聚合前即被拦截，
   因此无论是并发重复提交、重启后重放还是位点回退重试，都不会重复计数。

判定示例（W=1000，L=500，基准 B）：

```
t=B+100         ACCEPTED  w0, WM=B-400
t=B+200         ACCEPTED  w0, WM=B-300
t=B+50          ACCEPTED  w0   (乱序但未迟到，WM 仍在事件之前)
t=B+1000        ACCEPTED  w1, WM=B+500   -> w0 end=B+1000 > WM，w0 仍 open
t=B+1500        ACCEPTED  w1, WM=B+1000  -> w0 finalized
t=B+900 (<WM)   DROPPED   w0 已定稿，超过迟到容忍
若 L=2000：t=B+300 在 WM=B+500 时到达，w0 仍 open -> AMENDED 补算
```

---

## 3. 背压与过载保护（有界、可查询、不静默丢失）

每个分区维护两个有界量：

* `buffered`：已持久化到 inbox、但尚未完成窗口处理的事件数；上限 `queue_capacity`（默认 256）。
* `in_flight`：正在占用处理槽位的请求数；上限 `max_inflight`（默认 64）。

处理模型：每分区一把锁，窗口判定/水位推进在提交线程内**同步、串行**完成，
因此不需要无界内存队列即可支持多客户端并发；内存占用被上面两个上限严格约束。

过载时的两条确定路径：

1. **在制槽位饱和**（`in_flight >= max_inflight`，代表消费变慢）：请求在落盘前快速失败，
   返回 `503 REJECTED_OVERLOAD` 并携带 `(buffered/capacity, in_flight/max)`。
   服务端不缓冲该事件 → 客户端可安全重试。
2. **已落盘但未处理**：事件已写入持久 inbox（例如进程在处理中途崩溃、或未来得及处理），
   不会丢失；服务重启或调用 `POST /admin/recover` 时按到达顺序重放，
   由 `event_id` 去重保证恰好一次生效。

`GET /v1/backlog/{tenant}/{stream}` 可查询每个分区的：
`queued, in_flight, queue_capacity, backpressure_ratio, rejected_overload, last_watermark_ms`。

---

## 4. 权限边界

* 所有读写都要求请求头 `X-Principal`。主体到租户是**静态一对一绑定**（`main.py` / `AppConfig`）。
* 一个主体只能访问其所属租户的流；跨租户写、读窗口、查积压、提交位点一律
  `403 TENANT_FORBIDDEN`；主体缺失 `MISSING_PRINCIPAL`，未注册 `UNKNOWN_PRINCIPAL`。
* `POST /admin/recover` 仅允许 `AppConfig.admin_principals` 中的管理员主体
  （默认 `admin`），其他主体得到 `403 ADMIN_FORBIDDEN`。
* 数据按 `(tenant, stream, partition)` 物理隔离存储；没有任何跨租户查询接口。

---

## 5. 位点与重启恢复

### 5.1 持久化

SQLite（文件模式开启 WAL；测试可用 `:memory:`）保存四类状态：

* `events`：原始收件箱 + 全局去重表，`processed` 标记是否已聚合；
* `windows`：物化窗口聚合（含 finalized/amended/水位标注）；
* `watermarks`：每分区事件时间水位；
* `offsets`：每消费组的提交位点。

### 5.2 启动恢复

服务启动时（可由 `recover_on_startup` 关闭）扫描 `events.processed=0` 的分区，
在各分区锁内按 `rowid` 顺序重放聚合。重复执行恢复是安全的：已 ack 的行不会再处理。

### 5.3 位点提交规则

`POST /v1/offset/commit`，按 `(tenant, stream, partition, group)`：

| 情况 | 结果 |
| --- | --- |
| 首次提交 | `200 OK` |
| 相同位点重复提交 | `200 DUPLICATE`（幂等，无副作用） |
| 位点回退（小于已存值） | `409 REJECTED_REGRESSION`，响应带 `stored_offset` 与 `requested_offset` |
| 提供 `max_offset` 且提交值超出 | `409 REJECTED_OFFSET_GAP`，拒绝跳点 |
| 单调前进 | `200 OK` |

位点、窗口、水位都在重启后继续生效；重复提交与回退都不会造成重复结果或数据错乱。

---

## 6. HTTP 接口一览

| 方法/路径 | 说明 |
| --- | --- |
| `GET /healthz` | 健康检查 |
| `POST /v1/events` | 接入单条事件（头 `X-Principal`） |
| `GET /v1/windows/{tenant}/{stream}/{partition}?finalized_only=` | 查询窗口结果 |
| `GET /v1/backlog/{tenant}/{stream}` | 查询分区积压/过载计数 |
| `GET /v1/offset/{tenant}/{stream}/{partition}?group=` | 查询消费位点 |
| `POST /v1/offset/commit` | 幂等、单调位点提交 |
| `POST /admin/recover` | 手动重放未处理 inbox（管理员） |

---

## 7. 本地验证方法

```bash
# 1) 安装依赖（FastAPI + pytest/httpx，全部为常见开源依赖）
uv sync

# 2) 运行全部测试（-s 可查看 输入 / 窗口判定 / 恢复前后状态 日志）
uv run pytest -s

# 3) 启动本地服务（默认数据文件 data/stream.db）
APP_PORT=8080 uv run python main.py

# 4) 示例请求
curl -s -X POST http://127.0.0.1:8080/v1/events \
  -H 'Content-Type: application/json' -H 'X-Principal: tenant-a-key' \
  -d '{"event_id":"e1","tenant":"tenant-a","stream":"clicks",
       "partition":0,"event_time_ms":1700000000100,"value":3.5}'

curl -s http://127.0.0.1:8080/v1/windows/tenant-a/clicks/0 \
  -H 'X-Principal: tenant-a-key'
curl -s http://127.0.0.1:8080/v1/backlog/tenant-a/clicks \
  -H 'X-Principal: tenant-a-key'
```

演示主体（仅本地，见 `main.py`）：`tenant-a-key -> tenant-a`、
`tenant-b-key -> tenant-b`、`admin -> _admin`（管理员）。
窗口大小/迟到/缓冲可用 `APP_WINDOW_MS`、`APP_LATENESS_MS`、
`APP_QUEUE_CAPACITY`、`APP_MAX_INFLIGHT` 环境变量调整。

### 测试覆盖矩阵

| 测试文件 | 覆盖点 |
| --- | --- |
| `tests/test_windows.py` | 准时、乱序、去重、AMENDED 补算、DROPPED 迟到丢弃、未来事件拒绝 |
| `tests/test_concurrency.py` | 8 线程并发写恰好一次、重复风暴、在制饱和显式 503、积压可查询、spool 重放不重复 |
| `tests/test_auth.py` | 无主体/未知主体/跨租户拒绝、租户读写隔离、管理员端点 |
| `tests/test_recovery.py` | 位点幂等/回退/跳点拒绝、重启后窗口/水位/位点保留、inbox 启动重放 |
| `tests/test_api_e2e.py` | HTTP 全链路、参数校验、backlog、聚合 sum/min/max |

测试日志在每个场景打印：输入事件/请求、每次窗口判定（状态/窗口起点/水位/原因）、
恢复前后的窗口与位点快照。

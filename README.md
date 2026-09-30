# 本地实时事件流接入与窗口聚合（realtime-stream-aggregator）

一个**完全本地化、可离线运行**的实时事件接入与滚动窗口聚合服务：多租户、多流、
多分区键并发写入；事件时间窗口；确定性的乱序/迟到处理；有界背压与过载保护；
基于静态 Token 的租户隔离；以及基于本地 WAL + 快照的重启恢复。

* 仅依赖：Python 3.14、FastAPI（本地 HTTP）与标准库；**不接入任何外部消息
  服务、数据库或真实凭据**。
* 持久化只用本地文件：`<data_dir>/wal.jsonl`（追加 WAL，每次写入 fsync）与
  `<data_dir>/snapshot.json`（原子替换的一致性快照）。

## 1. 快速开始

```bash
uv sync                       # 安装依赖
uv run pytest -s              # 全部测试（日志会打印输入、窗口判定、恢复前后状态）
uv run python main.py         # 启动 HTTP 服务，默认 127.0.0.1:8000
# 打开 http://127.0.0.1:8000/docs 可交互调试
```

环境变量：`STREAMAGG_DATA_DIR`（默认 `./data`）、`STREAMAGG_QUEUE_CAPACITY`
（默认 `1000`）、`PORT`（默认 `8000`）。

内置本地静态令牌（仅用于本地验证，见 `streamagg.api.DEFAULT_TOKENS`）：

| Token                | 租户     |
|----------------------|----------|
| `token-tenant-a`     | tenantA  |
| `token-tenant-b`     | tenantB  |

请求示例：

```bash
curl -s -X POST http://127.0.0.1:8000/v1/tenants/tenantA/streams/orders/events \
  -H 'Authorization: Bearer token-tenant-a' -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-1","timestamp_ms":1000,"partition_key":"u1","payload":{"v":1}}'
```

## 2. 事件语义（Event semantics）

事件结构：`event_id`（租户/流内去重键）、`tenant`、`stream`、`timestamp_ms`
（事件时间，毫秒，必须 ≥ 0）、`partition_key`（分区标签，参与同一流的时间线）、
`payload`（任意 JSON，仅随事件保存，不参与聚合）。

每次接入都返回**确定的结果分类（outcome）与原因（reason）**，把“拒绝”“丢弃”
“补算”严格区分开，任何结果都不会静默丢失：

| outcome | 含义 | reason |
|---|---|---|
| `ACCEPTED` | 事件落在仍打开的窗口，正常计入 | `null` |
| `LATE_ACCEPTED` | 窗口已关闭但仍在迟到宽限期内，**补算**进已输出窗口，窗口 `version` +1 | `late_within_allowed_lateness` |
| `DUPLICATE` | `event_id` 重复，幂等忽略，绝不重复计数 | `duplicate_event_id` |
| `DROPPED` | 请求合法、已接入，但事件晚于宽限期，**明确丢弃**（不计入、可观测） | `beyond_allowed_lateness` |
| `REJECTED` | **接入前拒绝**：鉴权失败、参数非法、过载、或窗口已过保留期被退休 | 见各场景 |

接入前拒绝（HTTP 非 2xx）与已接入丢弃（HTTP 200 + `DROPPED`）刻意分开：

* `401 invalid_token`：缺失/无效令牌；
* `403 tenant_mismatch`：令牌所属租户与目标租户不一致；
* `400 invalid_request`：事件字段非法（空 id、负时间戳等）；
* `503 overloaded`：有界队列已满，明确过载拒绝（详见背压一节）；
* `200 REJECTED/window_retired`：目标窗口已超出保留期、状态已退休，无法再计入
  （与仍可查询窗口上的 `DROPPED` 区分）。

## 3. 窗口与乱序/迟到处理规则（确定性）

* **滚动窗口（tumbling）**：固定窗口长度 `window_ms`（默认 10s），窗口
  `[k*W,(k+1)*W)` 按**事件时间**划分。
* **水位线（watermark）**：等于该流“已见最大事件时间戳”，单调不减。没有引入
  真实外部时钟，因此规则完全由输入决定、可复现。
* **窗口关闭（finalize）**：当 `watermark >= window_end` 时窗口被关闭并按关闭
  顺序分配单调的 `ordinal`（消费位点使用）。
* **乱序（out-of-order）**：若事件所属窗口尚未关闭，正常 `ACCEPTED`；若事件是
  某窗口的第一条数据但到达时窗口已被水位越过，则在计入前先关闭该窗口，按
  `LATE_ACCEPTED` 补算。
* **迟到宽限期（allowed lateness）**：窗口关闭后，只要
  `watermark <= window_end + allowed_lateness_ms`（默认 5s），迟到事件以
  `LATE_ACCEPTED` 补算进**同一个已输出窗口**，`count` 增加且 `version` 递增，
  消费者可据此区分“稳定结果”与“补算后版本”。
* **超宽限丢弃**：当 `watermark > window_end + L`，事件 `DROPPED`（
  `beyond_allowed_lateness`），窗口仍保留可查，但该事件绝不计数。
* **保留期与退休**：以时间槽位计算保留边界，保留最近 `retention_windows`
  （默认 6）个已过期窗口槽；更老的窗口及其去重 id 被退休。发往退休窗口的事件
  返回 `REJECTED/window_retired`。注意：去重幂等在保留窗口范围内始终成立；窗口
  退休后其 id 随之老化，对极老事件的重放将被明确拒绝（仍然绝不重复计数）。
* **去重优先级**：`DUPLICATE` 判定先于水位推进与窗口分类，因此重复重放不会因为
  水位推进而变成补算/丢弃。

## 4. 背压与过载保护（有界、可查询、不静默丢失）

* 写入先进入一个**硬上界有界内存队列**（`queue_capacity`，默认 1000）。整个系统
  只有一个工作线程按“入队顺序”串行执行 `WAL fsync → 聚合`，因此并发写入者看到
  的事件顺序确定，聚合结果可复现。
* 队列满时 `put_nowait` **立即抛出 `503 overloaded`**，响应体携带当前
  `queue_depth / queue_capacity / in_flight`，调用方可以明确退避重试；服务**绝不
  阻塞等待**、绝不无界堆积内存，也不会“悄悄丢一条然后返回成功”。
* 积压状态可随时查询：`GET /backlog` 返回
  `queue_depth`（排队中）、`in_flight`（已取出未处理完，真实在途 = 二者之和）、
  `total_enqueued`、`total_rejected_overload` 以及每流积压 `per_stream`。
* 语义总结：处理跟得上 → 全部计入；跟不上 → **明确拒绝（503）**，拒绝数与积压
  可观测，不存在第三种“静默丢失”。
* `POST /admin/drain` 等待已接受的事件全部处理完，便于验证时得到确定结果。

## 5. 权限边界（租户隔离）

* 鉴权基于本地静态 Bearer Token → 租户的映射（`Config.token_tenant`），不依赖
  外部身份服务或真实凭据。
* 每个 Token 只属于一个租户。所有路径都形如
  `/v1/tenants/{tenant}/...`，服务端校验“令牌租户 == 路径租户”，不匹配返回
  `403 tenant_mismatch`；无令牌/坏令牌返回 `401 invalid_token`。
* 隔离对象包括：事件写入、窗口查询、消费拉取、位点提交。租户 A 既不能写也不能
  读租户 B 的同名流；不同租户同名流的计数彼此独立。
* 消费位点键为 `(tenant, stream, consumer)`，同样在租户边界内。

## 6. 消费位点与恢复方式

* **位点语义**：位点是某消费者“已消费到的最后一个已关闭窗口 `ordinal`”。
  `GET .../consume?consumer=c` 返回严格大于当前位点的已关闭窗口（拉取不推进
  位点）；`POST .../consumers/c/offset` 提交位点，提交后立即写快照保证持久。
* 提交校验（全部明确、绝不静默）：
  * 重复提交相同位点 → 幂等，返回当前位点；
  * 位点回退（`< current`）→ `409 offset_rolled_back`；
  * 携带 `expected` 的乐观锁并发提交，期望不符 → `409 offset_conflict`；
  * 提交超过当前最大已关闭窗口 → `409 offset_ahead_of_data`。
* **持久化与恢复**：
  1. 每条事件先以 `fsync` 追加到 `wal.jsonl`，赋予单调 `seq`，然后才计入聚合；
  2. 周期性（以及位点提交、优雅停机时）把 `{seq, 聚合状态, 位点}` 原子写入
     `snapshot.json`（临时文件 + `os.replace`），并截断已覆盖的 WAL；
  3. 重启恢复 = 载入快照 + 重放 `seq` 更大的 WAL 后缀。WAL 末尾因崩溃产生的
     半行（torn line）会被**确定性地忽略**并记录日志。
* 恢复后：窗口计数/版本、水位线、去重集合、退休边界与消费位点均与停机前一致；
  重复事件重放仍是 `DUPLICATE`，位点回退仍被 `409` 拒绝。

## 7. HTTP 接口一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/tenants/{t}/streams/{s}/events` | 接入单条事件，返回 outcome/reason/窗口/水位 |
| GET  | `/v1/tenants/{t}/streams/{s}/windows?include_open=true` | 查询窗口计数与版本 |
| GET  | `/v1/tenants/{t}/streams/{s}/consume?consumer=c&max_events=100` | 拉取位点之后的已关闭窗口 |
| POST | `/v1/tenants/{t}/streams/{s}/consumers/{c}/offset` | 提交消费位点（支持 `expected`） |
| GET  | `/backlog` | 查询积压与过载计数 |
| POST | `/admin/drain` | 等待已接受事件处理完（验证用） |
| POST | `/admin/snapshot` | 立即做一次持久快照 |
| GET  | `/health` | 健康检查 |

## 8. 本地验证方法（测试）

```bash
uv run pytest -s          # -s 打印日志：每条输入、窗口判定、恢复前后状态
uv run pytest tests/test_overload.py -s
```

覆盖场景：

* `tests/test_event_semantics.py`：乱序、迟到补算、版本递增、超宽限丢弃、退休
  拒绝、重复幂等；
* `tests/test_auth_isolation.py`：401/403、跨租户读写隔离、同名流互不干扰；
* `tests/test_concurrency.py`：20 线程 × 50 唯一事件恰好计数一次；30 线程并发
  重复提交同一 `event_id` 只计一次；
* `tests/test_overload.py`：消费变慢时队列触顶、503 明确拒绝、拒绝数与积压可查、
  队列深度永不超过容量；
* `tests/test_recovery.py`：重启后聚合/去重/位点一致、重复重放安全、位点回退仍
  被拒，并打印恢复前后状态；
* `tests/test_storage_recovery.py`：WAL 崩溃半行恢复、快照 + WAL 后缀重放。

代码结构：

```
streamagg/
  models.py      # Event / EventOutcome / WindowAgg / BacklogMetrics
  errors.py      # 带 code 与 http_status 的类型化错误
  config.py      # 窗口、宽限、保留期、队列容量等配置
  storage.py     # WAL(fsync) + 原子快照 + 截断 + 崩溃尾部处理
  aggregator.py  # 滚动窗口/水位线/去重/迟到-丢弃-拒绝判定
  queue.py       # 有界队列 + 过载拒绝 + 积压指标
  auth.py        # 静态 Token 租户隔离
  offsets.py     # 单调位点提交与校验
  worker.py      # 单工作线程：WAL→聚合→快照，重启重放
  api.py         # FastAPI 路由与错误映射
main.py          # 本地启动入口
```

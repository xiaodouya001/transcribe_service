# 并发与容量说明（Kafka / Redis / 服务端）

本文档面向约 **1000** 路 WebSocket 同时在线、每路多轮消息的压力场景，说明代码路径、默认配置下的主要瓶颈，以及调参方向。

---

## 1. 结论摘要

| 组件 | 代码层是否存在硬性限制 | 默认配置下的主要风险 |
|------|------------------------------|----------------|
| **Transcribe 服务（WS + 编排）** | 否；无硬编码连接上限 | 单机单进程：**事件循环 + CPU**；极高峰时单条消息延迟上升，Mock 端可能出现「10s 无 ACK」 |
| **Redis** | 否 | **`REDIS_MAX_CONNECTIONS` 默认 100**：远小于 1000 路会话同时打 Redis 时的瞬时需求，**连接池排队 → 延迟与超时** |
| **Kafka** | 否 | **单 `AIOKafkaProducer` + `send_and_wait` + 2s 超时**：吞吐不足时 **大量发送超时**；分区数需与负载匹配 |
| **Kafka Broker / Redis 容器** | — | `docker compose` 默认单机资源有限，需结合 **CPU / 内存 / 网络 / IO** 评估 |

---

## 2. 握手路径与连接管理

根据 `main.py` 与 `transport/websocket_handler.py` 的实现，`ws.accept()` 之前不依赖 Redis/Kafka 访问，因此不会因业务侧 I/O 阻塞而主动拖慢握手；`limit_concurrency` 当前未启用。

| 项 | 结论 |
|----|------|
| 握手路径 | 握手前由 middleware 执行准入检查；缺少 `conversationId`、服务 `draining`、超过 `WS_MAX_CONNECTIONS` 都会在 `ws.accept()` 前拒连。 |
| Uvicorn | 已暴露 **`HTTP_BACKLOG`**（默认 **4096**），降低 SYN/accept 队列吃满时客户端读到一半 EOF 的概率；仍受 OS 限制。 |
| `ConnectionRegistry` | 当前按 **`conversationId -> list[WebSocket]`** 追踪真实连接数；`remove(conversationId, ws)` 仅移除目标实例，避免旧连接 `finally` 误删其它登记。 |
| `WS_PING_INTERVAL` / `WS_PING_TIMEOUT` | 经 `main.py` 传入 Uvicorn；在 **`ws="websockets"`** 下驱动 **RFC Ping/Pong 保活**。在 **`wsproto`** backend 下，这两项不会驱动主动发 Ping。 |

**结论**：当出现 `EOFError: connection closed while reading HTTP status line` 时，通常不是 Python 应用在握手阶段主动关闭连接，而是连接已在 **TCP/HTTP 层** 被对端、中间件或本机内核终止（例如过载、队列溢出或网络栈问题）。服务端侧可通过增大 backlog、确保连接登记正确，以及降低 Redis / Kafka 压力来改善表现。

---

## 3. 服务端（WebSocket 与编排）

- **接入**：`ConnectionRegistry` 按真实 socket 持有连接，无固定上限；Linux/Windows 上仍受 **`ulimit` / 句柄数**、**反向代理空闲超时**（如 ALB）限制。
- **模型**：单 **Uvicorn** 进程、**单 asyncio 事件循环**，每条连接在 `receive_text` → `orchestrator.handle_message` → Redis/Kafka **await** 链上交替运行，理论上可支撑大量并发 **I/O**，前提是下游不拖死。
- **CPU**：每条消息都涉及 Pydantic 校验与序列化；1000 路高频发包时 **CPU 会抬高延迟**，表现为 Mock 侧 **ACK 缺失或超时**（Mock 客户端对单轮回复的等待上限约为 **10s**）。

---

## 4. Redis（`RedisStateMachine`）

- 实现：`redis.asyncio.Redis.from_url(..., max_connections=settings.redis_max_connections)`（见 `state_machine/redis_state.py`）。
- **默认 `REDIS_MAX_CONNECTIONS=100`**（`config/settings.py`）：同一时刻只有约 **100 条 TCP 到 Redis**；更多协程在池上 **排队**。
- 每条上行消息至少 **1 次 `EVAL`（prepare）**；正常路径还有 **commit** 等。1000 连接同时发消息时，**池子过小会直接拉长尾延迟**，进而拖垮整条处理链。

压测或预发验证 1000 路连接时，可优先关注：

- 将 `REDIS_MAX_CONNECTIONS` 提到 **256～1024**（不要超过 Redis `maxclients` 与实例规格）。
- 确认 Redis 服务器 **`maxclients`**、内存与网络。

---

## 5. Kafka（`KafkaProducer`）

- **分区**：默认 `KAFKA_TOPIC_NUM_PARTITIONS=50`，Key 为 `conversationId`，负载可摊到多分区；**50 分区通常能消化 1000 路不同会话**，但单机 broker 仍有吞吐上限。
- **生产者**：**一个** `AIOKafkaProducer`，每条成功路径 `send_and_wait`，并带 **`KAFKA_SEND_TIMEOUT_SEC`（默认 2s）**。  
  当 broker / 磁盘 / 网络跟不上时，**先在应用侧超时**，日志里会看到 Kafka 发送超时类错误，Mock 侧则多为 **无 ACK / 非预期帧**。
- **幂等**：`enable_idempotence=True` 会限制 in-flight 行为（由 aiokafka/Kafka 协议约束），高负载下更要注意 **broker 与网络**。

容量核查要点：

- 压测时监控 **broker lag、请求耗时**；必要时 **调大 `KAFKA_SEND_TIMEOUT_SEC`**（同时理解「慢失败」掩盖问题）。
- 生产 Topic 常由运维预建：保证 **分区数 ≥ 预期会话并行度**（否则局部热点）。

---

## 6. 与 Mock Client 现象的对应关系

- Mock 压测里 **「错误」** 常见原因：服务端 **迟迟不返回 `TRANSCRIPT_ACK`**（`recv` **10s 超时**）或返回 **非 ACK**。
- 根因往往在 **Redis 池排队、Kafka 超时、或单机 CPU**，而不是「Kafka/Redis 在代码里写死只支持 N 连接」。

---

## 7. 客户端报「did not receive a valid HTTP response」但服务端无异常日志

可能情况：

1. **请求没进到 `ws_endpoint`**：TCP/SYN 队列、内核丢包、本机出站端口耗尽等，应用层**没有任何** `accept` 日志。此时可开 **Uvicorn access log**（`main` 中已默认 `access_log=True`）看是否出现对应请求行。
2. **进了路由但 `await ws.accept()` 失败**：应出现 **`Transport: WebSocket accept 失败`**（WARNING）日志；若没有，检查日志级别是否为 INFO 及以上。
3. **只有 Mock 侧报错**：请更新 Mock 客户端 `ws_driver` 后重试，连接失败时会附带 **`__cause__`** 与部分 **HTTP 状态** 片段，便于区分 RST 与真正的 HTTP 错误页。

---

## 8. 千级连接压测建议

1. `.env`：**`REDIS_MAX_CONNECTIONS=512`**（或更高，按实例调整）。
2. 观察 **`GET /metrics`** 的 `active_connections` 与日志中的 Redis/Kafka 错误。
3. 调大 Kafka / Redis 容器或集群资源；必要时 **水平扩展** Transcribe 多实例 + 上游会话路由。
4. 见 [deployment.md](deployment.md) 负载与扩缩容说明。

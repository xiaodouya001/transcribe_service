# 架构设计

本文档说明 Transcription Ingest 的架构设计、关键模块原理及异常恢复机制。

---

## 1. 角色说明

| 角色 | 代码模块 | 职责 |
|------|----------|------|
| **Connector** | connector/ | 连接 STT Provider，接收 SSE/WebSocket 推送的 JSON |
| **Buffer 写入端** | buffer/RedisBuffer | 将 Connector 收到的 payload 写入 Redis Stream |
| **Buffer 消费端** | buffer/RedisBufferConsumer | 从 Redis Stream 读取 → 去重 → 清洗 → 写入 Kafka |
| **Dedup** | dedup/ | 按 Key 去重，发送失败时撤销记录以支持重试 |
| **Cleaner** | transform/ | 数据清洗，输出 `raw` + `cleaned` |
| **Producer** | producer/ | 写入 Kafka |

> 说明：本服务只**生产** Kafka 消息，不消费 Kafka。下游业务（NLP、质检等）自行消费 Kafka。

---

## 2. 模块调用图

```mermaid
flowchart TB
    subgraph Direct [直连模式]
        Connector1[Connector]
        Dedup1[Dedup]
        Cleaner1[Cleaner]
        Producer1[Producer]
        Connector1 --> Dedup1
        Dedup1 --> Cleaner1
        Cleaner1 --> Producer1
    end
    subgraph Buffer [Buffer 模式]
        Connector2[Connector]
        BufferWrite[Buffer 写入端]
        BufferRead[Buffer 消费端]
        Dedup2[Dedup]
        Cleaner2[Cleaner]
        Producer2[Producer]
        Connector2 --> BufferWrite
        BufferWrite --> Redis[(Redis Stream)]
        Redis --> BufferRead
        BufferRead --> Dedup2
        Dedup2 --> Cleaner2
        Cleaner2 --> Producer2
    end
    STT[STT Provider] --> Connector1
    STT --> Connector2
    Producer1 --> Kafka[(Kafka)]
    Producer2 --> Kafka
```

---

## 3. 数据流

| 模式 | 数据流 |
|------|--------|
| 直连 | STT Provider → Connector → Dedup → Cleaner → Producer → Kafka |
| Buffer | STT Provider → Connector → Buffer 写入端 → Redis Stream → Buffer 消费端 → Dedup → Cleaner → Producer → Kafka |

Buffer 模式下，数据先落 Redis Stream，再由 Buffer 消费端异步读取并写入 Kafka；服务中断或 Kafka 不可用时，消息保留在 Stream，恢复后自动重试。

---

## 4. 时序图

### 4.1 启动阶段

```mermaid
sequenceDiagram
    autonumber
    participant Main as main.py
    participant Redis as Redis
    participant Kafka as Kafka

    Main->>Main: 加载配置（Settings）
    Main->>Main: 初始化 Dedup / Producer / Cleaner（get_* 工厂）
    Main->>Main: 注册 SIGTERM/SIGINT 信号

    Main->>Redis: ping()
    alt Redis 不可用
        Redis-->>Main: 连接失败
        Main->>Main: 退出（日志：启动失败）
    else Redis 正常
        Redis-->>Main: PONG
    end

    Main->>Kafka: ensure_ready()
    alt Kafka 不可用
        Kafka-->>Main: 连接失败
        Main->>Main: 退出（日志：启动失败）
    else Kafka 正常
        Kafka-->>Main: 就绪
    end

    Main->>Main: 进入 connect_fn（内部通过 get_connector 获取 Connector）
```

### 4.2 Buffer 模式（默认）

```mermaid
sequenceDiagram
    autonumber
    participant STT as STT Provider
    participant Conn as Connector
    participant Buf as Buffer 写入端
    participant Stream as Redis Stream
    participant Consumer as Buffer 消费端
    participant Dedup as Dedup
    participant Clean as Cleaner
    participant Prod as Producer
    participant Kafka as Kafka

    Note over Conn, STT: SSE/WebSocket 长连接

    loop 持续推送转录
        STT->>Conn: 推送 JSON payload
        Conn->>Conn: 解析 JSON
        Conn->>Buf: push(payload)
        Buf->>Stream: XADD（写入 Stream）
    end

    loop 消费循环（异步并行）
        Consumer->>Stream: XREADGROUP（读取新消息）
        Stream-->>Consumer: msg_id + payload
        Consumer->>Consumer: JSON 解析，展开 transcripts
        loop 每条 transcript
            Consumer->>Dedup: should_emit(session_id, seq_no)
            alt 首次到达（SETNX 返回 1）
                Dedup-->>Consumer: True
                Consumer->>Clean: clean(payload, event)
                Clean-->>Consumer: raw + cleaned
                Consumer->>Prod: send(session_id, transcript, ...)
                Prod->>Kafka: send_and_wait(topic, key, value)
                alt 发送成功
                    Kafka-->>Prod: ACK
                    Prod-->>Consumer: 完成
                    Consumer->>Stream: XACK + XDEL
                else 发送超时/失败
                    Kafka-->>Prod: 超时
                    Consumer->>Dedup: remove(session_id, seq_no)
                    Note over Consumer: 不 XACK，消息保留在 Stream 待重试
                end
            else 重复数据（SETNX 返回 0）
                Dedup-->>Consumer: False
                Note over Consumer: 跳过，不发送
            end
        end
    end
```

### 4.3 直连模式

```mermaid
sequenceDiagram
    autonumber
    participant STT as STT Provider
    participant Conn as Connector
    participant Dedup as Dedup
    participant Clean as Cleaner
    participant Prod as Producer
    participant Kafka as Kafka

    loop 持续推送转录
        STT->>Conn: 推送 JSON payload
        Conn->>Conn: 解析 JSON，展开 transcripts
        loop 每条 transcript
            Conn->>Dedup: should_emit(session_id, seq_no)
            alt 首次到达
                Dedup-->>Conn: True
                Conn->>Clean: clean(payload, event)
                Clean-->>Conn: raw + cleaned
                Conn->>Prod: send(session_id, transcript, ...)
                Prod->>Kafka: send_and_wait(topic, key, value)
                Kafka-->>Prod: ACK
            else 重复
                Dedup-->>Conn: False
                Note over Conn: 跳过
            end
        end
    end
```

### 4.4 STT 断连与重连

由 `connector.reconnect.run_with_reconnect` 驱动：每次循环调用 `connect_fn(last_event_id)`，`connect_fn` 内通过 `get_connector(settings, last_event_id)` 得到 Connector 后连接 STT。断连时 `connect_fn` **抛出异常**，不返回值；`last_event_id` 仅在一次**正常返回**时更新，断连后重试沿用上一轮的值（SSE 重连时带该值作为 `Last-Event-ID`）。

```mermaid
sequenceDiagram
    autonumber
    participant Loop as run_with_reconnect
    participant Fn as connect_fn
    participant Conn as Connector
    participant STT as STT Provider

    Loop->>Fn: connect_fn(last_event_id)
    Fn->>Conn: get_connector(settings, last_event_id)
    Conn->>STT: 建立 SSE/WebSocket 连接
    STT-->>Conn: 连接成功，开始推送
    Note over Conn, STT: ...正常传输...
    STT--xConn: 连接断开（502/网络异常）
    Conn-->>Fn: 抛出异常
    Fn-->>Loop: 抛出异常（不返回；last_event_id 保持上一轮）

    Loop->>Loop: 记录日志，计算退避延迟
    Loop->>Loop: sleep(delay)

    Loop->>Fn: connect_fn(last_event_id)
    Fn->>Conn: get_connector(settings, last_event_id)
    Conn->>STT: 重连（SSE 时带 Last-Event-ID）
    STT-->>Conn: 连接成功，从断点继续推送
```

### 4.5 优雅停机

```mermaid
sequenceDiagram
    autonumber
    participant OS as 操作系统
    participant Shutdown as GracefulShutdown
    participant Main as main.py
    participant Consumer as Buffer 消费端
    participant Prod as Producer

    OS->>Shutdown: SIGTERM / SIGINT
    Shutdown->>Shutdown: draining = True
    Main->>Main: 检测 draining，退出主循环
    Main->>Consumer: stop()
    Main->>Consumer: 消费剩余消息（最多 10 轮）
    Main->>Prod: flush()
    Main->>Prod: close()
    Main->>Main: 日志：已安全退出
```

---

## 5. 关键模块设计原理

### 5.1 Connector

**职责**：建立与 STT Provider 的长连接，解析推送的 JSON，按 `result.transcripts` 展开为 `TranscriptionEvent`。

**创建方式**：与 Dedup / Producer / Cleaner 一致，通过包级工厂注入。入口（如 `main.py`）调用 `get_connector(settings, last_event_id)`，根据 `settings.mode` 返回 `SseConnector` 或 `WebSocketConnector`。重连循环由 `connector.reconnect.run_with_reconnect` 管理，不通过 `connector` 包顶层 `__init__` 导出。

**SSE vs WebSocket**：

- **SSE**：基于 HTTP GET，单向推送，支持 `Last-Event-ID` 断点续传。适合 STT Provider 以 HTTP 流式返回的场景。
- **WebSocket**：双向通道，支持 ping/pong 保活。配置 `WS_PING_INTERVAL`、`WS_PING_TIMEOUT` 控制心跳。

**断点续传**：SSE 模式下，`last_event_id` 在重连时传给 `connect_fn`，下次重连会带上 `Last-Event-ID` 请求头，STT Provider 可从该位置继续推送。

**重连策略**：由 `reconnect.run_with_reconnect` 统一管理，指数退避（`initial_delay * backoff_factor^attempt`）。

---

### 5.2 Buffer（Redis Stream）

**选型理由**：Redis Stream 支持持久化、消费组、ACK 机制，适合 Kafka 不可用时暂存消息。

**写入端**（`RedisBuffer`）：

- `XADD` 将 payload 写入 Stream，`maxlen` 可限制 Stream 长度，避免内存溢出。
- 每条消息格式：`{payload: JSON.stringify(raw)}`。

**消费端**（`RedisBufferConsumer`）：

- `XREADGROUP` 消费组消费：先读新消息（`>`），再读未 ACK 的 pending（`0`）。
- 处理成功：`XACK` + `XDEL`；失败则**不** XACK，消息保留在 Stream，下次重试。
- `block=200` 毫秒轮询，兼顾新消息与 pending。

**与 Dedup 的关系**：消费端在发送 Kafka 前做 dedup；发送失败时调用 `dedup.remove()` 撤销 dedup 记录，保证重试时能再次发送。

---

### 5.3 Dedup

**原理**：Redis `SET key "1" NX EX ttl`，key 不存在则设置成功返回 True，否则返回 False。

**Key 组成**：由 `DEDUP_KEY_PARTS` 配置，支持 `session_id`、`processing_id`、`seq_no`、`created_at` 组合，例如 `dedup:s1:p1:0`。

**TTL**：`DEDUP_TTL_SECONDS` 控制 key 过期时间。需大于「同一 transcript 可能重复到达」的最大时间间隔（如 STT 重连重放）。

**remove()**：发送 Kafka 失败时调用，删除 dedup key，使重试时 `should_emit` 再次返回 True。

---

### 5.4 Producer

**Key 设计**：使用 `session_id` 作为 Kafka 消息 Key，相同 session 落入同一分区，分区内有序。

**启动校验**：`ensure_ready()` 在启动时调用，确保 Kafka 可达，失败则退出并输出明确错误。

**发送超时**：`asyncio.wait_for(producer.send(), timeout)`，默认 10 秒。超时后抛出 `RuntimeError`，Buffer 消费端捕获后不 XACK，消息保留待重试。

**Topic 创建**：首次启动时 `AIOKafkaAdminClient.create_topics`，若 Topic 已存在则忽略异常。

---

### 5.5 Shutdown

**SIGTERM/SIGINT**：注册信号处理器，收到后设置 `draining=True`，主循环检查 `draining` 后退出。

**Windows**：`add_signal_handler` 不支持，改用 `signal.signal`。

**stop_timeout**：`wait_for_sessions_or_timeout` 等待活跃 session 结束，超时后强制退出并打日志。

---

## 6. 异常与恢复

| 场景 | 行为 | 日志 |
|------|------|------|
| Redis 不可用 | 启动时 `ping()` 失败，立即退出 | `Transcription Ingest: 启动失败（Redis 不可用）` |
| Kafka 不可用 | 启动时 `ensure_ready()` 失败，立即退出 | `Transcription Ingest: 启动失败（Kafka 不可用）` |
| Kafka 发送超时 | Buffer 消费端不 XACK，消息保留；调用 `dedup.remove()` | `Buffer Consumer: 处理消息失败（Kafka 不可用，消息已保留在 Buffer，将自动重试）` |
| STT 断连 | 重连循环按指数退避重试 | `Reconnect: 连接 STT 失败（STT 提供商服务未就绪，将自动重试）` |
| STT 502/503/504 | 同上，视为 STT 不可用 | 同上 |

---

## 7. 与下游关系

本服务只**生产** Kafka 消息，不消费 Kafka。下游业务（NLP、质检等）需自行订阅 Topic `transcription_topic` 消费。

消息格式：`{ raw: {...}, cleaned: {...} }`，其中 `cleaned` 为结构化字段（`session_id`、`seq_no`、`transcript`、`role` 等）。

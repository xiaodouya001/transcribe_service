# 并发档位完整配置（300 / 400 / 500）

## 说明

- 以下为**单实例（单 Pod / 单 Task）**目标并发配置。
- 用于实时低延迟场景，优先保证 `P95` 与稳定性。
- `KAFKA_TOPIC_NUM_PARTITIONS` 仅对新建 topic 生效，已有 topic 需手动扩分区。
- `WS_PING_INTERVAL` / `WS_PING_TIMEOUT` 由 **`main.py` 传入 Uvicorn**（`ws="websockets"`），用于 **RFC WebSocket Ping/Pong** 保活；与业务 JSON 无关。

---

## 一、服务端 `.env` 配置表

| 配置项 | 300 并发/实例（低延迟） | 400 并发/实例（平衡） | 500 并发/实例（高负载） |
|---|---:|---:|---:|
| `WS_MAX_CONNECTIONS` | 360 | 480 | 600 |
| `REDIS_MAX_CONNECTIONS` | 900 | 1200 | 1600 |
| `HTTP_BACKLOG` | 4096 | 4096 | 4096 |
| `KAFKA_COMPRESSION_TYPE` | lz4 | lz4 | lz4 |
| `KAFKA_LINGER_MS` | 1 | 1 | 1 |
| `KAFKA_BATCH_SIZE` | 32768 | 32768 | 32768 |
| `KAFKA_SEND_TIMEOUT_SEC` | 5 | 5 | 5 |
| `KAFKA_TOPIC_NUM_PARTITIONS` | 100 | 100 | 100 |
| `LOG_LEVEL` | WARNING | WARNING | WARNING |
| `WS_PING_INTERVAL` | 20.0 | 20.0 | 20.0 |
| `WS_PING_TIMEOUT` | 20.0 | 20.0 | 20.0 |
| `STOP_TIMEOUT` | 120 | 120 | 120 |

---

## 二、压测参数建议（Mock Client）

| 参数 | 300 并发 | 400 并发 | 500 并发 |
|---|---:|---:|---:|
| 并发连接数 | 300 | 400 | 500 |
| 每连接消息总数（起步） | 100 | 100 | 100 |
| `interval_ms` | 60~80 | 70~85 | 80~90 |
| `ramp_up_ms` | 20000~30000 | 25000~35000 | 30000~45000 |

---

## 三、自动扩缩容阈值（每实例）

| 档位 | 扩容触发（持续 2 分钟） | 缩容触发（持续 10 分钟） |
|---|---|---|
| 300 | `active_connections > 260` | `active_connections < 160` |
| 400 | `active_connections > 350` | `active_connections < 220` |
| 500 | `active_connections > 430` | `active_connections < 280` |

---

## 四、快速复制片段（按档位替换）

### 300 并发/实例

```env
WS_MAX_CONNECTIONS=360
REDIS_MAX_CONNECTIONS=900
HTTP_BACKLOG=4096
KAFKA_COMPRESSION_TYPE=lz4
KAFKA_LINGER_MS=1
KAFKA_BATCH_SIZE=32768
KAFKA_SEND_TIMEOUT_SEC=5
KAFKA_TOPIC_NUM_PARTITIONS=100
LOG_LEVEL=WARNING
```

### 400 并发/实例

```env
WS_MAX_CONNECTIONS=480
REDIS_MAX_CONNECTIONS=1200
HTTP_BACKLOG=4096
KAFKA_COMPRESSION_TYPE=lz4
KAFKA_LINGER_MS=1
KAFKA_BATCH_SIZE=32768
KAFKA_SEND_TIMEOUT_SEC=5
KAFKA_TOPIC_NUM_PARTITIONS=100
LOG_LEVEL=WARNING
```

### 500 并发/实例

```env
WS_MAX_CONNECTIONS=600
REDIS_MAX_CONNECTIONS=1600
HTTP_BACKLOG=4096
KAFKA_COMPRESSION_TYPE=lz4
KAFKA_LINGER_MS=1
KAFKA_BATCH_SIZE=32768
KAFKA_SEND_TIMEOUT_SEC=5
KAFKA_TOPIC_NUM_PARTITIONS=100
LOG_LEVEL=WARNING
```


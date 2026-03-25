# 配置说明

本文档说明 Transcribe Service 的环境变量配置，与 [config/settings.py](../config/settings.py) 对应。

---

## 1. 环境变量

复制 `.env.example` 为 `.env` 并修改。**环境变量统一使用 UPPER_SNAKE_CASE**。

```bash
cp .env.example .env
```

---

## 2. 配置项一览

### Redis（序列状态机 + 发送所有权守卫）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `REDIS_URL` | redis://127.0.0.1:6379/0 | Redis 连接地址 |
| `REDIS_MAX_CONNECTIONS` | 100 | 连接池大小；高并发 WebSocket（如约 1000 路）场景可提升至 256～1024，见 [concurrency-capacity.md](concurrency-capacity.md) |
| `REDIS_ACTIVE_TTL_SEC` | 3600 | 活跃会话 TTL（秒），每次写入自动续期 |
| `REDIS_FINAL_TTL_SEC` | 60 | SESSION_COMPLETE 后残留 TTL（秒） |
| `REDIS_OWNERSHIP_GUARD_TTL_SEC` | 30 | 单个 `conversationId` 会话发送所有权键（conversation ownership key）的 TTL（秒）；服务端在连接建立时 claim 所有权，并在连接存活期间周期 refresh，用于跨 pod 保证“同会话同一时刻仅一个连接发送消息” |
| `REDIS_SEQUENCE_STATE_KEY_PREFIX` | transcript:session | Redis Sequence State Machine 键前缀 |
| `REDIS_OWNERSHIP_GUARD_KEY_PREFIX` | transcript:owner | Redis Ownership Guard 键前缀 |

### Kafka

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `KAFKA_BOOTSTRAP_SERVERS` | 127.0.0.1:9092 | Kafka 集群地址 |
| `KAFKA_TOPIC` | cc.transcript.realtime.v1 | Topic 名称 |
| `KAFKA_TOPIC_NUM_PARTITIONS` | 50 | 新建 Topic 时的分区数 |
| `KAFKA_REPLICATION_FACTOR` | 1 | 副本因子（生产环境≥2） |
| `KAFKA_COMPRESSION_TYPE` | zstd | 压缩：`none`、`gzip`、`snappy`、`lz4`、`zstd` |
| `KAFKA_SEND_TIMEOUT_SEC` | 2.0 | 发送超时（秒），用于快速失败；高负载下如出现误判，可结合 broker 能力适当调大，见 [concurrency-capacity.md](concurrency-capacity.md) |
| `KAFKA_LINGER_MS` | 1 | Producer 聚合等待时间（毫秒）；越小延迟越低，越大更利于批量吞吐 |
| `KAFKA_BATCH_SIZE` | 32768 | Producer 批大小（bytes）；影响单批聚合上限与吞吐/延迟平衡 |

### WebSocket

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `WS_PING_INTERVAL` | 20.0 | 秒；**Uvicorn `websockets` 后端**下为服务端发出 **WebSocket Ping** 的间隔，用于保活（如防 ALB 空闲断开） |
| `WS_PING_TIMEOUT` | 20.0 | 秒；等待 **Pong** 的超时；超时会关闭连接（由 Uvicorn/websockets 库处理） |
| `WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC` | 5.0 | 秒；会话发送所有权守卫的后台续租周期 |
| `WS_MAX_CONNECTIONS` | 0 | 最大同时在线 WebSocket；`0` 表示不限制；超限握手返回 429，见 [concurrency-capacity.md](concurrency-capacity.md) |

> **说明**：本服务通过 `uvicorn.Config(ws="websockets", …)` 启用 WebSocket 运行时；`WS_PING_INTERVAL` 与 `WS_PING_TIMEOUT` 由 Uvicorn `websockets` backend 负责执行。

### HTTP / Uvicorn

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `HTTP_HOST` | 0.0.0.0 | 监听地址（容器内通常 0.0.0.0） |
| `HTTP_PORT` | 8080 | 监听端口 |
| `HTTP_BACKLOG` | 4096 | Uvicorn `listen(backlog)`；瞬时大量 WebSocket 握手时可减小对端「读 101 前被关」概率 |

### 启动检查

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `KAFKA_STARTUP_TIMEOUT_SEC` | 30.0 | Kafka 启动连通性检查超时（秒） |

### 其它

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `STOP_TIMEOUT` | 120 | 优雅停机超时（秒） |
| `LOG_LEVEL` | INFO | 日志级别 |
| `LOG_FORMAT` | auto | 日志格式：`json`、`console`、`auto` |
| `LOG_WS_ERROR_FRAMES` | false | 是否打印服务端发出的完整 ERROR 响应 JSON；排障时可开启，压测场景通常保持关闭 |

---

## 3. 配置示例

**本地开发**：

```env
REDIS_URL=redis://127.0.0.1:6379/0
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_COMPRESSION_TYPE=zstd
LOG_FORMAT=console
```

**生产**：

```env
REDIS_URL=redis://your-elasticache:6379/0
KAFKA_BOOTSTRAP_SERVERS=your-msk:9092
KAFKA_TOPIC=cc.transcript.realtime.v1
KAFKA_TOPIC_NUM_PARTITIONS=100
KAFKA_REPLICATION_FACTOR=3
LOG_FORMAT=json
```

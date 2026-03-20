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

### Redis（状态机）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `REDIS_URL` | redis://localhost:6379/0 | Redis 连接地址 |
| `REDIS_MAX_CONNECTIONS` | 100 | 连接池大小；**高并发 WebSocket（如 ~1000 路）时建议调至 256～1024**，见 [concurrency-capacity.md](concurrency-capacity.md) |
| `REDIS_ACTIVE_TTL_SEC` | 3600 | 活跃会话 TTL（秒），每次写入自动续期 |
| `REDIS_FINAL_TTL_SEC` | 60 | SESSION_COMPLETE 后残留 TTL（秒） |

### Kafka

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka 集群地址 |
| `KAFKA_TOPIC` | cc.transcript.realtime.v1 | Topic 名称 |
| `KAFKA_TOPIC_NUM_PARTITIONS` | 50 | 新建 Topic 时的分区数 |
| `KAFKA_REPLICATION_FACTOR` | 1 | 副本因子（生产环境≥2） |
| `KAFKA_COMPRESSION_TYPE` | zstd | 压缩：`none`、`gzip`、`snappy`、`lz4`、`zstd` |
| `KAFKA_SEND_TIMEOUT_SEC` | 2.0 | 发送超时（秒），快速失败；高负载下若误杀可酌情调大，需结合 broker 能力，见 [concurrency-capacity.md](concurrency-capacity.md) |

### WebSocket

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `WS_PING_INTERVAL` | 20.0 | Ping 间隔（秒），防 ALB 60s 空闲超时 |
| `WS_PING_TIMEOUT` | 20.0 | Pong 超时（秒） |
| `WS_MAX_SIZE` | 1048576 | 单消息最大字节数（1MB） |

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

---

## 3. 配置示例

**本地开发**：

```env
REDIS_URL=redis://localhost:6379/0
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
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

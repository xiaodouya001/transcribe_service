# 配置说明

本文档说明 Transcription Ingest 的环境变量配置，与 [config/settings.py](../config/settings.py) 对应。

---

## 1. 环境变量

复制 `.env.example` 为 `.env` 并修改。**环境变量统一使用 UPPER_SNAKE_CASE**。

```bash
cp .env.example .env
```

---

## 2. 配置项一览

### Fanolab ASR

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `FANOLAB_URL` | http://localhost:8765/sse | Fanolab SSE/WebSocket 地址 |
| `MODE` | sse | 传输协议：`sse` 或 `websocket` |
| `SSE_READ_TIMEOUT` | 空 | SSE 读超时（秒），空=无限制 |
| `WS_PING_INTERVAL` | 20.0 | WebSocket ping 间隔（秒） |
| `WS_PING_TIMEOUT` | 20.0 | WebSocket pong 超时（秒） |

### Redis

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `REDIS_URL` | redis://localhost:6379/0 | Redis 连接地址 |
| `DEDUP_KEY_PARTS` | session_id,processing_id,seq_no | 去重 Key 组成 |
| `DEDUP_TTL_SECONDS` | 60 | 去重 Key 过期时间（秒） |

### Redis Buffer

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `REDIS_BUFFER_ENABLED` | true | 是否启用 Redis Stream 缓冲 |
| `REDIS_BUFFER_STREAM` | transcription:ingest:buffer | Redis Stream 名称 |
| `REDIS_BUFFER_CONSUMER_GROUP` | transcription:ingest:consumer | Buffer 消费端使用的消费组名称 |
| `REDIS_BUFFER_MAXLEN` | 10000 | Stream 最大长度 |

### Kafka

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka 集群地址 |
| `KAFKA_TOPIC` | asr_realtime_text | Topic 名称 |
| `KAFKA_COMPRESSION_TYPE` | none | 压缩：`none`、`gzip`、`snappy`、`lz4` |
| `KAFKA_SEND_TIMEOUT_SEC` | 10 | 发送超时（秒），Kafka 不可用时超时并输出错误日志 |

### 长连接与重连

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `RECONNECT_ENABLED` | true | 自动重连 |
| `RECONNECT_MAX_RETRIES` | 0 | 最大重试次数，0=无限 |
| `RECONNECT_INITIAL_DELAY` | 1.0 | 初始退避延迟（秒） |
| `RECONNECT_MAX_DELAY` | 60.0 | 最大退避延迟（秒） |
| `RECONNECT_BACKOFF_FACTOR` | 2.0 | 退避因子 |

### 其它

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `CLEANER_MODE` | default | 数据清洗：`default`（raw+cleaned）、`identity`（透传） |
| `STOP_TIMEOUT` | 120 | 优雅停机超时（秒） |
| `LOG_LEVEL` | INFO | 日志级别 |
| `LOG_FORMAT` | auto | 日志格式：`json`、`console`、`auto` |

---

## 3. 配置示例

**本地开发**：

```env
FANOLAB_URL=http://localhost:8765/sse
MODE=sse
REDIS_URL=redis://localhost:6379/0
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

**生产**：

```env
FANOLAB_URL=https://your-fanolab.example.com/sse
MODE=sse
REDIS_URL=redis://your-elasticache:6379/0
KAFKA_BOOTSTRAP_SERVERS=your-msk:9092
KAFKA_COMPRESSION_TYPE=gzip
LOG_FORMAT=json
```

---

## 4. CLEANER_MODE 说明

| 值 | 说明 | Kafka 输出 |
|------|------|------------|
| `default` | 提取结构化字段 + 保留原始 | `{raw, cleaned}` |
| `identity` | 透传原始 payload | `{raw}` |

---

## 5. 相关文档

- [pyproject-config.md](pyproject-config.md) - pyproject.toml 构建与依赖配置

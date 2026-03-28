# Configuration

This document describes the Realtime Transcribe Service environment variables and maps directly to [src/realtime_transcribe_service/config/settings.py](../src/realtime_transcribe_service/config/settings.py).

---

## 1. Environment Setup

**Local development**

Copy `.env.example` to `.env`, keep `APP_ENV=local`, and edit values as needed.

```bash
cp .env.example .env
```

**Deployed environments**

Do not rely on `.env`. Inject configuration as process environment variables and set `APP_ENV=deployed`.

**Validation behavior**

- `APP_ENV` is required in every environment
- Blank string values fail startup
- Unknown keys in `.env` fail startup
- `REDIS_URL` and `KAFKA_BOOTSTRAP_SERVERS` are required when `APP_ENV=deployed`

---

## 2. Configuration Reference

### Runtime Mode

| Variable | Default | Description |
|------|------|------|
| `APP_ENV` | None | Required. `local` enables localhost fallbacks for Redis/Kafka. `deployed` requires explicit dependency addresses |

### Redis: Sequence State Machine + Ownership Guard

| Variable | Default | Description |
|------|------|------|
| `REDIS_URL` | local only: `redis://127.0.0.1:6379/0` | Redis connection string. Required when `APP_ENV=deployed` |
| `REDIS_MAX_CONNECTIONS` | 100 | Connection-pool size. Must be `> 0` |
| `REDIS_ACTIVE_TTL_SEC` | 3600 | TTL for active conversations in seconds. Must be `> 0` |
| `REDIS_FINAL_TTL_SEC` | 60 | Residual TTL after `SESSION_COMPLETE`. Must be `> 0` and `<= REDIS_ACTIVE_TTL_SEC` |
| `REDIS_OWNERSHIP_GUARD_TTL_SEC` | 30 | TTL for the per-`conversationId` ownership key. Must be `> 0` |
| `REDIS_SEQUENCE_STATE_KEY_PREFIX` | `realtime-transcribe-service:expect-transcript-seq-num` | Key prefix for the Redis sequence state machine. Must not be empty |
| `REDIS_OWNERSHIP_GUARD_KEY_PREFIX` | `realtime-transcribe-service:conversation-owner` | Key prefix for the Redis ownership guard. Must not be empty |

### Kafka

| Variable | Default | Description |
|------|------|------|
| `KAFKA_BOOTSTRAP_SERVERS` | local only: `127.0.0.1:9092` | Kafka bootstrap servers. Required when `APP_ENV=deployed` |
| `KAFKA_TOPIC` | `AI_STAGING_TRANSCRIPTION` | Topic name. Must not be empty |
| `KAFKA_TOPIC_NUM_PARTITIONS` | 50 | Partition count when the service creates a new topic. Must be `> 0` |
| `KAFKA_REPLICATION_FACTOR` | 1 | Replication factor. Must be `> 0`. Use `>= 2` in production |
| `KAFKA_COMPRESSION_TYPE` | `zstd` | Compression codec: `none`, `gzip`, `snappy`, `lz4`, or `zstd` |
| `KAFKA_SEND_TIMEOUT_SEC` | 2.0 | Kafka send timeout in seconds. Must be `> 0` |
| `KAFKA_LINGER_MS` | 1 | Producer linger in milliseconds. Must be `>= 0` |
| `KAFKA_BATCH_SIZE` | 32768 | Producer batch size in bytes. Must be `> 0` |

### WebSocket

| Variable | Default | Description |
|------|------|------|
| `WS_PING_INTERVAL` | 20.0 | Seconds. Must be `> 0` |
| `WS_PING_TIMEOUT` | 10.0 | Seconds. Must be `> 0` |
| `WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC` | 5.0 | Seconds between ownership-guard refreshes. Must be `> 0` and `< REDIS_OWNERSHIP_GUARD_TTL_SEC` |
| `WS_MAX_CONNECTIONS` | 0 | Maximum concurrent WebSocket connections. Must be `>= 0`. `0` means unlimited |

> The service enables the WebSocket runtime with `uvicorn.Config(ws="websockets", ...)`. `WS_PING_INTERVAL` and `WS_PING_TIMEOUT` are enforced by the Uvicorn `websockets` backend rather than by application-level JSON messages.

### HTTP / Uvicorn

| Variable | Default | Description |
|------|------|------|
| `HTTP_HOST` | `0.0.0.0` | Bind address. Must not be empty |
| `HTTP_PORT` | 8080 | Listen port. Must be in `1..65535` |
| `HTTP_BACKLOG` | 4096 | Uvicorn `listen(backlog)` value. Must be `> 0` |

### Startup Checks

| Variable | Default | Description |
|------|------|------|
| `KAFKA_STARTUP_TIMEOUT_SEC` | 30.0 | Timeout for Kafka connectivity checks during startup. Must be `> 0` |

### Other

| Variable | Default | Description |
|------|------|------|
| `STOP_TIMEOUT` | 120.0 | Total graceful-shutdown budget in seconds. Must be `> 0` |
| `LOG_LEVEL` | `INFO` | Log level. One of `CRITICAL`, `ERROR`, `WARNING`, `INFO`, or `DEBUG` |
| `LOG_FORMAT` | `auto` | Log format: `json`, `console`, or `auto` |
| `LOG_WS_ERROR_FRAMES` | false | Whether to log the full outbound `ERROR` response JSON |
| `LOG_SLOW_MESSAGE_THRESHOLD_MS` | 0.0 | Slow-message warning threshold in milliseconds. Must be `>= 0`. `0` disables it |

---

## 3. Example Configurations

**Local development**

```env
APP_ENV=local
REDIS_URL=redis://127.0.0.1:6379/0
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_COMPRESSION_TYPE=zstd
LOG_FORMAT=console
```

**Production / deployed**

```env
APP_ENV=deployed
REDIS_URL=redis://your-elasticache:6379/0
KAFKA_BOOTSTRAP_SERVERS=your-msk:9092
KAFKA_TOPIC=AI_STAGING_TRANSCRIPTION
KAFKA_TOPIC_NUM_PARTITIONS=100
KAFKA_REPLICATION_FACTOR=3
LOG_FORMAT=json
```

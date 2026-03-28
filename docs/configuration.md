# Configuration

This document describes the Realtime Transcribe Service environment variables and maps directly to [config/settings.py](../config/settings.py).

---

## 1. Environment Setup

Copy `.env.example` to `.env` and edit the values as needed. All environment variables use `UPPER_SNAKE_CASE`.

```bash
cp .env.example .env
```

---

## 2. Configuration Reference

### Redis: Sequence State Machine + Ownership Guard

| Variable | Default | Description |
|------|--------|------|
| `REDIS_URL` | redis://127.0.0.1:6379/0 | Redis connection string |
| `REDIS_MAX_CONNECTIONS` | 100 | Connection-pool size. For high WebSocket concurrency, raise this to roughly 256-1024 as needed |
| `REDIS_ACTIVE_TTL_SEC` | 3600 | TTL for active conversations in seconds; refreshed automatically on each write |
| `REDIS_FINAL_TTL_SEC` | 60 | Residual TTL in seconds after `SESSION_COMPLETE` |
| `REDIS_OWNERSHIP_GUARD_TTL_SEC` | 30 | TTL for the per-`conversationId` ownership key. The server claims ownership when a connection is established and refreshes it periodically while the connection stays alive, enforcing single-sender semantics across pods |
| `REDIS_SEQUENCE_STATE_KEY_PREFIX` | realtime-transcribe-service:transcript-checker | Key prefix for the Redis sequence state machine |
| `REDIS_OWNERSHIP_GUARD_KEY_PREFIX` | realtime-transcribe-service:conversation-owner | Key prefix for the Redis ownership guard |

### Kafka

| Variable | Default | Description |
|------|--------|------|
| `KAFKA_BOOTSTRAP_SERVERS` | 127.0.0.1:9092 | Kafka bootstrap servers |
| `KAFKA_TOPIC` | AI_STAGING_TRANSCRIPTION | Topic name |
| `KAFKA_TOPIC_NUM_PARTITIONS` | 50 | Partition count when the service creates a new topic |
| `KAFKA_REPLICATION_FACTOR` | 1 | Replication factor. Use `>= 2` in production |
| `KAFKA_COMPRESSION_TYPE` | zstd | Compression codec: `none`, `gzip`, `snappy`, `lz4`, or `zstd` |
| `KAFKA_SEND_TIMEOUT_SEC` | 2.0 | Kafka send timeout in seconds. This is intentionally short to fail fast; raise it carefully if high-load false positives appear |
| `KAFKA_LINGER_MS` | 1 | Producer linger in milliseconds. Lower values reduce latency; higher values improve batching |
| `KAFKA_BATCH_SIZE` | 32768 | Producer batch size in bytes; affects throughput/latency tradeoffs |

### WebSocket

| Variable | Default | Description |
|------|--------|------|
| `WS_PING_INTERVAL` | 20.0 | Seconds. With the Uvicorn `websockets` backend, the first Ping is sent after this interval, then one Ping per interval afterward |
| `WS_PING_TIMEOUT` | 10.0 | Seconds. Maximum time to wait for the peer Pong after each Ping. On timeout, `websockets` closes the connection, typically with WebSocket close code `1011` |
| `WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC` | 5.0 | Seconds between ownership-guard refreshes |
| `WS_MAX_CONNECTIONS` | 0 | Maximum concurrent WebSocket connections. `0` means unlimited. Excess handshakes are rejected with HTTP 429 |

> The service enables the WebSocket runtime with `uvicorn.Config(ws="websockets", ...)`. `WS_PING_INTERVAL` and `WS_PING_TIMEOUT` are enforced by the Uvicorn `websockets` backend rather than by application-level JSON messages.

### HTTP / Uvicorn

| Variable | Default | Description |
|------|--------|------|
| `HTTP_HOST` | 0.0.0.0 | Bind address. Containers usually use `0.0.0.0` |
| `HTTP_PORT` | 8080 | Listen port |
| `HTTP_BACKLOG` | 4096 | Uvicorn `listen(backlog)` value. Increasing this helps reduce handshake drops during connection spikes |

### Startup Checks

| Variable | Default | Description |
|------|--------|------|
| `KAFKA_STARTUP_TIMEOUT_SEC` | 30.0 | Timeout for Kafka connectivity checks during startup |

### Other

| Variable | Default | Description |
|------|--------|------|
| `STOP_TIMEOUT` | 120 | Total graceful-shutdown budget in seconds, covering `close_all`, Kafka flush, and server-loop exit |
| `LOG_LEVEL` | INFO | Log level |
| `LOG_FORMAT` | auto | Log format: `json`, `console`, or `auto` |
| `LOG_WS_ERROR_FRAMES` | false | Whether to log the full outbound `ERROR` response JSON. Useful for debugging, usually disabled during load tests |
| `LOG_SLOW_MESSAGE_THRESHOLD_MS` | 0.0 | Slow-message warning threshold in milliseconds. `0` disables it. When enabled, warnings include stage timings such as `decode/validate/prepare/kafka_send/commit/cleanup/ack_build/send/total` |

---

## 3. Example Configurations

**Local development**

```env
REDIS_URL=redis://127.0.0.1:6379/0
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_COMPRESSION_TYPE=zstd
LOG_FORMAT=console
```

**Production**

```env
REDIS_URL=redis://your-elasticache:6379/0
KAFKA_BOOTSTRAP_SERVERS=your-msk:9092
KAFKA_TOPIC=AI_STAGING_TRANSCRIPTION
KAFKA_TOPIC_NUM_PARTITIONS=100
KAFKA_REPLICATION_FACTOR=3
LOG_FORMAT=json
```

# Full Concurrency Profiles (300 / 400 / 500)

## Notes

- These profiles target **one service instance** (one pod or one ECS task)
- They are tuned for low-latency real-time traffic, prioritizing `P95` and stability over raw peak throughput
- `KAFKA_TOPIC_NUM_PARTITIONS` only applies when creating a new topic; existing topics must be repartitioned separately
- `WS_PING_INTERVAL` and `WS_PING_TIMEOUT` are passed from `main.py` into Uvicorn with `ws="websockets"` and control RFC WebSocket Ping/Pong keepalive, not business JSON behavior
- If handshake JWT authentication is enabled, `AUTH_ENABLED`, `AUTH_JWT_SIGNING_MATERIAL`, and `AUTH_JWT_ALGORITHM` must also be set. The concurrency profile does not change those values; they are shared across all three profiles.
- `HTTP_ENABLE_DOCS` only controls `/docs`, `/redoc`, and `/openapi.json`. It does not protect `/health`, `/ready`, or `/metrics`; those HTTP routes should be limited at the edge or on the internal network.

---

## 1. Suggested Server `.env` Values

| Setting | 300 concurrency / instance (low latency) | 400 concurrency / instance (balanced) | 500 concurrency / instance (high load) |
| ---------------------------- | -------------- | ------------- | -------------- |
| `WS_MAX_CONNECTIONS`         | 360            | 480           | 600            |
| `REDIS_MAX_CONNECTIONS`      | 900            | 1200          | 1600           |
| `REDIS_OWNERSHIP_GUARD_TTL_SEC` | 30       | 30            | 30             |
| `REDIS_SEQUENCE_STATE_KEY_PREFIX` | realtime-transcribe-service:expect-transcript-seq-num | realtime-transcribe-service:expect-transcript-seq-num | realtime-transcribe-service:expect-transcript-seq-num |
| `REDIS_OWNERSHIP_GUARD_KEY_PREFIX` | realtime-transcribe-service:conversation-owner | realtime-transcribe-service:conversation-owner | realtime-transcribe-service:conversation-owner |
| `HTTP_BACKLOG`               | 4096           | 4096          | 4096           |
| `HTTP_ENABLE_DOCS`           | false          | false         | false          |
| `KAFKA_COMPRESSION_TYPE`     | lz4            | lz4           | lz4            |
| `KAFKA_LINGER_MS`            | 1              | 1             | 1              |
| `KAFKA_BATCH_SIZE`           | 32768          | 32768         | 32768          |
| `KAFKA_SEND_TIMEOUT_SEC`     | 5              | 5             | 5              |
| `KAFKA_TOPIC_NUM_PARTITIONS` | 100            | 100           | 100            |
| `LOG_LEVEL`                  | WARNING        | WARNING       | WARNING        |
| `WS_PING_INTERVAL`           | 20.0           | 20.0          | 20.0           |
| `WS_PING_TIMEOUT`            | 10.0           | 10.0          | 10.0           |
| `WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC` | 5.0 | 5.0 | 5.0 |
| `AUTH_ENABLED`               | false by default; set true when handshake auth is enabled | false by default; set true when handshake auth is enabled | false by default; set true when handshake auth is enabled |
| `AUTH_JWT_SIGNING_MATERIAL`  | signing material from secure config when auth is enabled | signing material from secure config when auth is enabled | signing material from secure config when auth is enabled |
| `AUTH_JWT_ALGORITHM`         | HS256          | HS256         | HS256          |
| `STOP_TIMEOUT`               | 120            | 120           | 120            |

---

## 2. Suggested Mock Client Load-Test Parameters

| Parameter | 300 concurrency | 400 concurrency | 500 concurrency |
| ------------- | ----------- | ----------- | ----------- |
| Concurrent connections | 300 | 400 | 500 |
| Messages per connection (starting point) | 100 | 100 | 100 |
| **Message interval (ms)** | 60-80 | 70-85 | 80-90 |
| **Ramp-up window (ms)** | 20000-30000 | 25000-35000 | 30000-45000 |

---

## 3. Autoscaling Thresholds per Instance

| Profile | Scale-out trigger (for 2 minutes) | Scale-in trigger (for 10 minutes) |
| --- | -------------------------- | -------------------------- |
| 300 | `active_connections > 260` | `active_connections < 160` |
| 400 | `active_connections > 350` | `active_connections < 220` |
| 500 | `active_connections > 430` | `active_connections < 280` |

---

## 4. Copy/Paste Snippets

### 300 concurrency per instance

```env
WS_MAX_CONNECTIONS=360
REDIS_MAX_CONNECTIONS=900
REDIS_OWNERSHIP_GUARD_TTL_SEC=30
REDIS_SEQUENCE_STATE_KEY_PREFIX=realtime-transcribe-service:expect-transcript-seq-num
REDIS_OWNERSHIP_GUARD_KEY_PREFIX=realtime-transcribe-service:conversation-owner
HTTP_BACKLOG=4096
HTTP_ENABLE_DOCS=false
KAFKA_COMPRESSION_TYPE=lz4
KAFKA_LINGER_MS=1
KAFKA_BATCH_SIZE=32768
KAFKA_SEND_TIMEOUT_SEC=5
KAFKA_TOPIC_NUM_PARTITIONS=100
LOG_LEVEL=WARNING
WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC=5.0
AUTH_ENABLED=true
AUTH_JWT_SIGNING_MATERIAL=replace-with-signing-material-from-secure-config
AUTH_JWT_ALGORITHM=HS256
```

### 400 concurrency per instance

```env
WS_MAX_CONNECTIONS=480
REDIS_MAX_CONNECTIONS=1200
REDIS_OWNERSHIP_GUARD_TTL_SEC=30
REDIS_SEQUENCE_STATE_KEY_PREFIX=realtime-transcribe-service:expect-transcript-seq-num
REDIS_OWNERSHIP_GUARD_KEY_PREFIX=realtime-transcribe-service:conversation-owner
HTTP_BACKLOG=4096
HTTP_ENABLE_DOCS=false
KAFKA_COMPRESSION_TYPE=lz4
KAFKA_LINGER_MS=1
KAFKA_BATCH_SIZE=32768
KAFKA_SEND_TIMEOUT_SEC=5
KAFKA_TOPIC_NUM_PARTITIONS=100
LOG_LEVEL=WARNING
WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC=5.0
AUTH_ENABLED=true
AUTH_JWT_SIGNING_MATERIAL=replace-with-signing-material-from-secure-config
AUTH_JWT_ALGORITHM=HS256
```

### 500 concurrency per instance

```env
WS_MAX_CONNECTIONS=600
REDIS_MAX_CONNECTIONS=1600
REDIS_OWNERSHIP_GUARD_TTL_SEC=30
REDIS_SEQUENCE_STATE_KEY_PREFIX=realtime-transcribe-service:expect-transcript-seq-num
REDIS_OWNERSHIP_GUARD_KEY_PREFIX=realtime-transcribe-service:conversation-owner
HTTP_BACKLOG=4096
HTTP_ENABLE_DOCS=false
KAFKA_COMPRESSION_TYPE=lz4
KAFKA_LINGER_MS=1
KAFKA_BATCH_SIZE=32768
KAFKA_SEND_TIMEOUT_SEC=5
KAFKA_TOPIC_NUM_PARTITIONS=100
LOG_LEVEL=WARNING
WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC=5.0
AUTH_ENABLED=true
AUTH_JWT_SIGNING_MATERIAL=replace-with-signing-material-from-secure-config
AUTH_JWT_ALGORITHM=HS256
```

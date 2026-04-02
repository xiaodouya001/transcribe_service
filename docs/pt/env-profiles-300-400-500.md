# Full Concurrency Profiles (300 / 400 / 500)

## Notes

- These profiles target **one service instance** (one pod or one ECS task)
- They are tuned for low-latency real-time traffic, prioritizing `P95` and stability over raw peak throughput
- The service **does not** auto-create Kafka topics. Provision `KAFKA_TOPIC` (name and partition count) yourself before running the service; changing partition count later requires repartitioning or a new topic
- `WS_PING_INTERVAL` and `WS_PING_TIMEOUT` are applied when building the Uvicorn server in `service_runtime.create_uvicorn_server` (`ws="websockets"`) and control RFC WebSocket Ping/Pong keepalive, not business JSON behavior
- If handshake JWT authentication is enabled, `AUTH_ENABLED`, `AUTH_JWT_SIGNING_MATERIAL`, and `AUTH_JWT_ALGORITHM` must also be set. The concurrency profile does not change those values; they are shared across all three profiles.
- `HTTP_ENABLE_DOCS` only controls `/docs`, `/redoc`, and `/openapi.json`. It does not protect `/health`, `/ready`, or `/metrics`; those HTTP routes should be limited at the edge or on the internal network.
- `REDIS_SEQUENCE_STATE_KEY_PREFIX` and `REDIS_OWNERSHIP_GUARD_KEY_PREFIX` below match the **service default** (no account/environment namespace). In AWS ElastiCache (and similar), replace with prefixes that satisfy **your** user ACL key patterns for that environment.

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
| `LOG_LEVEL`                  | INFO           | INFO          | INFO           |
| `WS_PING_INTERVAL`           | 20.0           | 20.0          | 20.0           |
| `WS_PING_TIMEOUT`            | 10.0           | 10.0          | 10.0           |
| `WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC` | 15.0 | 15.0 | 15.0 |
| `AUTH_ENABLED`               | false by default; set true when handshake auth is enabled | false by default; set true when handshake auth is enabled | false by default; set true when handshake auth is enabled |
| `AUTH_JWT_SIGNING_MATERIAL`  | signing material from secure config when auth is enabled | signing material from secure config when auth is enabled | signing material from secure config when auth is enabled |
| `AUTH_JWT_ALGORITHM`         | HS256          | HS256         | HS256          |
| `STOP_TIMEOUT`               | 120            | 120           | 120            |

---

## 2. Autoscaling Thresholds per Instance

| Profile | Scale-out trigger (for 2 minutes) | Scale-in trigger (for 10 minutes) |
| --- | -------------------------- | -------------------------- |
| 300 | `active_connections > 260` | `active_connections < 160` |
| 400 | `active_connections > 350` | `active_connections < 220` |
| 500 | `active_connections > 430` | `active_connections < 280` |

---

## 3. Copy/Paste Snippets

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
LOG_LEVEL=INFO
WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC=15.0
AUTH_ENABLED=false
AUTH_JWT_SIGNING_MATERIAL=replace-with-signing-material
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
LOG_LEVEL=INFO
WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC=15.0
AUTH_ENABLED=false
AUTH_JWT_SIGNING_MATERIAL=replace-with-signing-material
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
LOG_LEVEL=INFO
WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC=15.0
AUTH_ENABLED=false
AUTH_JWT_SIGNING_MATERIAL=replace-with-signing-material
AUTH_JWT_ALGORITHM=HS256
```

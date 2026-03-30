# Troubleshooting

---

## 1. Startup Failures

### Redis unavailable during startup

**Cause:** the service cannot connect to Redis.

**Checks:**

1. Confirm Redis is up with `docker compose ps` or `redis-cli ping`
2. Verify `REDIS_URL`
3. Inside a Docker network, use the service name instead of `127.0.0.1`

### Kafka unavailable during startup

**Cause:** the service cannot complete Kafka startup checks before timeout.

**Checks:**

1. Confirm Kafka is healthy with `docker compose ps`
2. Verify `KAFKA_BOOTSTRAP_SERVERS`
3. Kafka startup can be slow; wait 30-60 seconds and retry if needed

---

## 2. WebSocket Connection Problems

### Handshake rejected with HTTP 401

**Cause:** handshake authentication is enabled and the client did not send a valid `Authorization: Bearer <JWT>` header. The service rejects the request with `E1010` before the WebSocket upgrade completes.

**Resolution:** confirm `AUTH_ENABLED`, send a non-expired HS256 Bearer token, and make sure the client and service use the same signing material.

### Handshake rejected with HTTP 503

**Cause:** the service is draining during graceful shutdown.

**Resolution:** reconnect after the replacement pod or task becomes ready.

### Connection closed with WebSocket code 1008

**Cause:** schema validation failed, business rules were violated, or the sequence number was out of order.

**Resolution:** inspect `error.code` and `error.details` in the `ERROR` frame.

### Connection closed with WebSocket code 1013

**Cause:** Kafka is unavailable or timed out.

**Resolution:** treat this as a temporary downstream failure and reconnect after Kafka recovers.

---

## 3. Kafka Delivery Problems

### `Kafka: Send timed out`

**Cause:** the Kafka broker did not acknowledge the send before `KAFKA_SEND_TIMEOUT_SEC` elapsed.

**Checks:**

1. Verify Kafka cluster health
2. Confirm the topic exists and is writable in Kafka UI
3. Check network latency between the service and brokers

---

## 4. Log Keywords

| Keyword | Meaning |
|--------|------|
| `Realtime Transcribe Service: Started` | Startup completed successfully |
| `Transport: Connection established` | A WebSocket connection was accepted |
| `StateMachine.prepare` | Redis Lua pre-check result |
| `StateMachine.commit` | Sequence state advanced |
| `Kafka: Sent` | The message was written to Kafka |
| `Orchestrator: Idempotent replay hit, returning ACK directly` | A duplicate packet was short-circuited |
| `Orchestrator: Sequence number out of order` | An out-of-order packet was rejected |
| `Shutdown: Starting graceful shutdown` | The service began shutdown handling |

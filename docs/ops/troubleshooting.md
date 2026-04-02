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

### Local `ws` vs production `wss`

The [API contract §1.1](../design/api-contract.md) specifies **`wss`** (TLS) as the normative transport for V1. Local `docker compose` and default Uvicorn HTTP use **`ws://`** only for developer convenience. Production or external clients must use TLS (ingress, load balancer, or mTLS as required) and connect with **`wss`**.

### Handshake rejected with HTTP 401

**Cause:** handshake authentication is enabled and the client did not send a valid `Authorization: Bearer <JWT>` header. The service rejects the request with `E1010` before the WebSocket upgrade completes.

**Resolution:** confirm `AUTH_ENABLED`, send a non-expired HS256 Bearer token, and make sure the client and service use the same signing material.

### Handshake rejected with HTTP 503

**Causes (both use application code `E1008`; use `error.message` / `error.details` to tell them apart):**

1. **Service draining** — rolling deploy or scale-in; the process is rejecting new upgrades while existing sockets are closed.
2. **Ownership guard store unavailable** — Redis (or the configured guard backend) raised while claiming send ownership during handshake admission.

**Resolution:** for (1), reconnect to a healthy task once it is ready. For (2), fix Redis connectivity or credentials, then retry; see also [protocol-scenario-matrix.md §1 E-11 (pre-handshake)](../design/protocol-scenario-matrix.md).

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

In JSON logs, fixed context is often grouped under an `identity` object (`service`, `version`, `conversation_id`). Events may still include `conversation_id` at the top level when bound per call. See [configuration.md — Logging (structured output)](../config/configuration.md#logging-structured-output).

| Keyword | Level | Meaning |
|--------|-------|---------|
| `Realtime Transcribe Service: Started` | INFO | Startup completed successfully |
| `Transport: Connection established` | INFO | WebSocket accepted after handshake |
| `Transport: Slow message stage timings` | WARNING | End-to-end handling exceeded `LOG_SLOW_MESSAGE_THRESHOLD_MS` (rate-limited: at most one emit per rolling second per process) |
| `StateMachine.prepare` | DEBUG | Redis Lua pre-check outcome (`result`, `expected_sequence`, …) |
| `StateMachine.commit` | DEBUG | Sequence advanced after Kafka ACK (`next_expected`, …) |
| `StateMachine.cleanup` | INFO | `SESSION_COMPLETE` TTL shrink completed |
| `Kafka: Sent` | DEBUG | Kafka acknowledged the send (high volume; not at INFO) |
| `Kafka: Send timed out` / `Kafka: Send failed` | ERROR | Downstream send failure (see `E1011` / `E1008` in contract) |
| `Orchestrator: Idempotent replay hit, returning ACK directly` | INFO | Duplicate `(conversationId, seq)` short-circuited |
| `Orchestrator: Sequence number out of order` | WARNING | Out-of-order seq (`E1006`) |
| `Shutdown: Starting graceful shutdown` | INFO | Shutdown handling began |

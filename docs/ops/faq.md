# FAQ

---

## Q1. Why does startup fail when Redis or Kafka is unavailable?

Startup performs explicit Redis and Kafka readiness checks. If either dependency is unreachable, the process exits before serving traffic.

**What to do:** make sure `docker compose up -d` has started the local stack, or verify that `REDIS_URL` and `KAFKA_BOOTSTRAP_SERVERS` point to reachable services.

---

## Q2. What causes `E1006` sequence out-of-order errors?

The Redis Lua state machine atomically validates sequence numbers. If `sequenceNumber > expected`, the state machine returns `OUT_OF_ORDER`, the server sends an `ERROR` frame, and the connection closes with code `1008`.

**What to do:** ensure Fano Assist sends `sequenceNumber` values strictly in ascending order for the same `conversationId`. After reconnecting, the server resumes from the expected sequence already stored in Redis.

---

## Q3. What happens if Kafka is down?

Kafka send timeout or failure results in `ERROR` (`E1008` or `E1011`) plus close code `1013`. The Redis commit step is skipped, so the expected sequence does not advance. After reconnecting, the client retries the same sequence number and the Redis pre-check still accepts it. That is how the service preserves lossless retry semantics.

---

## Q4. How are duplicate messages handled?

If the same `(conversationId, sequenceNumber)` arrives again, the state machine returns `IDEMPOTENT`. The service replies with the matching success ACK without writing to Kafka again and without advancing Redis state. `SESSION_ONGOING` maps to `TRANSCRIPT_ACK`; `SESSION_COMPLETE` maps to `EOL_ACK`.

---

## Q5. How is Kafka ordering preserved?

`conversationId` is used as the Kafka partition key, so each conversation is routed to one partition and remains ordered within that partition.

---

## Q6. How does graceful shutdown work?

On `SIGTERM`, the service marks itself as draining and rejects new connections, sends close code `1001` to existing connections, flushes Kafka, waits for the server loop to exit, and then releases Redis resources. The full shutdown budget is controlled by `STOP_TIMEOUT`. If that budget is exceeded, the service stops waiting and moves into forced cleanup.

---

## Q7. How long can a client ignore Ping/Pong before the service disconnects it?

The service uses the Uvicorn `websockets` stack for keepalive. After each server Ping, if the peer does not respond with Pong within `WS_PING_TIMEOUT` seconds (default `10s`), the connection is closed, typically with close code `1011`.

Because the first Ping is sent after `WS_PING_INTERVAL` seconds (default `20s`), a client that never replies to Pong is usually disconnected after about `30s` in total. See `docs/design/api-contract.md` section 1.4 for the canonical definition.

---

## Q8. What client-side rules are mandatory?

To preserve ordering, idempotency, and lossless retry behavior, clients must follow these rules:

- The WebSocket handshake must include the `conversationId` query parameter
- If the body includes `metaData.conversationId`, it must exactly match the handshake query
- Only one active sending connection may exist for the same `conversationId`
- `sequenceNumber` must start at `0` and advance strictly as `0, 1, 2, 3, ...`
- After an `ERROR`, a `1008`/`1013` close, or an ACK timeout, the client must reconnect and retry the same unacknowledged `sequenceNumber`
- Retries must keep the same `(conversationId, sequenceNumber)` idempotency key
- `SESSION_ONGOING` requires `callEndTimeStamp=null`
- `SESSION_COMPLETE` requires `callEndTimeStamp` and acts as the EOL control frame with `payload.speaker=System`
- `payload.isFinal` must always be `true`; interim transcripts are out of contract
- All required fields, timestamp formats, enum values, and conditional field rules must satisfy the API contract

For the canonical error-code matrix and normal/error flow examples, see:

- `docs/design/api-contract.md`
- `docs/design/protocol-scenario-matrix.md`

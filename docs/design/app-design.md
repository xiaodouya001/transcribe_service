# Realtime Transcribe Service Architecture Overview

---

## 1. Design Overview

### 1.1 Business Context

As the contact center platform evolves toward real-time AI assistance, Fano Assist acts as the client of the ASR engine. The engine itself is hosted and operated by the bank in its GCP environment.

**Realtime Transcribe Service** runs in the bank's AWS environment and serves as the real-time data gateway between the upstream transcription stream in GCP and the downstream data ecosystem in AWS.

### 1.2 Business Objectives

| Objective | Target |
| --- | --- |
| **Concurrent sessions** | 700 to 1,000 simultaneous calls during the morning peak (design target) |
| **End-to-end latency** | TBC, measured from GCP ingress to AWS Kafka |
| **Data integrity** | Strict ordering with zero data loss |

### 1.3 Scope Boundaries

| Category | Scope |
| --- | --- |
| **In scope** | Long-lived cross-cloud connection management; ordering enforcement based on `conversationId` and `sequenceNumber`; reliable delivery to Kafka |
| **Out of scope** | Audio streaming; business features such as intent recognition or sentiment analysis; downstream consumption logic, which is handled by Kafka consumers |

### 1.4 Architecture Highlights

- **Connection model**: Fano Assist acts as the WebSocket client and initiates the connection to Realtime Transcribe Service.
- **Ordering control**: Redis Conversation Ownership Guard plus Redis Sequence State Machine, backed by a two-phase commit flow.
- **Data path**: Fano Assist -> Realtime Transcribe Service -> Kafka. Downstream systems consume through Kafka consumer groups.

---

## 2. Architecture Overview

### 2.1 Deployment Topology

```mermaid
flowchart TB
    subgraph GCP ["GCP"]
        Assist["Fano Assist"]
    end

    subgraph AWS ["AWS"]
        subgraph ALB ["ALB"]
            ALB1["Realtime Transcribe Service ALB"]
        end
        subgraph ECS ["ECS Fargate"]
            Task0["Realtime Transcribe Service Task 0"]
            Task1["Realtime Transcribe Service Task 1"]
            TaskN["Realtime Transcribe Service Task N"]
        end
        subgraph Data ["Data Layer"]
            Redis["Redis: Ownership Guard + Sequence State Machine"]
            Kafka["Kafka"]
        end
    end

    Assist -->|"WebSocket client connection (WSS)"| ALB1
    ALB1 --> Task0
    ALB1 --> Task1
    Task0 --> Redis
    Task0 --> Kafka
    Task1 --> Redis
    Task1 --> Kafka
```

### 2.2 Core Sequence Diagrams

#### 2.2.1 Startup Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Main as main.py
    participant RedisCore as Redis (Ownership Guard + State Machine)
    participant Kafka as Kafka

    Main->>Main: Load settings and configure logging
    Main->>Main: create_runtime_bundle (service_runtime): shared Redis client, sequence machine, ownership guard, Kafka producer, orchestrator, ConnectionRegistry, FastAPI app, Uvicorn server
    Note over Main: Signal handlers are registered on GracefulShutdown inside the runtime bundle

    Main->>Main: Run Redis and Kafka startup checks in parallel
    par Redis startup check
        Main->>RedisCore: ping()
        alt Redis unavailable
            RedisCore-->>Main: Connection failure
            Main->>Main: Exit with startup failure log
        else Redis healthy
            RedisCore-->>Main: PONG
        end
    and Kafka startup check
        Main->>Kafka: ensure_ready()
        alt Kafka unavailable
            Kafka-->>Main: Connection failure
            Main->>Main: Exit with startup failure log
        else Kafka ready
            Kafka-->>Main: Ready
        end
    end

    Main->>Main: Record startup check duration
    Main->>Main: If AUTH_ENABLED, initialize JwtBearerAuthBackend
    Main->>Main: Start Uvicorn (FastAPI service on 0.0.0.0:8080)
```

#### 2.2.2 Main Processing Flow: Two-Phase Commit (`SESSION_ONGOING`)

```mermaid
sequenceDiagram
    autonumber
    participant Upstream as Fano Assist
    participant Trans as Realtime Transcribe Service
    participant RedisOwnership as Redis (Conversation Ownership Guard)
    participant RedisState as Redis (Sequence State Machine)
    participant Kafka as Kafka

    Upstream->>Trans: Start WebSocket handshake (conversationId, optional Authorization)
    alt AUTH_ENABLED and Authorization is missing or invalid
        Trans->>Trans: Validate Bearer JWT during handshake
        Trans-->>Upstream: Reject handshake (HTTP 401 + E1010)
    else Authentication passes or is disabled
        Trans->>RedisOwnership: claim_or_refresh(conversationId, ownershipToken) during handshake
        alt Ownership already held by another connection
            RedisOwnership-->>Trans: BUSY
            Trans-->>Upstream: Reject handshake (HTTP 403 + E1009)
        else Ownership acquired
            RedisOwnership-->>Trans: OWNED
            Trans->>Upstream: Accept WebSocket upgrade
            Trans->>Trans: Start background refresh loop
        end
    end

    Upstream->>Trans: Send SESSION_ONGOING (seq=N)
    Trans->>Trans: Decode request and validate schema
    Trans->>RedisState: Phase 1 - atomic pre-check (Lua script)

    alt seq < expected (duplicate)
        RedisState-->>Trans: IDEMPOTENT
        Trans-->>Upstream: Return TRANSCRIPT_ACK immediately
    else seq > expected (gap or out of order)
        RedisState-->>Trans: OUT_OF_ORDER
        Trans-->>Upstream: Return error and require resend
    else seq == expected
        RedisState-->>Trans: PRE_CHECK_OK (state not advanced yet)

        Trans->>Kafka: Phase 2 - async send
        Kafka-->>Trans: Delivery ACK

        Trans->>RedisState: Phase 3 - commit (advance expected sequence to N+1 and refresh TTL)
        RedisState-->>Trans: State updated

        Trans-->>Upstream: Return TRANSCRIPT_ACK (seq=N)
    end

    Note over Trans,RedisOwnership: While the connection stays alive, the service keeps refreshing ownership TTL in the background and releases ownership when the connection ends
```

#### 2.2.3 `SESSION_COMPLETE` and Connection Release

> The protocol identifies `SESSION_COMPLETE` by `eventType=SESSION_COMPLETE` together with `payload.speaker=System`. In examples, `payload.transcript` is often `"EOL"`, but the server does not enforce a fixed literal.

```mermaid
sequenceDiagram
    autonumber
    participant Upstream as Fano Assist
    participant Trans as Realtime Transcribe Service
    participant RedisOwnership as Redis (Conversation Ownership Guard)
    participant RedisState as Redis (Sequence State Machine)
    participant Kafka as Kafka

    Note over Trans,RedisOwnership: Ownership is claimed during handshake and refreshed while the connection is alive

    Upstream->>Trans: Send SESSION_COMPLETE (seq=M, system EOL control event)
    Trans->>Trans: Validate schema and recognize completion event
    Trans->>RedisState: Phase 1 - final sequence pre-check (Lua script)

    alt Sequence mismatch (out of order or stale replay)
        RedisState-->>Trans: Error result
        Trans-->>Upstream: Return ERROR (seq=M)
        Trans->>Trans: Mark the session abnormal and prepare forced disconnect
    else Sequence matches
        RedisState-->>Trans: PRE_CHECK_OK

        Trans->>Kafka: Phase 2 - send the EOL control event
        Kafka-->>Trans: Delivery ACK

        Trans->>RedisState: Phase 3 - commit (expected sequence becomes M+1)
        RedisState-->>Trans: Commit successful

        Trans->>RedisState: Phase 4 - shrink state-machine TTL into the 30 to 60 second grace window
        alt cleanup succeeds
            RedisState-->>Trans: TTL shortened
        else cleanup fails
            RedisState-->>Trans: Error
            Trans->>Trans: Emit warning but do not reverse the successful commit
        end

        Trans-->>Upstream: Return EOL_ACK (seq=M)
    end

    Trans->>RedisOwnership: release(conversationId, ownershipToken)
    RedisOwnership-->>Trans: released or no-op
    Trans->>Trans: Release coroutine resources and call WebSocket.close()
    Trans->>Upstream: Close the WebSocket proactively (Close Code 1000)
    Upstream-->>Trans: Confirm close handshake
```

#### 2.2.4 Exception Handling and Error Response

When validation fails or a downstream dependency becomes unavailable, Realtime Transcribe Service sends an `ERROR` frame first and then closes the WebSocket according to policy. See [API Contract Section 4](api-contract.md#4-status-codes-and-error-codes) for the error-code and close-code mapping.

```mermaid
sequenceDiagram
    autonumber
    participant Upstream as Fano Assist
    participant Trans as Realtime Transcribe Service
    participant RedisOwnership as Redis (Conversation Ownership Guard)
    participant RedisState as Redis (Sequence State Machine)
    participant Kafka as Kafka

    Upstream->>Trans: Send message (SESSION_ONGOING or SESSION_COMPLETE)
    Trans->>Trans: Handshake admission already passed before message processing starts
    Trans->>Trans: Background ownership refresh continues during request processing
    Trans->>Trans: Validate schema and orchestrate the request

    alt Unhandled exception at any processing stage (E1007)
        Trans->>Trans: Unexpected internal exception
        Trans-->>Upstream: Send ERROR frame (E1007)
        Trans->>Upstream: Close connection (Close Code 1011)
    else Ownership refresh store unavailable (E1008)
        Trans->>RedisOwnership: refresh
        RedisOwnership-->>Trans: Error
        Trans-->>Upstream: Send ERROR frame (E1008)
        Trans->>Upstream: Close connection (Close Code 1013)
    else Ownership refresh detects conflict (E1009)
        Trans->>RedisOwnership: refresh
        RedisOwnership-->>Trans: BUSY
        Trans-->>Upstream: Send ERROR frame (E1009, only one sender connection is allowed)
        Trans->>Upstream: Close connection (Close Code 1008)
    else Schema or business-rule validation fails (E1002/E1003/E1004/E1005/E1009)
        Trans-->>Upstream: Send ERROR frame (code, message, details)
        Trans->>Upstream: Close connection (Close Code 1008)
    else Duplicate message (IDEMPOTENT)
        Trans->>RedisState: Phase 1 - atomic pre-check (Lua script)
        RedisState-->>Trans: IDEMPOTENT
        Trans-->>Upstream: Return the matching success ACK immediately
    else Out-of-order sequence (E1006)
        Trans->>RedisState: Phase 1 - atomic pre-check (Lua script)
        RedisState-->>Trans: OUT_OF_ORDER
        Trans-->>Upstream: Send ERROR frame (E1006)
        Trans->>Upstream: Close connection (Close Code 1008)
    else Downstream unavailable or timed out (E1008 or E1011)
        Trans->>RedisState: Phase 1 - atomic pre-check (Lua script)
        RedisState-->>Trans: PRE_CHECK_OK
        Trans->>Kafka: Phase 2 - async send
        Kafka-->>Trans: Timeout or send failure
        Trans-->>Upstream: Send ERROR frame (E1008 or E1011)
        Trans->>Upstream: Close connection (Close Code 1013)
    else Normal success path
        Trans->>RedisState: Phase 1 - atomic pre-check (Lua script)
        RedisState-->>Trans: PRE_CHECK_OK
        Trans->>Kafka: Phase 2 - async send
        Kafka-->>Trans: ACK
        Trans->>RedisState: Phase 3 - commit
        RedisState-->>Trans: State advanced
        Trans-->>Upstream: Return the matching success ACK
    end
```

#### 2.2.5 Graceful Shutdown Sequence

```mermaid
sequenceDiagram
    autonumber
    participant AWS as AWS Fargate / ECS
    participant Trans as Realtime Transcribe Service
    participant Upstream as Fano Assist
    participant RedisState as Redis (Sequence State Machine)
    participant RedisOwnership as Redis (Conversation Ownership Guard)
    participant Kafka as Kafka

    AWS->>Trans: Send SIGTERM

    Trans->>Trans: Enter drain mode and stop accepting new connections
    Trans->>Trans: Inspect active sessions and prepare migration signals

    Trans->>Trans: Build close code 1001 (Going Away)
    Trans->>Upstream: Send WebSocket close frame (Code 1001)

    Trans->>Trans: Wait for in-flight async tasks to finish
    Trans->>Kafka: Run Producer.flush()
    Kafka-->>Trans: Confirm all buffered messages are persisted

    Trans->>RedisState: Release the state-machine Redis pool explicitly
    Trans->>RedisOwnership: Close the ownership-guard Redis client explicitly
    Trans->>Trans: Finish cleanup and exit safely (Exit 0)
```

---

## 3. Core Design

### 3.1 Modules and Responsibilities

The application follows a dependency-inversion architecture. The orchestrator sits at the center, while network I/O, protocol validation, and storage integrations are isolated behind interface contracts.

| Module | Primary Responsibility | Allowed Core Actions | Architectural No-Go Zone |
| --- | --- | --- | --- |
| `main.py` | Process entrypoint and shutdown orchestration | Load settings; configure logging; `create_runtime_bundle`; parallel Redis/Kafka startup checks; run Uvicorn; on exit, `close_all` + `flush` then `close_runtime_bundle` | No dependency graph assembly details; no per-message JSON parsing |
| `service_runtime.py` | `RuntimeBundle` assembly and teardown | `create_runtime_bundle`, `build_web_app`, `create_uvicorn_server`, `close_runtime_bundle` | No WebSocket message loop; no orchestrator business rules beyond wiring |
| `config/` | Environment-backed settings and logging bootstrap | Load and validate `Settings` (Pydantic Settings / `.env`); derive local defaults vs. deployed required keys; configure structlog and stdlib logging (`LOG_LEVEL`, `LOG_FORMAT`) | No WebSocket or protocol handling; no Redis, Kafka, or orchestration calls |
| `auth/` | Handshake authentication boundary | Validate `Authorization: Bearer <JWT>` during handshake and expose the authenticated subject to the transport scope; `auth/runtime.py` builds the backend from settings | No ownership claims, no sequence advancement, and no Kafka/orchestrator calls |
| `schemas/` | Protocol contract and validation layer | Validate fields, types, timestamps, and business rules; build standard responses; `error_codes.py` / `error_scenarios.py` map contract codes to HTTP/WS close behavior | No network I/O and no data-store calls |
| `converter/` | Kafka outbound conversion layer | Build `KafkaOutboundMessage` from validated `InboundMessage`, set `enrich.eventProduceTimestamp` immediately before `producer.send`, and validate outbound schema | Must not perform network I/O or mutate caller input |
| `utils/` | Shared utility helpers | Provide reusable pure helpers such as canonical UTC timestamp formatting | No business orchestration and no network/data-store I/O |
| `transport/` | WebSocket ingress layer | `app.py`: handshake admission ASGI middleware and `create_app` factory; `session.py`: per-connection receive loop, schema/consistency checks, ERROR frames and close codes; `registry.py`: active sockets; `metrics.py`: runtime counters | No embedded two-phase commit or Redis/Kafka logic beyond delegating to the orchestrator and shared backends |
| `redis/ownership_guard.py` | Conversation ownership control | Claim, refresh, and release send ownership for a conversation | No sequence advancement, field validation, or message delivery |
| `redis/runtime.py` | Redis client and guard factories | Shared async Redis client, `create_sequence_state_machine`, `create_ownership_guard`, ordered `close_redis_runtime` | No WebSocket or Kafka calls |
| `redis/sequence_state_machine.py` | Sequence state machine | Atomic Lua pre-check and state advancement; manage active and final TTL | No Kafka awareness and no downstream business logic |
| `producer/` | Kafka delivery layer | Async send, partition routing, and send-timeout handling; `producer/runtime.py` builds the producer from settings | Must not mutate the original message payload |
| `orchestrator/` | Two-phase commit orchestration | Call state pre-check, invoke Kafka send, commit state, and return ACK | Depends only on `protocols.py` abstractions, not concrete implementations |

### 3.2 Technology Stack and Concurrency Model

| Component | Choice |
| --- | --- |
| **Framework** | FastAPI (ASGI) + Uvicorn |
| **Async ecosystem** | `redis.asyncio`, `aiokafka` |
| **Concurrency model** | Single-threaded asyncio, one worker per vCPU |
| **WebSocket library** | `websockets` |

**Why this stack**: the workload is I/O-bound and latency-sensitive. Asyncio avoids unnecessary GIL contention and context-switch overhead, while one process per vCPU keeps horizontal scaling straightforward.

### 3.3 Connection Lifecycle and Keepalive

The handshake query parameter `conversationId` is the connection-level session identifier. Redis Conversation Ownership Guard enforces the rule that only one connection may actively send data for a conversation at any time.

| Mechanism | Design |
| --- | --- |
| **Connection identity** | The handshake query `conversationId` is the unique connection-level identifier. If `metaData.conversationId` is provided as a string in the message body, it must match the handshake value |
| **Handshake authentication** | When `AUTH_ENABLED=true`, the service validates `Authorization: Bearer <JWT>` before shutdown admission or ownership claim. Missing, malformed, invalid, or expired credentials reject the handshake with HTTP `401` + `E1010` |
| **Ownership acquisition** | During the handshake, the service calls `claim_or_refresh(conversationId, ownershipToken)` before accepting the WebSocket upgrade |
| **Conflict handling** | If ownership is already held by another connection, the service rejects the handshake immediately with HTTP `403` and `E1009`; the request never reaches the orchestrator |
| **Liveness during the session** | After the connection is established, a background task keeps refreshing the ownership TTL. If refresh detects a conflict or the backing store becomes unavailable, the service sends `ERROR` and closes the connection |
| **Release timing** | Ownership is released on successful `SESSION_COMPLETE`, client disconnect, or server-side teardown after an abnormal end |
| **Business events** | `SESSION_ONGOING` and `SESSION_COMPLETE` (the final EOL control event) |
| **Protocol keepalive** | Server sends WebSocket Ping every `ping_interval` (default 20s; first Ping after connection uptime reaches that interval); client must Pong within `ping_timeout` (default 10s) per Ping—see API Contract §1.4. Keeps traffic below typical ALB 60-second idle timeout |

### 3.4 Redis as the Session Control Plane

All control state is held in Redis; there is no local in-memory session state. Redis responsibilities are split into two layers:

| Redis Component | Scope | Core Responsibility | Key Operations | Explicitly Not Responsible For |
| --- | --- | --- | --- | --- |
| **Conversation Ownership Guard** | Connection level | Guarantee that only one active sender owns a given `conversationId` at a time | `claim_or_refresh`, `refresh`, `release` | Sequence advancement, Kafka delivery, or business-field validation |
| **Sequence State Machine** | Message level | Guarantee that `sequenceNumber` advances exactly as expected and manage active and final TTL | `prepare`, `commit`, `cleanup` | Deciding who owns the send connection |

Ownership Guard answers **who is allowed to send**, while Sequence State Machine answers **whether the next message is the correct one to send**. Together they form the Redis control plane for the session.

The table below describes the message-level advancement semantics of the **Sequence State Machine**, implemented with atomic Lua scripts.

#### 3.4.1 Pessimistic Locking (`SET NX`) vs. Optimistic Sequencing (Lua + Sequence)

##### 3.4.1.1 Pessimistic Locking

**Core idea**: serialize access through a lock. Only the worker that acquires the lock may proceed, and all others wait until the lock is released.

**Pros**

- Strong consistency: only one worker can process the call at a time.
- Simple success path once the lock is acquired: no sequence replay logic is needed.

**Cons**

- Latency overhead: every 200 ms audio-derived transcript slice would require lock, process, unlock.
- Lock risk: if a worker stalls after acquiring the lock, the transcription flow can be blocked until the lock expires or is cleaned up.
- Poor fit for real-time traffic: queueing delays accumulate under load and hurt live-call responsiveness.

##### 3.4.1.2 Optimistic Sequencing

**Core idea**: let processing stay concurrent, and decide validity at commit time by comparing sequence numbers. Late or stale messages are rejected by policy.

**Pros**

- Very low latency: no blocking queue, only an atomic Lua validation step.
- High throughput: ideal for frequent writes where the critical check is a numeric comparison.
- Natural deduplication: duplicate or late packets are filtered by `sequenceNumber`.

**Cons**

- Intentional rejection of stale data: a late message can be dropped in order to preserve sequence correctness.
- The client contract is slightly stricter: resend and compensation behavior must follow the protocol rules.

| Dimension | Pessimistic Locking (`SET NX`) | Optimistic Sequencing (Lua + Sequence) | Design Choice |
| --- | --- | --- | --- |
| Conflict model | Treat every conflict as blocking | Allow invalid attempts to fail fast | **Optimistic sequencing** |
| Response-time target | Can tolerate queueing | Extremely latency-sensitive | **Optimistic sequencing** |
| Operational risk | Lock cleanup and deadlock handling | Replay, duplicate, and out-of-order control | **Optimistic sequencing** |
| Primary concern | Absolute serialized access | Ordering and deduplication | **Optimistic sequencing** |

### 3.5 Two-Phase Commit

The system does not rely on a distributed transaction manager. Instead, it achieves consistency by delaying state advancement until Kafka confirms persistence.

1. **Prepare**: Fano Assist sends `seq=5`. Realtime Transcribe Service runs the Lua pre-check.
2. **Persistence**: The converter assembles the Kafka value (`metaData + payload + enrich`); the service writes it to Kafka with `acks=all`.
3. **Commit**: After Kafka ACK, Redis advances the expected value to `6`.
4. **ACK**: The service returns the corresponding success ACK (`TRANSCRIPT_ACK` for a transcript event, `EOL_ACK` for a completion event).
5. **Failure behavior**: If Kafka write fails, step 3 does not run. When the upstream retries `seq=5`, Redis still expects `5`, so the pre-check succeeds again and the retry remains lossless.

| Phase | Operation |
| --- | --- |
| **Prepare** | Lua pre-check verifies that `payload.sequenceNumber` matches `{REDIS_SEQUENCE_STATE_KEY_PREFIX}:{conversationId}` without incrementing it |
| **Persistence** | Converter-assembled Kafka value, then Kafka write keyed by `conversationId`, with `acks=all` |
| **Commit** | After Kafka ACK, increment `{REDIS_SEQUENCE_STATE_KEY_PREFIX}:{conversationId}` |
| **ACK** | Return `TRANSCRIPT_ACK` or `EOL_ACK` based on the processed event |

### 3.6 Container Replacement and Graceful Shutdown

When ECS Fargate triggers a rolling deployment or scales the service down, the system must drain long-lived connections without dropping validated data.

- After receiving `SIGTERM`, Realtime Transcribe Service stops accepting new connections immediately.
- The service sends Close Code `1001` to existing WebSocket connections so the upstream can reconnect to a healthy node.
- The process blocks shutdown until the last validated in-memory records are flushed to Kafka, protecting the zero-loss guarantee during task replacement.

| Step | Action |
| --- | --- |
| 1 | Stop accepting new connections after `SIGTERM` |
| 2 | Send Close Code `1001` to existing connections |
| 3 | Flush the Kafka producer buffer |
| 4 | Exit only after in-flight messages are persisted |

---

## 4. Infrastructure and Capacity

### 4.1 Kafka Constraints

| Item | Configuration | Description |
| --- | --- | --- |
| Topic | `AI_STAGING_TRANSCRIPTION` | Default topic name |
| Partition key | `conversationId` | Keeps each conversation pinned to one partition |
| Message key | `conversationId` | UTF-8 bytes |
| Message value | JSON containing `metaData + payload + enrich` | `enrich.eventProduceTimestamp` is generated immediately before each Kafka send attempt |
| Partition count | 50 or 100 | Decided when the topic is provisioned in each environment |
| `acks` | `all` | Required before the service can commit Redis state |
| Compression | `zstd` | Default compression strategy |

The full Kafka write contract is documented in [api-contract.md](api-contract.md#6-kafka-persistence-contract).

### 4.2 Redis Constraints

| Item | Configuration | Description |
| --- | --- | --- |
| Sequence state key | `{REDIS_SEQUENCE_STATE_KEY_PREFIX}:{conversationId}` | Stores the next expected `sequenceNumber` (prefix is settings-driven, not environment-specific in code) |
| Ownership guard key | `{REDIS_OWNERSHIP_GUARD_KEY_PREFIX}:{conversationId}` | Enforces the single-sender rule |
| Value | Sequence state: integer string; ownership guard: ownership token | Used for sequence advancement and sender ownership respectively |
| Update strategy | Lua pre-check plus commit, and `SET NX`-based lease renewal | Keeps sequence control and single-sender enforcement atomic enough for the use case |
| TTL | Ownership guard TTL defaults to 30 seconds; active TTL defaults to 3600 seconds; final TTL defaults to 60 seconds | All values are environment-configurable |
| Memory footprint | Small | Only the minimum session-level control state is stored |

---

## Appendix A - API Contract Reference

> The canonical API contract is documented in [Realtime Transcribe Service API Contract](api-contract.md).

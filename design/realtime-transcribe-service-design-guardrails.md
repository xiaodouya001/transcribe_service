# Design Guardrails

This document prevents the project from drifting away from its original design as the service evolves.

It does not replace the detailed design documents. Its role is to freeze the long-lived design invariants and testing guardrails that must remain stable, so reviews, iterations, and implementation changes can be evaluated against a fixed baseline.

---

## 1. Confirmed Scope Boundaries

The two items below are explicitly **out of current defect scope**. If they need to be delivered, they must be handled as separate work items and must not be introduced incompletely as part of routine code changes:

- Authentication and authorization (`Authorization` / token validation) are not implemented yet. `Authorization` and `E1010` remain reserved in the contract only.
- Kafka `max_in_flight=1` and a circuit breaker are not implemented yet.

---

## 2. Core Design Invariants

The items below define the long-term design semantics that must remain stable. Code, tests, and documentation should stay aligned around these constraints.

### 2.1 Protocol and Validation

- Every inbound message must pass schema validation, and all timestamp fields must use ISO-8601 UTC.
- The handshake query parameter `conversationId` is the connection-level identity.
- `metaData` carries session-level fields only. `agentId` and `customerId` belong to `payload` and are conditionally required based on `speaker`.
- If `metaData.conversationId` exists in the message body and is a string, it must match the handshake query value. A mismatch must be rejected directly in the transport layer as `E1009 + 1008`.
- Only one connection may send messages for the same `conversationId` at any moment. If a new connection conflicts with an existing sender, the handshake must fail with HTTP `403` + `E1009`, and the request must not enter the orchestrator.
- Missing fields, type errors, enum violations, and business-rule violations must continue to map to their established error codes. Implementation details must not silently change those mappings.

### 2.2 State Machine and Sequence Semantics

- Within one `conversationId`, `sequenceNumber` must advance strictly according to the state machine.
- Duplicate messages (`IDEMPOTENT`) must return the matching success ACK immediately. They must not write to Kafka and must not advance Redis state.
- A successful `SESSION_ONGOING` transcript returns `TRANSCRIPT_ACK`. A successful `SESSION_COMPLETE` EOL control frame returns `EOL_ACK`.
- Skipped or out-of-order messages (`OUT_OF_ORDER`) must return `E1006 + 1008`.
- `prepare` does not advance state. Only `commit` advances the expected sequence.

### 2.3 Two-Phase Commit and Lossless Retry

- The success path must remain: `prepare -> Kafka outbound assembly (converter) -> Kafka send -> commit -> ACK`.
- The Kafka outbound contract is `metaData + payload + enrich`, and `enrich.eventProduceTimestamp` must be regenerated before every `producer.send` attempt.
- The producer must stay transport-only: it delivers data but does not mutate payloads. Kafka enrichment is allowed only in the converter layer.
- On Kafka timeout or failure, the service must not commit, must return an error, and must allow the upstream client to retry the same seq.
- This "lossless retry after downstream failure" behavior is one of the core architectural promises.

### 2.4 Session Completion and TTL

- Active session keys use the active TTL. The default is 1 hour.
- The key TTL is shortened to the final TTL only after `SESSION_COMPLETE` is received.
- Abnormal client disconnects do not trigger cleanup proactively, so the key remains until the active TTL expires.
- `SESSION_COMPLETE` is a system-level EOL control event. It no longer means "the last transcript sentence."
- The required processing semantics for `SESSION_COMPLETE` are: Kafka succeeds, Redis commit succeeds, cleanup runs, and `EOL_ACK` is returned.
- If `cleanup()` fails after Kafka and commit have already succeeded, the result must be treated as a **warning downgrade**: still return `EOL_ACK`, still close normally with `1000`, and treat cleanup as a post-commit optimization rather than a primary transaction failure.

### 2.5 Graceful Shutdown

- The graceful shutdown order is fixed: stop accepting new connections, send `1001` to existing connections, flush Kafka, then release resources and exit.
- That order is itself a design constraint and must not be altered by "harmless refactors."

---

## 3. High-Value Guardrails Already Landed

Beyond 100% coverage, the project already includes the following high-value guardrails at the design-invariant level.

### 3.1 Lossless Retry Loop After Kafka Failure

Existing tests lock the following semantics:

- First request: `prepare OK -> Kafka fail -> return E1008/E1011 -> no commit`
- Retrying the same `conversationId + seq` still passes `prepare`
- After the retry succeeds, the service returns the matching ACK and performs Kafka send plus commit
- Replaying the same old seq again after success hits the idempotent ACK path and does not write Kafka again

These tests directly protect the core design line: two-phase commit plus lossless retry.

### 3.2 TTL and Resume Behavior After Abnormal Disconnect

Existing tests lock the following semantics:

- After an abnormal client disconnect, the key still keeps the active TTL
- Reconnecting within the active TTL window can continue from the next seq
- Replaying an old seq within that window still hits the idempotent ACK path

These tests directly protect the design choice of preserving state across disconnects.

### 3.3 Graceful Shutdown Order

Existing tests lock the following order:

- `close_all -> flush -> close producer/redis_sequence_state_machine/redis_ownership_guard`

These tests protect the shutdown sequence itself, not just the fact that the methods were called.

### 3.4 Post-Completion Failure Semantics for `SESSION_COMPLETE`

Existing tests lock the following semantics:

- After Kafka send and Redis commit succeed, the service still returns the final ACK even if `cleanup()` fails
- The final success response for `SESSION_COMPLETE` is `EOL_ACK`
- The connection still closes normally with `1000`
- A cleanup failure remains a warning and does not flip an already successful primary transaction

### 3.5 Contract-Level Scenario Matrix

The contract-level scenario matrix is consolidated in:

- [realtime-transcribe-service-protocol-scenario-matrix.md](realtime-transcribe-service-protocol-scenario-matrix.md)
- Matching tests: [test_contract_matrix.py](../tests/test_contract_matrix.py)

The matrix covers the following key scenarios:

- invalid JSON -> `E1001 + 1007`
- invalid enum -> `E1002 + 1008`
- missing field -> `E1003 + 1008`
- wrong type -> `E1004 + 1008`
- invalid UTC timestamp -> `E1005 + 1008`
- duplicate seq -> matching ACK + no disconnect
- out-of-order -> `E1006 + 1008`
- internal exception -> `E1007 + 1011`
- downstream failure or timeout -> `E1008/E1011 + 1013`
- `conversationId` mismatch -> `E1009 + 1008`
- business-rule violation -> `E1009 + 1008`
- concurrent sender conflict at handshake -> HTTP `403` + `E1009`

These tests intentionally avoid implementation detail checks and instead protect the protocol contract directly.

---

## 4. Minimum Process Constraints for Changes

When modifying business logic, the following minimum process must be followed.

### 4.1 Three Questions That Must Be Answered Before a Change

Before every change, make the following explicit:

1. Which design invariants does this change touch?
2. Which tests are being added or updated to prove the design still holds?
3. If behavior changes, which documents must be updated in sync?

If these three questions cannot be answered, the business code should not be changed yet.

### 4.2 Prefer Keeping Tests, Documentation, and Implementation in Sync

Do not focus only on "making the code pass."

Preferred order:

1. Write or update tests first to express the target semantics
2. Change the implementation
3. Update the documentation last

This reduces the risk that implementation changes land before the team's shared design understanding catches up.

### 4.3 Distinguish Coverage from Design Guardrails

High coverage only proves that lines or branches ran. It does not automatically prove that the design is still intact.

When evaluating future changes, prioritize these two questions:

- Did the change touch a core design invariant?
- Were the matching scenario-level tests added or updated?

### 4.4 Use Hard Assertions for Critical Orderings and Code Mappings

The following categories are especially easy to break accidentally and therefore require explicit assertions:

- call ordering
- error-code and close-code mappings
- retry and idempotency semantics
- TTL transition timing
- responsibility boundaries between transport and orchestrator

### 4.5 Run a Contract Review After Every Meaningful Change

After any non-trivial change, review at least the following documents to confirm that they are still true:

- `design/realtime-transcribe-service-api-contract.md`
- `design/realtime-transcribe-service-app-design.md`
- `docs/faq.md`
- any `docs/*.md` files affected by the behavior change

If code and documentation diverge, update the documentation first or explicitly declare a design change.

### 4.6 Contract-First Rule

If the UI, tests, and implementation conflict with the API contract, the source of truth is [realtime-transcribe-service-api-contract.md](realtime-transcribe-service-api-contract.md).

All other documents and mock tools must follow the contract and must not reinterpret contract semantics.

---

## 5. Maintenance Principles

This file is the long-term maintenance baseline. It is no longer just a suggestion list.

As the project evolves, the following principles should be followed first:

1. When a new design constraint appears, add it to the "Core Design Invariants" section of this file first.
2. When a new behavior change lands, add a scenario-level guardrail test before chasing coverage numbers.
3. Any change that affects error codes, close codes, TTL timing, shutdown order, or idempotency and retry semantics must update this file and the related contract documents in sync.
4. If a design assumption is no longer valid, modify this file directly instead of letting chat logs or temporary conclusions continue to act as the source of truth.

---

## 6. How to Use This Document

This document is intended for the following recurring review points:

- before changing business logic
- during code review
- after fixing a production issue and adding regression tests
- when updating design documentation

If the design changes, update the invariants and the landed guardrails in this document before continuing with the next implementation iteration.

# Protocol Scenario Matrix

This document consolidates the key WebSocket protocol scenarios across normal flow, error flow, pre-handshake rejection, close codes, and representative JSON responses. It serves as:

- a companion view to the [API contract](api-contract.md)
- the concrete scenario-matrix document referenced by the long-term guardrails in [design-guardrails.md](design-guardrails.md)
- the documentation-side counterpart of the contract-level matrix tests in [../../tests/test_contract_matrix.py](../../tests/test_contract_matrix.py)

If this matrix conflicts with the [API contract](api-contract.md), the API contract wins.

Scenario identifiers **E-01** … **E-17** match the normative matrix in the API contract **§4.3.2**; worked JSON in this file is illustrative.

Business constraints such as handshake/body `conversationId` matching, continuous sequence numbers starting at `0`, single-sender semantics, and retrying the same failed sequence number are defined by **section 2.3 Business Rules** in the API contract. This matrix focuses on mapping **scenario -> error code / close code**.

---

## 1. Error Flow Matrix

| **ID** | **Scenario** | **Handshake stage** | **Error code** | **HTTP status** | **WS close code** | **Disconnected?** | **JSON example** |
| ------ | ------------------------------------------------------------------ | -------- | --------- | ------------ | --------------------------- | ------- | ----------- |
| E-01   | Missing `conversationId` query parameter | Pre-handshake | **E1003** | **400** | - | Yes, handshake rejected | See E-01 below |
| E-02   | Service is draining | Pre-handshake | **E1008** | **503** | - | Yes, handshake rejected | See E-02 below |
| E-03   | Connection limit exceeded (`WS_MAX_CONNECTIONS`) | Pre-handshake | **E1008** | **429** | - | Yes, handshake rejected | See E-03 below |
| E-04   | JSON decode failed | Post-handshake | **E1001** | - | **1007** (`Invalid Payload`) | Yes | See E-04 below |
| E-05   | Invalid enum value, such as `eventType` | Post-handshake | **E1002** | - | **1008** (`Policy Violation`) | Yes | See E-05 below |
| E-06   | Missing required field, such as `payload.dialect` | Post-handshake | **E1003** | - | **1008** (`Policy Violation`) | Yes | See E-06 below |
| E-07   | Field type mismatch or unexpected extra field | Post-handshake | **E1004** | - | **1008** (`Policy Violation`) | Yes | See E-07 below |
| E-08   | Invalid timestamp format | Post-handshake | **E1005** | - | **1008** (`Policy Violation`) | Yes | See E-08 below |
| E-09   | Sequence number out of order | Post-handshake | **E1006** | - | **1008** (`Policy Violation`) | Yes | See E-09 below |
| E-10   | Kafka timeout | Post-handshake | **E1011** | - | **1013** (`Try Again Later`) | Yes | See E-10 below |
| E-11   | Kafka send failure or other downstream outage after the WebSocket session is up | Post-handshake | **E1008** | - | **1013** (`Try Again Later`) | Yes | See E-11 below |
| E-11   | Conversation ownership guard store error during handshake (for example Redis unavailable on `claim_or_refresh`) | Pre-handshake | **E1008** | **503** | - | Yes, handshake rejected | See E-11 (pre-handshake) below |
| E-12   | Unhandled exception in the orchestrator layer | Post-handshake | **E1007** | - | **1011** (`Internal Error`) | Yes | See E-12 below |
| E-13   | Unhandled exception in the transport layer | Post-handshake | **E1007** | - | **1011** (`Internal Error`) | Yes | See E-13 below |
| E-14   | Query `conversationId` does not match `metaData.conversationId` in the body | Post-handshake | **E1009** | - | **1008** (`Policy Violation`) | Yes | See E-14 below |
| E-15   | Business-rule validation failed, for example `SESSION_ONGOING` with `callEndTimeStamp` or `isFinal=false` | Post-handshake | **E1009** | - | **1008** (`Policy Violation`) | Yes | See E-15 below |
| E-16   | A second concurrent sender tries to use the same `conversationId` | Pre-handshake | **E1009** | **403** | - | Yes, handshake rejected | See E-16 below |
| E-17   | Missing or invalid Bearer JWT during the handshake | Pre-handshake | **E1010** | **401** | - | Yes, handshake rejected | See E-17 below |

> During pre-handshake rejection, the WebSocket connection has not been established yet, so the service cannot send a WebSocket text frame. Only HTTP + JSON is available. Post-handshake errors send a WebSocket `ERROR` frame and then close the connection with the mapped close code.

> **E-11** uses the same application code **E1008** in two shapes: post-handshake **ERROR + close 1013** (for example Kafka failure while sending), and pre-handshake **HTTP 503 + JSON** when the ownership guard store fails during admission.

---

## 2. Normal Flow Matrix

| **ID**   | **Scenario** | **Handshake stage** | **WS close code** | **Disconnected?** | **Response JSON** |
| -------- | --------------------- | --------- | --------------------- | ------- | --------------------------------------- |
| **N-01** | `SESSION_ONGOING` processed successfully | Post-handshake | - | No | See N-01 below |
| **N-02** | Idempotent replay hit | Post-handshake | - | No | See N-02 below |
| **N-03** | `SESSION_COMPLETE` processed successfully | Post-handshake | **1000** (`Normal`) | Yes | See N-03 below |
| **N-04** | Graceful shutdown `close_all` on existing connections | Post-handshake | **1001** (`Going Away`) | Yes | No JSON response; the server sends a WebSocket close frame directly |

---

## 3. Normal Response Examples

Field definitions follow **section 3.1** of the API contract, including optional `serverProcessingMs`. Request-side timestamp validation now uses `speakTimeStamp` and `transcriptGenerateTimeStamp`; the ACK / ERROR examples below intentionally keep response-side `createdAtTimeStamp`.

### N-01 `SESSION_ONGOING` succeeds

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "TRANSCRIPT_ACK" },
  "payload": {
    "sequenceNumber": 0,
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z",
    "serverProcessingMs": 1.23
  }
}
```

### N-02 Idempotent replay hit

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "TRANSCRIPT_ACK" },
  "payload": {
    "sequenceNumber": 0,
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z",
    "serverProcessingMs": 0.85
  }
}
```

> This example uses a duplicated `SESSION_ONGOING` frame. If the replayed frame were `SESSION_COMPLETE`, the success response type would be `EOL_ACK`.

### N-03 `SESSION_COMPLETE` succeeds

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "EOL_ACK" },
  "payload": {
    "sequenceNumber": 42,
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z",
    "serverProcessingMs": 1.56
  }
}
```

> The corresponding request frame is the system-level EOL control event: `eventType=SESSION_COMPLETE` with `payload.speaker=System`. In example data the transcript string is `"EOL"`, but the server does not validate that exact literal.

### N-04 Graceful shutdown `close_all`

This scenario does not produce a business JSON response. The server sends a WebSocket close frame with code **1001** to existing connections.

---

## 4. Error Response Examples

### E-01 Missing `conversationId` query parameter (HTTP 400)

```json
{
  "metaData": { "conversationId": "", "eventType": "ERROR" },
  "error": {
    "code": "E1003",
    "message": "Missing required field",
    "details": "Query parameter 'conversationId' is required",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-02 Service is draining (HTTP 503)

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1008",
    "message": "Service draining",
    "details": "Server is shutting down, try again later",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-03 Connection limit exceeded (HTTP 429)

```json
{
  "metaData": { "conversationId": "conv-2", "eventType": "ERROR" },
  "error": {
    "code": "E1008",
    "message": "Too many connections",
    "details": "Active 1 >= limit 1",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-04 JSON decode failed (close 1007)

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1001",
    "message": "Invalid JSON",
    "details": "unexpected character: line 1 column 1 (char 0)",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-05 Invalid enum value (close 1008)

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1002",
    "message": "Validation failed",
    "details": "eventType must be one of the allowed enum values",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-06 Missing required field (close 1008)

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1003",
    "message": "Validation failed",
    "details": "Field required: payload.dialect",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-07 Field type mismatch or unexpected extra field (close 1008)

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1004",
    "message": "Validation failed",
    "details": "metaData.conversationId must be a string",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-08 Invalid timestamp format (close 1008)

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1005",
    "message": "Validation failed",
    "details": "speakTimeStamp must be a valid ISO-8601 UTC timestamp",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

This mapping also covers schema-rejected extra fields, such as legacy request fields that are no longer part of the contract.

### E-09 Sequence number out of order (close 1008)

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1006",
    "message": "Sequence number out of order",
    "details": "sequenceNumber=5 is not expected",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-10 Kafka timeout (close 1013)

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1011",
    "message": "Downstream timeout",
    "details": "Kafka send timed out",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-11 Kafka failure or downstream unavailable (close 1013)

Typical case: Kafka send failure. A Redis or other dependency outage on the **message path** (after handshake) also maps to **E1008** + **1013**, which matches contract **§4.3.3** for `E1008`.

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1008",
    "message": "Downstream unavailable",
    "details": "KafkaError: ...",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-11 Conversation ownership guard store unavailable (HTTP 503)

If the service is configured with a conversation ownership guard and the backing store raises while claiming ownership during handshake admission, the upgrade is rejected with **HTTP 503 + E1008** (same scenario id **E-11**, different delivery than the post-handshake row above). No WebSocket session is established and no WebSocket close frame is sent.

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1008",
    "message": "Downstream unavailable",
    "details": "Conversation ownership guard store unavailable",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-12 Unhandled orchestrator exception (close 1011)

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1007",
    "message": "Internal server error",
    "details": "RuntimeError: boom",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-13 Unhandled transport exception (close 1011)

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1007",
    "message": "Internal server error",
    "details": "RuntimeError: boom",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-14 Query/body `conversationId` mismatch (close 1008)

Validation happens after JSON decode succeeds but before orchestration. If `metaData.conversationId` is a string and differs from the handshake query `conversationId`, the service returns **E1009** and closes with **1008**. The orchestrator is not called, and no Redis or Kafka write is attempted.

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1009",
    "message": "conversationId mismatch",
    "details": "metaData.conversationId must match query parameter 'conversationId' ('conv-1')",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-15 Business-rule validation failed (close 1008)

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1009",
    "message": "Validation failed",
    "details": "callEndTimeStamp must be null when eventType=SESSION_ONGOING",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-16 Concurrent sender conflict for the same conversation (HTTP 403)

Validation happens during handshake. If another active sending connection already owns the same `conversationId`, the service rejects the handshake directly with **HTTP 403 + E1009**. No WebSocket session is established, the orchestrator is never entered, and no WebSocket close frame is sent.

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1009",
    "message": "Only one sender connection is allowed",
    "details": "another connection is already sending messages for this conversation",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

---

### E-17 Missing or invalid Bearer JWT during the handshake (HTTP 401)

Validation happens during handshake. If deployment authentication is enabled and the caller does not send a valid `Authorization: Bearer <JWT>` header, the service rejects the handshake directly with **HTTP 401 + E1010**. No WebSocket session is established, the orchestrator is never entered, and no WebSocket close frame is sent.

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1010",
    "message": "Authentication failed",
    "details": "Bearer token is invalid",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

---

## 5. Error-Code Notes

- `E1008` is reused for several downstream and capacity cases. **E-11** is documented twice in the error matrix: post-handshake `ERROR + 1013` (for example Kafka failure) and pre-handshake `HTTP 503` when the ownership guard store fails during admission (overlapping error code with **E-02** service draining, which uses a different `message` / `details` shape)
- `E1009` is intentionally reused for three categories: query/body `conversationId` mismatch, business-rule validation after schema validation passes, and concurrent-sender conflicts on the same `conversationId`. The first two are post-handshake `ERROR + 1008`; the last one is a handshake-time `HTTP 403`
- `E1010` is reserved exclusively for handshake authentication failure. In V1 it is emitted as `HTTP 401` before the WebSocket upgrade completes

---

## 6. WebSocket Control-Layer Closure

This matrix describes application-level `ERROR` frames paired with close codes. That is different from WebSocket control-plane failures such as RFC 6455 Ping/Pong keepalive failure, where the connection may be closed directly at the WebSocket layer, typically with close code **1011**, without going through the `ERROR` frame semantics described above. See **section 1.4** of the API contract.

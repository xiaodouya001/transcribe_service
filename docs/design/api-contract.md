# Realtime Transcribe Service API Contract

## Document identification


| Attribute             | Value                                                                               |
| --------------------- | ----------------------------------------------------------------------------------- |
| **API major version** | **V1** (aligned with the WebSocket path prefix `/ws/v1/`)                           |
| **Document version**  | **1.4.1** (semantic version of *this* contract document; independent patch counter) |


### Revision policy (normative)

**API major version** (`V1`, `V2`, …) identifies the protocol generation. An incompatible protocol uses a new API major version and, as a rule, a new URL prefix.

**Document version** follows `MAJOR.MINOR.PATCH` (from **1.0.0** within the V1 line). It identifies the revision of **this** contract file only. It must not be equated with a Git reference or an application release identifier. It increments when the document’s substantive content changes relative to the preceding revision.

#### Roles of the header table and §7


| Item                        | Definition                                                                           |
| --------------------------- | ------------------------------------------------------------------------------------ |
| Header **Document version** | The authoritative identifier of the **current** published revision.                  |
| **§7 Revision History**     | The chronological record of material revisions: document version, date, and summary. |


Any normative edit to **§1–§6** must be accompanied by an update to the header **Document version** and a new row in **§7 Revision History** such that both reflect the same revision.

**Semver increments**


| Increment | Criterion                                                                               | Typical changes                                                                                                            |
| --------- | --------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| **PATCH** | No change to implementer obligations under the contract                                 | Editorial correction, formatting, illustrative examples, non-normative clarification                                       |
| **MINOR** | Backward-compatible extension                                                           | New optional fields, additional non-breaking enumeration values, additive sections                                         |
| **MAJOR** | Breaking change: a conforming implementation under the prior text may no longer conform | Removal or redefinition of fields, stricter validation, altered semantics of error codes or of Kafka or handshake behavior |

**Pre-integration exception (normative)**

Before the first external integration or published consumer commitment for **V1**, an unreleased contract refinement that would otherwise qualify as **MAJOR** may be recorded as a **MINOR** document-version increment when it creates no migration obligation for any integrated consumer. Once an external integration or published consumer commitment exists, the standard **MAJOR / MINOR / PATCH** rules apply without this exception.


A new API major version (e.g. **V2**) and its URL prefix must be specified together with the protocol; the **Document version** field alone does not establish a new API major.

**Procedure on modification**

1. Modify **§1–§6** as required.
2. Against the latest entry in **§7**, determine the semver increment and compute the new **Document version**, applying the pre-integration exception above only when its conditions are satisfied.
3. Set the header **Document version** to that value.
4. Append a row to **§7 Revision History** giving the new version, the date (UTC), and a concise summary of the change.

Edits that affect normative behavior, codes, fields, or examples require a **§7** entry and **Document version** increment as specified above.

---

## Document Structure


| Section                                | Coverage                                                                        |
| -------------------------------------- | ------------------------------------------------------------------------------- |
| Document identification                | API major version, document version, revision policy (normative)              |
| 1. Protocol Overview                   | WebSocket endpoint, headers, Ping/Pong keepalive, event types, and message flow |
| 2. Request Contract                    | Client-to-server message shape, field definitions, and business rules           |
| 3. Response Contract                   | Server-to-client success and error payloads                                     |
| 4. Status Codes and Error Codes        | HTTP handshake status, WebSocket close codes, and application error mapping     |
| 5. End-to-End Examples                 | Request and response examples for typical flows                                 |
| 6. Kafka Persistence Contract          | Server-to-Kafka message key, value, and write rules                             |
| 7. Revision History                    | Document version (V1 / semver) and changelog                                    |


---

## 1. Protocol Overview

### 1.1 WebSocket Endpoint


| Item               | Contract                            |
| ------------------ | ----------------------------------- |
| **Endpoint**       | `/ws/v1/realtime-transcriptions`    |
| **Method**         | WebSocket Upgrade                   |
| **Payload Format** | `application/json` encoded in UTF-8 |
| **Transport**      | `wss` (TLS/mTLS required)           |


**Query parameters**


| Parameter        | Required | Type   | Description                                                                | Example                                                                              |
| ---------------- | -------- | ------ | -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `conversationId` | Yes      | string | Uses the Genesys Call ID and uniquely identifies the transcription session | `/ws/v1/realtime-transcriptions?conversationId=39449992-32f3-4581-a8a1-99d4109f37d4` |


### 1.2 Headers

> The full handshake header set may expand later. In V1, `Authorization` is the only normative handshake header beyond the standard WebSocket upgrade headers. When deployment authentication is enabled, the service validates the Bearer JWT signature and `exp` claim during the handshake.


| Header          | Required    | Type   | Max Length | Description                                                                                       |
| --------------- | ----------- | ------ | ---------- | ------------------------------------------------------------------------------------------------- |
| `Authorization` | Conditional | string | 4096       | `Bearer <JWT>` signed with HS256; required when deployment authentication is enabled, otherwise not required |


### 1.3 Event Types and Message Flow

This protocol is built for one active sender connection per `conversationId`. The client streams transcript events to the service, the service persists successful messages to Kafka, and then returns either an ACK or an ERROR frame.

**Client to Server**


| `eventType`        | Description                        |
| ------------------ | ---------------------------------- |
| `SESSION_ONGOING`  | Regular transcript event           |
| `SESSION_COMPLETE` | Final end-of-session control event |


**Server to Client**


| `eventType`      | Description                               |
| ---------------- | ----------------------------------------- |
| `TRANSCRIPT_ACK` | ACK for a regular transcript event        |
| `EOL_ACK`        | ACK for a successful end-of-session event |
| `ERROR`          | Validation or processing error            |


### 1.4 WebSocket Ping/Pong (connection keepalive)

Keepalive uses **RFC 6455** WebSocket **Ping** and **Pong** control frames. They are not JSON messages and do not use the `application/json` payload channel described elsewhere in this contract.


| Item                  | Contract                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Initiator**         | After the WebSocket upgrade succeeds, the **server** sends **Ping** frames on a **periodic** schedule.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| **Client obligation** | The **client** must answer each **Ping** with a **Pong** in accordance with RFC 6455.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| **Timing**            | The **ping interval** is chosen so that control traffic recurs within typical intermediary **idle timeouts** (for example, load balancers that drop idle connections after about one minute). After each **Ping**, the server waits a **finite** time for the matching **Pong**; if that **Pong** is not received in time, the server **may close** the WebSocket (keepalive failure). Numeric intervals and timeouts are **deployment-specific**; clients must implement standards-compliant **Pong** handling and must not assume a fixed schedule unless agreed for a given integration. |
| **Close behavior**    | A keepalive failure is signaled at the **WebSocket layer** (for example close code **1011**). It is **not** an application-level JSON `ERROR` frame as defined in §3–§4.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| **Protocol scope**    | **Ping/Pong frames must not** advance `sequenceNumber`, nor satisfy transcript persistence or the session semantics in this contract. They exist solely for connection liveness.                                                                                                                                                                                                                                                                                                                                                                                                            |


---

## 2. Request Contract

*Client-to-server message format*

### 2.1 JSON Schema Examples

**Agent**

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "callStartTimeStamp": "2025-03-21T10:30:02.327Z",
    "callEndTimeStamp": null,
    "eventType": "SESSION_ONGOING"
  },
  "payload": {
    "sequenceNumber": 0,
    "speaker": "Agent",
    "transcript": "thank you",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true,
    "speakTimeStamp": "2025-03-21T10:32:18.000Z",
    "transcriptGenerateTimeStamp": "2025-03-21T10:32:20.000Z"
  }
}
```

**Customer**

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "callStartTimeStamp": "2025-03-21T10:30:02.327Z",
    "callEndTimeStamp": null,
    "eventType": "SESSION_ONGOING"
  },
  "payload": {
    "sequenceNumber": 0,
    "speaker": "Customer",
    "transcript": "Hello",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true,
    "speakTimeStamp": "2025-03-21T10:32:18.000Z",
    "transcriptGenerateTimeStamp": "2025-03-21T10:32:20.000Z"
  }
}
```

**System**

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "callStartTimeStamp": "2025-03-21T10:30:02.327Z",
    "callEndTimeStamp": "2025-03-21T10:45:00.000Z",
    "eventType": "SESSION_COMPLETE"
  },
  "payload": {
    "sequenceNumber": 42,
    "speaker": "System",
    "transcript": "EOL",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true
  }
}
```

### 2.2 Field Definitions

#### `metaData`


| Field                | Required    | Type   | Max Length | Allowed Values / Format                           | Description                                   |
| -------------------- | ----------- | ------ | ---------- | ------------------------------------------------- | --------------------------------------------- |
| `conversationId`     | Yes         | string | 64         | Unique ID for each call                           | Session identifier, using the Genesys Call ID |
| `callStartTimeStamp` | Yes         | string | 32         | ISO-8601 UTC                                      | Call start time                               |
| `callEndTimeStamp`   | Conditional | string | 32         | ISO-8601 UTC; only present for `SESSION_COMPLETE` | Call end time                                 |
| `eventType`          | Yes         | string | 32         | `SESSION_ONGOING` or `SESSION_COMPLETE`           | Upstream event type                           |


#### `payload`


| Field                         | Required    | Type    | Max Length | Allowed Values / Format                                                                 | Description                                  |
| ----------------------------- | ----------- | ------- | ---------- | --------------------------------------------------------------------------------------- | -------------------------------------------- |
| `sequenceNumber`              | Yes         | integer | -          | `>= 0`; strictly increasing within the same `conversationId`                            | Transcript sequence number                   |
| `speaker`                     | Yes         | string  | 16         | `Agent`, `Customer`, or `System`                                                        | Speaker role                                 |
| `transcript`                  | Yes         | string  | 8000       | Any regular string                                                                      | Transcript text or system control text       |
| `engineProvider`              | Yes         | string  | 64         | For example `FanoLabs`                                                                  | Speech-to-text engine provider               |
| `dialect`                     | Yes         | string  | 32         | BCP-47, for example `yue-x-auto`                                                        | Language or dialect                          |
| `isFinal`                     | Yes         | boolean | -          | Must be `true`                                                                          | Indicates that the transcript chunk is final |
| `speakTimeStamp`              | Conditional | string  | 32         | ISO-8601 UTC; required when `eventType=SESSION_ONGOING`, omitted for `SESSION_COMPLETE` | Time when the corresponding speaker spoke    |
| `transcriptGenerateTimeStamp` | Conditional | string  | 32         | ISO-8601 UTC; required when `eventType=SESSION_ONGOING`, omitted for `SESSION_COMPLETE` | Time when ASR generated the transcript       |


### 2.3 Business Rules

1. **Handshake and body identity**: `metaData.conversationId` must equal the `conversationId` query parameter on the WebSocket handshake URL.
2. **Sequence progression**: Within the same `conversationId`, the first message must use `sequenceNumber` **0**, and each subsequent successful message must use the **next consecutive integer** (`1`, `2`, `3`, …). Gaps, reordering, or sending a higher number before the expected one is invalid (see `E1006` in §4).
3. **SESSION_ONGOING rule**: `callEndTimeStamp` must be `null`, and `payload.speaker` must be `Agent` or `Customer`.
4. **SESSION_COMPLETE rule**: `callEndTimeStamp` must be present, and `payload.speaker` must be `System`.
5. **Request payload timestamps**:
  - When `eventType=SESSION_ONGOING`, both `speakTimeStamp` and `transcriptGenerateTimeStamp` are required.
  - When `eventType=SESSION_COMPLETE`, both `speakTimeStamp` and `transcriptGenerateTimeStamp` must be omitted.
6. **Idempotency**:
  - The pair `(conversationId, sequenceNumber)` is treated as an idempotency key.
  - When the server receives the same pair again, it returns the corresponding ACK again.
7. **Single active sender**:
  - Only one connection is allowed to keep sending messages for the same `conversationId` at any time.
  - The client must not use multiple concurrent WebSocket connections or parallel send paths for the same `conversationId`.
  - If the server detects another active sender during the handshake, it rejects the upgrade with HTTP `403` and application error `E1009`.
8. **Retry without advancing sequence**: If the client does not receive the expected `TRANSCRIPT_ACK` or `EOL_ACK` for a message (including after an `ERROR` frame, WebSocket closure as listed in §4, or a client-side wait timeout), it must reconnect if needed and **resend the same** `sequenceNumber` until acknowledged; it must not send a higher `sequenceNumber` until the current one has been acknowledged.
9. **Completion ACK**: A successful `SESSION_COMPLETE` message returns `EOL_ACK`.
10. **Completion transcript**: `payload.transcript` is still a required string field. `"EOL"` is recommended, but the server does not enforce a fixed literal.

---

## 3. Response Contract

*Server-to-client message format*

### 3.1 Success ACKs

**Transcript ACK**

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "eventType": "TRANSCRIPT_ACK"
  },
  "payload": {
    "sequenceNumber": 0,
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z",
    "serverProcessingMs": 1.23
  }
}
```

**EOL ACK**

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "eventType": "EOL_ACK"
  },
  "payload": {
    "sequenceNumber": 42,
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z",
    "serverProcessingMs": 1.56
  }
}
```

#### Fields (success ACKs)

| Field                        | Required | Type    | Max Length | Allowed Values / Format       | Description                         |
| ---------------------------- | -------- | ------- | ---------- | ----------------------------- | ----------------------------------- |
| `metaData.conversationId`    | Yes      | string  | 64         | Session ID                    | Echoes the request `conversationId` |
| `metaData.eventType`         | Yes      | string  | 32         | `TRANSCRIPT_ACK` or `EOL_ACK` | ACK event type                      |
| `payload.sequenceNumber`     | Yes      | integer | -          | `>= 0`                        | Echoes the request `sequenceNumber` |
| `payload.createdAtTimeStamp` | Yes      | string  | 32         | ISO-8601 UTC                  | Server-side ACK timestamp           |
| `payload.serverProcessingMs` | No       | number  | —          | Non-negative milliseconds     | Optional server-side latency for the message path; omitted when not produced |


### 3.2 ERROR Response

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "eventType": "ERROR"
  },
  "error": {
    "code": "E1003",
    "message": "Missing required field",
    "details": "callEndTimeStamp must be provided when eventType=SESSION_COMPLETE",
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z"
  }
}
```

#### Fields (ERROR)

| Field                      | Required | Type   | Max Length | Allowed Values / Format | Description                     |
| -------------------------- | -------- | ------ | ---------- | ----------------------- | ------------------------------- |
| `metaData.conversationId`  | Yes      | string | 64         | Session ID              | Session identifier              |
| `metaData.eventType`       | Yes      | string | 32         | `ERROR`                 | Event type                      |
| `error.code`               | Yes      | string | 16         | See Section 4           | Application error code          |
| `error.message`            | Yes      | string | 256        | Any string              | Short error summary             |
| `error.details`            | No       | string | 2048       | Any string              | Validation or processing detail |
| `error.createdAtTimeStamp` | Yes      | string | 32         | ISO-8601 UTC            | Server-side timestamp           |


---

## 4. Status Codes and Error Codes

### 4.1 HTTP Status Codes During Handshake


| Scenario                                    | Status | Meaning                                                                                                                   |
| ------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------- |
| WebSocket upgrade accepted                  | 101    | Switching Protocols                                                                                                       |
| Invalid request, query parameter, or header | 400    | Bad Request                                                                                                               |
| Unauthorized                                | 401    | Invalid, expired, missing, or malformed Bearer credentials during the handshake                                           |
| Forbidden                                   | 403    | Forbidden; used for policy conflicts known at handshake time, such as an existing active sender for the same conversation |
| Rate limited                                | 429    | Too Many Requests                                                                                                         |
| Internal handshake error                    | 500    | Internal Server Error                                                                                                     |
| Service unavailable                         | 503    | Temporarily unavailable                                                                                                   |


### 4.2 WebSocket Close Codes


| Scenario                               | Close Code | Meaning                                            |
| -------------------------------------- | ---------- | -------------------------------------------------- |
| Normal closure                         | 1000       | Normal closure                                     |
| Server going away                      | 1001       | Going away                                         |
| Unsupported data type                  | 1003       | Reserved and not used by this service              |
| Invalid payload format                 | 1007       | JSON parsing or decode error                       |
| Policy violation                       | 1008       | Business rule, authentication, or policy violation |
| Internal server error                  | 1011       | Server-side processing exception                   |
| Temporary overload or dependency issue | 1013       | Try again later                                    |


> For `1000` and `1001`, the service may close the connection without sending an `ERROR` frame.

### 4.3 Application error mapping

The tables below are aligned with the scenario matrix in [protocol-scenario-matrix.md](protocol-scenario-matrix.md) (**E-01** … **E-17**). That document adds worked JSON examples and test references. If anything conflicts, **this section is normative**.

#### 4.3.1 Delivery channels (normative)

| Channel | When it applies | What the client receives |
| ------- | ---------------- | ------------------------- |
| **Pre-handshake HTTP + JSON** | The WebSocket upgrade has **not** completed | An HTTP status (for example **400**, **401**, **403**, **429**, **503**) and a JSON body using the same `metaData` + `error` shape as §3. **No** WebSocket text frame and **no** WebSocket close code apply to that failed upgrade. |
| **Post-handshake WebSocket `ERROR` + close** | The upgrade has **already** succeeded | A WebSocket text frame with `metaData.eventType=ERROR`, then a WebSocket **close** with the mapped code. **No** new HTTP status is produced for that failure. |

For each row in **§4.3.2**, a cell shows **—** when the channel does not apply (for example **HTTP status —** after handshake, or **WS close —** on handshake rejection).

#### 4.3.2 Scenario matrix (normative)

All rows use `eventType=ERROR` in the JSON error payload. **Retry** means the client may reconnect if needed and resend the same `sequenceNumber` where §2.3 applies, or retry the handshake for pre-handshake failures.

| Scenario ID | Scenario (summary) | Handshake stage | Application error code | HTTP status | WS close | Disconnect | Retry |
| ----------- | ------------------- | --------------- | ---------------------- | ----------- | -------- | ---------- | ----- |
| E-01 | Missing query `conversationId` | Pre | E1003 | 400 | — | Yes | Yes |
| E-02 | Service draining | Pre | E1008 | 503 | — | Yes | Yes |
| E-03 | Connection limit exceeded | Pre | E1008 | 429 | — | Yes | Yes |
| E-04 | JSON decode failed | Post | E1001 | — | 1007 | Yes | Yes |
| E-05 | Invalid enum (for example `eventType`) | Post | E1002 | — | 1008 | Yes | Yes |
| E-06 | Missing required field (for example `payload.dialect`) | Post | E1003 | — | 1008 | Yes | Yes |
| E-07 | Field type mismatch or disallowed extra field | Post | E1004 | — | 1008 | Yes | Yes |
| E-08 | Timestamp not valid ISO-8601 UTC | Post | E1005 | — | 1008 | Yes | Yes |
| E-09 | Sequence number out of order (not idempotent duplicate) | Post | E1006 | — | 1008 | Yes | Yes |
| E-10 | Downstream timeout (for example Kafka send timeout) | Post | E1011 | — | 1013 | Yes | Yes |
| E-11 | Downstream send failure or dependency outage **after** handshake (for example Kafka) | Post | E1008 | — | 1013 | Yes | Yes |
| E-11 | Conversation ownership store error **during** handshake (for example Redis error on claim) | Pre | E1008 | 503 | — | Yes | Yes |
| E-12 | Unhandled exception in orchestration / commit path | Post | E1007 | — | 1011 | Yes | Yes |
| E-13 | Unhandled exception in transport / framing path | Post | E1007 | — | 1011 | Yes | Yes |
| E-14 | Query `conversationId` ≠ body `metaData.conversationId` | Post | E1009 | — | 1008 | Yes | Yes |
| E-15 | Business-rule violation after schema passes (for example invalid field combination) | Post | E1009 | — | 1008 | Yes | Yes |
| E-16 | Second concurrent sender for the same `conversationId` | Pre | E1009 | 403 | — | Yes | Yes |
| E-17 | Missing or invalid Bearer JWT when auth is enabled | Pre | E1010 | 401 | — | Yes | Yes |

**Idempotent duplicate** (same `conversationId` and `sequenceNumber` as already accepted) returns the matching success ACK and does **not** use an error code; it is **N-02** in the protocol matrix, not **E-09**.

#### 4.3.3 Application codes — semantics and reuse (normative)

| Application error code | Meaning | Scenario ID (§4.3.2) |
| ---------------------- | ------- | -------------------- |
| E1001 | Invalid JSON payload (decode / parse) | E-04 only (post-handshake **1007**) |
| E1002 | Invalid enum value | E-05 |
| E1003 | Missing required input | E-01 (pre **400**); E-06 (post **1008**) |
| E1004 | Wrong type or disallowed extra field | E-07 |
| E1005 | Invalid UTC timestamp format | E-08 |
| E1006 | Sequence not the next expected value | E-09 (duplicates use idempotent ACK, not E1006) |
| E1007 | Unexpected internal failure | E-12, E-13 (post **1011**) |
| E1008 | Downstream / capacity / guard-store failure | E-02 **503**, E-03 **429**, E-11 post **1013** or pre **503** (ownership store); distinguish by `message` / `details` and stage |
| E1009 | Policy conflict | E-14, E-15 (post **1008**); E-16 (pre **403**) |
| E1010 | Handshake authentication failed | E-17 (pre **401** only in V1) |
| E1011 | Downstream **timeout** waiting on dependency | E-10 (post **1013**); not used for handshake rejection in V1 |

> **Service draining (E-02)** and **ownership store unavailable at handshake (E-11 pre-handshake)** both use **E1008** with HTTP **503**. Clients must use **`message` / `details`** (and operational context) to tell them apart.

Before closing the connection for a post-handshake error, the service sends an `ERROR` frame such as:

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "eventType": "ERROR"
  },
  "error": {
    "code": "E1003",
    "message": "Missing required field",
    "details": "callEndTimeStamp must be provided when eventType=SESSION_COMPLETE",
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z"
  }
}
```

---

## 5. End-to-End Examples

### 5.1 Ongoing Session

**Request**

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "callStartTimeStamp": "2025-03-21T10:30:02.327Z",
    "callEndTimeStamp": null,
    "eventType": "SESSION_ONGOING"
  },
  "payload": {
    "sequenceNumber": 0,
    "speaker": "Customer",
    "transcript": "Hello",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true,
    "speakTimeStamp": "2025-03-21T10:32:18.000Z",
    "transcriptGenerateTimeStamp": "2025-03-21T10:32:20.000Z"
  }
}
```

**Response**

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "eventType": "TRANSCRIPT_ACK"
  },
  "payload": {
    "sequenceNumber": 0,
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z"
  }
}
```

### 5.2 Session Complete

**Request**

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "callStartTimeStamp": "2025-03-21T10:30:02.327Z",
    "callEndTimeStamp": "2026-02-05T08:49:01.048Z",
    "eventType": "SESSION_COMPLETE"
  },
  "payload": {
    "sequenceNumber": 42,
    "speaker": "System",
    "transcript": "EOL",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true
  }
}
```

**Response**

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "eventType": "EOL_ACK"
  },
  "payload": {
    "sequenceNumber": 42,
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z"
  }
}
```

### 5.3 Error Response Example (`E1003`)

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "eventType": "ERROR"
  },
  "error": {
    "code": "E1003",
    "message": "Validation failed",
    "details": "Field required: metaData.conversationId",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

---

## 6. Kafka Persistence Contract

This section defines the message contract written by the service on the success path, in other words the internal server-to-Kafka format.

### 6.1 Write Rules


| Item                              | Contract                                                                                                         |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Topic                             | Controlled by `KAFKA_TOPIC`, defaulting to `AI_STAGING_TRANSCRIPTION`                                            |
| Message Key                       | UTF-8 bytes of `conversationId`                                                                                  |
| Message Value                     | UTF-8 bytes containing JSON                                                                                      |
| Business structure of the value   | The same validated business structure as the upstream request plus service enrich: `metaData + payload + enrich` |
| Required enrich field             | `enrich.eventProduceTimestamp` (ISO-8601 UTC with millisecond precision, format: `YYYY-MM-DDTHH:MM:SS.mmmZ`)     |
| `eventProduceTimestamp` semantics | Generated by the service immediately before each `producer.send` attempt                                         |
| Server-added fields               | The service appends only `enrich.eventProduceTimestamp`; it does not append ACK, ERROR, or `serverProcessingMs`  |
| Partition routing                 | Kafka partitions by the message key, which is `conversationId`                                                   |


### 6.2 Kafka Message Value Examples

**SESSION_ONGOING**

Kafka Message Key:

```text
39449992-32f3-4581-a8a1-99d4109f37d4
```

Kafka Message Value:

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "callStartTimeStamp": "2025-03-21T10:30:02.327Z",
    "callEndTimeStamp": null,
    "eventType": "SESSION_ONGOING"
  },
  "payload": {
    "sequenceNumber": 0,
    "speaker": "Agent",
    "transcript": "thank you",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true,
    "speakTimeStamp": "2025-03-21T10:32:18.000Z",
    "transcriptGenerateTimeStamp": "2025-03-21T10:32:20.000Z"
  },
  "enrich": {
    "eventProduceTimestamp": "2026-03-27T10:11:12.345Z"
  }
}
```

**SESSION_COMPLETE**

Kafka Message Key:

```text
39449992-32f3-4581-a8a1-99d4109f37d4
```

Kafka Message Value:

```json
{
  "metaData": {
    "conversationId": "39449992-32f3-4581-a8a1-99d4109f37d4",
    "callStartTimeStamp": "2025-03-21T10:30:02.327Z",
    "callEndTimeStamp": "2025-03-21T10:45:00.000Z",
    "eventType": "SESSION_COMPLETE"
  },
  "payload": {
    "sequenceNumber": 42,
    "speaker": "System",
    "transcript": "EOL",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true
  },
  "enrich": {
    "eventProduceTimestamp": "2026-03-27T10:11:58.901Z"
  }
}
```

### 6.3 Scenarios That Must Not Write to Kafka

- Messages that fail schema validation, business-rule validation, or handshake validation are not written to Kafka.
- Duplicate messages that hit the idempotent path return the matching ACK immediately and are not written again.
- Out-of-order messages return `E1006` and are not written to Kafka.
- Only messages that pass sequence validation, ownership checks, and the actual Kafka send step are persisted.

---

## 7. Revision History

The **Doc ver.** column states the **Document version** at each revision. The **API major version** remains **V1** until a new protocol generation is published under a distinct API major.


| Doc ver. | Date (UTC) | Summary                                                                                                                                                                                                                            |
| -------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1.4.1    | 2026-04-03 | **§4.3.2** / **§4.3.3** Rename table columns **Scenario ID** and **Application error code** (replacing generic **ID** / **Code**). |
| 1.4.0    | 2026-04-03 | **§4.3** Replace the single code-centric table with **§4.3.1** delivery channels, **§4.3.2** scenario matrix (**E-01**–**E-17**, aligned with [protocol-scenario-matrix.md](protocol-scenario-matrix.md)), and **§4.3.3** code semantics; clarify pre-handshake **—** vs post-handshake **—** for HTTP/WS close; correct **E1010** / **E1011** rows for V1 (handshake-only auth; no HTTP **504** row for **E1011**). |
| 1.3.2    | 2026-04-02 | **§4.2** Clarify that WebSocket close code `1007` is used for JSON parsing / decode failures only, aligning the close-code summary with the detailed error mapping and implementation behavior.                                 |
| 1.3.1    | 2026-03-31 | **§4.3** Clarify that the HTTP Handshake column applies only to pre-upgrade failures; post-handshake errors use WS `ERROR` + close codes without a further HTTP status.                                                           |
| 1.3.0    | 2026-03-30 | Enable optional handshake authentication for V1 deployments using `Authorization: Bearer <JWT>` with `E1010` for missing, malformed, invalid, or expired credentials, and implement the minimum HS256-based validation flow. |
| 1.2.1    | 2026-03-30 | Tighten the request payload contract so `payload.dialect` is a required field for both `SESSION_ONGOING` and `SESSION_COMPLETE`. |
| 1.2.0    | 2026-03-30 | Pre-integration V1 refinement: allow unreleased contract changes without consumer migration obligations to use a MINOR document-version increment; request payload removes `agentId` / `customerId`, renames `createdAtTimeStamp` to `speakTimeStamp`, adds `transcriptGenerateTimeStamp`, and omits both request timestamps for `SESSION_COMPLETE`. |
| 1.1.6    | 2026-03-26 | **§3.1** Document optional `payload.serverProcessingMs` (aligned with `AckPayload` / transport); examples updated.                                                                                                                |
| 1.1.5    | 2026-03-26 | Editorial: deduplicate headings (`## Document identification`, `### Revision policy`, §3 field tables); shorten redundant labels.                                                                                                  |
| 1.1.4    | 2026-03-26 | **§2.3** Align with integration checklist: explicit query/body `conversationId` match; consecutive sequence from **0**; client single sender; lossless retry (resend same `sequenceNumber` until ACK).                             |
| 1.1.3    | 2026-03-26 | **§1.4** Revise wording to omit environment variables, framework names, and internal timing defaults; state protocol obligations and deployment-specific timing only.                                                              |
| 1.1.2    | 2026-03-26 | **§1.4** Reference defaults: `ping_timeout` **10s** (`WS_PING_TIMEOUT`), worst-case ~**30s** to close if Pong never arrives (`20 + 10`); align with `.env` / `config.Settings`.                                                    |
| 1.1.1    | 2026-03-26 | **§1.4** Clarify `websockets` keepalive timing: `ping_timeout` applies after each server Ping; first Ping after `ping_interval`; default worst-case ~40s to close if Pong never arrives; close code **1011** on keepalive timeout. |
| 1.1.0    | 2026-03-26 | **§1.4** Add normative WebSocket Ping/Pong keepalive (RFC 6455, server Ping, client Pong, defaults 20s/20s, no business side effects); align with application design §3.3 and deployment configuration.                            |
| 1.0.9    | 2026-03-26 | Add `## Document identification and versioning` as the main heading for the version block; extend **Document Structure** accordingly.                                                                                              |
| 1.0.8    | 2026-03-26 | Rewrite the document-versioning block and the §7 introduction in normative specification style (definitions, criteria, procedure).                                                                                                 |
| 1.0.7    | 2026-03-26 | Expand **Versioning rules**: header vs §7, change workflow, PATCH/MINOR/MAJOR table; clarify doc semver vs Git/service release.                                                                                                    |
| 1.0.6    | 2025-03-27 | Baseline: formalize V1 + semver `1.0.0`, document structure through §6, and this revision history.                                                                                                                                 |


---


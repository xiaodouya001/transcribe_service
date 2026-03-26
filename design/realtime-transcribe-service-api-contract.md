# Realtime Transcribe Service API Contract

---

## Document Structure

| Section | Coverage |
| --- | --- |
| 1. Protocol Overview | WebSocket endpoint, headers, event types, and message flow |
| 2. Request Contract | Client-to-server message shape, field definitions, and business rules |
| 3. Response Contract | Server-to-client success and error payloads |
| 4. Status Codes and Error Codes | HTTP handshake status, WebSocket close codes, and application error mapping |
| 5. End-to-End Examples | Request and response examples for typical flows |
| 6. Kafka Persistence Contract | Server-to-Kafka message key, value, and write rules |

---

## 1. Protocol Overview

### 1.1 WebSocket Endpoint

| Item | Contract |
| --- | --- |
| **Endpoint** | `/ws/v1/realtime-transcriptions` |
| **Method** | WebSocket Upgrade |
| **Payload Format** | `application/json` encoded in UTF-8 |
| **Transport** | `wss` (TLS/mTLS required) |

**Query parameters**

| Parameter | Required | Type | Description | Example |
| --- | --- | --- | --- | --- |
| `conversationId` | Yes | string | Uses the Genesys Call ID and uniquely identifies the transcription session | `/ws/v1/realtime-transcriptions?conversationId=39449992-32f3-4581-a8a1-99d4109f37d4` |

### 1.2 Headers (Reserved)

> The complete header list is still under review. Authentication is not enforced in the current implementation, so the header below remains reserved.

| Header | Required | Type | Max Length | Description |
| --- | --- | --- | --- | --- |
| `Authorization` | No, reserved | string | 4096 | Placeholder for a bearer token or other credentials. Whether it becomes mandatory will be decided when the authentication design is finalized. |

### 1.3 Event Types and Message Flow

This protocol is built for one active sender connection per `conversationId`. The client streams transcript events to the service, the service persists successful messages to Kafka, and then returns either an ACK or an ERROR frame.

**Client to Server**

| `eventType` | Description |
| --- | --- |
| `SESSION_ONGOING` | Regular transcript event |
| `SESSION_COMPLETE` | Final end-of-session control event |

**Server to Client**

| `eventType` | Description |
| --- | --- |
| `TRANSCRIPT_ACK` | ACK for a regular transcript event |
| `EOL_ACK` | ACK for a successful end-of-session event |
| `ERROR` | Validation or processing error |

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
    "agentId": "3210001",
    "customerId": null,
    "sequenceNumber": 0,
    "speaker": "Agent",
    "transcript": "thank you",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true,
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z"
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
    "agentId": null,
    "customerId": "12345678",
    "sequenceNumber": 0,
    "speaker": "Customer",
    "transcript": "Hello",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true,
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z"
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
    "agentId": null,
    "customerId": null,
    "sequenceNumber": 42,
    "speaker": "System",
    "transcript": "EOL",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true,
    "createdAtTimeStamp": "2025-03-21T10:44:58.000Z"
  }
}
```

### 2.2 Field Definitions

#### `metaData`

| Field | Required | Type | Max Length | Allowed Values / Format | Description |
| --- | --- | --- | --- | --- | --- |
| `conversationId` | Yes | string | 64 | Unique ID for each call | Session identifier, using the Genesys Call ID |
| `callStartTimeStamp` | Yes | string | 32 | ISO-8601 UTC | Call start time |
| `callEndTimeStamp` | Conditional | string | 32 | ISO-8601 UTC; only present for `SESSION_COMPLETE` | Call end time |
| `eventType` | Yes | string | 32 | `SESSION_ONGOING` or `SESSION_COMPLETE` | Upstream event type |

#### `payload`

| Field | Required | Type | Max Length | Allowed Values / Format | Description |
| --- | --- | --- | --- | --- | --- |
| `agentId` | Conditional | string | 32 | Agent staff ID; required when `speaker=Agent`, otherwise omitted or `null` | Agent identifier |
| `customerId` | Conditional | string | 64 | Customer number; required when `speaker=Customer`, otherwise omitted or `null` | Customer identifier |
| `sequenceNumber` | Yes | integer | - | `>= 0`; strictly increasing within the same `conversationId` | Transcript sequence number |
| `speaker` | Yes | string | 16 | `Agent`, `Customer`, or `System` | Speaker role |
| `transcript` | Yes | string | 8000 | Any regular string | Transcript text or system control text |
| `engineProvider` | Yes | string | 64 | For example `FanoLabs` | Speech-to-text engine provider |
| `dialect` | No | string | 32 | BCP-47, for example `yue-x-auto`; may be omitted or `null` | Language or dialect |
| `isFinal` | Yes | boolean | - | Must be `true` | Indicates that the transcript chunk is final |
| `createdAtTimeStamp` | Yes | string | 32 | ISO-8601 UTC | Client-side transcript creation timestamp |

### 2.3 Business Rules

1. **Sequence progression**: Within the same `conversationId`, `sequenceNumber` must increase strictly in order.
2. **`SESSION_ONGOING` rule**: `callEndTimeStamp` must be `null`.
3. **`SESSION_COMPLETE` rule**: `callEndTimeStamp` must be present, and `payload.speaker` must be `System`.
4. **Participant fields**:
   - When `speaker=Agent`, `agentId` is required and `customerId` must be omitted or `null`.
   - When `speaker=Customer`, `customerId` is required and `agentId` must be omitted or `null`.
   - When `speaker=System`, both `agentId` and `customerId` must be omitted or `null`.
5. **Idempotency**:
   - The pair `(conversationId, sequenceNumber)` is treated as an idempotency key.
   - When the server receives the same pair again, it returns the corresponding ACK again.
6. **Single active sender**:
   - Only one connection is allowed to keep sending messages for the same `conversationId` at any time.
   - If the server detects another active sender during the handshake, it rejects the upgrade with HTTP `403` and application error `E1009`.
7. **Completion ACK**: A successful `SESSION_COMPLETE` message returns `EOL_ACK`.
8. **Completion transcript**: `payload.transcript` is still a required string field. `"EOL"` is recommended, but the server does not enforce a fixed literal.

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
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z"
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
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z"
  }
}
```

**Field definitions**

| Field | Required | Type | Max Length | Allowed Values / Format | Description |
| --- | --- | --- | --- | --- | --- |
| `metaData.conversationId` | Yes | string | 64 | Session ID | Echoes the request `conversationId` |
| `metaData.eventType` | Yes | string | 32 | `TRANSCRIPT_ACK` or `EOL_ACK` | ACK event type |
| `payload.sequenceNumber` | Yes | integer | - | `>= 0` | Echoes the request `sequenceNumber` |
| `payload.createdAtTimeStamp` | Yes | string | 32 | ISO-8601 UTC | Server-side ACK timestamp |

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

**Field definitions**

| Field | Required | Type | Max Length | Allowed Values / Format | Description |
| --- | --- | --- | --- | --- | --- |
| `metaData.conversationId` | Yes | string | 64 | Session ID | Session identifier |
| `metaData.eventType` | Yes | string | 32 | `ERROR` | Event type |
| `error.code` | Yes | string | 16 | See Section 4 | Application error code |
| `error.message` | Yes | string | 256 | Any string | Short error summary |
| `error.details` | No | string | 2048 | Any string | Validation or processing detail |
| `error.createdAtTimeStamp` | Yes | string | 32 | ISO-8601 UTC | Server-side timestamp |

---

## 4. Status Codes and Error Codes

### 4.1 HTTP Status Codes During Handshake

| Scenario | Status | Meaning |
| --- | --- | --- |
| WebSocket upgrade accepted | 101 | Switching Protocols |
| Invalid request, query parameter, or header | 400 | Bad Request |
| Unauthorized | 401 | Reserved for invalid or expired credentials once authentication is enabled |
| Forbidden | 403 | Forbidden; used for policy conflicts known at handshake time, such as an existing active sender for the same conversation |
| Rate limited | 429 | Too Many Requests |
| Internal handshake error | 500 | Internal Server Error |
| Service unavailable | 503 | Temporarily unavailable |

### 4.2 WebSocket Close Codes

| Scenario | Close Code | Meaning |
| --- | --- | --- |
| Normal closure | 1000 | Normal closure |
| Server going away | 1001 | Going away |
| Unsupported data type | 1003 | Reserved and not used by this service |
| Invalid payload format | 1007 | JSON parsing, type, or format error |
| Policy violation | 1008 | Business rule, authentication, or policy violation |
| Internal server error | 1011 | Server-side processing exception |
| Temporary overload or dependency issue | 1013 | Try again later |

> For `1000` and `1001`, the service may close the connection without sending an `ERROR` frame.

### 4.3 Application Error Mapping

| Error Code | `eventType` | HTTP Handshake | WS Close | Disconnect | Client Should Retry / Reconnect | Typical Scenario |
| --- | --- | --- | --- | --- | --- | --- |
| E1001 | ERROR | 400 | 1007 | Yes | Yes | JSON parsing fails or the client sends a payload that cannot be decoded |
| E1002 | ERROR | 400 | 1008 | Yes | Yes | Enum value is outside the allowed set, such as an invalid `eventType` |
| E1003 | ERROR | 400 | 1008 | Yes | Yes | A required contract field is missing, such as `conversationId` or `agentId` |
| E1004 | ERROR | 400 | 1008 | Yes | Yes | A field type does not match the contract, for example a string where an integer is required |
| E1005 | ERROR | 400 | 1008 | Yes | Yes | A timestamp is missing UTC format or is not valid ISO-8601 UTC |
| E1006 | ERROR | 400 | 1008 | Yes | Yes | The sequence is not the expected next value; duplicate messages are handled idempotently and return ACK instead |
| E1007 | ERROR | 500 | 1011 | Yes | Yes | Unexpected server-side exception that is not caused by client input |
| E1008 | ERROR | 503 / 429 | 1013 | Yes | Yes | A downstream dependency such as Kafka or Redis is unavailable, or the service is throttling requests |
| E1009 | ERROR | 403 for initial concurrent sender conflict, otherwise not applicable during handshake | 1008 for post-handshake policy violations | Yes | Yes | Disallowed business action or policy conflict, such as a concurrent sender at handshake time or a `conversationId` mismatch after handshake |
| E1010 | ERROR | 401 | 1008 | Yes | Yes | Reserved. Authentication is not enabled yet; once enabled, this code is used for missing, invalid, or unauthorized credentials |
| E1011 | ERROR | 504 | 1013 | Yes | Yes | Timeout when waiting for an upstream or downstream dependency such as Kafka |

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

### 5.1 Ongoing Session (`SESSION_ONGOING`)

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
    "agentId": null,
    "customerId": "12345678",
    "sequenceNumber": 0,
    "speaker": "Customer",
    "transcript": "Hello",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true,
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z"
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

### 5.2 Session Complete (`SESSION_COMPLETE`)

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
    "agentId": null,
    "customerId": null,
    "sequenceNumber": 42,
    "speaker": "System",
    "transcript": "EOL",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true,
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z"
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

| Item | Contract |
| --- | --- |
| Topic | Controlled by `KAFKA_TOPIC`, defaulting to `AI_STAGING_TRANSCRIPTION` |
| Message Key | UTF-8 bytes of `conversationId` |
| Message Value | UTF-8 bytes containing JSON |
| Business structure of the value | The same validated business structure as the upstream request plus service enrich: `metaData + payload + enrich` |
| Required enrich field | `enrich.eventProduceTimestamp` (ISO-8601 UTC with millisecond precision, format: `YYYY-MM-DDTHH:MM:SS.mmmZ`) |
| `eventProduceTimestamp` semantics | Generated by the service immediately before each `producer.send` attempt |
| Server-added fields | The service appends only `enrich.eventProduceTimestamp`; it does not append ACK, ERROR, or `serverProcessingMs` |
| Partition routing | Kafka partitions by the message key, which is `conversationId` |

### 6.2 Kafka Message Value Examples

**`SESSION_ONGOING`**

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
    "agentId": "3210001",
    "customerId": null,
    "sequenceNumber": 0,
    "speaker": "Agent",
    "transcript": "thank you",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true,
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z"
  },
  "enrich": {
    "eventProduceTimestamp": "2026-03-27T10:11:12.345Z"
  }
}
```

**`SESSION_COMPLETE`**

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
    "agentId": null,
    "customerId": null,
    "sequenceNumber": 42,
    "speaker": "System",
    "transcript": "EOL",
    "engineProvider": "FanoLabs",
    "dialect": "yue-x-auto",
    "isFinal": true,
    "createdAtTimeStamp": "2025-03-21T10:44:58.000Z"
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

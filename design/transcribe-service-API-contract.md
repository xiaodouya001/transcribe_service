# Transcribe Service API Contract 契约文档

> 基于 Confluence API Contract 整理，涵盖 WebSocket 端点、消息结构、请求/响应契约、业务规则、状态码及错误码定义。

---

## 文档结构


| 章节         | 内容                             |
| ---------- | ------------------------------ |
| 1. 协议概览    | WebSocket 端点、Header、事件类型与流转    |
| 2. 请求契约    | Client → Server 消息结构、字段定义、业务规则 |
| 3. 响应契约    | Server → Client 成功/错误响应结构      |
| 4. 状态码与错误码 | HTTP 握手码、WebSocket 关闭码、应用错误码映射 |
| 5. 完整示例    | 请求与响应对照示例                      |
| 6. Kafka 落盘契约 | Server → Kafka 的消息 Key/Value 与写入规则 |


---

## 1.协议概览 (Protocol Overview)

### 1.1 WebSocket 端点 (Endpoint)


| 项目             | 说明                               |
| -------------- | -------------------------------- |
| **Endpoint**   | `/ws/v1/realtime-transcriptions` |
| **Method**     | WebSocket Upgrade                |
| **Payload 格式** | `application/json` (UTF-8)       |
| **传输协议**       | `wss` (TLS/mTLS 必需)              |


**URL 参数：**


| 参数               | 必填  | 类型     | 说明                            | 示例                                                                                   |
| ---------------- | --- | ------ | ----------------------------- | ------------------------------------------------------------------------------------ |
| `conversationId` | 是   | string | 使用 Genesys Call Id，唯一标识本次转写会话 | `/ws/v1/realtime-transcriptions?conversationId=39449992-32f3-4581-a8a1-99d4109f37d4` |


### 1.2 Header

当前契约未定义业务必填 Header。WebSocket 升级所需标准 HTTP Header 由客户端与框架自行处理；鉴权 Header 不在本契约范围内。


### 1.3 事件类型与流转 (Event Types and Flow)

**Client → Server：**


| eventType          | 说明              |
| ------------------ | --------------- |
| `SESSION_ONGOING`  | 正常转写事件          |
| `SESSION_COMPLETE` | 最终 EOL 事件（会话结束） |


**Server → Client：**


| eventType        | 说明                   |
| ---------------- | -------------------- |
| `TRANSCRIPT_ACK` | 普通转写 ACK            |
| `EOL_ACK`        | 会话结束 ACK            |
| `ERROR`          | 校验或处理错误              |


---

## 2. 请求契约 (Request Body)

*Client → Server 消息格式*

### 2.1 消息结构 (JSON Schema)

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

### 2.2 字段定义 (Field Contract)

#### metaData


| 字段 | 必填 | 类型 | 最大长度 | 取值/格式 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `conversationId` | 是 | string | 64 | 每通电话唯一 ID | 会话标识（Genesys Call ID） |
| `callStartTimeStamp` | 是 | string | 32 | ISO-8601 UTC | 通话开始时间 |
| `callEndTimeStamp` | 条件 | string | 32 | ISO-8601 UTC；仅 `SESSION_COMPLETE` 时提供 | 通话结束时间 |
| `eventType` | 是 | string | 32 | `SESSION_ONGOING` 或 `SESSION_COMPLETE` | 上行事件类型 |


#### payload


| 字段 | 必填 | 类型 | 最大长度 | 取值/格式 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `agentId` | 条件 | string | 32 | Agent Staff ID；`speaker=Agent` 时必填，其它场景可省略或为 `null` | 坐席标识 |
| `customerId` | 条件 | string | 64 | 客户号码；`speaker=Customer` 时必填，其它场景可省略或为 `null` | 客户标识 |
| `sequenceNumber` | 是 | integer | — | `>= 0`，同一 `conversationId` 内单调递增 | 转写序列号 |
| `speaker` | 是 | string | 16 | `Agent`、`Customer`、`System` | 说话人角色 |
| `transcript` | 是 | string | 8000 | 任意普通字符串 | 转写文本或系统控制文本 |
| `engineProvider` | 是 | string | 64 | 如 `FanoLabs` | STT 引擎提供商 |
| `dialect` | 否 | string | 32 | BCP-47，如 `yue-x-auto`；可省略或为 `null` | 语言或方言 |
| `isFinal` | 是 | boolean | — | 必须为 `true` | 是否为最终结果 |
| `createdAtTimeStamp` | 是 | string | 32 | ISO-8601 UTC | 客户端转写创建时间 |


### 2.3 业务规则 (Business Rules)

1. **序列号**：同一 `conversationId` 下 `sequenceNumber` 必须严格单调递增。
2. **SESSION_ONGOING**：`callEndTimeStamp` 必须为 `null`。
3. **SESSION_COMPLETE**：`callEndTimeStamp` 必须提供，`payload.speaker` 必须为 `System`。
4. **参与者字段**：
  - `speaker=Agent` 时，`agentId` 必填，`customerId` 必须省略或为 `null`。
  - `speaker=Customer` 时，`customerId` 必填，`agentId` 必须省略或为 `null`。
  - `speaker=System` 时，`agentId` 与 `customerId` 都必须省略或为 `null`。
5. **幂等性**：
  - `(conversationId, sequenceNumber)` 组合应视为幂等。
  - 服务端对相同组合会再次返回 ACK。
6. **单连接发送**：
  - 同一 `conversationId` 在任一时刻只允许一个连接持续发送消息。
  - 若服务端在握手阶段检测到另一个连接已在发送该会话消息，则拒绝握手并返回 HTTP `403` + `E1009`。
7. **结束事件 ACK**：`SESSION_COMPLETE` 成功时返回 `EOL_ACK`。
8. **结束事件 transcript**：`payload.transcript` 为必填字符串，但服务端不校验固定字面值。

---

## 3. 响应契约 (Response Body)

*Server → Client 消息格式*

### 3.1 成功响应 (ACK)

**结构示例：**

**Transcription ACK**

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
    "sequenceNumber": 43,
    "createdAtTimeStamp": "2025-03-21T10:32:20.000Z"
  }
}
```

**字段说明：**


| 字段 | 必填 | 类型 | 最大长度 | 取值/格式 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `metaData.conversationId` | 是 | string | 64 | 会话 ID | 回显请求中的 `conversationId` |
| `metaData.eventType` | 是 | string | 32 | `TRANSCRIPT_ACK` 或 `EOL_ACK` | ACK 事件类型 |
| `payload.sequenceNumber` | 是 | integer | — | `>= 0` | 回显请求中的 `sequenceNumber` |
| `payload.createdAtTimeStamp` | 是 | string | 32 | ISO-8601 UTC | 服务端 ACK 时间戳 |


### 3.2  错误响应 (ERROR)

**结构示例：**

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

**字段说明：**


| 字段                         | 必填  | 类型     | 最大长度 | 取值/格式          | 说明      |
| -------------------------- | --- | ------ | ---- | -------------- | ------- |
| `metaData.conversationId`  | 是   | string | 64   | 会话 ID          | 会话标识    |
| `metaData.eventType`       | 是   | string | 32   | `ERROR`        | 事件类型    |
| `error.code`               | 是   | string | 16   | 见「四、状态码与错误码」章节 | 应用错误码   |
| `error.message`            | 是   | string | 256  | 任意             | 简短错误描述  |
| `error.details`            | 否   | string | 2048 | 任意             | 校验/处理详情 |
| `error.createdAtTimeStamp` | 是   | string | 32   | ISO-8601 UTC   | 服务端时间戳  |


---

## 4. 状态码与错误码

### 4.1 HTTP 握手阶段 (Upgrade)


| 场景             | 状态码 | 含义                            |
| -------------- | --- | ----------------------------- |
| WebSocket 升级成功 | 101 | Switching Protocols           |
| 无效请求/参数/Header | 400 | Bad Request                   |
| 未授权            | 401 | 保留；鉴权启用时用于无效或过期凭证         |
| 禁止访问           | 403 | Forbidden；用于握手阶段已知不允许的策略冲突，如同会话初始并发发送连接 |
| 限流             | 429 | Too Many Requests             |
| 握手内部错误         | 500 | Internal Server Error         |
| 服务不可用          | 503 | Temporary unavailable         |


### 4.2 WebSocket 关闭码 (Close Codes)


| 场景 | Close Code | 含义 |
| --- | --- | --- |
| 正常关闭 | 1000 | Normal closure |
| 服务端关闭/离开 | 1001 | Going away |
| 不支持的数据类型 | 1003 | 保留，不在本服务中使用 |
| 负载格式无效 | 1007 | JSON 解析、类型或格式错误 |
| 策略违规 | 1008 | 业务规则、鉴权或策略违规 |
| 服务端内部错误 | 1011 | Server-side processing exception |
| 临时过载 | 1013 | Try again later |


> 对于 `1000` 和 `1001`，若不发送错误帧，可省略 `eventType`。

### 4.3 应用错误码映射表


| 错误码 | eventType | HTTP（握手） | WS Close | 服务端是否断连 | 客户端是否应重连或重发 | 典型场景 |
| --- | --- | --- | --- | --- | --- | --- |
| E1001 | ERROR | 400 | 1007 | 是 | 是 | JSON 解析失败或客户端发送的数据格式无效，服务端无法解析请求体 |
| E1002 | ERROR | 400 | 1008 | 是 | 是 | 枚举值不在允许范围，字段校验未通过（如 `eventType` 非法） |
| E1003 | ERROR | 400 | 1008 | 是 | 是 | 缺少协议规定的必填字段，如缺少 `conversationId`、`agentId` 等 |
| E1004 | ERROR | 400 | 1008 | 是 | 是 | 某字段类型与定义不符，例如应为整数却为字符串 |
| E1005 | ERROR | 400 | 1008 | 是 | 是 | 时间字段格式无效或不是 ISO-8601 UTC 格式 |
| E1006 | ERROR | 400 | 1008 | 是 | 是 | 序列号未递增或乱序；重复包按幂等返回 ACK |
| E1007 | ERROR | 500 | 1011 | 是 | 是 | 服务端内部处理异常（非用户输入问题） |
| E1008 | ERROR | 503/429 | 1013 | 是 | 是 | 下游（如 Kafka、Redis）不可用或服务进行限流，暂时无法处理 |
| E1009 | ERROR | 403（初始并发发送冲突） / —（握手后策略违规） | 1008（仅握手后） | 是 | 是 | 不允许的业务操作或策略冲突；同会话初始并发发送冲突在握手阶段返回 403，query 与 `metaData.conversationId` 不一致等握手后违规返回 1008 |
| E1010 | ERROR | 401 | 1008 | 是 | 是 | 保留错误码；鉴权启用时用于缺少、无效或无权限的凭证 |
| E1011 | ERROR | 504 | 1013 | 是 | 是 | 上游或下游服务（如 STT Provider、Kafka）响应超时 |


在关闭连接前，可先发送如下错误帧：

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

## 5. 完整示例

### 5.1 进行中会话 (SESSION_ONGOING)

**请求：**

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

**响应：**

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

### 5.2 结束会话 (SESSION_COMPLETE)

**请求：**

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

**响应：**

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

### 5.3 错误响应 (E1003示例)

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

## 6. Kafka 落盘契约

本节描述服务端在成功路径中写入 Kafka 的消息契约，即 **Server → Kafka** 的内部数据格式。

### 6.1 写入规则

| 项目 | 契约 |
| --- | --- |
| Topic | 由 `KAFKA_TOPIC` 配置决定，默认 `cc.transcript.realtime.v1` |
| Message Key | `conversationId` 的 UTF-8 字节 |
| Message Value | UTF-8 JSON 字节 |
| Value 业务结构 | 与通过校验的上行请求保持同一业务结构，即 `metaData + payload` |
| 服务端是否附加字段 | 否；不追加 ACK、ERROR、`serverProcessingMs` 或 Kafka wrapper |
| 分区路由 | 由 Kafka 根据 Key（`conversationId`）进行分区 |
| 写入时机 | `prepare` 通过后、`commit` 前 |

### 6.2 Kafka Message Value 示例

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

### 6.3 不写入 Kafka 的场景

- Schema 校验失败、业务规则校验失败、握手校验失败时，不写入 Kafka。
- 重复包（IDEMPOTENT）直接返回对应 ACK，不重复写入 Kafka。
- 乱序包（OUT_OF_ORDER）返回 `E1006`，不写入 Kafka。
- 只有通过 `prepare` 且实际执行 Kafka send 的消息，才会进入 Kafka。

---

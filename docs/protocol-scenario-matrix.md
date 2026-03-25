# 协议场景矩阵

本文将 WebSocket 协议在“正常流、错误流、握手前拒绝、关闭码、典型 JSON 响应”上的关键场景集中到一处，作为：

- [API 契约](../design/transcribe-service-API-contract.md) 的补充视图
- [设计护栏定稿](design-guardrails.md) 中“契约级场景矩阵”的落地文档
- [契约级矩阵测试](../tests/test_contract_matrix.py) 的文档对照版本

如本矩阵与 [API 契约](../design/transcribe-service-API-contract.md) 存在冲突，以 API 契约为准。

---

## 一、错误流程（统一视图）


| **ID** | **场景**                                                             | **握手阶段** | **错误码**   | **HTTP 状态码** | **WS Close Code**           | **断连?** | **JSON 示例** |
| ------ | ------------------------------------------------------------------ | -------- | --------- | ------------ | --------------------------- | ------- | ----------- |
| E-01   | 缺少 `conversationId` 参数                                             | 握手前      | **E1003** | **400**      | —                           | 是（拒绝握手） | 见下文 E-01    |
| E-02   | 服务正在停机 (draining)                                                  | 握手前      | **E1008** | **503**      | —                           | 是（拒绝握手） | 见下文 E-02    |
| E-03   | 连接数超限 (`WS_MAX_CONNECTIONS`)                                       | 握手前      | **E1008** | **429**      | —                           | 是（拒绝握手） | 见下文 E-03    |
| E-04   | JSON 解析失败                                                          | 握手后      | **E1001** | —            | **1007** (Invalid Payload)  | 是       | 见下文 E-04    |
| E-05   | 枚举值非法 (如 eventType)                                                | 握手后      | **E1002** | —            | **1008** (Policy Violation) | 是       | 见下文 E-05    |
| E-06   | 缺少必填字段                                                             | 握手后      | **E1003** | —            | **1008** (Policy Violation) | 是       | 见下文 E-06    |
| E-07   | 字段类型不符                                                             | 握手后      | **E1004** | —            | **1008** (Policy Violation) | 是       | 见下文 E-07    |
| E-08   | 时间格式无效                                                             | 握手后      | **E1005** | —            | **1008** (Policy Violation) | 是       | 见下文 E-08    |
| E-09   | 序列号乱序                                                              | 握手后      | **E1006** | —            | **1008** (Policy Violation) | 是       | 见下文 E-09    |
| E-10   | Kafka 超时                                                           | 握手后      | **E1011** | —            | **1013** (Try Again Later)  | 是       | 见下文 E-10    |
| E-11   | Kafka 失败                                                           | 握手后      | **E1008** | —            | **1013** (Try Again Later)  | 是       | 见下文 E-11    |
| E-12   | 编排层未捕获异常                                                           | 握手后      | **E1007** | —            | **1011** (Internal Error)   | 是       | 见下文 E-12    |
| E-13   | 传输层未捕获异常                                                           | 握手后      | **E1007** | —            | **1011** (Internal Error)   | 是       | 见下文 E-13    |
| E-14   | query 与 `metaData.conversationId` 不一致（均为字符串）                       | 握手后      | **E1009** | —            | **1008** (Policy Violation) | 是       | 见下文 E-14    |
| E-15   | 业务规则校验失败（如 `SESSION_ONGOING` 带 `callEndTimeStamp`、`isFinal=false`） | 握手后      | **E1009** | —            | **1008** (Policy Violation) | 是       | 见下文 E-15    |
| E-16   | 第二个连接并发发送同一 `conversationId`                                       | 握手后      | **E1009** | —            | **1008** (Policy Violation) | 是       | 见下文 E-16    |


> 握手前阶段 WebSocket 连接尚未建立，无法发送 WebSocket 文本帧；只能返回 HTTP + JSON body。握手后错误才会发送 WebSocket ERROR 帧并配合 Close Code 断连。

---

## 二、正常流程（单独视图）


| **ID**   | **场景**                | **握手阶段**  | **WS Close Code**     | **断连?** | **Response Json**                       |
| -------- | --------------------- | --------- | --------------------- | ------- | --------------------------------------- |
| **N-01** | SESSION_ONGOING 正常处理  | 握手后       | —                     | 否       | 见下文 N-01                                |
| **N-02** | 幂等命中（重复 seq）          | 握手后       | —                     | 否       | 见下文 N-02                                |
| **N-03** | SESSION_COMPLETE 正常处理（EOL） | 握手后       | **1000** (Normal)     | 是       | 见下文 N-03                                |
| **N-04** | 优雅停机 close_all        | 握手后（存量连接） | **1001** (Going Away) | 是       | *(无 JSON 响应，服务端直接发送 WebSocket close 帧)* |


---

## 三、正常流程 Response JSON 示例

### N-01 SESSION_ONGOING 正常处理

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

### N-02 幂等命中（重复 seq）

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

> 当前示例展示的是 `SESSION_ONGOING` 的重复包；若重复的是 `SESSION_COMPLETE`，则成功响应类型应为 `EOL_ACK`。

### N-03 SESSION_COMPLETE 正常处理

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

> 该场景对应的请求帧是系统级 EOL 控制消息：`eventType=SESSION_COMPLETE`，`payload.speaker=System`。示例中 `payload.transcript` 可写为 `"EOL"`，但服务端当前不校验固定字面值。

### N-04 优雅停机 close_all

该场景不发送业务 JSON 响应；服务端直接向存量连接发送 WebSocket close 帧，关闭码为 **1001**。

---

## 四、错误场景 Response 实例

### E-01 缺少 `conversationId` 参数（HTTP 400）

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

### E-02 服务正在停机 (draining)（HTTP 503）

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

### E-03 连接数超限 (`WS_MAX_CONNECTIONS`)（HTTP 429）

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

### E-04 JSON 解析失败（Close 1007）

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

### E-05 枚举值非法（Close 1008）

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

### E-06 缺少必填字段（Close 1008）

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1003",
    "message": "Validation failed",
    "details": "Field required: metaData.conversationId",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-07 字段类型不符（Close 1008）

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

### E-08 时间格式无效（Close 1008）

```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1005",
    "message": "Validation failed",
    "details": "createdAtTimeStamp must be a valid ISO-8601 UTC timestamp",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-09 序列号乱序（Close 1008）

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

### E-10 Kafka 超时（Close 1013）

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

### E-11 Kafka 失败（Close 1013）

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

### E-12 编排层未捕获异常（Close 1011）

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

### E-13 传输层未捕获异常（Close 1011）

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

### E-14 query 与 `metaData.conversationId` 不一致（Close 1008）

在 JSON 解析成功后、进入编排前校验：若 `metaData.conversationId` 为字符串且与握手 query 中的 `conversationId` 不同，则返回 **E1009** 并断开 **1008**；不调用编排器，不写入 Redis/Kafka。

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

### E-15 业务规则校验失败（Close 1008）

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

### E-16 同会话并发发送冲突（Close 1008）

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

## 五、实现说明


| **错误码**             | **Contract 定义** | **代码现状**                                               |
| ------------------- | --------------- | ------------------------------------------------------ |
| **E1010**           | 鉴权失败 (401/1008) | **未实现** — 无 Auth 中间件                                   |


**E1009** 当前用于三类场景：**传输层 query / body `conversationId` 字符串不一致**、**schema 通过后触发的业务规则校验失败**，以及 **同一 `conversationId` 出现第二个并发发送连接**。**E1010** 仍为预留鉴权错误码，后续若引入 Auth，应同步补充契约、矩阵文档与测试。

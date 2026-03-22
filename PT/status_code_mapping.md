## **一、错误流程（统一视图）**

| **ID** | **场景** | **握手阶段** | **错误码** | **HTTP 状态码** | **WS Close Code** | **断连?** | **JSON 示例** |
| --- | --- | --- | --- | --- | --- | --- | --- |
| E-01 | 缺少 `conversationId` 参数 | 握手前 | **E1003** | **400** | — | 是（拒绝握手） | 见 [E-01](#e-01-缺少-conversationid-参数-http-400) |
| E-02 | 服务正在停机 (draining) | 握手前 | **E1008** | **503** | — | 是（拒绝握手） | 见 [E-02](#e-02-服务正在停机-draining-http-503) |
| E-03 | 连接数超限 (`WS_MAX_CONNECTIONS`) | 握手前 | **E1008** | **429** | — | 是（拒绝握手） | 见 [E-03](#e-03-连接数超限-ws_max_connections-http-429) |
| E-04 | **场景 D1**: JSON 解析失败 | 握手后 | **E1001** | — | **1007** (Invalid Payload) | 是 | 见 [E-04](#e-04-场景-d1-json-解析失败-close-1007) |
| E-05 | **场景 D2**: 枚举值非法 (如 eventType) | 握手后 | **E1002** | — | **1008** (Policy Violation) | 是 | 见 [E-05](#e-05-场景-d2d5-校验失败close-1008) |
| E-06 | **场景 D3**: 缺少必填字段 | 握手后 | **E1003** | — | **1008** (Policy Violation) | 是 | 见 [E-05](#e-05-场景-d2d5-校验失败close-1008) |
| E-07 | **场景 D4**: 字段类型不符 | 握手后 | **E1004** | — | **1008** (Policy Violation) | 是 | 见 [E-05](#e-05-场景-d2d5-校验失败close-1008) |
| E-08 | **场景 D5**: 时间格式无效 | 握手后 | **E1005** | — | **1008** (Policy Violation) | 是 | 见 [E-05](#e-05-场景-d2d5-校验失败close-1008) |
| E-09 | **场景 C**: 序列号乱序 | 握手后 | **E1006** | — | **1008** (Policy Violation) | 是 | 见 [E-06](#e-06-场景-c-序列号乱序close-1008) |
| E-10 | **场景 E1**: Kafka 超时 | 握手后 | **E1012** | — | **1013** (Try Again Later) | 是 | 见 [E-07](#e-07-场景-e1-kafka-超时close-1013) |
| E-11 | **场景 E2**: Kafka 失败 | 握手后 | **E1008** | — | **1013** (Try Again Later) | 是 | 见 [E-08](#e-08-场景-e2-kafka-失败close-1013) |
| E-12 | **场景 F1**: 编排层未捕获异常 | 握手后 | **E1007** | — | **1011** (Internal Error) | 是 | 见 [E-09](#e-09-场景-f1f2-未捕获异常close-1011) |
| E-13 | **场景 F2**: 传输层未捕获异常 | 握手后 | **E1007** | — | **1011** (Internal Error) | 是 | 见 [E-09](#e-09-场景-f1f2-未捕获异常close-1011) |

> 握手前阶段 WebSocket 连接尚未建立，无法发送 WebSocket 文本帧；只能返回 HTTP + JSON body。握手后错误才会发送 WebSocket ERROR 帧并配合 Close Code 断连。

---

## **二、正常流程（单独视图）**

| **场景** | **握手阶段** | **WS Close Code** | **断连?** | **Response Json** |
| --- | --- | --- | --- | --- |
| **场景 A**: SESSION_ONGOING 正常处理 | 握手后 | — | 否 | 见 [N-01](#n-01-场景-a-session_ongoing-正常处理) |
| **场景 B**: 幂等命中（重复 seq） | 握手后 | — | 否 | 见 [N-02](#n-02-场景-b-幂等命中重复-seq) |
| **场景 G**: SESSION_COMPLETE 正常处理 | 握手后 | **1000** (Normal) | 是 | 见 [N-03](#n-03-场景-g-session_complete-正常处理) |
| 优雅停机 close_all | 握手后（存量连接） | **1001** (Going Away) | 是 | *(无 JSON 响应，服务端直接发送 WebSocket close 帧)* |

---

## **三、正常流程 Response JSON 示例（可渲染）**

### N-01 场景 A: SESSION_ONGOING 正常处理
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

### N-02 场景 B: 幂等命中（重复 seq）
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

### N-03 场景 G: SESSION_COMPLETE 正常处理
```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "TRANSCRIPT_ACK" },
  "payload": {
    "sequenceNumber": 42,
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z",
    "serverProcessingMs": 1.56
  }
}
```

---

## **四、错误 JSON 示例（可渲染）**

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

### E-04 场景 D1: JSON 解析失败（Close 1007）
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

### E-05 场景 D2~D5: 校验失败（Close 1008）
```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1003",
    "message": "Validation failed",
    "details": "1 validation error for InboundMessage ...",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-06 场景 C: 序列号乱序（Close 1008）
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

### E-07 场景 E1: Kafka 超时（Close 1013）
```json
{
  "metaData": { "conversationId": "conv-1", "eventType": "ERROR" },
  "error": {
    "code": "E1012",
    "message": "Downstream timeout",
    "details": "Kafka send timed out",
    "createdAtTimeStamp": "2026-03-21T03:00:00.000Z"
  }
}
```

### E-08 场景 E2: Kafka 失败（Close 1013）
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

### E-09 场景 F1/F2: 未捕获异常（Close 1011）
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

---

## **五、与 API Contract 的差异**

| **错误码** | **Contract 定义** | **代码现状** |
| --- | --- | --- |
| **E1010** | 鉴权失败 (401/1008) | **未实现** — 无 Auth 中间件 |
| **E1011** | 资源未找到 (404/1008) | **未实现** — 无对应业务检查 |
| `conversationId` 缺失 | Contract 要求 400 | **已对齐：实际 400**（由 `_WsGuardMiddleware` 统一返回 JSON ERROR） |

E1009/E1010/E1011 枚举值和 `close_code_for_error` 映射都已在 `errors.py` 中定义好了，后续实现鉴权和业务规则时可以直接使用。

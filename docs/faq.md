# 常见问题

---

## Q1：启动报 Redis/Kafka 不可用？

启动前会校验 Redis、Kafka 连通性，失败则退出。

**解决**：确保 `docker compose up -d` 已执行，或配置正确的 `REDIS_URL`、`KAFKA_BOOTSTRAP_SERVERS`。

---

## Q2：序列号乱序（E1006）？

状态机通过 Redis Lua 原子预检，若 `sequenceNumber > expected`（跳号）则返回 `OUT_OF_ORDER`，服务端发送 ERROR 帧并断开连接（Close Code 1008）。

**解决**：确保上游 STT Provider 在同一 `conversationId` 下严格递增发送 `sequenceNumber`。断连后重连会从 Redis 中已保存的 expected 序号继续。

---

## Q3：Kafka 挂了会怎样？

Kafka 发送超时/失败 → 返回 ERROR 帧（E1008/E1011）→ 断连（Close Code 1013）→ 不执行 Redis commit（expected 不前进）。上游重连后重发同一 seq，Redis 预检通过，实现无损重试。

---

## Q4：重复消息如何处理？

同一 `(conversationId, sequenceNumber)` 的重复消息命中 IDEMPOTENT，服务端直接返回 TRANSCRIPT_ACK，不再写入 Kafka，不推进 Redis 状态。

---

## Q5：Kafka 消息顺序？

使用 `conversationId` 作为 Kafka Partition Key，同一通话路由到同一分区，分区内有序。

---

## Q6：优雅停机如何工作？

收到 SIGTERM 后：标记 Drain（拒绝新连接）→ 向存量连接发送 Close 1001 → flush Kafka 缓冲区 → 释放 Redis 连接 → 退出。

---

## Q7：当前设计要求客户端严格遵守哪些接入约束？

为满足当前版本的顺序性、幂等性与无损重试语义，客户端必须严格遵守以下要求：

- **握手标识**：WebSocket 握手必须携带 query 参数 `conversationId`；若消息体中显式提供 `metaData.conversationId`，其值必须与 query 中的 `conversationId` 完全一致。
- **单会话单发送链路**：同一 `conversationId` 在任一时刻应只保留一条活跃发送链路；不要为同一会话建立多条并发发送连接，也不要由多个 worker/线程并发发送同一会话消息。
- **严格顺序**：同一 `conversationId` 下，`sequenceNumber` 必须从 `0` 开始并按 `0, 1, 2, 3...` 连续推进；不允许跳号、不允许乱序、不允许先发 `N+1` 再补发 `N`。
- **ACK 推进发送窗口**：客户端应以 `TRANSCRIPT_ACK(seq=N)` 作为发送窗口推进条件；收到 `N` 的 ACK 后再发送 `N+1`。当前设计不提供服务端乱序重排能力。
- **失败后重发同一 seq**：若收到 `ERROR`（尤其 `E1008` / `E1011`）、WebSocket 被 `1008` / `1013` 关闭，或客户端等待 ACK 超时，重连后必须重发上一个未被 ACK 的同一 `sequenceNumber`，不得跳到下一条。
- **重复重试要保持幂等键不变**：同一次业务重试必须保持 `(conversationId, sequenceNumber)` 不变；服务端会按幂等语义返回 ACK，不会重复写 Kafka。
- **事件语义**：中间过程使用 `SESSION_ONGOING`，此时 `callEndTimeStamp` 必须为 `null`；结束时发送 `SESSION_COMPLETE`，并提供 `callEndTimeStamp`，作为最终 EOL 事件。
- **仅发送 final transcript**：`payload.isFinal` 必须为 `true`；当前服务不接收 partial / interim transcript。
- **请求体必须满足契约字段要求**：必填字段、时间戳格式、`speaker` 取值、`dialect` 格式等都必须满足 API 契约；详细字段定义以 `design/transcribe-service-API-contract.md` 为准。

协议错误码、关闭码与典型正常/异常流，请统一参考：

- `design/transcribe-service-API-contract.md`
- `docs/protocol-scenario-matrix.md`

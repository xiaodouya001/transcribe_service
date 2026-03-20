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

Kafka 发送超时/失败 → 返回 ERROR 帧（E1008/E1012）→ 断连（Close Code 1013）→ 不执行 Redis commit（expected 不前进）。上游重连后重发同一 seq，Redis 预检通过，实现无损重试。

---

## Q4：重复消息如何处理？

同一 `(conversationId, sequenceNumber)` 的重复消息命中 IDEMPOTENT，服务端直接返回 TRANSCRIPT_ACK，不再写入 Kafka，不推进 Redis 状态。

---

## Q5：Kafka 消息顺序？

使用 `conversationId` 作为 Kafka Partition Key，同一通话路由到同一分区，分区内有序。

---

## Q6：优雅停机如何工作？

收到 SIGTERM 后：标记 Drain（拒绝新连接）→ 向存量连接发送 Close 1001 → flush Kafka 缓冲区 → 释放 Redis 连接 → 退出。

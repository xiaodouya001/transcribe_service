# 常见问题

---

## Q1：启动报 Redis/Kafka 不可用？

启动前会校验 Redis、Kafka 连通性，失败则输出 `Transcribe Service: 启动失败（Redis 不可用）` 或 `Transcribe Service: 启动失败（Kafka 不可用）` 并退出。

**解决**：确保 `docker compose up -d` 已执行，或配置正确的 `REDIS_URL`、`KAFKA_BOOTSTRAP_SERVERS`。

---

## Q2：连接 STT 失败，502？

STT 连接地址由 Vendor 通过 Webhook 提供（ws_url、sse_url）。若服务未就绪，会输出 `Reconnect: 连接 STT 失败（STT 提供商服务未就绪，将自动重试）`。

**解决**：检查 STT Provider 服务是否启动，或使用 Demo 模式 `python -m transcribe_service.demo.run_local` 本地验证。

---

## Q3：Kafka 挂了会怎样？

Connector 发送 Kafka 超时会抛出异常，会话重连会重试。无 Buffer 持久化，断连期间的 transcript 可能丢失，需依赖 STT Vendor 重放能力。

---

## Q4：如何修改去重 Key？

通过 `DEDUP_KEY_PARTS` 配置，支持 `session_id`、`processing_id`、`seq_no`、`created_at` 的组合，逗号分隔。例如：`session_id,seq_no`。

---

## Q5：Kafka 消息顺序？

使用 `session_id` 作为 Kafka 消息 Key，相同 session 落入同一分区，分区内有序。

---

## Q6：Dedup 如何工作？

按 `(session_id, processing_id, seq_no)` 去重。同一 transcript 首次到达时发送，重复到达时过滤。

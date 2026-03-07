# 常见问题

---

## Q1：启动报 Redis/Kafka 不可用？

启动前会校验 Redis、Kafka 连通性，失败则输出 `Transcription Ingest: 启动失败（Redis 不可用）` 或 `Transcription Ingest: 启动失败（Kafka 不可用）` 并退出。

**解决**：确保 `docker compose up -d` 已执行，或配置正确的 `REDIS_URL`、`KAFKA_BOOTSTRAP_SERVERS`。

---

## Q2：连接 ASR 失败，502？

配置 `FANOLAB_URL` 为真实 ASR 地址。若服务未就绪，会输出 `Reconnect: 连接 ASR 失败（Fanolab 服务未就绪，将自动重试）`。

**解决**：检查 Fanolab 服务是否启动，或使用 Demo 模式 `python -m transcription_ingest.demo.run_local` 本地验证。

---

## Q3：Kafka 挂了会怎样？

Buffer 模式下，Buffer 消费端发送失败时消息保留在 Redis Stream，不执行 XACK；约 10 秒超时后输出 `Buffer Consumer: 处理消息失败（Kafka 不可用，消息已保留在 Buffer，将自动重试）`。Kafka 恢复后自动重试。

---

## Q4：如何修改去重 Key？

通过 `DEDUP_KEY_PARTS` 配置，支持 `session_id`、`processing_id`、`seq_no`、`created_at` 的组合，逗号分隔。例如：`session_id,seq_no`。

---

## Q5：Kafka 消息顺序？

使用 `session_id` 作为 Kafka 消息 Key，相同 session 落入同一分区，分区内有序。

---

## Q6：Buffer 和 Dedup 如何配合？

- **Buffer**：Redis Stream 持久化 raw payload。Buffer 消费端读取后，成功写入 Kafka 则 XACK + XDEL；发送失败则不 XACK，消息保留在 Stream 待重试。
- **Dedup**：按 `(session_id, processing_id, seq_no)` 去重。发送失败时撤销 dedup 记录，重试时可再次发送。

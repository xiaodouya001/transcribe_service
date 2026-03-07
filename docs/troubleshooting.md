# 故障排查

本文档说明常见错误及排查步骤。

---

## 1. 启动失败

### Transcription Ingest: 启动失败（Redis 不可用）

**原因**：无法连接 Redis。

**排查**：

1. 确认 Redis 已启动：`docker compose ps` 或 `redis-cli ping`
2. 检查 `REDIS_URL` 是否正确（host、port、db）
3. 若在 Docker 网络内，使用服务名而非 localhost

### Pipeline: 启动失败（Kafka 不可用）

**原因**：无法连接 Kafka。

**排查**：

1. 确认 Kafka 已启动：`docker compose ps`，Kafka 健康检查通过
2. 检查 `KAFKA_BOOTSTRAP_SERVERS` 地址
3. Kafka 启动较慢，可等待 30–60 秒后重试

---

## 2. 连接 STT 失败

### Reconnect: 连接 STT 失败（STT 提供商服务未就绪，将自动重试）

**原因**：502/503/504、connection refused 等，视为 STT 不可用。

**排查**：

1. 确认 `STT_PROVIDER_URL` 正确
2. 检查 STT 服务是否启动、端口是否开放
3. 若为 HTTPS，检查证书、代理

### 其他连接错误

非 502/503/504 的错误会输出 `Reconnect: 连接 STT 失败（将自动重试）`，同样会按指数退避重试。

---

## 3. Kafka 发送失败

### Buffer Consumer: 处理消息失败（Kafka 不可用，消息已保留在 Buffer，将自动重试）

**原因**：发送 Kafka 超时或异常。

**行为**：消息保留在 Redis Stream，不 XACK；dedup 记录已撤销，Kafka 恢复后会自动重试。

**排查**：

1. 检查 Kafka 集群状态
2. 检查 `KAFKA_SEND_TIMEOUT_SEC` 是否过短（默认 10 秒）
3. 通过 Kafka UI 确认 Topic 存在、可写

---

## 4. 日志关键字


| 关键字                       | 含义            |
| ------------------------- | ------------- |
| `Transcription Ingest: 已启动`           | 启动成功          |
| `Transcription Ingest: 正在关闭连接`        | 优雅停机中         |
| `Dedup: 通过（新 transcript）` | 去重通过，将发送      |
| `Dedup: 已过滤重复`            | 重复消息已过滤       |
| `Kafka Producer: 已发送`     | 消息已成功写入 Kafka |



# Transcribe Service 行业最佳实践及其他

本文档汇总 Transcribe Service 直连模式的可靠性、可观测性、安全及部署运维最佳实践。

---

## 1. 可靠性

### 1.1 连接管理

- **每会话独立 Connector**：失败隔离，单会话异常不影响其他会话
- **指数退避重连**：沿用 [connector/reconnect.py](../../src/transcription_ingest/connector/reconnect.py) 设计
- **连接失败**：仅连接失败时使用指数退避；连接正常结束（流关闭）时短延迟重连

### 1.2 优雅停机

- 收到 SIGTERM/SIGINT 后 `GracefulShutdown` 置 `draining=True`
- 停止接收新会话，等待当前会话处理完毕（`stop_timeout`）
- 最后 `producer.flush()`、`producer.close()`、`dedup.close()`

### 1.3 背压

- Kafka 发送超时（`kafka_send_timeout_sec`）时记录并重试
- 避免阻塞 Connector，避免长时间占用 STT 连接

---

## 2. 可观测性

### 2.1 指标

- 每 Pod 活跃会话数
- 每会话消息数
- Dedup 命中率
- Kafka 发送延迟/失败率

### 2.2 日志

- 结构化日志（structlog）
- 包含 `session_id`、`seq_no`、`stage` 等关键字段

### 2.3 追踪

- 可选 OpenTelemetry，从 Connector 到 Kafka 全链路 trace

---

## 3. 安全

### 3.1 Webhook 认证

**建议 Vendor STT 使用 HTTPS + HMAC 方式进行 Webhook 认证**：

- **HTTPS**：生产环境 Webhook 接收地址必须使用 HTTPS
- **HMAC-SHA256 签名**：Vendor 对 raw body 计算 `HMAC-SHA256(secret, body)`，通过 `X-Webhook-Signature: sha256=<hex>` 传递；Transcribe Service 使用相同 secret 校验，防止伪造与重放
- **Secret 管理**：由 Transcribe Service 侧生成（如 `openssl rand -hex 32`），通过 `TRANSCRIBE_SERVICE_WEBHOOK_SECRET` 配置，并安全交付给 Vendor
- **可选**：IP 白名单、API Key 等作为补充手段

详见 [04-vendor-interface-confirmation.md](04-vendor-interface-confirmation.md) 第 5 节。

### 3.2 STT 连接认证

- 连接 ws_url/sse_url 时携带 Token、API Key 等（视 Vendor 支持）

### 3.3 网络

- 最小权限安全组
- Transcribe Service 暴露 Webhook 入站端口及出站访问 Vendor

---

## 4. 部署与运维

### 4.1 滚动更新

- 先缩容再扩容，或蓝绿部署
- 避免会话中断

### 4.2 容量规划

- 600 会话 / 12 Pod ≈ 50 会话/Pod
- 预留 20% 余量

---

## 5. 相关文档

- [01-application-design.md](01-application-design.md) - 应用设计
- [02-infrastructure-design.md](02-infrastructure-design.md) - Infra 设计

# 故障排查

---

## 1. 启动失败

### 启动失败: Redis 不可用

**原因**：无法连接 Redis。

**排查**：

1. 确认 Redis 已启动：`docker compose ps` 或 `redis-cli ping`
2. 检查 `REDIS_URL` 是否正确
3. 若在 Docker 网络内，使用服务名而非 localhost

### 启动失败: Kafka 不可用

**原因**：无法连接 Kafka（30s 超时）。

**排查**：

1. 确认 Kafka 已启动：`docker compose ps`，健康检查通过
2. 检查 `KAFKA_BOOTSTRAP_SERVERS` 地址
3. Kafka 启动较慢，可等待 30–60 秒后重试

---

## 2. WebSocket 连接问题

### 客户端连接被拒绝（503）

**原因**：服务处于 Drain 模式（优雅停机中）。

**解决**：等待新版本 Pod 就绪后重连。

### 连接被关闭（Close Code 1008）

**原因**：Schema 校验失败或序列号乱序。查看 ERROR 帧中的 `error.code` 和 `error.details`。

### 连接被关闭（Close Code 1013）

**原因**：Kafka 不可用或超时。服务端暂时无法处理，建议稍后重连。

---

## 3. Kafka 发送问题

### Kafka: 发送超时

**原因**：Kafka 集群响应超过 `KAFKA_SEND_TIMEOUT_SEC`（默认 2s）。

**排查**：

1. 检查 Kafka 集群状态
2. 通过 Kafka UI 确认 Topic 存在、可写
3. 检查网络延迟

---

## 4. 日志关键字

| 关键字 | 含义 |
|--------|------|
| `Transcribe Service: 已启动` | 启动成功 |
| `Transport: 连接已建立` | WebSocket 连接建立 |
| `StateMachine.prepare` | Lua 预检结果 |
| `StateMachine.commit` | 序列号推进 |
| `Kafka: 已发送` | 消息已写入 Kafka |
| `Orchestrator: 幂等命中` | 重复包被拦截 |
| `Orchestrator: 序列号乱序` | 乱序包被拒绝 |
| `Shutdown: 开始优雅停机` | 收到 SIGTERM |

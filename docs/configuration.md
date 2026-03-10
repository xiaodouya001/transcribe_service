# 配置说明

本文档说明 Transcribe Service 的环境变量配置，与 [config/settings.py](../config/settings.py) 对应。

---

## 1. 环境变量

复制 `.env.example` 为 `.env` 并修改。**环境变量统一使用 UPPER_SNAKE_CASE**。

```bash
cp .env.example .env
```

---

## 2. 配置项一览

### Transcribe Service（Webhook 模式）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `TRANSCRIBE_SERVICE_MAX_SESSIONS_PER_POD` | 100 | 单 Pod 最大会话数 |
| `TRANSCRIBE_SERVICE_PROTOCOL` | sse | 协议：`sse` 或 `websocket`（Webhook 收到 ws_url/sse_url 后按此选择） |
| `TRANSCRIBE_SERVICE_WEBHOOK_SECRET` | 空 | Webhook HMAC 签名密钥；配置后 Vendor 需在 `X-Webhook-Signature: sha256=<hex>` 中携带签名；空则跳过校验（仅 Demo） |
| `TRANSCRIBE_SERVICE_SSRF_ALLOW_LOCALHOST` | false | 是否允许 ws_url/sse_url 指向 127.0.0.1；仅 Demo 可设为 true |

Webhook 路径 `/webhook/session`、host `0.0.0.0`、port `8080` 固定于代码，由 Docker/ECS 编排。**建议 Vendor 使用 HTTPS + HMAC 认证**，见 [04-vendor-interface-confirmation.md](specs/04-vendor-interface-confirmation.md) 第 5 节。

### Redis

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `REDIS_URL` | redis://localhost:6379/0 | Redis 连接地址 |
| `DEDUP_KEY_PARTS` | session_id,processing_id,seq_no | 去重 Key 组成 |
| `DEDUP_TTL_SECONDS` | 60 | 去重 Key 过期时间（秒） |

### Kafka

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka 集群地址 |
| `KAFKA_TOPIC` | transcription_topic | Topic 名称 |
| `KAFKA_COMPRESSION_TYPE` | none | 压缩：`none`、`gzip`、`snappy`、`lz4` |
| `KAFKA_SEND_TIMEOUT_SEC` | 10 | 发送超时（秒），Kafka 不可用时超时并输出错误日志 |

### 长连接与重连

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `RECONNECT_ENABLED` | true | 自动重连 |
| `RECONNECT_MAX_RETRIES` | 0 | 最大重试次数，0=无限 |
| `RECONNECT_INITIAL_DELAY` | 1.0 | 初始退避延迟（秒） |
| `RECONNECT_MAX_DELAY` | 60.0 | 最大退避延迟（秒） |
| `RECONNECT_BACKOFF_FACTOR` | 2.0 | 退避因子 |
| `SSE_READ_TIMEOUT` | 省略 | SSE 读超时（秒）；`none` 或省略=无限制；或设为秒数 |
| `WS_PING_INTERVAL` | 20.0 | WebSocket ping 间隔（秒） |
| `WS_PING_TIMEOUT` | 20.0 | WebSocket pong 超时（秒） |

### 其它

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `CLEANER_MODE` | default | 数据清洗：`default`（raw+cleaned）、`identity`（透传） |
| `STOP_TIMEOUT` | 120 | 优雅停机超时（秒） |
| `LOG_LEVEL` | INFO | 日志级别 |
| `LOG_FORMAT` | auto | 日志格式：`json`、`console`、`auto` |

---

## 3. 配置示例

**本地开发**：

```env
REDIS_URL=redis://localhost:6379/0
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
TRANSCRIBE_SERVICE_PROTOCOL=sse
```

**生产**：

```env
REDIS_URL=redis://your-elasticache:6379/0
KAFKA_BOOTSTRAP_SERVERS=your-msk:9092
KAFKA_COMPRESSION_TYPE=gzip
TRANSCRIBE_SERVICE_PROTOCOL=sse
LOG_FORMAT=json
```

---

## 4. CLEANER_MODE 说明

| 值 | 说明 | Kafka 输出 |
|------|------|------------|
| `default` | 提取结构化字段 + 保留原始 | `{raw, cleaned}` |
| `identity` | 透传原始 payload | `{raw}` |

---

## 5. 相关文档

- [pyproject-config.md](pyproject-config.md) - pyproject.toml 构建与依赖配置
- [docs/specs/01-application-design.md](specs/01-application-design.md) - 应用设计

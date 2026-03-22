# 部署指南

本文档说明 Transcribe Service 的构建与部署流程，与当前代码（WebSocket 网关 + Redis 状态机 + Kafka）一致。

---

## 1. 构建镜像

```bash
docker build -f docker/Dockerfile -t transcribe-service:latest .
```

镜像基于 `python:3.12-slim`，多阶段构建，非 root 用户运行，入口为 `python -m transcribe_service.main`。

---

## 2. 目标环境

**AWS ECS Fargate**，需：

- **VPC**：服务与 ElastiCache、MSK 同网段或可路由
- **ElastiCache Redis**：提供 `REDIS_URL`（序列守卫 / 2PC 状态）
- **MSK**：提供 `KAFKA_BOOTSTRAP_SERVERS`
- **负载均衡**：上游 STT Provider 通过 **WSS** 连接；通常使用 **ALB**（空闲超时需大于 WebSocket 心跳，见设计文档），目标组健康检查指向 HTTP 端点（见下文）

**协议说明**：本服务是 **WebSocket 服务端**，不再主动连接外部 STT；上游客户端连接：

`wss://<your-host>/ws/v1/realtime-transcriptions?conversationId=<id>`

完整契约见 [design/transcribe-service-API-contract.md](../design/transcribe-service-API-contract.md)。

---

## 3. 环境变量

生产最小集建议配置（更多项见 [.env.example](../.env.example) 与 [configuration.md](configuration.md)）：

| 变量 | 说明 |
|------|------|
| `REDIS_URL` | ElastiCache Redis 连接串 |
| `KAFKA_BOOTSTRAP_SERVERS` | MSK broker 地址 |
| `KAFKA_TOPIC` | 默认 `cc.transcript.realtime.v1`（须与 Topic 实际名称一致） |
| `KAFKA_TOPIC_NUM_PARTITIONS` | 仅当服务负责建 Topic 时有效；生产 Topic 通常由运维预建 |
| `KAFKA_REPLICATION_FACTOR` | 生产建议 ≥ 2（与 MSK 策略一致） |
| `KAFKA_COMPRESSION_TYPE` | 默认 `zstd`；也可设为 `gzip` 等 |
| `HTTP_HOST` / `HTTP_PORT` | 监听地址与端口（容器内多为 `0.0.0.0:8080`） |
| `KAFKA_STARTUP_TIMEOUT_SEC` | 启动时 Kafka 连通性检查超时 |
| `LOG_FORMAT` | 生产建议 `json` |
| `LOG_LEVEL` | 如 `INFO` |

可通过 ECS Task Definition 的 `environment` 或 AWS Secrets Manager 注入。

**已废弃**：不再使用 `STT_PROVIDER_URL`（旧「客户端连 STT」模式）。

---

## 4. 健康检查

服务提供 **HTTP** 探针（便于 ALB / ECS）：

| 路径 | 用途 |
|------|------|
| `GET /health` | 存活（进程可用） |
| `GET /ready` | 就绪（Redis + Kafka 可连通） |
| `GET /metrics` | 示例指标（如活跃 WebSocket 数） |

启动时 `main` 会在监听前执行 `_check_redis`、`_check_kafka`，失败则进程退出。

---

## 5. 扩缩容

- 建议同一 **conversationId** 的会话在同一时刻由上游路由到单一实例持续处理；当前服务端不强制拒绝同 `conversationId` 的重复建连，水平扩展时仍建议通过上游 **路由或重连** 收敛到目标实例（部署/缩容时常用 `Close 1001` 等信号）。
- **跨实例一致性** 依赖 **Redis Lua 状态机**（期望序号与 2PC），非旧版 SETNX Dedup。
- Kafka 以 `conversationId` 为分区键，保证单路通话在分区内有序。

---

## 6. 相关文档

- [configuration.md](configuration.md) 环境变量完整说明
- [concurrency-capacity.md](concurrency-capacity.md) **Kafka / Redis / 单机** 与千级并发 WebSocket 的瓶颈与调参
- [design/application-design_zh.md](../design/application-design_zh.md) 架构与优雅停机

# 部署指南

本文档说明 Transcription Ingest 的构建与部署流程。

---

## 1. 构建镜像

```bash
docker build -f docker/Dockerfile -t transcription-ingest:latest .
```

镜像基于 `python:3.12-slim`，多阶段构建，非 root 用户运行。

---

## 2. 目标环境

**AWS ECS Fargate**，需：

- **VPC**：服务与 Redis、Kafka 同网段或可路由
- **ElastiCache Redis**：提供 `REDIS_URL`
- **MSK**：提供 `KAFKA_BOOTSTRAP_SERVERS`

---

## 3. 环境变量

生产环境需配置：

| 变量 | 说明 |
|------|------|
| `FANOLAB_URL` | Fanolab ASR 地址 |
| `REDIS_URL` | ElastiCache Redis 连接串 |
| `KAFKA_BOOTSTRAP_SERVERS` | MSK broker 地址 |
| `LOG_FORMAT` | 建议 `json`，便于日志采集 |
| `KAFKA_COMPRESSION_TYPE` | 建议 `gzip` 节省带宽 |

可通过 ECS Task Definition 的 `environment` 或 AWS Secrets Manager 注入。

---

## 4. 健康检查

服务为长连接型，无 HTTP 健康检查端点。建议：

- ECS 使用 `HEALTHCHECK` 或通过 CloudWatch 监控进程存活
- 启动时 `_check_redis`、`_check_kafka` 会校验连通性，失败则退出

---

## 5. 扩缩容

单实例可处理一条 ASR 长连接。多会话场景可水平扩展多实例，Dedup 依赖 Redis 保证去重一致性。

# Transcribe Service

> 实时转录接入与分发服务。Vendor 通过 Webhook 推送会话，ConnectorManager 建连 STT，去重后异步推送到 Kafka。

---

## 快速开始

```bash
# 1. 安装
poetry install --with dev
poetry shell

# 2. 启动依赖
docker compose up -d

# 3. 运行（Demo 模式）
python -m transcribe_service.demo.run_local
# 或生产模式
python -m transcribe_service.main
```

---

## 文档

| 文档 | 说明 |
|------|------|
| [docs/README.md](docs/README.md) | **文档索引**（推荐入口） |
| [docs/design-overview.md](docs/design-overview.md) | 设计总览（应用、基础设施、协议、架构） |
| [docs/architecture.md](docs/architecture.md) | 架构设计 |
| [docs/configuration.md](docs/configuration.md) | 配置说明 |
| [docs/development.md](docs/development.md) | 本地开发与 UT |
| [docs/cicd.md](docs/cicd.md) | CI/CD |
| [docs/deployment.md](docs/deployment.md) | 部署指南 |
| [docs/faq.md](docs/faq.md) | 常见问题 |
| [docs/troubleshooting.md](docs/troubleshooting.md) | 故障排查 |

---

## 项目结构

```
transcribe_service/
├── config/              # Pydantic Settings
├── src/transcribe_service/      # 主逻辑
│   ├── main.py          # 入口（Webhook 模式）
│   ├── webhook/         # Webhook HTTP 端点
│   ├── connector/       # SSE/WebSocket 接入 + ConnectorManager
│   ├── dedup/           # 去重
│   ├── transform/       # 数据清洗
│   ├── producer/        # Kafka 输出
│   └── demo/            # Mock + 前端
├── tests/
├── docker/
├── docs/
└── docker-compose.yml   # Redis + Kafka + Kafka UI
```

---

## 部署

```bash
docker build -f docker/Dockerfile -t transcribe-service:latest .
```

目标环境：AWS ECS Fargate。详见 [docs/deployment.md](docs/deployment.md)。

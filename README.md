# Transcribe Service

> 多云实时数据网关。FanoLabs STT Provider 通过 WebSocket 主动连接本服务，Transcribe Service 执行两阶段提交（Redis Lua 保序 + Kafka 持久化），将转写文本可靠投递至 Kafka。

---

## 快速开始

```bash
# 1. 安装
poetry install --with dev
poetry shell

# 2. 启动依赖
docker compose up -d

# 3. 运行
python -m transcribe_service.main
```

---

## 项目结构

```
transcribe_service/
├── config/                          # Pydantic Settings
├── src/transcribe_service/
│   ├── main.py                      # 主控入口（DI + 生命周期）
│   ├── schemas/                     # 契约层：Pydantic 请求/响应模型
│   ├── transport/                   # 接入层：WebSocket 服务端
│   ├── state_machine/               # 状态机层：Redis Lua 序列守卫
│   ├── producer/                    # 投递层：Kafka 生产者
│   ├── orchestrator/                # 调度层：两阶段提交编排
│   └── shutdown/                    # 优雅停机
├── tests/
├── design/                          # 设计文档
├── docs/                            # 运维文档
└── docker-compose.yml               # Redis + Kafka + Kafka UI
```

---

## 设计文档

| 文档 | 说明 |
|------|------|
| [design/application-design_zh.md](design/application-design_zh.md) | 应用设计总览 |
| [design/transcribe-service-API-contract.md](design/transcribe-service-API-contract.md) | API 契约 |

---

## 运维文档

| 文档 | 说明 |
|------|------|
| [docs/configuration.md](docs/configuration.md) | 配置说明 |
| [docs/development.md](docs/development.md) | 本地开发与测试 |
| [docs/deployment.md](docs/deployment.md) | 部署指南 |
| [docs/cicd.md](docs/cicd.md) | CI/CD |
| [docs/faq.md](docs/faq.md) | 常见问题 |
| [docs/troubleshooting.md](docs/troubleshooting.md) | 故障排查 |

---

## 部署

```bash
docker build -f docker/Dockerfile -t transcribe-service:latest .
```

目标环境：AWS ECS Fargate。详见 [docs/deployment.md](docs/deployment.md)。

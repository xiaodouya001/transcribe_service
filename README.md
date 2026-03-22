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

## 文档入口

完整文档索引见 [docs/README.md](docs/README.md)。该目录页是 `docs/` 下所有设计、配置、开发、部署和排障文档的唯一索引入口。

常用入口：


| 文档                                                                                     | 说明     |
| -------------------------------------------------------------------------------------- | ------ |
| [design/application-design_zh.md](design/application-design_zh.md)                     | 应用设计总览 |
| [design/transcribe-service-API-contract.md](design/transcribe-service-API-contract.md) | API 契约 |
| [docs/README.md](docs/README.md)                                                       | 文档总索引  |

---

## 部署

```bash
docker build -f docker/Dockerfile -t transcribe-service:latest .
```

目标环境：AWS ECS Fargate。详见 [docs/deployment.md](docs/deployment.md)。

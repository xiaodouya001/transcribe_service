# Realtime Transcribe Service

> 多云实时数据网关。Fano Assist 通过 WebSocket 主动连接本服务，Realtime Transcribe Service 执行两阶段提交（Redis Lua 保序 + Kafka 持久化），将转写文本可靠投递至 Kafka。

---

## 快速开始

```bash
# 1. 安装
poetry install --with dev
poetry shell

# 2. 启动依赖
docker compose up -d

# 3. 运行
python -m realtime_transcribe_service.main
```

---

## 项目结构

```
realtime_transcribe_service/
├── config/                          # Pydantic Settings
├── src/realtime_transcribe_service/
│   ├── main.py                      # 主控入口（DI + 生命周期）
│   ├── schemas/                     # 契约层：Pydantic 请求/响应模型
│   ├── transport/                   # 接入层：WebSocket 服务端
│   ├── redis/                       # Redis 基础设施：序列状态机 + 会话发送所有权守卫
│   ├── converter/                   # Kafka 出站转换层：增加enrich节点并校验出站契约
│   ├── utils/                       # 通用工具层：时间格式等跨层可复用 helper
│   ├── producer/                    # 投递层：Kafka 生产者
│   ├── orchestrator/                # 调度层：两阶段提交编排
│   └── shutdown/                    # 优雅停机
├── tests/
├── design/                          # 设计文档、护栏、场景矩阵与 API 契约
├── docs/                            # 配置、部署、开发与排障
├── tools/mock_client/               # 场景测试、压测与 Kafka 回显工具
└── docker-compose.yml               # Redis + Kafka + Kafka UI
```

## 文档入口

完整文档索引见 [docs/README.md](docs/README.md)。该目录页汇总 `design/`、`docs/` 和关键工具文档的主要入口。

常用入口：


| 文档 | 说明 |
|------|------|
| [design/realtime-transcribe-service-app-design_zh.md](design/realtime-transcribe-service-app-design_zh.md) | 应用设计总览（中文） |
| [design/realtime-transcribe-service-app-design.md](design/realtime-transcribe-service-app-design.md) | Application architecture overview (English) |
| [design/realtime-transcribe-service-api-contract.md](design/realtime-transcribe-service-api-contract.md) | API contract (English) |
| [design/realtime-transcribe-service-design-guardrails.md](design/realtime-transcribe-service-design-guardrails.md) | 长期维护护栏与变更约束 |
| [design/realtime-transcribe-service-protocol-scenario-matrix.md](design/realtime-transcribe-service-protocol-scenario-matrix.md) | 协议场景矩阵 |
| [tools/mock_client/README.md](tools/mock_client/README.md) | Mock Client、场景测试与压测说明 |
| [docs/README.md](docs/README.md) | 文档总索引 |

---

## 部署

```bash
docker build -f docker/Dockerfile -t realtime-transcribe-service:latest .
```

目标环境：AWS ECS Fargate。详见 [docs/deployment.md](docs/deployment.md)。





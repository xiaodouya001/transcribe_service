# CI/CD

本文档说明 CI/CD 流程及 GitHub Actions 配置。

---

## 1. CI 流程

| 步骤 | 说明 |
|------|------|
| **Lint** | （可选）ruff / black 代码格式检查 |
| **UT** | pytest + coverage ≥90% |
| **Docker 构建** | 验证 `docker build` 成功 |

UT 使用 fakeredis 和 unittest.mock，**不依赖真实 Redis/Kafka**，CI 环境无需启动中间件。

---

## 2. GitHub Actions 配置

项目包含 `.github/workflows/ci.yml`，在 push / PR 时自动执行：

- **test**：安装依赖，运行 `pytest`（含覆盖率）
- **docker**：构建 `docker/Dockerfile` 镜像

### 2.1 环境变量

CI 中 UT 不需要 Redis/Kafka。若后续有集成测试需连接中间件，可在 GitHub Secrets 中配置：

- `REDIS_URL`
- `KAFKA_BOOTSTRAP_SERVERS`

并在 workflow 中通过 `env` 注入。

---

## 3. CD 说明

目标部署环境：**AWS ECS Fargate**，需 VPC、ElastiCache Redis、MSK。

CD 流程（需根据实际基础设施补充）：

1. 构建镜像并推送到 ECR
2. 更新 ECS 服务或 Task Definition
3. 环境变量通过 ECS Task Definition 或 Secrets Manager 注入

详见 [deployment.md](deployment.md)。

# CI/CD

本文档说明 CI/CD 流程及 GitHub Actions 配置。

---

## 1. CI 流程

| 步骤 | 说明 |
|------|------|
| **Lint** | 如启用，可执行 ruff / black 代码格式检查 |
| **UT** | `pytest` + 覆盖率报告（见 `pyproject.toml` 的 `[tool.pytest.ini_options].addopts`） |
| **Docker 构建** | 验证 `docker build` 成功 |

UT 使用 `fakeredis[lua]`（Lua 与生产 Redis 脚本一致）和 `unittest.mock`，**不依赖真实 Redis/Kafka**，CI 环境无需启动中间件。

---

## 2. GitHub Actions 配置

项目包含 [.github/workflows/ci.yml](../.github/workflows/ci.yml)，在 push / PR 到 `main` / `master` 时执行：

- **test**：Python 3.12，`poetry install --with dev`，`poetry run pytest`
- **docker**：`docker/build-push-action` 构建 `docker/Dockerfile`（不推送到 Registry）

### 2.1 环境变量

默认 UT 不依赖 Redis/Kafka。需要引入依赖真实中间件的集成测试时，可在 GitHub Secrets 中配置并在 workflow 的 `env` 中注入，例如：

- `REDIS_URL`
- `KAFKA_BOOTSTRAP_SERVERS`

---

## 3. CD 说明

目标部署环境：**AWS ECS Fargate**，需 VPC、ElastiCache Redis、MSK。

CD 流程（由团队基础设施定义）：

1. 构建镜像并推送到 ECR
2. 更新 ECS 服务或 Task Definition
3. 环境变量通过 ECS Task Definition 或 Secrets Manager 注入

详见 [deployment.md](deployment.md)。

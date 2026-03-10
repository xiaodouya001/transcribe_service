# pyproject.toml 配置说明

本文档说明 `pyproject.toml` 中各配置块的用途，以及针对 Local / Dev / Production 环境的使用方式。

---

## 1. 配置块说明

### 1.1 [project]

PEP 621 标准项目元数据，供 pip、Poetry 等工具读取。

| 字段 | 说明 |
|------|------|
| `name` | 包名，`pip install` 时使用 |
| `version` | 版本号 |
| `description` | 项目描述 |
| `requires-python` | Python 版本要求（>=3.11） |
| `dependencies` | **运行时依赖**，生产环境必须安装 |

### 1.2 [project.optional-dependencies]

可选依赖组，按需安装。

| 组名 | 说明 | 包含 |
|------|------|------|
| `dev` | 开发/测试/演示 | pytest、pytest-asyncio、fakeredis |

安装方式：`pip install -e ".[dev]"` 或 `poetry install --with dev`

### 1.3 [tool.pytest.ini_options]

pytest 配置，无需单独 `pytest.ini`。

| 选项 | 说明 |
|------|------|
| `asyncio_mode = "auto"` | 自动识别 async 测试 |
| `asyncio_default_fixture_loop_scope` | fixture 作用域 |

### 1.4 [build-system]

构建后端，`pip install -e .` 时使用。

### 1.5 [tool.setuptools.packages.find]

setuptools 包发现，指定 `config` 和 `transcription_ingest` 的路径。

### 1.6 [tool.poetry]

Poetry 专用配置（与 pip/venv 二选一）。`packages` 定义包结构，`[tool.poetry.group.dev.dependencies]` 对应 dev 依赖。

---

## 2. 环境使用指南

### 2.1 Local（本地快速体验）

需 Redis + Kafka（`docker compose up -d`），使用 Mock 服务器。

```bash
# 安装（仅运行时依赖）
pip install -e .
# 或
poetry install

# 运行生产服务（配置 STT_PROVIDER_URL 后）
python -m transcription_ingest.main
```

- **依赖**：`[project].dependencies`（不含 dev）
- **配置**：`.env` 中 `REDIS_URL`、`KAFKA_BOOTSTRAP_SERVERS`（默认 localhost）

### 2.2 Dev（开发 / 测试 / 演示）

需要运行测试、使用 fakeredis 等。

```bash
# 安装（含 dev 依赖）
pip install -e ".[dev]"
# 或
poetry install --with dev

# 运行测试
pytest tests/ -v

```

- **依赖**：`[project].dependencies` + `[project.optional-dependencies].dev`
- **配置**：`.env` 中 Redis、Kafka 地址，本地 Mock 或真实 STT Provider URL

### 2.3 Production（生产部署）

连接真实 Redis、Kafka。生产模式下自动启用长连接重连、WebSocket 心跳与优雅停机。

```bash
# 安装（仅运行时依赖，不要 dev）
pip install .
# 或
poetry install --no-dev

# 运行服务
python -m transcription_ingest.main
```

- **依赖**：仅 `[project].dependencies`
- **配置**：`.env` 或环境变量：
  - `REDIS_URL`：ElastiCache 等
  - `KAFKA_BOOTSTRAP_SERVERS`：MSK 等
  - `TRANSCRIBE_SERVICE_PROTOCOL`：sse 或 websocket

---

## 3. 依赖安装对照表

| 环境 | 命令 | 包含 |
|------|------|------|
| Local | `pip install -e .` | 运行时 |
| Dev | `pip install -e ".[dev]"` | 运行时 + pytest、fakeredis 等 |
| Production | `pip install .` | 仅运行时（无 -e，非可编辑） |

Poetry 用户：`poetry install`（默认含 dev）、`poetry install --with dev`、`poetry install --no-dev`。

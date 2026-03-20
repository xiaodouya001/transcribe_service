# pyproject.toml 配置说明

本文档说明 `pyproject.toml` 中各配置块的用途，以及 Local / Dev / Production 下的使用方式。

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

| 组名 | 说明 | 包含（摘要） |
|------|------|--------------|
| `dev` | 开发/测试 | pytest、pytest-asyncio、pytest-cov、`fakeredis[lua]`、httpx |

- `fakeredis[lua]`：单元测试需执行与生产一致的 Redis Lua 脚本。
- `httpx`：ASGI/HTTP 测试客户端（如 WebSocket 相关测试）。

安装：`pip install -e ".[dev]"` 或 `poetry install --with dev`。

### 1.3 [tool.pytest.ini_options]

pytest 配置，无需单独 `pytest.ini`。

| 选项 | 说明 |
|------|------|
| `asyncio_mode = "auto"` | 自动识别 async 测试 |
| `asyncio_default_fixture_loop_scope` | fixture 作用域 |
| `addopts` | 默认带 `--cov=...` 与覆盖率报告；**不设** `--cov-fail-under` 时不会因覆盖率阈值失败 |

### 1.4 [tool.coverage.run]

覆盖率收集范围与 `omit`（如排除 `main.py`、`logging_config.py`）。

### 1.5 [build-system]

构建后端，`pip install -e .` 时使用。

### 1.6 [tool.setuptools.packages.find]

setuptools 包发现，包含 `config` 与 `transcribe_service`（`src/`）。

### 1.7 [tool.poetry]（可选）

Poetry 与 pip 二选一。`[tool.poetry.group.dev.dependencies]` 与 `[project.optional-dependencies].dev` 应对齐用途；版本写法可能略有差异（如 `pytest-cov`），以实际锁文件为准。

---

## 2. 环境使用指南

### 2.1 Local（本地运行）

需 Redis + Kafka（`docker compose up -d`），再启动服务：

```bash
pip install -e .
# 或
poetry install

python -m transcribe_service.main
```

- **依赖**：`[project].dependencies`
- **配置**：`.env` 中 `REDIS_URL`、`KAFKA_BOOTSTRAP_SERVERS` 等，见 [.env.example](../.env.example)

当前架构为 **WebSocket 服务端**，上游客户端连接 `ws://.../ws/v1/realtime-transcriptions?conversationId=...`，无需配置旧版 `STT_PROVIDER_URL` 或 `TRANSCRIBE_SERVICE_PROTOCOL`。

### 2.2 Dev（开发 / 测试）

```bash
pip install -e ".[dev]"
# 或
poetry install --with dev

pytest tests/ -v
# 或
poetry run pytest
```

- **依赖**：运行时 + `[project.optional-dependencies].dev`

### 2.3 Production（生产部署）

```bash
pip install .
# 或
poetry install --no-dev

python -m transcribe_service.main
```

- **依赖**：仅 `[project].dependencies`
- **配置**：环境变量，见 [configuration.md](configuration.md)、[deployment.md](deployment.md)

---

## 3. 依赖安装对照表

| 环境 | 命令 | 包含 |
|------|------|------|
| Local | `pip install -e .` | 运行时 |
| Dev | `pip install -e ".[dev]"` | 运行时 + dev 组 |
| Production | `pip install .` | 仅运行时 |

也可使用项目根目录的 `requirements.txt` / `requirements-dev.txt`（与 `pyproject` 对齐时）配合 `pip install -r`。

Poetry：`poetry install`（默认可安装 dev）、`poetry install --without dev` 等以 Poetry 版本文档为准。

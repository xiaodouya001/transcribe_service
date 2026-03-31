# `pyproject.toml` Reference

This document explains the purpose of each `pyproject.toml` section and how the project uses them in local development, testing, and production.

---

## 1. Configuration Blocks

### 1.1 `[project]`

This is the PEP 621 metadata block consumed by tooling such as pip and Poetry.

| Field | Description |
|------|------|
| `name` | Package name used by `pip install` |
| `version` | Package version |
| `description` | Project description |
| `requires-python` | Supported Python version, currently `>=3.11` |
| `dependencies` | Runtime dependencies required in production (includes `aws-msk-iam-sasl-signer-python` for `KAFKA_MODE=aws_msk`; unused imports are avoided until MSK IAM is used) |

### 1.2 `[project.optional-dependencies]`

Optional dependency groups installed only when needed.

| Group | Purpose | Notable packages |
|------|------|--------------|
| `dev` | Development and testing | `pytest`, `pytest-asyncio`, `pytest-cov`, `fakeredis[lua]`, `httpx` |

- `fakeredis[lua]` is required because unit tests execute the same Redis Lua logic used in production
- `httpx` is used for ASGI and HTTP test flows

Install with `pip install -e ".[dev]"` or `poetry install --with dev`.

### 1.3 `[tool.pytest.ini_options]`

Repository-wide pytest configuration lives in `pyproject.toml` and applies to the service test suite under `tests/`.

| Option | Description |
|------|------|
| `asyncio_mode = "auto"` | Auto-detect async tests |
| `asyncio_default_fixture_loop_scope` | Fixture loop scope |
| `testpaths` | Collect the service test suite from `tests` during a root-level run |
| `addopts` | Default coverage flags and `--cov-fail-under=100` |

### 1.4 `[tool.coverage.run]`

Defines the coverage source root: `src`.

### 1.5 `[build-system]`

Declares the build backend used by `pip install -e .`.

### 1.6 `[tool.setuptools.packages.find]`

Configures setuptools package discovery for `realtime_transcribe_service` under `src/`.

### 1.7 `[tool.poetry]` and `[tool.poetry.group.dev.dependencies]`

Poetry support is optional. The Poetry development dependencies serve the same purpose as `[project.optional-dependencies].dev`, but through Poetry’s installation workflow.

---

## 2. Environment Usage

### 2.1 Local runtime

Start Redis and Kafka with `docker compose up -d`, then run:

```bash
pip install -e .
# or
poetry install

cp .env.example .env
python -m realtime_transcribe_service.main
```

- Dependencies: `[project].dependencies`
- Configuration: `.env` values such as `APP_ENV`, `REDIS_URL`, and `KAFKA_BOOTSTRAP_SERVERS`

This service runs as a WebSocket server. Upstream systems connect to `ws://.../ws/v1/realtime-transcriptions?conversationId=...`. Legacy client-side settings such as `STT_PROVIDER_URL` are not part of this runtime model.

### 2.2 Development and testing

```bash
pip install -e ".[dev]"
# or
poetry install --with dev

pytest
# or
poetry run pytest
```

- Dependencies: runtime plus `[project.optional-dependencies].dev`
- A root-level pytest run collects the service tests from `tests`

### 2.3 Production

```bash
pip install .
# or
poetry install --without dev

python -m realtime_transcribe_service.main
```

- Dependencies: runtime only
- Configuration: injected process environment variables described in [configuration.md](configuration.md) and [ci-cd.md](../cicd/ci-cd.md); deployed environments should set `APP_ENV=deployed` and should not rely on `.env`

---

## 3. Installation Matrix

| Environment | Command | Includes |
|------|------|------|
| Local | `pip install -e .` | Runtime dependencies |
| Dev | `pip install -e ".[dev]"` | Runtime + dev group |
| Production | `pip install .` | Runtime only |

If the repository also ships synchronized `requirements.txt` or `requirements-dev.txt`, those can be used with `pip install -r ...`, but `pyproject.toml` remains the primary source of truth.

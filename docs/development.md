# Local Development and Testing

---

## 1. Environment Requirements

| Item | Requirement |
|------|------|
| Python | 3.11+ |
| Production dependencies | Redis (ElastiCache) and Kafka (MSK) |
| Local development | Redis and Kafka via `docker compose up -d` |

---

## 2. Installation

### 2.1 Poetry (recommended)

```bash
poetry install
poetry install --with dev
poetry shell
```

The `dev` group includes `pytest`, `pytest-cov`, `fakeredis[lua]`, and `httpx`.

### 2.2 pip + venv

```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # Windows
source .venv/bin/activate      # Linux/Mac
pip install -e ".[dev]"
```

### 2.3 Verify the install

```bash
python -c "import realtime_transcribe_service; print('OK')"
```

---

## 3. Run Locally

### 3.1 Start dependencies

```bash
docker compose up -d
```

This starts Redis, Kafka, and Kafka UI.

### 3.2 Start the service

```bash
python -m realtime_transcribe_service.main
```

The service listens on `0.0.0.0:8080`, with the WebSocket endpoint at `/ws/v1/realtime-transcriptions?conversationId=xxx`.

### 3.3 Local addresses

| Service | Address |
|------|------|
| Redis | 127.0.0.1:6379 |
| Kafka | 127.0.0.1:9092 |
| Kafka UI | http://127.0.0.1:8090 |

See [kafka-ui-usage.md](kafka-ui-usage.md) for Kafka UI details.

---

## 4. Unit Tests

### 4.1 Run tests

```bash
poetry run pytest tests/ -v
poetry run pytest
```

### 4.2 Mocking strategy

Unit tests do not require a live Kafka or Redis instance:

| Component | Mock strategy |
|------|-----------|
| Redis state machine | [fakeredis[lua]](https://github.com/cunla/fakeredis-py) with Lua support |
| Kafka | `unittest.mock.AsyncMock` |
| WebSocket | `starlette.testclient.TestClient` for ASGI testing |

### 4.3 Coverage

- Coverage is collected by `pytest-cov` through the default `addopts` in `pyproject.toml`
- Coverage includes both `src/realtime_transcribe_service` and `config`
- The project currently enforces a **100%** coverage threshold

---

## 5. Debugging Tips

- Set `LOG_LEVEL=DEBUG` for verbose diagnostics
- Set `LOG_FORMAT=console` for more readable local logs
- Inspect `AI_STAGING_TRANSCRIPTION` through Kafka UI at `http://127.0.0.1:8090`
- Attach breakpoints to `python -m realtime_transcribe_service.main` or `python -m pytest`

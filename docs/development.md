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

The `dev` group includes `pytest`, `pytest-asyncio`, `pytest-cov`, `fakeredis[lua]`, and `httpx`.

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

### 3.1 Prepare local configuration

```bash
cp .env.example .env
```

This sets `APP_ENV=local`, which allows Redis and Kafka to default to the local Docker Compose addresses.

### 3.2 Start dependencies

```bash
docker compose up -d
```

This starts Redis, Kafka, and Kafka UI.

### 3.3 Start the service

```bash
python -m realtime_transcribe_service.main
```

The service listens on `0.0.0.0:8080`, with the WebSocket endpoint at `/ws/v1/realtime-transcriptions?conversationId=xxx`.

### 3.4 Local addresses

| Service | Address |
|------|------|
| Redis | 127.0.0.1:6379 |
| Kafka | 127.0.0.1:9092 |
| Kafka UI | http://127.0.0.1:8090 |

See [kafka-ui-usage.md](kafka-ui-usage.md) for Kafka UI details.

---

## 4. Tests

### 4.1 Run tests

```bash
poetry run pytest
```

The repository-level run collects both `tests/` and `tools/mock_client/tests/`.

Run only the mock-client tests from their own directory:

```bash
cd tools/mock_client
pip install -r requirements-dev.txt
pytest
```

### 4.2 Test strategy

Default tests do not require a live Kafka or Redis instance:

| Component | Mock strategy |
|------|-----------|
| Main service unit tests (`tests/`) | Redis state machine via [fakeredis[lua]](https://github.com/cunla/fakeredis-py), Kafka via `unittest.mock.AsyncMock`, and ASGI flows via `starlette.testclient.TestClient` |
| Mock-client tests (`tools/mock_client/tests/`) | Local module tests plus scenario checks using an in-process Uvicorn server fixture |

### 4.3 Coverage

- Coverage is collected by `pytest-cov` through the default `addopts` in `pyproject.toml`
- Coverage includes `src/realtime_transcribe_service`
- The project currently enforces a **100%** coverage threshold

---

## 5. Debugging Tips

- Set `LOG_LEVEL=DEBUG` for verbose diagnostics
- Set `LOG_FORMAT=console` for more readable local logs
- Inspect `AI_STAGING_TRANSCRIPTION` through Kafka UI at `http://127.0.0.1:8090`
- Attach breakpoints to `python -m realtime_transcribe_service.main` or `python -m pytest`

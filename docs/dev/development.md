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

### 3.2 Optional: enable local handshake JWT authentication

The current V1 implementation uses **HS256 signing-material JWTs**. It does **not** use an RSA private/public key pair, so there is no `private key` to generate for local testing.

Generate one HS256 signing-material value in PowerShell:

```powershell
$bytes = New-Object byte[] 48
[System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
[Convert]::ToBase64String($bytes)
```

On macOS, generate the same kind of signing-material value with:

```bash
openssl rand -base64 48
```

Copy the output into the repository-root `.env`:

```env
AUTH_ENABLED=true
AUTH_JWT_SIGNING_MATERIAL=replace-with-generated-signing-material
AUTH_JWT_ALGORITHM=HS256
```

If you still want to generate one Bearer JWT manually with the same signing material:

```python
from datetime import datetime, timedelta, timezone
import jwt

signing_material = "replace-with-generated-signing-material"
now = datetime.now(timezone.utc)
claims = {
    "sub": "fano-backend",
    "iat": int(now.timestamp()),
    "exp": int((now + timedelta(days=30)).timestamp()),
    "jti": "local-dev-token-001",
}

token = jwt.encode(claims, signing_material, algorithm="HS256")
print(token)
```

Use the token as:

```text
Authorization: Bearer <token>
```

### 3.3 Start dependencies

```bash
docker compose up -d
```

This starts Redis, Kafka, and Kafka UI.

### 3.4 Start the service

```bash
python -m realtime_transcribe_service.main
```

The service listens on `0.0.0.0:8080`, with the WebSocket endpoint at `/ws/v1/realtime-transcriptions?conversationId=xxx`.

### 3.5 Local addresses

| Service | Address |
|------|------|
| Redis | 127.0.0.1:6379 |
| Kafka | 127.0.0.1:9092 |
| Kafka UI | http://127.0.0.1:8090 |

See [kafka-ui-usage.md](../ops/kafka-ui-usage.md) for Kafka UI details.

---

## 4. Tests

### 4.1 Run tests

```bash
poetry run pytest
```

The repository-level run collects the service tests from `tests/`.

### 4.2 Test strategy

Default tests do not require a live Kafka or Redis instance:

| Component | Mock strategy |
|------|-----------|
| Main service unit tests (`tests/`) | Redis state machine via [fakeredis[lua]](https://github.com/cunla/fakeredis-py), Kafka via `unittest.mock.AsyncMock`, and ASGI flows via `starlette.testclient.TestClient` |

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

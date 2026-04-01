# Configuration

This document describes the Realtime Transcribe Service environment variables and maps directly to [src/realtime_transcribe_service/config/settings.py](../src/realtime_transcribe_service/config/settings.py).

---

## 1. Environment Setup

**Local development**

Copy `.env.example` to `.env`, keep `APP_ENV=local`, and edit values as needed.

```bash
cp .env.example .env
```

**Deployed environments (`APP_ENV=deployed`)**

1. **Bootstrap (plain task / container environment)** — must be set outside the secret so the process knows how to load the rest:
   - `APP_ENV=deployed`
   - `AWS_SECRETS_MANAGER_SECRET_ID` — name or ARN of the secret whose **SecretString** is JSON (see below)
   - `AWS_REGION` or `AWS_DEFAULT_REGION` — optional but recommended for the Secrets Manager client

2. **Application configuration** — loaded once at startup when `get_settings()` runs. Merge order (**later overrides earlier**):

   1. **`.env`** in the process **current working directory** (optional; keys normalized to uppercase like the secret)
   2. **Process environment** as it existed before the merge (e.g. ECS task definition)
   3. **Secrets Manager** JSON (highest precedence for application keys)

   Bootstrap variables (`APP_ENV`, `AWS_SECRETS_MANAGER_SECRET_ID`, `AWS_REGION`, `AWS_DEFAULT_REGION`) always keep their **original** process values so the secret body cannot replace them.

   After the merge, `Settings` is built with `_env_file=None` so pydantic does **not** read `.env` a second time.

Secret JSON rules:

- Root must be a **JSON object** (not an array).
- Keys should match the usual environment variable names (e.g. `REDIS_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_MODE`, `KAFKA_AWS_REGION`). See [§2 Configuration Reference](#2-configuration-reference) and `.env.example` for the full set.
- Values must be JSON **string**, **number**, **boolean**, or **null** (`null` becomes an empty string in the environment). Nested objects are not supported.

IAM: the task role (or other AWS credential chain) must allow `secretsmanager:GetSecretValue` on the configured secret.

**Local development (`APP_ENV=local`)**

Uses pydantic-settings as today: **process environment overrides `.env`** for the same variable. Secrets Manager is **not** used.

**Validation behavior**

- `APP_ENV` is required in every environment
- Blank string values fail startup
- Unknown keys in `.env` fail startup (local only; deployed merges `.env` once into `os.environ` before `Settings`, then `Settings` does not re-read the file)
- `REDIS_URL` and `KAFKA_BOOTSTRAP_SERVERS` are required when `APP_ENV=deployed`
- `APP_ENV=deployed` requires `KAFKA_MODE=aws_msk` (MSK IAM). `KAFKA_MODE=local` is for local docker-compose only
- `AUTH_JWT_SIGNING_MATERIAL` is required when `AUTH_ENABLED=true`

---

## 2. Configuration Reference

### Runtime Mode

| Variable | Default | Description |
|------|------|------|
| `APP_ENV` | None | Required. `local` enables localhost fallbacks for Redis/Kafka. `deployed` requires explicit dependency addresses |

### Redis: Sequence State Machine + Ownership Guard

| Variable | Default | Description |
|------|------|------|
| `REDIS_URL` | local only: `redis://127.0.0.1:6379/0` | Redis connection string. Required when `APP_ENV=deployed` |
| `REDIS_MAX_CONNECTIONS` | 100 | Connection-pool size. Must be `> 0` |
| `REDIS_ACTIVE_TTL_SEC` | 3600 | TTL for active conversations in seconds. Must be `> 0` |
| `REDIS_FINAL_TTL_SEC` | 60 | Residual TTL after `SESSION_COMPLETE`. Must be `> 0` and `<= REDIS_ACTIVE_TTL_SEC` |
| `REDIS_OWNERSHIP_GUARD_TTL_SEC` | 30 | TTL for the per-`conversationId` ownership key. Must be `> 0` |
| `REDIS_SEQUENCE_STATE_KEY_PREFIX` | `realtime-transcribe-service:expect-transcript-seq-num` | Key prefix for the Redis sequence state machine. Must not be empty |
| `REDIS_OWNERSHIP_GUARD_KEY_PREFIX` | `realtime-transcribe-service:conversation-owner` | Key prefix for the Redis ownership guard. Must not be empty |

### Kafka

| Variable | Default | Description |
|------|------|------|
| `KAFKA_BOOTSTRAP_SERVERS` | local only: `127.0.0.1:9092` | Kafka bootstrap servers. Required when `APP_ENV=deployed` |
| `KAFKA_MODE` | `local` | `local` = local docker-compose only: **PLAINTEXT fixed in code**; **does not** create topics (create `KAFKA_TOPIC` yourself, e.g. via Kafka UI / CLI). `aws_msk` = deployed / remote MSK: **SASL_SSL + OAUTHBEARER (MSK IAM) fixed in code**; topic must exist |
| `KAFKA_TOPIC` | `AI_STAGING_TRANSCRIPTION` | Topic name. Must not be empty |
| `KAFKA_COMPRESSION_TYPE` | `zstd` | Compression codec: `none`, `gzip`, `snappy`, `lz4`, or `zstd` |
| `KAFKA_SSL_CA_FILE` | None | Optional CA bundle path for `KAFKA_MODE=aws_msk` when the broker or NLB certificate chain is not trusted by the system default trust store. **Not allowed** when `KAFKA_MODE=local` |
| `KAFKA_AWS_REGION` | None | Required when `KAFKA_MODE=aws_msk`. Unused when `KAFKA_MODE=local` |
| `KAFKA_AWS_DEBUG_CREDS` | false | Only effective when `KAFKA_MODE=aws_msk`. When `true`, the IAM signer logs which AWS identity was used (troubleshooting only; keep `false` in production) |
| `KAFKA_SEND_TIMEOUT_SEC` | 2.0 | Kafka send timeout in seconds. Must be `> 0` |
| `KAFKA_LINGER_MS` | 1 | Producer linger in milliseconds. Must be `>= 0` |
| `KAFKA_BATCH_SIZE` | 32768 | Producer batch size in bytes. Must be `> 0` |

#### Kafka: which settings go together

Pick **one** row and treat it as a bundle. Do not rely on “leftover” variables from another bundle (for example `KAFKA_AWS_REGION` does nothing when `KAFKA_MODE=local`).

| Scenario | `KAFKA_MODE` | Turn **on** / set | Turn **off** / omit / ignore |
| -------- | ------------ | ------------------- | ---------------------------- |
| **Local docker-compose** (this repo) | `local` | `APP_ENV=local`, `KAFKA_BOOTSTRAP_SERVERS` (e.g. `127.0.0.1:9092`); **create `KAFKA_TOPIC` before starting the service**; **PLAINTEXT is fixed in code** | Omit `KAFKA_AWS_REGION`, `KAFKA_SSL_CA_FILE`. Keep `KAFKA_AWS_DEBUG_CREDS=false` (or omit) |
| **Deployed / remote Kafka (ECS, etc.)** | `aws_msk` | `APP_ENV=deployed`, `KAFKA_AWS_REGION`, `KAFKA_BOOTSTRAP_SERVERS` on **MSK IAM** endpoints (commonly `:9098`), AWS credentials via the [default credential chain](https://docs.aws.amazon.com/sdkref/latest/guide/overview.html); optional `KAFKA_SSL_CA_FILE` if TLS chain is non-public; **SASL_SSL + OAUTHBEARER is fixed in code** | Create `KAFKA_TOPIC` **before** deploy |
| **Debug “which AWS identity signs the token?”** | `aws_msk` only | Temporarily set `KAFKA_AWS_DEBUG_CREDS=true`, reproduce once, read logs | Set back to **`false`** after troubleshooting (avoid in steady-state production) |

**Do not mix:** `APP_ENV=deployed` **must** use `KAFKA_MODE=aws_msk`. There is **no** `KAFKA_SECURITY_PROTOCOL` (or similar) environment variable; wire security follows `KAFKA_MODE` only. **`KAFKA_MODE=local` never uses MSK IAM** and must not set `KAFKA_SSL_CA_FILE`; `KAFKA_AWS_REGION` / `KAFKA_AWS_DEBUG_CREDS` have **no effect** on the Kafka client in local mode.

#### Kafka authentication

- **Not supported:** SASL **SCRAM** or SASL **PLAIN** (username / password) to Kafka. There are no `KAFKA_SASL_*` settings.
- **`KAFKA_MODE=local`:** **Local docker-compose only**. **PLAINTEXT** only (not configurable). **No Kafka Admin topic creation** — create `KAFKA_TOPIC` out of band. Do not use for `APP_ENV=deployed`.
- **`KAFKA_MODE=aws_msk`:** **AWS MSK IAM** for all deployed / remote Kafka. The client always uses **`SASL_SSL`** + **`OAUTHBEARER`** (not configurable). Tokens from **`aws-msk-iam-sasl-signer-python`**. Set **`KAFKA_AWS_REGION`** and AWS credentials (e.g. ECS task role). Optional **`KAFKA_SSL_CA_FILE`** if the broker chain is not in the default trust store.

> `KAFKA_MODE=local` is only for local debugging with this repo’s docker-compose broker.
> Both modes expect the topic to exist before the service starts; the service does not auto-create it.
> In `KAFKA_MODE=aws_msk`, AWS credentials come from the standard AWS default credential chain, for example ECS task role, exported STS credentials, or `AWS_PROFILE`.

### WebSocket

| Variable | Default | Description |
|------|------|------|
| `WS_PING_INTERVAL` | 20.0 | Seconds. Must be `> 0` |
| `WS_PING_TIMEOUT` | 10.0 | Seconds. Must be `> 0` |
| `WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC` | 5.0 | Seconds between ownership-guard refreshes. Must be `> 0` and `< REDIS_OWNERSHIP_GUARD_TTL_SEC` |
| `WS_MAX_CONNECTIONS` | 0 | Maximum concurrent WebSocket connections. Must be `>= 0`. `0` means unlimited |

> The service enables the WebSocket runtime with `uvicorn.Config(ws="websockets", ...)`. `WS_PING_INTERVAL` and `WS_PING_TIMEOUT` are enforced by the Uvicorn `websockets` backend rather than by application-level JSON messages.

### Handshake Authentication

| Variable | Default | Description |
|------|------|------|
| `AUTH_ENABLED` | false | Enables handshake-time `Authorization: Bearer <JWT>` validation |
| `AUTH_JWT_SIGNING_MATERIAL` | None | Signing material for HS256 Bearer JWT validation. Required when `AUTH_ENABLED=true` |
| `AUTH_JWT_ALGORITHM` | `HS256` | JWT algorithm. V1 currently supports only `HS256` |

> V1 currently uses **HS256 signing material**. It does not use an RSA `private key` / `public key` pair, so there is no private-key generation step in the current implementation.

### HTTP / Uvicorn

| Variable | Default | Description |
|------|------|------|
| `HTTP_HOST` | `0.0.0.0` | Bind address. Must not be empty |
| `HTTP_PORT` | 8080 | Listen port. Must be in `1..65535` |
| `HTTP_BACKLOG` | 4096 | Uvicorn `listen(backlog)` value. Must be `> 0` |
| `HTTP_ENABLE_DOCS` | false | Only `true` exposes `/docs`, `/redoc`, and `/openapi.json`; any other value keeps them disabled |

> `HTTP_ENABLE_DOCS` controls only the FastAPI documentation surface. It does not protect or disable `/health`, `/ready`, or `/metrics`; those routes should be restricted by ingress, load balancer, security-group, or internal-network policy.

### Startup Checks

| Variable | Default | Description |
|------|------|------|
| `KAFKA_STARTUP_TIMEOUT_SEC` | 30.0 | Timeout for Kafka connectivity checks during startup. Must be `> 0` |

### Other

| Variable | Default | Description |
|------|------|------|
| `STOP_TIMEOUT` | 120.0 | Total graceful-shutdown budget in seconds. Must be `> 0` |
| `LOG_LEVEL` | `INFO` | Log level. One of `CRITICAL`, `ERROR`, `WARNING`, `INFO`, or `DEBUG` |
| `LOG_FORMAT` | `auto` | Log format: `json`, `console`, or `auto` |
| `LOG_WS_ERROR_FRAMES` | false | Whether to log the full outbound `ERROR` response JSON |
| `LOG_SLOW_MESSAGE_THRESHOLD_MS` | 0.0 | Slow-message warning threshold in milliseconds. Must be `>= 0`. `0` disables it |

---

## 3. Example Configurations

**Local development**

```env
APP_ENV=local
REDIS_URL=redis://127.0.0.1:6379/0
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_MODE=local
KAFKA_COMPRESSION_TYPE=zstd
LOG_FORMAT=console
HTTP_ENABLE_DOCS=false
```

**Deployed on AWS MSK with IAM**

```env
APP_ENV=deployed
REDIS_URL=redis://your-elasticache:6379/0
KAFKA_BOOTSTRAP_SERVERS=b-1.example.msk.amazonaws.com:9098,b-2.example.msk.amazonaws.com:9098
KAFKA_MODE=aws_msk
KAFKA_TOPIC=AI_STAGING_TRANSCRIPTION
KAFKA_AWS_REGION=ap-east-1
KAFKA_SSL_CA_FILE=/path/to/custom-ca-chain.pem
KAFKA_AWS_DEBUG_CREDS=false
LOG_FORMAT=json
AUTH_ENABLED=true
AUTH_JWT_SIGNING_MATERIAL=replace-with-signing-material
HTTP_ENABLE_DOCS=false
```

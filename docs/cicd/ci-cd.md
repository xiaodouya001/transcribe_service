# CI/CD and Deployment Guide

This document consolidates continuous integration, delivery flow, deployment setup, and operational runbook notes for Realtime Transcribe Service.

---

## 1. CI Flow

| Step | Description |
|------|------|
| **Lint / Type Check** | `ruff check .` and `pyright --project pyproject.toml` |
| **Tests** | `pytest` for the configured test paths |
| **Docker build** | Verifies that `docker build` succeeds |

Default tests rely on `fakeredis[lua]`, `unittest.mock`, and in-process fixtures, so CI does **not** require a live Redis or Kafka instance.

---

## 2. GitHub Actions

The repository includes [.github/workflows/ci.yml](../../.github/workflows/ci.yml). It runs on pushes and pull requests targeting `main` or `master`.

- **quality** job: Python 3.12 + Node 20, `poetry install --with dev`, `python -m pip install ruff`, then `poetry run ruff check .` and `npx pyright --project pyproject.toml`
- **test** job: Python 3.11/3.12 matrix, `poetry install --with dev`, then `poetry run pytest -q` (collects the service test suite from `tests` and `ci-cd/tests` via `pyproject.toml` `testpaths`)
- **docker** job: builds `docker/Dockerfile` through `docker/build-push-action` without pushing to a registry

### 2.1 Environment variables

Default tests do not require Redis or Kafka. If you introduce integration tests backed by real middleware, inject the required values through GitHub Secrets and workflow `env`, for example:

- `REDIS_URL`
- `KAFKA_BOOTSTRAP_SERVERS`

For the opt-in real Kafka connectivity check in [tests/test_kafka_real_connectivity.py](../../tests/test_kafka_real_connectivity.py), set:

- `RUN_REAL_KAFKA_TEST=true`
- `REAL_KAFKA_BOOTSTRAP_SERVERS`
- `REAL_KAFKA_MODE` — defaults to **`local`** (PLAINTEXT, local/docker Kafka only); set **`aws_msk`** for MSK IAM (requires `REAL_KAFKA_AWS_REGION`)
- optional `REAL_KAFKA_SSL_CA_FILE` for **`aws_msk`** when the broker TLS chain is non-public
- `REAL_KAFKA_AWS_REGION` when `REAL_KAFKA_MODE=aws_msk`
- optional `REAL_KAFKA_AWS_DEBUG_CREDS=true` if you need the MSK IAM signer to log which AWS identity was used
- `REAL_KAFKA_TOPIC` and `REAL_KAFKA_ASSERT_SEND=true` if you also want one real send to succeed

---

## 3. Build the Image

```bash
docker build -f docker/Dockerfile -t realtime-transcribe-service:latest .
```

The image is based on `python:3.12-slim`, uses a multi-stage build, runs as a non-root user, and starts with `python -m realtime_transcribe_service.main`.

---

## 4. CD Overview

The target runtime is **AWS ECS Fargate** backed by a VPC, ElastiCache Redis, and MSK.

The common sequence is:

1. Build the image and push it to ECR.
2. Update the ECS service or task definition.
3. Inject environment variables through the ECS task definition or Secrets Manager.

---

## 5. Target Deployment Environment

The primary deployment target is **AWS ECS Fargate** with:

- A **VPC** where the service can reach ElastiCache and MSK.
- **ElastiCache Redis** exposed through `REDIS_URL` and used for:
  - the sequence state machine / 2PC state
  - the conversation ownership guard that enforces single-sender semantics
- **MSK** exposed through `KAFKA_BOOTSTRAP_SERVERS`
- An upstream **load balancer**, typically **ALB**, terminating WSS for Fano Assist. Its idle timeout must exceed the WebSocket keepalive interval described in the design docs.

Protocol note: this service is a **WebSocket server**. It does not open outbound STT connections. Upstream clients connect to:

`wss://<your-host>/ws/v1/realtime-transcriptions?conversationId=<id>`

See the full protocol definition in [design/api-contract.md](../design/api-contract.md).

---

## 6. Runtime Configuration in Deployment

**Recommended (this service):** store **all** application settings as one JSON object in **AWS Secrets Manager** and load it automatically when `APP_ENV=deployed`. See [configuration.md §2 Deployed Environments](../config/configuration.md#2-deployed-environments-app_envdeployed).

**ECS task definition (bootstrap only):**

| Variable | Description |
|------|------|
| `APP_ENV` | Must be `deployed` |
| `AWS_SECRETS_MANAGER_SECRET_ID` | Secret name or ARN whose `SecretString` is the JSON config |
| `AWS_REGION` / `AWS_DEFAULT_REGION` | Optional; recommended for the Secrets Manager client |

The task role must allow `secretsmanager:GetSecretValue` on that secret. After load, the same keys apply as in [configuration.md](../config/configuration.md) (e.g. `REDIS_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_MODE=aws_msk`, optional `KAFKA_AWS_REGION` override, optional `AUTH_*`). The container does **not** read `.env`.

With `APP_ENV=deployed`, **`AWS_SECRETS_MANAGER_SECRET_ID` is required**; there is no per-variable-only mode in this build.

For MSK IAM, the task role (or instance profile) must still provide AWS credentials for token signing in addition to Secrets Manager read access.

### 6.1 ECS task bootstrap/runtime secret source of truth

This repository now provides a **repo-managed render/sync flow** under the top-level `ci-cd/` directory:

- `ci-cd/sync_secret.py` — render/sync CLI entrypoint
- `ci-cd/secrets/base.toml` — shared non-sensitive defaults
- `ci-cd/secrets/dev.toml`, `ci-cd/secrets/preprod.toml`, `ci-cd/secrets/prod.toml` — environment-specific AWS metadata and non-sensitive overrides
- `ci-cd/secrets/<env>.secrets.toml` — local-only sensitive values, ignored by git

Bootstrap variables stay outside the secret body and are the **only** values that should remain in the ECS task `environment` list:

- `APP_ENV=deployed`
- `AWS_REGION`
- `AWS_SECRETS_MANAGER_SECRET_ID`

All other supported application settings belong in the single JSON secret payload.

### 6.2 Create or update the secret

Create `ci-cd/secrets/<env>.secrets.toml` locally with the environment-specific sensitive values you do **not** want committed. Typical entries include:

- `REDIS_URL`
- optional `REDIS_USERNAME`
- optional `REDIS_PASSWORD`
- `KAFKA_BOOTSTRAP_SERVERS`
- optional `AUTH_JWT_SIGNING_MATERIAL`

Then render, validate, and preview without writing to AWS:

```bash
python ci-cd/sync_secret.py --env dev --dry-run
```

When the preview looks correct, create or update the Secrets Manager entry:

```bash
python ci-cd/sync_secret.py --env dev --sync
```

Which parameters are required:

| Mode | Required parameters | Optional parameters | Writes AWS | Local output files |
|------|------|------|------|------|
| `dry-run` | `--env <env> --dry-run` | `--output-dir <dir>`, `--config-dir <dir>` | No | Always writes `<env>.bootstrap.json` and `<env>.secret.json` |
| `sync` | `--env <env> --sync` | `--config-dir <dir>` | Yes. Creates or updates the target secret | No |

Defaults:

- `--config-dir` defaults to `ci-cd/secrets`
- `--output-dir` defaults to `ci-cd/build` for `dry-run`

CLI parameters:

| Parameter | Meaning |
|------|------|
| `--env` | Target environment. Supported values: `dev`, `preprod`, `prod` |
| `--dry-run` | Render and validate locally, then print the redacted payload without writing to AWS |
| `--sync` | Create or update the target Secrets Manager secret in AWS |
| `--output-dir` | Dry-run only. Output directory for local inspection files. The command writes `<env>.bootstrap.json` and `<env>.secret.json` here. Defaults to `ci-cd/build` |
| `--config-dir` | Override the default config directory `ci-cd/secrets`. Mainly useful for tests or ad-hoc local experiments |

The command:

- merges config in the order `base.toml < <env>.toml < <env>.secrets.toml`
- rejects bootstrap keys such as `APP_ENV` inside the secret body
- rejects unknown application keys
- validates the rendered payload against the current `Settings(_env_file=None)` deployed contract before any AWS write
- creates the secret if missing, otherwise writes a new secret version with `PutSecretValue`

### 6.3 ECS task definition usage

`dry-run` also writes local inspection files:

- `ci-cd/build/<env>.bootstrap.json`
- `ci-cd/build/<env>.secret.json`

Those files are local review artifacts only. The sync command does **not** read them back. The real input remains:

1. `ci-cd/secrets/base.toml`
2. `ci-cd/secrets/<env>.toml`
3. `ci-cd/secrets/<env>.secrets.toml`

Use `<env>.bootstrap.json` as the exact task bootstrap fragment to copy into the container definition, for example:

```json
{
  "environment": [
    {"name": "APP_ENV", "value": "deployed"},
    {"name": "AWS_REGION", "value": "ap-east-1"},
    {"name": "AWS_SECRETS_MANAGER_SECRET_ID", "value": "realtime-transcribe-service/dev/app-config"}
  ]
}
```

Do **not** maintain separate `.env.dev`, `.env.preprod`, or `.env.prod` runtime files for ECS. The deployment runtime source of truth is:

1. ECS bootstrap `environment` for the three loader variables
2. one environment-level Secrets Manager JSON secret for everything else

Startup is fail-fast:

- `APP_ENV` is required and must be `deployed` outside local development
- `REDIS_URL` and `KAFKA_BOOTSTRAP_SERVERS` are required when `APP_ENV=deployed`
- `KAFKA_TOPIC` must already exist on the cluster (the service does not create it)
- missing or blank required values cause configuration validation to fail before Redis or Kafka startup checks run

For local development only, `.env` remains supported and should set `APP_ENV=local`.

This deployment mode assumes that upstream systems connect directly over WebSocket.

---

## 7. Health Checks

The service exposes HTTP probes suitable for ALB and ECS:

| Path | Purpose |
|------|------|
| `GET /health` | Liveness: process is up |
| `GET /ready` | Readiness: Redis and Kafka are reachable |

Before listening for traffic, `main` runs `_check_redis` and `_check_kafka`. If either check fails, startup aborts.

---

## 8. Scaling Notes

- Only one active sender connection is allowed per `conversationId` at any moment. The server enforces this with the Redis ownership key. If a second connection attempts to send concurrently for the same conversation, it is rejected with `E1009`.
- Cross-instance consistency depends on the Redis Lua state machine, which stores the expected sequence and 2PC state; it is not implemented as a one-off deduplication key.
- Kafka uses `conversationId` as the partition key so each call remains ordered within a single partition.

---

## 9. Related Documents

- [configuration.md](../config/configuration.md) for the full environment-variable reference
- [design/app-design.md](../design/app-design.md) for architecture and graceful-shutdown flow

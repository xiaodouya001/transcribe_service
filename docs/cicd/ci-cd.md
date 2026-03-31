# CI/CD and Deployment Guide

This document consolidates continuous integration, delivery flow, deployment setup, and operational runbook notes for Realtime Transcribe Service.

---

## 1. CI Flow

| Step | Description |
|------|------|
| **Lint** | Optional formatting and static checks such as `ruff` or `black` |
| **Tests** | `pytest` for the configured test paths |
| **Docker build** | Verifies that `docker build` succeeds |

Default tests rely on `fakeredis[lua]`, `unittest.mock`, and in-process fixtures, so CI does **not** require a live Redis or Kafka instance.

---

## 2. GitHub Actions

The repository includes [.github/workflows/ci.yml](../../.github/workflows/ci.yml). It runs on pushes and pull requests targeting `main` or `master`.

- **test** job: Python 3.12, `poetry install --with dev`, then `poetry run pytest -v` (collects the service test suite from `tests` via `pyproject.toml` `testpaths`)
- **docker** job: builds `docker/Dockerfile` through `docker/build-push-action` without pushing to a registry

### 2.1 Environment variables

Default tests do not require Redis or Kafka. If you introduce integration tests backed by real middleware, inject the required values through GitHub Secrets and workflow `env`, for example:

- `REDIS_URL`
- `KAFKA_BOOTSTRAP_SERVERS`

For the opt-in real Kafka connectivity check in [tests/test_kafka_real_connectivity.py](../../tests/test_kafka_real_connectivity.py), set:

- `RUN_REAL_KAFKA_TEST=true`
- `REAL_KAFKA_BOOTSTRAP_SERVERS`
- `REAL_KAFKA_MODE` (`admin` or `aws_msk`) when non-default behavior is required
- `REAL_KAFKA_SECURITY_PROTOCOL` plus the matching `REAL_KAFKA_SASL_*` variables for SASL/SCRAM environments
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

The minimum production configuration is:

| Variable | Description |
|------|------|
| `APP_ENV` | Must be `deployed` in non-local environments |
| `REDIS_URL` | ElastiCache Redis connection string |
| `KAFKA_BOOTSTRAP_SERVERS` | MSK broker endpoints |
| `KAFKA_TOPIC` | Usually `AI_STAGING_TRANSCRIPTION`; must match the actual topic name |
| `KAFKA_TOPIC_NUM_PARTITIONS` | Only relevant when the service is allowed to create the topic |
| `KAFKA_REPLICATION_FACTOR` | Typically `>= 2` in production |
| `KAFKA_COMPRESSION_TYPE` | Defaults to `zstd`, but other supported codecs can be used |
| `HTTP_HOST` / `HTTP_PORT` | Bind address and port, typically `0.0.0.0:8080` inside the container |
| `KAFKA_STARTUP_TIMEOUT_SEC` | Kafka startup connectivity timeout |
| `LOG_FORMAT` | Usually `json` in production |
| `LOG_LEVEL` | Typical values include `INFO` or `WARNING` |

If handshake authentication is enabled for the deployment, also inject:

| Variable | Description |
|------|------|
| `AUTH_ENABLED` | Set to `true` to enforce handshake-time `Authorization: Bearer <JWT>` validation |
| `AUTH_JWT_SIGNING_MATERIAL` | HS256 signing material shared with trusted clients |
| `AUTH_JWT_ALGORITHM` | Defaults to `HS256`; V1 currently supports only `HS256` |

Deployed environments should inject these values as real process environment variables. Do not rely on `.env` files in ECS or other deployed runtimes.

For AWS ECS, use:

- task definition `environment` for non-sensitive values such as `APP_ENV`, `KAFKA_TOPIC`, `HTTP_HOST`, and `HTTP_PORT`
- task definition `secrets` or AWS Secrets Manager / SSM for sensitive values such as `REDIS_URL` and `AUTH_JWT_SIGNING_MATERIAL`

Startup is fail-fast:

- `APP_ENV` is required and must be `deployed` outside local development
- `REDIS_URL` and `KAFKA_BOOTSTRAP_SERVERS` are required when `APP_ENV=deployed`
- missing or blank required values cause configuration validation to fail before Redis or Kafka startup checks run

For local development only, `.env` remains supported and should set `APP_ENV=local`.

This deployment mode assumes that upstream systems connect directly over WebSocket. Legacy client-side STT provider settings such as `STT_PROVIDER_URL` are not part of this service.

---

## 7. Health Checks

The service exposes HTTP probes suitable for ALB and ECS:

| Path | Purpose |
|------|------|
| `GET /health` | Liveness: process is up |
| `GET /ready` | Readiness: Redis and Kafka are reachable |
| `GET /metrics` | Runtime metrics, such as active WebSocket counts |

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

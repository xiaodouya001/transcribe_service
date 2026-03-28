# CI/CD

This document summarizes the CI/CD flow and the GitHub Actions setup for the repository.

---

## 1. CI Flow

| Step | Description |
|------|------|
| **Lint** | Optional formatting and static checks such as `ruff` or `black` |
| **Tests** | `pytest` for the configured test paths in CI |
| **Docker build** | Verifies that `docker build` succeeds |

Default tests rely on `fakeredis[lua]`, `unittest.mock`, and in-process fixtures, so CI does **not** require a live Redis or Kafka instance.

---

## 2. GitHub Actions

The repository includes [.github/workflows/ci.yml](../.github/workflows/ci.yml). It runs on pushes and pull requests targeting `main` or `master`.

- **test** job: Python 3.12, `poetry install --with dev`, then `poetry run pytest -v` (collects `tests` and `tools/mock_client/tests` from `pyproject.toml` `testpaths`)
- **docker** job: builds `docker/Dockerfile` through `docker/build-push-action` without pushing to a registry

### 2.1 Environment variables

Default unit tests do not require Redis or Kafka. If you introduce integration tests backed by real middleware, inject the required values through GitHub Secrets and workflow `env`, for example:

- `REDIS_URL`
- `KAFKA_BOOTSTRAP_SERVERS`

---

## 3. CD Overview

The target runtime is **AWS ECS Fargate** backed by a VPC, ElastiCache Redis, and MSK.

The exact CD pipeline is usually defined by the team’s infrastructure layer, but the common sequence is:

1. Build the image and push it to ECR
2. Update the ECS service or task definition
3. Inject environment variables through the ECS task definition or Secrets Manager

See [deployment.md](deployment.md) for deployment details.

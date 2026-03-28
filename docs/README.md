# Documentation Index

This page collects the main documentation entry points for the Realtime Transcribe Service repository across `design/`, `docs/`, and the supporting tooling guides.

---

## Core Design

| Document | Description |
|------|------|
| [realtime-transcribe-service-app-design.md](../design/realtime-transcribe-service-app-design.md) | Application architecture overview |
| [realtime-transcribe-service-api-contract.md](../design/realtime-transcribe-service-api-contract.md) | API contract and protocol semantics |
| [realtime-transcribe-service-design-guardrails.md](../design/realtime-transcribe-service-design-guardrails.md) | Long-term guardrails, testing strategy, and change constraints |
| [realtime-transcribe-service-protocol-scenario-matrix.md](../design/realtime-transcribe-service-protocol-scenario-matrix.md) | Unified scenario matrix for normal flow, errors, close codes, and example payloads |

---

## Configuration, Capacity, and Deployment

| Document | Description |
|------|------|
| [configuration.md](configuration.md) | Environment variable reference |
| [deployment.md](deployment.md) | Build and deployment guide |
| [pyproject-config.md](pyproject-config.md) | `pyproject.toml` reference |

---

## Development, Testing, and Operations

| Document | Description |
|------|------|
| [development.md](development.md) | Local development, unit tests, and debugging |
| [cicd.md](cicd.md) | CI/CD flow and GitHub Actions |
| [troubleshooting.md](troubleshooting.md) | Operational troubleshooting guide |
| [faq.md](faq.md) | Frequently asked questions |
| [kafka-ui-usage.md](kafka-ui-usage.md) | Kafka UI usage guide |

---

## Tooling Guides

| Document | Description |
|------|------|
| [tools/mock_client/README.md](../tools/mock_client/README.md) | Mock Client guide for scenario tests, load tests, and Kafka replay |

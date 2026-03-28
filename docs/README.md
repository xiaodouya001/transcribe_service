# Documentation Index

This page collects the main documentation entry points for the Realtime Transcribe Service repository across `docs/design`, `docs/pt`, and the scenario-oriented documentation categories under `docs/`.

---

## Core Design


| Document                                                                                                                  | Description                                                                        |
| ------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| [app-design.md](design/app-design.md)                             | Application architecture overview                                                  |
| [api-contract.md](design/api-contract.md)                         | API contract and protocol semantics                                                |
| [design-guardrails.md](design/design-guardrails.md)               | Long-term guardrails, testing strategy, and change constraints                     |
| [protocol-scenario-matrix.md](design/protocol-scenario-matrix.md) | Unified scenario matrix for normal flow, errors, close codes, and example payloads |
| [env-profiles-300-400-500.md](pt/env-profiles-300-400-500.md)                                                             | Performance tuning env profiles for 300/400/500 concurrency targets                |


---

## Configuration and CI/CD


| Document                                                 | Description                                            |
| -------------------------------------------------------- | ------------------------------------------------------ |
| [config/configuration.md](config/configuration.md)       | Environment variable reference                         |
| [config/pyproject-config.md](config/pyproject-config.md) | `pyproject.toml` reference                             |
| [cicd/ci-cd.md](cicd/ci-cd.md)                           | CI pipeline, build, deployment, and operations runbook |


---

## Development, Testing and Operations


| Document                                         | Description                                  |
| ------------------------------------------------ | -------------------------------------------- |
| [dev/development.md](dev/development.md)         | Local development, unit tests, and debugging |
| [ops/troubleshooting.md](ops/troubleshooting.md) | Operational troubleshooting guide            |
| [ops/faq.md](ops/faq.md)                         | Frequently asked questions                   |


---

## Tooling Guides


| Document                                                     | Description                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| [mock_client/README.md](../mock_client/README.md) | Mock Client guide for scenario tests, load tests, and Kafka replay |
| [ops/kafka-ui-usage.md](ops/kafka-ui-usage.md)               | Kafka UI usage guide                                         |

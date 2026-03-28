# Realtime Transcribe Service

> A multi-cloud real-time data gateway. Fano Assist connects to this service over WebSocket, and Realtime Transcribe Service runs a two-phase commit flow (Redis Lua for sequence ordering plus Kafka persistence) to deliver transcripts reliably into Kafka.

---

## Quick Start

```bash
# 1. Install
poetry install --with dev
poetry shell

# 2. Start dependencies
docker compose up -d

# 3. Run
python -m realtime_transcribe_service.main
```

---

## Project Structure

```
realtime_transcribe_service/
├── config/                          # Pydantic Settings
├── src/realtime_transcribe_service/
│   ├── main.py                      # Main entrypoint (DI + lifecycle wiring)
│   ├── schemas/                     # Contract layer: Pydantic request/response models
│   ├── transport/                   # Ingress layer: WebSocket server
│   ├── redis/                       # Redis infrastructure: sequence state machine + sender ownership guard
│   ├── converter/                   # Kafka outbound conversion layer: add enrich and validate the outbound contract
│   ├── utils/                       # Shared utility layer: reusable helpers such as timestamp formatting
│   ├── producer/                    # Delivery layer: Kafka producer
│   ├── orchestrator/                # Orchestration layer: two-phase commit flow
│   └── shutdown/                    # Graceful shutdown
├── tests/
├── design/                          # Design docs, guardrails, scenario matrix, and API contract
├── docs/                            # Configuration, deployment, development, and troubleshooting docs
├── tools/mock_client/               # Scenario testing, load testing, and Kafka replay tools
└── docker-compose.yml               # Redis + Kafka + Kafka UI
```

## Documentation

For the full documentation index, see [docs/README.md](docs/README.md). That page collects the main entry points across `design/`, `docs/`, and the key tool documents.

Common entry points:


| Document                                                                                                                         | Description                                      |
| -------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| [design/realtime-transcribe-service-app-design.md](design/realtime-transcribe-service-app-design.md)                             | Application architecture overview                |
| [design/realtime-transcribe-service-api-contract.md](design/realtime-transcribe-service-api-contract.md)                         | API contract                                     |
| [design/realtime-transcribe-service-design-guardrails.md](design/realtime-transcribe-service-design-guardrails.md)               | Long-term guardrails and change constraints      |
| [design/realtime-transcribe-service-protocol-scenario-matrix.md](design/realtime-transcribe-service-protocol-scenario-matrix.md) | Protocol scenario matrix                         |
| [tools/mock_client/README.md](tools/mock_client/README.md)                                                                       | Mock Client, scenario tests, and load-test guide |
| [docs/README.md](docs/README.md)                                                                                                 | Documentation index                              |


---

## Deployment

```bash
docker build -f docker/Dockerfile -t realtime-transcribe-service:latest .
```

Target environment: AWS ECS Fargate. See [docs/deployment.md](docs/deployment.md) for details.
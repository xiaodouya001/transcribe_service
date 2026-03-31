# Realtime Transcribe Service

> A multi-cloud real-time data gateway. Fano Assist connects to this service over WebSocket, and Realtime Transcribe Service runs a two-phase commit flow (Redis Lua for sequence ordering plus Kafka persistence) to deliver transcripts reliably into Kafka, with optional handshake-time Bearer JWT authentication.

---

## Quick Start

```bash
# 1. Install
poetry install --with dev
poetry shell

# 2. Prepare local config
cp .env.example .env

# 3. Start dependencies
docker compose up -d

# 4. Run
python -m realtime_transcribe_service.main
```

---

## Project Structure

```
realtime_transcribe_service/
├── src/realtime_transcribe_service/
│   ├── config/                      # Runtime settings and logging configuration
│   ├── auth/                        # Optional handshake JWT authentication
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
├── docs/                            # Documentation hub (root keeps only README.md)
│   ├── cicd/
│   ├── config/
│   ├── design/
│   ├── dev/
│   ├── ops/
│   └── pt/
└── docker-compose.yml               # Redis + Kafka + Kafka UI
```

## Testing

Run the full repository test suite from the root:

```bash
poetry run pytest
```

## Documentation

For the full documentation index, see [docs/README.md](docs/README.md). That page collects the main entry points across `docs/design`, `docs/pt`, and the service-specific documents under `docs/`.

Common entry points:


| Document                                                                                                                         | Description                                      |
| -------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| [docs/design/app-design.md](docs/design/app-design.md)                             | Application architecture overview                |
| [docs/design/api-contract.md](docs/design/api-contract.md)                         | API contract                                     |
| [docs/design/design-guardrails.md](docs/design/design-guardrails.md)               | Long-term guardrails and change constraints      |
| [docs/design/protocol-scenario-matrix.md](docs/design/protocol-scenario-matrix.md) | Protocol scenario matrix                         |
| [docs/README.md](docs/README.md)                                                                                                 | Documentation index                              |


---

## Deployment

```bash
docker build -f docker/Dockerfile -t realtime-transcribe-service:latest .
```

Run the container with explicit runtime configuration:

```bash
docker run --rm -p 8080:8080 \
  -e APP_ENV=deployed \
  -e REDIS_URL=redis://<redis-host>:6379/0 \
  -e KAFKA_BOOTSTRAP_SERVERS=<broker-1>:9092,<broker-2>:9092 \
  realtime-transcribe-service:latest
```

If handshake authentication is enabled for the deployment, also pass `AUTH_ENABLED=true` and `AUTH_JWT_SIGNING_MATERIAL=<shared-signing-material>` (the default `AUTH_JWT_ALGORITHM` is `HS256`).

Target environment: AWS ECS Fargate. See [docs/cicd/ci-cd.md](docs/cicd/ci-cd.md) for details.

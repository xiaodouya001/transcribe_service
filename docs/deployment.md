# Deployment Guide

This document describes how to build, deploy, and operate Realtime Transcribe Service, including the WebSocket gateway, the Redis sequence state machine plus ownership guard, and Kafka delivery.

---

## 1. Build the Image

```bash
docker build -f docker/Dockerfile -t realtime-transcribe-service:latest .
```

The image is based on `python:3.12-slim`, uses a multi-stage build, runs as a non-root user, and starts with `python -m realtime_transcribe_service.main`.

---

## 2. Target Environment

The primary deployment target is **AWS ECS Fargate** with:

- A **VPC** where the service can reach ElastiCache and MSK
- **ElastiCache Redis** exposed through `REDIS_URL` and used for:
  - the sequence state machine / 2PC state
  - the conversation ownership guard that enforces single-sender semantics
- **MSK** exposed through `KAFKA_BOOTSTRAP_SERVERS`
- An upstream **load balancer**, typically **ALB**, terminating WSS for Fano Assist. Its idle timeout must exceed the WebSocket keepalive interval described in the design docs

Protocol note: this service is a **WebSocket server**. It does not open outbound STT connections. Upstream clients connect to:

`wss://<your-host>/ws/v1/realtime-transcriptions?conversationId=<id>`

See the full protocol definition in [design/realtime-transcribe-service-api-contract.md](../design/realtime-transcribe-service-api-contract.md).

---

## 3. Environment Variables

The minimum production configuration is:

| Variable | Description |
|------|------|
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

Inject them through the ECS task definition environment block or AWS Secrets Manager as appropriate.

This deployment mode assumes that upstream systems connect directly over WebSocket. Legacy client-side STT provider settings such as `STT_PROVIDER_URL` are not part of this service.

---

## 4. Health Checks

The service exposes HTTP probes suitable for ALB and ECS:

| Path | Purpose |
|------|------|
| `GET /health` | Liveness: process is up |
| `GET /ready` | Readiness: Redis and Kafka are reachable |
| `GET /metrics` | Runtime metrics, such as active WebSocket counts |

Before listening for traffic, `main` runs `_check_redis` and `_check_kafka`. If either check fails, startup aborts.

---

## 5. Scaling Notes

- Only one active sender connection is allowed per `conversationId` at any moment. The server enforces this with the Redis ownership key. If a second connection attempts to send concurrently for the same conversation, it is rejected with `E1009`
- Cross-instance consistency depends on the Redis Lua state machine, which stores the expected sequence and 2PC state; it is not implemented as a one-off deduplication key
- Kafka uses `conversationId` as the partition key so each call remains ordered within a single partition

---

## 6. Related Documents

- [configuration.md](configuration.md) for the full environment-variable reference
- [design/realtime-transcribe-service-app-design.md](../design/realtime-transcribe-service-app-design.md) for architecture and graceful-shutdown flow

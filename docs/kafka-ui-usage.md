# Kafka UI Guide

Kafka UI is an open-source web console for inspecting and managing Kafka clusters. Message values are displayed as UTF-8 text, which makes JSON payloads easy to inspect.

---

## 1. Start Kafka UI

### Option A: start it with `docker compose` (recommended)

```bash
docker compose up -d
```

This starts Redis, Kafka, and Kafka UI together. Kafka UI is available at **http://127.0.0.1:8090**. The host uses port `8090` so it does not conflict with this service on port `8080`.

### Option B: run Kafka UI separately

In the provided compose stack, Kafka is exposed to the host as `9092` and to other compose services as `broker:19092`. If Kafka UI runs outside the compose network, use `host.docker.internal:9092` on Windows or macOS, or the host IP plus `9092` on Linux.

```bash
docker run -d -p 8090:8080 \
  -e KAFKA_CLUSTERS_0_NAME=local \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=host.docker.internal:9092 \
  provectuslabs/kafka-ui:latest
```

Do not bind Kafka UI to host port `8080`, or it will collide with the service’s default HTTP/WebSocket port.

---

## 2. View Messages

1. Open **http://127.0.0.1:8090**
2. Select **Topics** in the left navigation
3. Choose the topic configured by `KAFKA_TOPIC`, which defaults to **`AI_STAGING_TRANSCRIPTION`**
4. Open the **Messages** tab to inspect payloads

Values are stored as UTF-8 JSON and typically mirror the contract structure from the API documentation, including `metaData` and `payload`.

---

## 3. Common Features

| Feature | Location |
|------|------|
| View topic list | `Topics` |
| Inspect messages | `Topic -> Messages` |
| Inspect consumer groups | `Consumers` |
| Search messages | Search box above the message list |
| Jump to an offset | Offset controls inside `Messages` |

---

## 4. Topic Conventions in This Project

- **Topic name:** `AI_STAGING_TRANSCRIPTION` by default, controlled by `KAFKA_TOPIC`
- **Partition key:** `conversationId`, so each conversation stays ordered in a single partition
- **Value:** JSON matching the contract or the deployment-specific outbound wrapper used in your environment

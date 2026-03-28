# Mock Client for Realtime Transcribe Service

This tool simulates Fano Assist client behavior, sends transcript messages to Realtime Transcribe Service over WebSocket, verifies the client-triggerable scenarios in the protocol matrix, and supports concurrent load tests plus Kafka message replay.

## Prerequisites

1. **Start the infrastructure** from the repository root:

```bash
docker compose up -d
```

2. **Start Realtime Transcribe Service**:

```bash
cp .env.example .env
python -m realtime_transcribe_service.main
# Default WebSocket endpoint: ws://127.0.0.1:8080/ws/v1/realtime-transcriptions
```

The service keeps its own runtime config in the repository-root `.env`. That file does not configure the mock client.

3. **Install mock-client dependencies** from its own manifest:

```bash
cd tools/mock_client
pip install -r requirements.txt
```

4. **Optional: configure mock-client defaults** with its own local env file:

```bash
cp .env.example .env
```

Supported mock-client variables:

- `MOCK_CLIENT_HOST`
- `MOCK_CLIENT_PORT`
- `MOCK_CLIENT_LOG_LEVEL`
- `MOCK_CLIENT_LOG_FORMAT`
- `MOCK_CLIENT_DEFAULT_WS_URL`
- `MOCK_CLIENT_DEFAULT_KAFKA_BOOTSTRAP`
- `MOCK_CLIENT_DEFAULT_KAFKA_TOPIC`

## Run Mock-Client Tests

Install the mock-client test dependencies and run its local suite:

```bash
cd tools/mock_client
pip install -r requirements-dev.txt
pytest
```

Run the full repository suite from the repository root when you want mock-client tests plus
main-service tests together:

```bash
poetry run pytest
```

## Start the Mock Client

```bash
cd tools/mock_client
python server.py
```

Then open **http://127.0.0.1:8088** in the browser.

## UI Overview

The page is split into:

- a top control area with scenario and load-test controls
- the scenario result panel on the left
- the Kafka message panel on the right

Real-time metrics such as sent count, ACK count, errors, active connections, TPS, and latency percentiles live inside the **Concurrent Load Test** card. Those counters are updated only by the load-test path, not by scenario tests. Each time you start a new load test, the dashboard resets and is repopulated from SSE `stats` events.

### 1. Control Panel

| Control | Description |
|------|------|
| WebSocket URL | The Realtime Transcribe Service endpoint, defaulting to `ws://127.0.0.1:8080/ws/v1/realtime-transcriptions` |
| Scenario test groups | **Group 1: Uses the scenario control value**: `N-01`, `N-02`, `N-03`, `E-09`. **Group 2: Fixed error scenarios**: `E-01`, `E-04`, `E-05`, `E-06`, `E-07`, `E-08`, `E-14`, `E-15` |
| Benchmark preset | Fills the `300 / 400 / 500` benchmark presets. The suggested values come from [env-profiles-300-400-500.md](../../docs/pt/env-profiles-300-400-500.md) |
| Scenario control value | The meaning changes by scenario; see the notes below |
| Run all | Executes `N-01 -> N-02 -> N-03 -> E-01 -> E-04 -> E-05 -> E-06 -> E-07 -> E-08 -> E-09 -> E-14 -> E-15` in order |
| Concurrent load test | Runs a normal success-path loop with multiple conversations. Each connection sends several `SESSION_ONGOING` events followed by one `SESSION_COMPLETE` |
| Concurrency / messages per connection / interval | `concurrency` is the number of simultaneously active conversations. `messages per connection` includes the final `SESSION_COMPLETE`. `interval` is the delay between messages within the same connection |
| Start / stop | Starts or stops the current load-test run |

Scenario control value meanings:

- `N-01`: number of `SESSION_ONGOING` messages to send
- `N-02`: number of sequence numbers to test, each sent twice
- `N-03`: total number of messages in one conversation, including the final `SESSION_COMPLETE`
- `E-09`: target sequence number of the out-of-order second message; the implementation uses `max(2, N)`
- Other error scenarios: the control value is ignored

### 2. Scenario Results

The **Clear** action in the panel header removes all scenario cards and restores the empty-state hint.

Each scenario run appears as a card containing:

- a single-line title with the scenario name, conversation ID, and PASS/FAIL badge
- detailed step logs showing what was sent, what came back, and whether the close code matched expectations

### 3. Kafka Messages

The Kafka panel header includes:

- visible message count
- clear list
- purge topic

| Control | Description |
|------|------|
| Bootstrap / Topic | Used when starting the consumer |
| `conversationId` filter | Filters the message list by conversation ID; choosing "All conversations" disables the filter |
| Start consumer / stop | Starts or stops the Kafka consumer |
| Purge topic | Uses Kafka `DeleteRecords` to remove committed messages from the current topic. Intended for development or test use |

Message-list behavior:

- Messages are shown in Kafka consumption order from top to bottom
- The list keeps up to 200 visible entries and discards the oldest first
- Clicking a message expands the full JSON body

## Scenario Reference

| Matrix ID | Internal name | Action | Expected result |
|------|------|------|----------|
| `N-01` | `N-01` | Send N consecutive `SESSION_ONGOING` messages | Each one receives `TRANSCRIPT_ACK`; the server keeps the connection open |
| `N-02` | `N-02` | For each `seq in [0, N)`, send one `SESSION_ONGOING` and then replay the same frame | Both the first attempt and the replay receive `TRANSCRIPT_ACK` |
| `N-03` | `N-03` | Send N total business messages including the final `SESSION_COMPLETE` EOL frame | The last frame receives `EOL_ACK`, followed by close code `1000` |
| `E-01` | `E-01` | Omit the `conversationId` query parameter during handshake | HTTP `400` + `E1003` |
| `E-04` | `E-04` | Send invalid JSON as the first frame | `ERROR(E1001)` + close `1007` |
| `E-05` | `E-05` | Send a message with `eventType=INVALID` after the connection opens | `ERROR(E1002)` + close `1008` |
| `E-06` | `E-06` | Send JSON missing required fields | `ERROR(E1003)` + close `1008` |
| `E-07` | `E-07` | Change `metaData.conversationId` to a non-string type | `ERROR(E1004)` + close `1008` |
| `E-08` | `E-08` | Change `createdAtTimeStamp` to a non-UTC or invalid timestamp | `ERROR(E1005)` + close `1008` |
| `E-09` | `E-09` | Send `seq 0`, then jump to `seq=max(2, N)` | `ERROR(E1006)` + close `1008` |
| `E-14` | `E-14` | Use different `conversationId` values in the query string and body | `ERROR(E1009)` + close `1008` |
| `E-15` | `E-15` | Build a message that violates a business rule, such as `isFinal=false` | `ERROR(E1009)` + close `1008` |

> Scenarios such as `E-02`, `E-03`, `E-10`, `E-11`, `E-12`, `E-13`, and `N-04` depend on service state, connection saturation, or failure injection and therefore cannot be triggered reliably by a normal client-only flow.

## HTTP API

The mock UI also exposes HTTP endpoints:

```bash
# Run a single scenario
curl -X POST "http://127.0.0.1:8088/api/scenario/run?name=N-01&n_messages=5"

# Run the whole scenario playlist
curl -X POST "http://127.0.0.1:8088/api/scenario/run-all"

# Start load testing: 10 concurrent conversations, 10 messages each, 20ms interval
curl -X POST "http://127.0.0.1:8088/api/load/start?concurrency=10&messages_per_conv=10&interval_ms=20"

# Stop load testing
curl -X POST "http://127.0.0.1:8088/api/load/stop"

# Read status, including recent error summaries
curl "http://127.0.0.1:8088/api/status"

# Start Kafka consumption
curl -X POST "http://127.0.0.1:8088/api/kafka/start"

# Stop Kafka consumption
curl -X POST "http://127.0.0.1:8088/api/kafka/stop"

# Purge committed Kafka records
curl -X POST "http://127.0.0.1:8088/api/kafka/purge?bootstrap=127.0.0.1:9092&topic=AI_STAGING_TRANSCRIPTION"
```

## File Layout

```text
tools/mock_client/
├── server.py          # FastAPI backend: API endpoints, SSE, and static files
├── ws_driver.py       # Message generator, scenario engine, and load-test driver
├── kafka_viewer.py    # Kafka consumer and queue broadcaster
├── tests/             # Mock-client-local tests (unit + integration)
├── static/
│   └── index.html     # Single-page browser UI
└── README.md          # This document
```

## FAQ

**Q: Why does the normal `N-01` scenario fail to connect?**  
Make sure Realtime Transcribe Service is running and listening at `ws://127.0.0.1:8080`.

**Q: Why do I not see Kafka messages?**  
Make sure you clicked **Start Consumer** and that the Kafka container is healthy. The consumer uses `auto_offset_reset=earliest` with no group ID, so each fresh start replays the topic from the beginning.

**Q: Why is TPS low during load tests?**  
`interval_ms` controls how long the client waits between messages on the same connection. Setting it to `0` sends as fast as possible. Increase concurrency or reduce interval if you need more throughput.

**Q: Why do I get `zstd` compression errors?**  
`requirements.txt` already includes `cramjam`. If you use a custom environment, make sure it is installed.

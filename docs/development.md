# 本地开发与测试

---

## 1. 环境要求

| 项目 | 要求 |
|------|------|
| Python | 3.11+ |
| 生产环境 | Redis (ElastiCache)、Kafka (MSK) |
| 本地开发 | Redis、Kafka（`docker compose up -d`） |

---

## 2. 安装

### 2.1 使用 Poetry（推荐）

```bash
poetry install
poetry install --with dev   # 含 pytest、pytest-cov、fakeredis[lua]、httpx
poetry shell
```

### 2.2 使用 pip + venv

```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # Windows
source .venv/bin/activate      # Linux/Mac
pip install -e ".[dev]"
```

### 2.3 验证

```bash
python -c "import realtime_transcribe_service; print('OK')"
```

---

## 3. 本地运行

### 3.1 启动依赖

```bash
docker compose up -d
```

会启动 Redis、Kafka、Kafka UI。

### 3.2 运行服务

```bash
python -m realtime_transcribe_service.main
```

服务启动后监听 `0.0.0.0:8080`，WebSocket 端点为 `/ws/v1/realtime-transcriptions?conversationId=xxx`。

### 3.3 服务地址（docker compose）

| 服务 | 地址 |
|------|------|
| Redis | 127.0.0.1:6379 |
| Kafka | 127.0.0.1:9092 |
| Kafka UI | http://127.0.0.1:8090 |

Kafka UI 使用说明见 [kafka-ui-usage.md](kafka-ui-usage.md)。

---

## 4. 单元测试

### 4.1 运行测试

```bash
poetry run pytest tests/ -v
# 或带覆盖率
poetry run pytest
```

### 4.2 Mock 策略

UT 不依赖真实 Kafka/Redis 环境：

| 组件 | Mock 方式 |
|------|-----------|
| **Redis（状态机）** | [fakeredis[lua]](https://github.com/cunla/fakeredis-py) 模拟，支持 Lua 脚本 |
| **Kafka** | `unittest.mock.AsyncMock` |
| **WebSocket** | `starlette.testclient.TestClient` ASGI 测试 |

### 4.3 覆盖率

- 覆盖率由 `pytest-cov` 按 `pyproject.toml` 中的 `addopts` 收集。
- 收集范围包括 `src/realtime_transcribe_service` 与 `config`。
- 覆盖率阈值为 **100%**。

---

## 5. 调试技巧

- **日志级别**：`LOG_LEVEL=DEBUG` 查看详细日志
- **日志格式**：`LOG_FORMAT=console` 本地开发时使用可读格式
- **Kafka 消息**：通过 Kafka UI (http://127.0.0.1:8090) 查看 Topic `AI_STAGING_TRANSCRIPTION`
- **断点调试**：以 `python -m realtime_transcribe_service.main` 或 `python -m pytest` 启动


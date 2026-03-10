# 本地开发与测试

本文档说明环境要求、安装、本地运行、UT 及调试技巧。

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
poetry install --with dev   # 含 pytest、pytest-cov、fakeredis
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
python -c "import transcription_ingest; print('OK')"
```

---

## 3. 本地运行

### 3.1 启动依赖

```bash
docker compose up -d
```

会启动 Redis、Kafka、Kafka UI。

### 3.2 本地 Demo（Mock + 前端注入）

```bash
python -m transcription_ingest.demo.run_local
```

浏览器打开 `http://127.0.0.1:8765/`，输入 JSON 点击「发送」，控制台打印完整链路日志。

### 3.3 生产模式本地运行

```bash
python -m transcription_ingest.main
```

需配置 `STT_PROVIDER_URL` 指向真实 STT 或 Mock 服务。

### 3.4 服务地址（docker compose）

| 服务 | 地址 |
|------|------|
| Redis | localhost:6379 |
| Kafka | localhost:9092 |
| Kafka UI | http://localhost:8090 |

Kafka UI 使用说明见 [kafka-ui-usage.md](kafka-ui-usage.md)。

---

## 4. 单元测试

### 4.1 运行测试

```bash
poetry run pytest tests/ -v
# 或带覆盖率（要求 ≥90%）
poetry run pytest
```

### 4.2 Mock 策略

UT 不依赖真实 Kafka/Redis 环境：

| 组件 | Mock 方式 |
|------|-----------|
| **Redis** | [fakeredis](https://github.com/cunla/fakeredis-py) 模拟内存 Redis |
| **Kafka** | `unittest.mock` 对 `AIOKafkaProducer` 做 patch |
| **SSE/WebSocket** | `unittest.mock` 模拟 HTTP/WebSocket 响应 |

### 4.3 覆盖率

- 目标：≥90%
- 排除：`main.py`、`demo/*`、`config/logging_config.py`
- 配置见 `pyproject.toml` 的 `[tool.coverage.run]`

---

## 5. 调试技巧

- **日志级别**：`LOG_LEVEL=DEBUG` 查看更详细日志
- **日志格式**：`LOG_FORMAT=console` 本地开发时使用可读格式
- **Kafka 消息**：通过 Kafka UI (http://localhost:8090) 查看 Topic `transcription_topic` 的消息
- **断点调试**：在 IDE 中设置断点，以 `python -m transcription_ingest.main` 或 `python -m pytest` 启动

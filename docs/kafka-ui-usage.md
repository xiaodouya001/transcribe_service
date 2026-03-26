# Kafka UI 使用说明

Kafka UI 是开源 Web 界面，用于查看和管理 Kafka 集群；消息 Value 会以 UTF-8 文本显示（JSON 可读）。

---

## 1. 启动

### 方式一：随 docker-compose 一起启动（推荐）

```bash
docker compose up -d
```

会启动 Redis、Kafka 和 Kafka UI。Kafka UI 地址：**http://127.0.0.1:8090**（主机 8090 映射到容器 8080，避免与本服务 HTTP/WebSocket 监听 **8080** 冲突）。

### 方式二：单独启动 Kafka UI（Kafka 已运行）

compose 内 Kafka 对宿主机暴露 **9092**，对同一 compose 网络内服务为 **broker:19092**。若 Kafka UI 在 compose 外单独起容器，bootstrap 通常填 `host.docker.internal:9092`（Windows/Mac）或宿主机 IP + `9092`。

```bash
docker run -d -p 8090:8080 \
  -e KAFKA_CLUSTERS_0_NAME=local \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=host.docker.internal:9092 \
  provectuslabs/kafka-ui:latest
```

**注意**：不要将 Kafka UI 映射到主机 **8080**，否则会与本服务默认端口冲突。

---

## 2. 查看消息

1. 浏览器打开 **http://127.0.0.1:8090**（随 compose 启动时）
2. 左侧选择 **Topics** → 选择与 [.env.example](../.env.example) / `KAFKA_TOPIC` 一致的 Topic（默认 **`AI_STAGING_TRANSCRIPTION`**）
3. 在 **Messages** 标签页查看消息
4. Value 为 UTF-8 JSON，对应 API 契约中的上行结构（`metaData` + `payload` 等）

---

## 3. 常用功能

| 功能 | 位置 |
|------|------|
| 查看 Topic 列表 | Topics |
| 查看消息内容 | Topic → Messages |
| 查看 Consumer Group | Consumers |
| 搜索消息 | Messages 上方搜索框 |
| 按 offset 跳转 | Messages 中指定 offset |

---

## 4. 与本项目的 Topic

- **Topic 名称**：默认 `AI_STAGING_TRANSCRIPTION`（由 `KAFKA_TOPIC` 配置）
- **Partition Key**：`conversationId`（UTF-8），同一通话路由到同一分区
- **Value**：JSON，与 [design/transcribe-service-API-contract.md](../design/transcribe-service-API-contract.md) 中 Client→Server 消息体一致（或按部署约定封装；生产以实际落盘格式为准）

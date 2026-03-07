# Kafka UI 使用说明

Kafka UI 是开源 Web 界面，用于查看和管理 Kafka 集群，消息会以 UTF-8 文本显示（JSON 可读）。

## 1. 启动

### 方式一：随 docker-compose 一起启动（推荐）

```bash
docker compose up -d
```

会启动 Redis、Kafka 和 Kafka UI。Kafka UI 地址：**http://localhost:8080**

### 方式二：单独启动 Kafka UI（Kafka 已运行）

若 Kafka 已在本地运行（如 `docker compose up -d kafka`）：

```bash
docker run -d -p 8080:8080 \
  -e KAFKA_CLUSTERS_0_NAME=local \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=localhost:9092 \
  provectuslabs/kafka-ui:latest
```

若 Kafka 在 Docker 网络内，需用 `host.docker.internal`（Windows/Mac）：

```bash
docker run -d -p 8080:8080 \
  -e KAFKA_CLUSTERS_0_NAME=local \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=host.docker.internal:9092 \
  provectuslabs/kafka-ui:latest
```

## 2. 查看消息

1. 浏览器打开 **http://localhost:8080**
2. 左侧选择 **Topics** → 点击 `asr_realtime_text`
3. 在 **Messages** 标签页查看消息
4. 点击某条消息可展开，Value 会以 UTF-8 文本显示，JSON 自动格式化

## 3. 常用功能

| 功能 | 位置 |
|------|------|
| 查看 Topic 列表 | Topics |
| 查看消息内容 | Topic → Messages |
| 查看 Consumer Group | Consumers |
| 搜索消息 | Messages 上方搜索框 |
| 按 offset 跳转 | Messages 中指定 offset |

## 4. 与本项目的 Topic

- **Topic 名称**：`asr_realtime_text`
- **Key**：`session_id`（UTF-8）
- **Value**：JSON，包含 `raw` 和 `cleaned` 字段

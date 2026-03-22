# Mock Client — Transcribe Service 虚拟测试客户端

模拟 FanoLabs 客户端行为，通过 WebSocket 向 Transcribe Service 发送转写消息，
验证契约矩阵中已落地的主要客户端可触发场景，并支持并发压测和 Kafka 消息回显。

## 前置条件

1. **基础设施已启动**（项目根目录）：

```bash
docker compose up -d   # Redis + Kafka + Kafka UI
```

2. **Transcribe Service 已启动**：

```bash
python -m transcribe_service.main
# 默认监听 ws://127.0.0.1:8080/ws/v1/realtime-transcriptions
```

3. **Python 依赖已安装**（均为项目已有依赖，无需额外安装）：
   - `websockets` — WebSocket 客户端
   - `aiokafka` — Kafka 消费者
   - `fastapi` / `uvicorn` — Mock Client 后端
   - `cramjam` — Kafka zstd 压缩支持（如 `.env` 配置了 `KAFKA_COMPRESSION_TYPE=zstd`）

## 启动

```bash
cd tools/mock_client
python server.py
```

浏览器打开 **http://127.0.0.1:8088**。

## UI 界面

界面布局：顶部为控制条（标题栏右侧 **收起 / 展开**，状态会记入浏览器本地）；中间一行左右为**场景结果**与 **Kafka 消息**（占满剩余高度）。**实时指标**（已发送、ACK、错误、活跃连接、TPS、延迟分位数）在控制条内 **并发压测** 卡片底部：仅随 **压测** 累计，**场景测试不改这些数**；每次点「启动压测」会先清零并由 SSE `stats` 推送；约每秒刷新一次。

### 1. 控制面板（顶部）

| 控件 | 说明 |
|------|------|
| WebSocket URL | Transcribe Service 的 WS 端点地址，默认 `ws://127.0.0.1:8080/ws/v1/realtime-transcriptions` |
| 场景测试（两块） | **① 使用场景控制值**：`N-01`、`N-02`、`N-03`、`E-09` + 「场景控制值」输入框（含义见下表）。**② 固定错误场景**：`E-01`、`E-04`、`E-05`、`E-06`、`E-07`、`E-08`、`E-14`、`E-15`（直接构造固定握手错误、协议错误或业务规则错误；不会读取场景控制值） |
| Benchmark 预设 | 可一键填充 `300 / 400 / 500` 并发基准档；参数参考 [env-profiles-300-400-500.md](../../PT/env-profiles-300-400-500.md) 的 Mock Client 建议，区间项默认取中值：`300 -> interval 70ms / ramp-up 25000ms`，`400 -> interval 78ms / ramp-up 30000ms`，`500 -> interval 85ms / ramp-up 37500ms`。手动修改任一字段后，下拉会自动回到「自定义」。 |
| 场景控制值 | 含义随场景变化，见下方分项说明。 |
| 全部运行 | 顺序 `N-01 → N-02 → N-03 → E-01 → E-04 → E-05 → E-06 → E-07 → E-08 → E-09 → E-14 → E-15`；仅 `N-01`、`N-02`、`N-03`、`E-09` 会读取场景控制值 |
| 并发压测 | **正常闭环负载**：每条连接内在「每连接消息总数」下按 `_session_message_split` 发若干 `SESSION_ONGOING` + 最后一条 `SESSION_COMPLETE`，均期望 `TRANSCRIPT_ACK`。<strong>不包含</strong> `N-02`、`E-01`、`E-04`、`E-05`、`E-06`、`E-07`、`E-08`、`E-09`、`E-14`、`E-15` 等边界场景；用于打吞吐、延迟、并发连接。 |
| 并发连接数 / 每连接消息数 / 消息间隔(ms) | **并发连接数** = 本轮**同时进行的对话路数**（≈ 同时在线 WebSocket 数），一轮共 **`concurrency` 路**，无额外倍率。「每连接消息数」= 每路业务消息**总数（含 COMPLETE）**；「消息间隔」= 同一路内相邻两条发送之间的间隔。再打一轮请再次点「启动压测」。 |
| 压测何时结束 | 本轮 **`concurrency` 路**会话全部发完并关闭后推送 `load_done`。「停止」后尚未开始建连的路不再执行。 |
| 启动压测 / 停止 | 启动或停止并发压测 |
| **实时指标** | 卡片内「启动/停止压测」下方；**仅压测**写入统计，场景测试不影响；SSE `stats`（约 1s）与 `load_done` 更新；启动压测时先清零，避免显示上一轮残留 |

**超高并发（如 1000 路）若 Mock UI 指标不刷新**：旧版曾向浏览器对**每路**发 `conversation_registered`，几分钟内几千条 SSE 会塞满队列并把订阅踢掉。现已默认**压测不发**该事件，并加大 SSE 缓冲 + 满则丢最旧帧。若仍长时间全 0，多半是 **Transcribe Service** 吃满 CPU/线程或拒连，请看其日志与机器资源。

场景控制值说明：

- `N-01`：表示发送多少条 `SESSION_ONGOING`
- `N-02`：表示要测试的 seq 个数，每个 seq 会发送两次
- `N-03`：表示每条通话的消息总数（含最后一条 COMPLETE）
- `E-09`：表示第二条乱序消息的目标 seq，实际取 `max(2,N)`
- 其余错误场景：不使用该参数

### 2. 场景结果（左侧主区域）

标题栏右侧 **清空** 可移除所有场景卡片并恢复占位提示。

每次运行的场景以卡片形式展示，包含：
- **一行标题**：场景名称 → **conversationId**（小标签，跟在名称后；点击可复制）→ PASS/FAIL 徽章
- 每一步的详细记录：发送了什么、收到了什么、close code 是否符合预期

### 3. Kafka 消息（右侧主区域）

面板**标题栏**（Kafka 消息 banner）右侧：**可见条数** + **清空列表** + **清空 Topic**，与「Kafka 消息」标题同一行。

| 控件 | 说明 |
|------|------|
| Bootstrap / Topic | 与本区域「开始消费」一起使用，先填好再启动消费者 |
| conversationId 下拉框 | 与「开始消费」同一行；自动登记 ID 后在此选择会话，用于筛选下方消息列表（选「全部会话」不做筛选） |
| 开始消费 / 停止 | 启动或停止 Kafka 消费者 |
| **清空 Topic** | 对当前填写的 Bootstrap / Topic 调用 Kafka **DeleteRecords**，删除各分区已提交的全部消息。会先停止本工具的消费者；若清空前正在消费**同一** Topic，默认在完成后**自动重新订阅**。生产环境请谨慎使用。 |

消息列表行为：

- 列表 **自上而下与 Kafka 消费顺序一致（FIFO）**：先消费的 seq 在上、后消费的在下；超过 200 条从顶部丢弃最旧。若正在向上滚动查看历史，不会强行滚到底部。
- 用下拉框按 `conversationId` 筛选；标题栏 **清空列表** 会同时清空消息列表与下拉里已缓存的会话 ID（新产生的场景/消息会重新加入）
- 点击单条消息可展开查看完整 JSON

## 场景说明

| 矩阵 ID | 内部名称 | 操作 | 预期结果 |
|------|------|------|----------|
| `N-01` | `N-01` | 连续发送 N 条 `SESSION_ONGOING`，验证每条都正常处理 | 每条都收到 `TRANSCRIPT_ACK`，服务端不主动断开 |
| `N-02` | `N-02` | 对每个 `seq ∈ [0,N)` 各发一次 `SESSION_ONGOING` 再重放同一帧 | 每次首次与重放均收到 `TRANSCRIPT_ACK` |
| `N-03` | `N-03` | 共 N 条业务消息（含最后一条 `SESSION_COMPLETE`），重点验证最终 COMPLETE 收尾 | 最后一条收到 `TRANSCRIPT_ACK`，随后 close `1000` |
| `E-01` | `E-01` | 握手时不携带 query `conversationId` | 收到 HTTP `400` + `E1003` |
| `E-04` | `E-04` | 连接后首包即非法 JSON（与 N 无关） | 收到 `ERROR(E1001)` + close `1007` |
| `E-05` | `E-05` | 建连成功后发送 `eventType=INVALID` 的消息 | 收到 `ERROR(E1002)` + close `1008` |
| `E-06` | `E-06` | 连接后首包即缺字段 JSON（与 N 无关） | 收到 `ERROR(E1003)` + close `1008` |
| `E-07` | `E-07` | 将 `metaData.conversationId` 改为非字符串类型 | 收到 `ERROR(E1004)` + close `1008` |
| `E-08` | `E-08` | 将 `createdAtTimeStamp` 改为非 UTC/非法时间格式 | 收到 `ERROR(E1005)` + close `1008` |
| `E-09` | `E-09` | seq 0 后跳到 `seq=max(2,N)`（默认 N=5 即跳 5） | 收到 `ERROR(E1006)` + close `1008` |
| `E-14` | `E-14` | query 中的 `conversationId` 与消息体里的 `metaData.conversationId` 不一致 | 收到 `ERROR(E1009)` + close `1008` |
| `E-15` | `E-15` | 构造违反业务规则的消息（当前使用 `isFinal=false`） | 收到 `ERROR(E1009)` + close `1008` |

> `E-02`、`E-03`、`E-10`、`E-11`、`E-12`、`E-13`、`N-04` 这类依赖服务状态、连接容量或故障注入的场景，无法稳定由普通客户端主动构造，需通过环境控制、测试桩或故障注入方式验证。

## API 接口

除了 UI 界面，也可以直接通过 HTTP API 调用：

```bash
# 运行单个场景
curl -X POST "http://127.0.0.1:8088/api/scenario/run?name=N-01&n_messages=5"

# 运行全部场景
curl -X POST "http://127.0.0.1:8088/api/scenario/run-all"

# 启动压测（10 路同时会话，每路 10 条消息，间隔 20ms）
curl -X POST "http://127.0.0.1:8088/api/load/start?concurrency=10&messages_per_conv=10&interval_ms=20"

# 停止压测
curl -X POST "http://127.0.0.1:8088/api/load/stop"

# 查看统计（含最近约 100 条压测错误摘要 recent_errors）
curl "http://127.0.0.1:8088/api/status"

# 启动 Kafka 消费（消息通过 SSE 推送到 UI）
curl -X POST "http://127.0.0.1:8088/api/kafka/start"

# 停止 Kafka 消费
curl -X POST "http://127.0.0.1:8088/api/kafka/stop"

# 清空 Topic 已提交消息（DeleteRecords；可与 kafka/start 相同 query 传 bootstrap、topic）
# restart_consumer=true（默认）：若清空前正在消费同一 bootstrap+topic，清空后自动再次 start
curl -X POST "http://127.0.0.1:8088/api/kafka/purge?bootstrap=127.0.0.1:9092&topic=cc.transcript.realtime.v1"
```

## 文件结构

```
tools/mock_client/
├── server.py          # FastAPI 后端：API 端点 + SSE 推送 + 静态文件托管
├── ws_driver.py       # 消息生成器 + 场景引擎 + 并发压测驱动
├── kafka_viewer.py    # Kafka 消费者 → asyncio.Queue 广播
├── static/
│   └── index.html     # 暗色主题单页 UI
└── README.md          # 本文件
```

## 常见问题

**Q: `N-01` 会话中正常处理连接失败？**
确认 Transcribe Service 已启动且监听在 `ws://127.0.0.1:8080`。

**Q: Kafka 消费看不到消息？**
确认已点击"开始消费"按钮，且 Kafka 容器健康（`docker compose ps`）。Consumer 使用 `auto_offset_reset=earliest`，无 group_id，每次启动会从 topic 最早的消息开始回放。

**Q: 压测 TPS 偏低？**
`interval_ms` 控制每条消息发完后等待多少毫秒再发下一条（默认 20ms）。设为 0 表示尽快发送。如需更高吞吐，增大并发数或减小间隔。

**Q: zstd 压缩报错？**
安装 `cramjam`：`pip install cramjam`。

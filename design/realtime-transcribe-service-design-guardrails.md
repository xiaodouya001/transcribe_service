# 设计护栏

本文档用于防止项目在持续迭代过程中逐步偏离初始设计。

本文档不替代详细设计文档；其作用是固化必须长期稳定的设计不变量和测试护栏，作为评审、迭代和实现变更时的固定依据。

---

## 1. 已确认的范围边界

以下两项已明确为**暂不纳入缺陷范围**。若需落地，应单独立项，不应在常规代码改动中以不完整形态引入：

- 鉴权/授权（`Authorization` / Token 校验）尚未实现；契约中的 `Authorization` / `E1010` 仅作预留
- Kafka `max_in_flight=1` / 熔断器（Circuit Breaker）尚未实现

---

## 2. 核心设计不变量

以下条目为本项目需长期保持稳定的设计语义。代码、测试与文档应围绕这些约束保持一致。

### 2.1 协议与校验

- 上行消息必须通过 schema 校验；时间字段必须是 ISO-8601 UTC。
- 握手 query 中的 `conversationId` 是连接级唯一标识。
- `metaData` 只承载会话级字段；`agentId`、`customerId` 属于 `payload`，并按 `speaker` 条件必填。
- 若消息体 `metaData.conversationId` 存在且为字符串，则必须与握手 query 一致；不一致时在 transport 层直接拒绝，返回 `E1009 + 1008`。
- 同一 `conversationId` 在任一时刻只允许一个连接发送消息；新连接若与现有发送连接冲突，必须在握手阶段返回 HTTP `403` + `E1009`，不得进入 orchestrator。
- 缺字段、类型错误、枚举错误、业务规则错误必须稳定映射到既定错误码，不允许“因为实现细节变化而改码”。

### 2.2 状态机与序列语义

- 同一 `conversationId` 下，`sequenceNumber` 必须严格按状态机推进。
- 重复包（IDEMPOTENT）必须直接返回对应成功 ACK，不得写 Kafka，不得推进 Redis。
- `SESSION_ONGOING` / 普通 transcript 成功时返回 `TRANSCRIPT_ACK`；`SESSION_COMPLETE` / EOL 控制帧成功时返回 `EOL_ACK`。
- 跳号/乱序包（OUT_OF_ORDER）必须返回 `E1006 + 1008`。
- `prepare` 不推进状态，只有 `commit` 才推进 expected sequence。

### 2.3 两阶段提交与无损重试

- 正常路径必须遵循：`prepare -> Kafka send -> commit -> ACK`。
- Kafka 出站契约为 `metaData + payload + enrich`，且 `enrich.eventProduceTimestamp` 必须在每次 `producer.send` 前重新生成。
- producer 必须保持 transport-only：只负责投递，不得修改 payload；Kafka enrich 只能在 converter 层完成。
- Kafka 超时/失败时，必须“不 commit、返回错误、允许上游重试同一 seq”。
- 这条“下游失败后无损重试”是架构的核心承诺之一。

### 2.4 会话结束与 TTL

- 活跃会话 key 使用 active TTL；默认值为 1 小时。
- 只有收到 `SESSION_COMPLETE` 后，才将 key TTL 缩短为 final TTL。
- 客户端异常断开不会主动触发 cleanup；因此 key 会继续按 active TTL 保留。
- `SESSION_COMPLETE` 是系统级 EOL 控制事件，不再表示“最后一句 transcript”。
- `SESSION_COMPLETE` 的处理语义是：Kafka 成功、Redis commit 成功、再 cleanup，并返回 `EOL_ACK`。
- 若 `cleanup()` 失败，但 Kafka 与 commit 已完成，则按**告警降级**处理：仍返回 `EOL_ACK`，并保持正常 `1000` 断连；cleanup 视为后置优化而非主交易失败。

### 2.5 优雅停机

- 优雅停机的顺序是：先停止接收新连接，再向存量连接发 `1001`，再 flush Kafka，最后释放资源并退出。
- 顺序本身是设计约束，不应被“无害重构”打乱。

---

## 3. 已落地的高价值护栏

在 100% 覆盖率之外，项目已经建立以下“设计不变量级”的高价值护栏。

### 3.1 Kafka 失败后的无损重试闭环

已落地测试明确锁死以下语义：

- 第一次请求：`prepare OK -> Kafka fail -> 返回 E1008/E1011 -> 不 commit`
- 第二次同一 `conversationId + seq` 重发：仍可通过 `prepare`
- 第二次成功后：返回对应 ACK，执行 Kafka send 与 commit
- 成功后再次重放旧 seq：命中对应幂等 ACK，不再重复写 Kafka

这条测试直接保护“两阶段提交 + 无损重试”这条核心设计主线。

### 3.2 异常断线后的 TTL 与续传

已落地测试明确锁死以下语义：

- 客户端异常断开后 key 仍保持 active TTL
- 在 active TTL 窗口内重连，下一条 seq 可以续传
- 在窗口内重发旧 seq 时，仍会命中幂等 ACK

这条测试直接保护“断线保状态”的设计取舍。

### 3.3 优雅停机顺序

已落地测试明确锁死以下顺序：

- `close_all -> flush -> close producer/redis_sequence_state_machine/redis_ownership_guard`

这条测试保护的是停机顺序本身，而不是单纯“方法被调用过”。

### 3.4 `SESSION_COMPLETE` 后置异常语义

已落地测试明确锁死以下语义：

- Kafka 成功、Redis commit 成功后，即使 `cleanup()` 失败，也仍返回最终 ACK
- `SESSION_COMPLETE` 的最终成功响应类型为 `EOL_ACK`
- 仍按正常结束走 `1000`
- `cleanup` 失败仅作为告警，不翻转已成功的主交易结果

### 3.5 契约级场景矩阵

契约级场景矩阵已经集中沉淀到：

- [realtime-transcribe-service-protocol-scenario-matrix.md](realtime-transcribe-service-protocol-scenario-matrix.md)
- 对应测试：[test_contract_matrix.py](../tests/test_contract_matrix.py)

矩阵覆盖的关键场景如下：

- invalid json -> `E1001 + 1007`
- enum invalid -> `E1002 + 1008`
- missing field -> `E1003 + 1008`
- wrong type -> `E1004 + 1008`
- invalid UTC timestamp -> `E1005 + 1008`
- duplicate seq -> 对应 ACK + 不断连
- out-of-order -> `E1006 + 1008`
- internal exception -> `E1007 + 1011`
- downstream fail/timeout -> `E1008/E1011 + 1013`
- `conversationId` mismatch -> `E1009 + 1008`
- business-rule violation -> `E1009 + 1008`
- concurrent sender conflict at handshake -> HTTP `403` + `E1009`

这类测试不追求模块实现细节，而是直接保护协议契约。

---

## 4. 变更最小流程约束

修改业务逻辑时，应执行以下最小流程。

### 4.1 修改前必须回答 3 个问题

每次改动前，先明确：

1. 这项改动触碰了哪些设计不变量？
2. 这项改动新增或修改了哪些测试来证明没有跑偏？
3. 如果行为变化了，哪些文档必须同步更新？

若无法回答这 3 个问题，就不应直接改业务代码。

### 4.2 优先同步“测试 + 文档 + 实现”

不要只关注“把代码改通”。

执行顺序：

1. 先写/改测试，表达目标语义
2. 再改实现
3. 最后同步文档

这样可以减少“实现改了，但团队对设计理解没跟上”的情况。

### 4.3 区分“覆盖率”与“设计护栏”

覆盖率高，只能说明代码行/分支被执行过；不能自动说明设计没偏。

以后评估改动时，优先看两件事：

- 是否触碰核心设计不变量
- 是否补上了对应的场景级测试

### 4.4 对关键顺序和错误码写死断言

以下类型最容易被误改，必须写成明确断言：

- 调用顺序
- 错误码与关闭码映射
- 重试/幂等语义
- TTL 变化时机
- transport 层与 orchestrator 层的职责边界

### 4.5 每次变更后做一次“契约复核”

每次较大改动后，至少复核以下文档是否仍然成立：

- `design/realtime-transcribe-service-api-contract.md`
- `design/realtime-transcribe-service-app-design_zh.md`
- `docs/faq.md`
- 与行为变化相关的 `docs/*.md`

若文档与代码不一致，优先修文档或明确宣布“设计已变更”。

### 4.6 契约优先规则

当 UI、测试、实现与 API 契约冲突时，以 [realtime-transcribe-service-api-contract.md](realtime-transcribe-service-api-contract.md) 为准。

其他文档和 mock tool 只能跟随契约，不得反过来改写契约语义。

---

## 5. 维护原则

本文件已作为长期维护基线，不再处于“建议清单”状态。

项目后续演进时，应优先遵循以下原则：

1. 新设计约束出现时，先把它补进本文件的“核心设计不变量”。
2. 新行为变更落地时，优先补“场景级护栏测试”，而不是只追覆盖率。
3. 任何会改变错误码、关闭码、TTL 时机、停机顺序、幂等/重试语义的改动，都必须同步更新本文件和相关契约文档。
4. 若某项设计明确不再适用，应直接修改本文件，而不是让聊天记录或临时结论继续充当事实来源。

---

## 6. 使用方式

本文档适用于以下固定检查场景：

- 开始改业务逻辑前
- 代码评审（Review）时
- 修复线上问题后补测试时
- 更新设计文档时

设计发生变更时，应优先更新本文档中的“不变量”和“已落地护栏”，再继续迭代实现。

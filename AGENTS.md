# Transcribe Service Agent Rules

## 契约优先

- 当 UI、测试、实现与 API 契约冲突时，以 `design/transcribe-service-API-contract.md` 为准。
- 其他文档、mock tool、场景矩阵和测试只能跟随契约，不得反过来改写契约语义。
- 如果需要变更契约，必须先明确这是契约变更，再同步更新相关文档、测试和实现。

## 修改约束

- 修改代码前，先确认触碰了哪些设计不变量。
- 任何会改变错误码、关闭码、幂等/重试语义、TTL 时机、停机顺序、握手校验的改动，都必须同步补测试。
- 不要只追求覆盖率；优先守住场景级护栏和契约级矩阵。

## 文档约束

- `docs/design-guardrails.md` 是长期维护基线。
- `docs/protocol-scenario-matrix.md` 是协议场景的唯一说明入口。
- 当实现与文档不一致时，优先修正文档或明确宣布契约已变更。

## ADDED Requirements

### Requirement: Codec 编解码为异步
`Encoder`/`Decoder`（及 `Codec`）trait 的 `encode`/`decode` 方法 SHALL 为 `async`，使 codec 实现可在方法体内执行异步 IO。现有 json/protobuf/debezium codec 的编解码行为与语义 SHALL 保持不变。

#### Scenario: 现有 codec 行为不变
- **WHEN** json/protobuf/debezium codec 经 async `encode`/`decode` 调用
- **THEN** 输入输出与重构前（sync）完全一致（现有 round-trip 与单元测试全绿）

#### Scenario: codec 实现可执行异步 IO
- **WHEN** 一个 codec 的 `decode` 需要异步操作（如 HTTP 查询外部服务）
- **THEN** 可在 async `decode` 内 `.await` 该操作，不阻塞 tokio runtime

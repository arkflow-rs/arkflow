## ADDED Requirements

### Requirement: JSON 解码 schema 推断 SHALL 覆盖全量记录
`json` codec 与 `json_to_arrow` 处理器对多消息合并解码时，schema 推断 SHALL 扫描待解码批次的全量记录：数值类型跨记录并集取宽（Int64 与 Float64 混见 SHALL 推断为 Float64，解码值不得截断）、字段集取并集（仅出现在后续记录的字段 SHALL 成为列）、出现 null 的列 SHALL 推断为 nullable。推断失败维持既有错误路径（`fail` 整批报错 / `skip` 逐条隔离）。

#### Scenario: 整数与浮点混见推断为 Float64 且不截断

- **WHEN** 合并解码 `{"v": 1}`、`{"v": 1.5}`、`{"v": 2.9}`
- **THEN** 输出列为 Float64，值为 `[1.0, 1.5, 2.9]`，无截断、无告警缺失

#### Scenario: 后续记录的新字段保留

- **WHEN** 合并解码 `{"v": 1}` 与 `{"v": 2, "tag": "x"}`
- **THEN** 输出含 `v` 与 `tag` 两列，首行 `tag` 为 null（列 nullable）

#### Scenario: 单消息路径行为不变

- **WHEN** 逐条解码（`on_error: skip` 或 Kafka 逐消息解码）
- **THEN** 每条按自身记录推断，语义与既有版本一致

#### Scenario: 全为整数时类型不变

- **WHEN** 合并解码的记录某列全为整数
- **THEN** 该列仍推断为 Int64（全量推断不无谓放宽类型）

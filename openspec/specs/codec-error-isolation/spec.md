# codec-error-isolation Specification

## Purpose
TBD - created by archiving change fix-plugin-p2-batch. Update Purpose after archive.
## Requirements
### Requirement: 坏消息隔离可配置
`json` 与 `protobuf_to_arrow` codec SHALL 支持配置 `on_error`，取值 `fail`（缺省）或 `skip`：`fail` 保持既有行为——解码失败时整批返回错误；`skip` 逐条解码，单条坏消息（JSON 解析/schema 推断失败、protobuf 解析失败）SHALL 被跳过并以 warn 日志记录（含消息序号与错误原因），不影响同批其他消息。`skip` 模式下若整批全部解码失败，codec SHALL 返回错误而非产出空批次。跳过路径产出的多消息批次 SHALL 经 schema 归一（并集 + 缺列 null 填充）后合并。

#### Scenario: 缺省行为保持整批失败
- **WHEN** codec 未配置 `on_error`（或配置为 `fail`）且 batch 中一条消息解码失败
- **THEN** codec 返回错误，整批不产出（与既有版本一致）

#### Scenario: skip 模式隔离坏消息
- **WHEN** codec 配置 `on_error: skip` 且 batch 含 3 条好消息与 1 条坏消息
- **THEN** codec 产出含 3 行的 `MessageBatch`，坏消息被跳过且 warn 日志含其序号与错误原因

#### Scenario: 全部坏消息时返回错误
- **WHEN** codec 配置 `on_error: skip` 且 batch 全部消息解码失败
- **THEN** codec 返回错误，不产出空批次

#### Scenario: 跳过路径合并异构好消息
- **WHEN** `skip` 模式下两条好消息的推断 schema 不完全一致（如一条缺某字段）
- **THEN** 合并为并集 schema 的批次，缺字段行以 null 填充，合并不报错

### Requirement: JSON 解码 schema 推断 SHALL 覆盖全量记录
`json` codec 与 `json_to_arrow` 处理器对多消息合并解码时，schema 推断 SHALL 扫描待解码批次的全量记录：数值类型跨记录并集取宽（Int64 与 Float64 混见 SHALL 推断为 Float64，解码值不得截断）、字段集取并集（仅出现在后续记录的字段 SHALL 成为列）、出现 null 的列 SHALL 推断为 nullable。推断失败维持既有错误路径（`fail` 整批报错 / `skip` 逐条隔离）。

#### Scenario: 整数与浮点混见推断为 Float64 且不截断

- **WHEN** 合并解码 `{"v": 1}`、`{"v": 1.5}`、`{"v": 2.9}`
- **THEN** 输出列为 Float64，值为 `[1.0, 1.5, 2.9]`，无截断、无告警缺失

#### Scenario: 后续记录的新字段保留

- **WHEN** 合并解码 `{"v": 1}` 与 `{"v": 2, "tag": "x"}`
- **THEN** 输出含 `v` 与 `tag` 两列，首行 `tag` 为 null（列 nullable）

#### Scenario: 单消息路径行为不变

- **WHEN** Kafka 逐消息解码，或 `on_error: skip` 模式逐条筛选坏消息
- **THEN** Kafka 逐消息路径每条按自身记录推断；`skip` 模式的逐条解析仅用于隔离坏消息（warn 并丢弃），幸存消息合入一次全量推断产出批次（并集/取宽语义同样适用）。两条路径语义与既有版本一致

#### Scenario: 全为整数时类型不变

- **WHEN** 合并解码的记录某列全为整数
- **THEN** 该列仍推断为 Int64（全量推断不无谓放宽类型）


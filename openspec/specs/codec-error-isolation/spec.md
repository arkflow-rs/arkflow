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


## ADDED Requirements

### Requirement: Protobuf 批级列式解码

一次 decode 调用内，同一 Protobuf schema id 的全部消息 SHALL 累积为单一多行 Arrow 批次（列式构造）：descriptor→Arrow 列映射（列集、类型、字段号）在该批内 SHALL 只计算一次，且 SHALL 在首条消息解码成功后的字段遍历中导出（kind 拒绝不先于消息解析错误）；codec SHALL NOT 为每条消息独立构造单行 RecordBatch 或独立的每消息列分配。列集 SHALL 为 descriptor 全字段集且全部 nullable。输出 SHALL 与既有"逐消息解码后归一合并"逐列一致：行序与组内消息序一致、proto3 隐式存在性的默认值语义不变（未设置标量字段取默认值而非 null）、错误类型、错误文案与首错归因不变。

#### Scenario: 同 id 多消息累积为单一批次

- **WHEN** 一次 decode 收到 N 条同一 Protobuf schema id 的消息
- **THEN** 产出一个多行批次（行数 = N），行序与消息序一致，与逐消息解码加归一合并的输出逐列相等

#### Scenario: 未设置字段保持 proto3 默认值语义

- **WHEN** 某消息未设置 string 标量字段
- **THEN** 该列为空字符串（默认值）而非 null，与既有逐消息路径一致

#### Scenario: kind 拒绝文案与归因不变

- **WHEN** descriptor 含不受支持的字段类型（嵌套 message/repeated/map/oneof）
- **THEN** 返回的拒绝文案与既有逐消息路径逐字一致，且归因于首条消息

#### Scenario: 混合组次序稳定

- **WHEN** 一个 batch 混有不同 schema id 的 Protobuf 消息
- **THEN** 各组按首次出现顺序参与归一合并，组内行序与消息序一致（与既有分组行为一致）

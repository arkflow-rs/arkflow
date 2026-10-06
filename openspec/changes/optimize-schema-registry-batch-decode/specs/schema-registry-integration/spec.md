## ADDED Requirements

### Requirement: 批级列式解码

一次 decode 调用内，单一 schema id 的消息 SHALL 累积为单一多行 Arrow 批次（列式构造）：schema→Arrow 列映射（列集、类型、nullability、decimal precision/scale）在该批内 SHALL 只计算一次，且 SHALL 与值校验按字段交错完成于首条消息的处理中（schema 形状校验既不先于 payload 解码错误，也不先于更早字段的值校验错误）；单一 schema id 的批次 SHALL NOT 为每条消息独立构造单行 RecordBatch 或独立的每消息列分配。单一 id 批次的输出 SHALL 与既有"逐消息解码后归一合并"输出逐列一致，含列 nullability 对消息数的既有依赖（N≥2 时全列 nullable，N=1 时按扁平映射）。混合 schema id 的批 SHALL 保持既有逐消息行为不变（允许单行构造）：输出行序与消息序一致，并集归一合并语义、错误行为、registry 请求副作用顺序均不变化。输入为空批时 SHALL 保持既有空批次返回。

#### Scenario: 同 id 多消息累积为单一批次

- **WHEN** 一次 decode 收到 N（N≥2）条同一 Avro schema id 的消息
- **THEN** 产出一个多行批次（行数 = N），行序与消息序一致，且与既有逐消息解码加归一合并的输出逐列相等（含全列 nullable）

#### Scenario: 单条消息保持映射 nullability

- **WHEN** 一次 decode 收到 1 条消息且某字段为非 union 类型
- **THEN** 该字段输出列 nullable=false，与既有单批直通行为一致

#### Scenario: 混合 id 行序与合并语义不变

- **WHEN** 一个 batch 混有 schema id=1（少一列）与 id=2（多一列）的消息，且 id=1 的消息首现更早
- **THEN** 输出行序与消息序一致，列序取 id=1 的列序并追加 id=2 新列，id=1 消息的新增列为 null——与既有逐消息归一合并完全一致

#### Scenario: 错误优先级与归因不变

- **WHEN** schema 含不受支持的类型（如嵌套 record）且首条消息 payload 解析失败
- **THEN** 返回 payload 解码错误（schema 形状错误不抢先）
- **WHEN** 首条消息的更早字段有值级错误（如 null 于非 nullable）而更晚字段有 schema 形状错误
- **THEN** 返回更早字段的值级错误（字段级交错顺序保持）
- **WHEN** 某消息校验失败而其前的消息正常
- **THEN** 错误文案与既有逐消息路径逐字一致，且归因于全局顺序上第一条失败的消息

#### Scenario: Protobuf 同 id 累积

- **WHEN** 一次 decode 收到 N 条同一 Protobuf schema id 的消息
- **THEN** 同样产出单一多行批次，列集为 descriptor 全字段集且全部 nullable，行序与消息序一致

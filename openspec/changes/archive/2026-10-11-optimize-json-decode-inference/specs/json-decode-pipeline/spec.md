## ADDED Requirements

### Requirement: JSON→Arrow 解码 SHALL 单批产出

`try_to_arrow` 对 NDJSON 输入的解码 SHALL 一次产出一个 `RecordBatch`：行数等于输入记录数；SHALL NOT 因解码器内部默认块大小（1024 行）分块后拼接——解码器容量 SHALL 按输入行数估计（NDJSON 每行恰一条记录时换行数即行数上界）且 SHALL 设有实现定义的上限以约束预分配（tape 预分配随行数 × schema 列数增长），常规输入（行数在上限内）无任何 `concat_batches` 中间拷贝；仅当解码行数超出该容量上限时才回退块拼接，产出仍为一个 `RecordBatch`。输出 schema、值与行序 SHALL 与改造前完全一致。输入无尾换行时末条记录 SHALL 正常结算；空输入（或全空行）SHALL 返回以推断 schema 构造的空批。

#### Scenario: 超过内部块大小的输入仍单批产出

- **WHEN** 解码包含 5000 行 NDJSON 的输入（在容量上限内）
- **THEN** 产出恰一个 5000 行的 `RecordBatch`，schema 与值和逐行期望一致，不存在多块拼接步骤

#### Scenario: 超出容量上限的输入回退拼接且产出仍为单批

- **WHEN** 解码行数超过估计上限的输入
- **THEN** 解码分多块 flush 后按顺序拼接，产出恰一个行数等于输入记录数的 `RecordBatch`，行序与值正确，预分配不随输入行数无界增长

#### Scenario: 无尾换行的末条记录正常结算

- **WHEN** 输入为两条记录且末条之后无 `\n`（codec `join(b"\n")` 的实际形态）
- **THEN** 产出 2 行批，末条不丢失、不报 truncated 错误

#### Scenario: 空输入返回推断形状的空批

- **WHEN** 输入为空字节或仅空白行
- **THEN** 返回空 `RecordBatch`，schema 为推断结果（空输入为空 schema），与既有行为一致

### Requirement: 流式 schema 推断 SHALL 与全量 Value 推断严格等价

JSON 解码的 schema 推断 SHALL 采用不物化逐条 Value 树的流式推断器，其输出（字段名与**字段序**、数据类型、nullable 标记）与错误行为 SHALL 与 arrow-json `infer_json_schema`（全量记录扫描）**完全相等**，从而原样保持 `codec-error-isolation` 钉死的全量 union 语义：Int64/Float64 跨记录取宽、仅后续记录出现的字段成列、出现 null 的列 nullable、全整数列保持 Int64。等价性 SHALL 由差分测试承载：固定边界语料与随机生成语料上，两实现对该语料的推断结果（含失败判定）逐项断言相等。

#### Scenario: 边界语料逐字节等价

- **WHEN** 差分测试跑固定语料（mixed arrays、嵌套 struct、struct-in-list、嵌套 list、null 矩阵、>i64::MAX 数值、非法 JSON、非对象顶层记录）
- **THEN** 流式推断器与 `infer_json_schema` 的输出 schema 完全相等（含字段首现序与全列 nullable），非法输入双方同为 Err

#### Scenario: 随机语料等价

- **WHEN** 差分测试对随机生成的 JSON 记录集（参数化嵌套深度与类型杂度）批量对比
- **THEN** 每一例的推断结果（成功 schema 或失败）两实现一致

#### Scenario: 解码端到端语义保持

- **WHEN** 以现有 json codec/processor 测试语料（取宽、后见字段、skip 隔离、空批、坏 JSON）通过新管线解码
- **THEN** 全部既有断言不修改而通过（`codec-error-isolation` 场景原样成立）

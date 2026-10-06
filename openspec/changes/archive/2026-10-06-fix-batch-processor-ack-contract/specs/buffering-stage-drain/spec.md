## ADDED Requirements

### Requirement: batch 处理器合并 SHALL 经 schema 归一
`batch` 处理器 flush 时对多个输入批的合并 SHALL 经 schema 归一：字段取并集、缺失列以 null 填充、同名列类型冲突以显式错误失败；SHALL NOT 以任一批的 schema 做位置式拼接（位置式拼接在同类型异键序输入下静默错列、在列数变化下越界失败）。触发计数的 `count` SHALL 按合并前的**消息行数**计，而非输入批数。

#### Scenario: 异键序同类型行合并不错列

- **WHEN** 缓冲合并 `{"a":1,"b":2}` 与 `{"b":5,"a":6}`（同类型、键序不同）
- **THEN** 输出第二行 a=6、b=5（按列名对齐），不错列、不报错

#### Scenario: 后到的多字段行保留并集

- **WHEN** 缓冲合并 `{"a":1,"b":2}` 与 `{"a":3,"b":4,"c":5}`
- **THEN** 输出含 a/b/c 三列，首行 c 为 null

#### Scenario: 同名列类型冲突显式失败

- **WHEN** 缓冲合并的两批中同名列类型冲突（如 Int64 与 Utf8）
- **THEN** flush 以明确的合并错误失败（fail-closed），不做静默强转

#### Scenario: count 按行数触发

- **WHEN** `count: 3` 且上游一批含 2 行、下一批含 1 行
- **THEN** 累计 3 行即触发 flush（而非等满 3 个输入批）

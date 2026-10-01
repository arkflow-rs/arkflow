# vrl-processor 增量

## ADDED Requirements

### Requirement: UInt64 超范围 SHALL 显式报错
输入 Arrow `UInt64` 列的值超出 `i64::MAX` 时，processor SHALL 返回指明列名与原值的错误，MUST NOT 静默回绕为负数（既有 `as i64` 裸转换行为废除）。值在 `i64` 范围内的 UInt64 列行为不变。

#### Scenario: 超大 UInt64 报错
- **WHEN** 一个 `UInt64` 列含值 `u64::MAX`
- **THEN** 处理返回错误，错误信息含列名与原值，不产出回绕后的负数

#### Scenario: 范围内 UInt64 正常处理
- **WHEN** 一个 `UInt64` 列的全部值 ≤ `i64::MAX`
- **THEN** 列正常进入 VRL 程序（与既有行为一致）

### Requirement: 不支持的输入类型 SHALL 显式报错
输入 Arrow 列的类型不在 VRL 映射支持集内（如 `List`/`Struct`/`Decimal`/`Dictionary`）或列 downcast 失败时，processor SHALL 返回指明列名与类型的错误，MUST NOT 以整列 Null 替代（既有静默转 Null 行为废除）。错误信息 SHALL 提示可通过 `filter_columns`/SQL 预投影移除该列。

#### Scenario: List 列报错而非静默 Null
- **WHEN** 输入批次含一个 `List` 类型的列
- **THEN** 处理返回错误，错误信息含列名、类型与预投影规避提示，不产出该列为 Null 的批次

#### Scenario: 支持类型不受影响
- **WHEN** 输入批次仅含 Utf8/数值/Boolean/时间戳等受支持类型列
- **THEN** 处理行为与既有版本一致

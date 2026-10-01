# expr-row-routing Specification

## Purpose

Defines the per-row destination (topic/key/subject/channel/field) evaluation contract of the shared `Expr` evaluator: `EvaluateResult::Vec` stays index-aligned with the batch rows, null values fail loud with a named error, and consumers of the result must never silently skip rows or panic on index access.

## Requirements

### Requirement: 逐行表达式求值结果 SHALL 与行按索引对齐
`Expr::evaluate_expr` 对列式（数组）结果的求值 SHALL 产出长度等于批次行数的 `EvaluateResult::Vec`：任何一行对应的结果单元存在且仅对应该行。结果单元为 null（表达式对该行求值为 NULL）时 MUST 返回显式错误，错误信息 SHALL 指名表达式文本与 null 所在行号；MUST NOT 通过丢弃 null 单元缩短结果向量。标量（Scalar）路径遇 NULL 沿用既有报错语义。

#### Scenario: 表达式产出含 null 的列
- **WHEN** 批次某列经逐行表达式（如 topic/key 配置为 `expr`）求值，结果数组中存在 null 单元
- **THEN** `evaluate_expr` 返回错误，信息包含表达式文本与首个 null 的行号；不返回缩短的向量

#### Scenario: 无 null 的列式求值保持对齐
- **WHEN** 表达式对批次的每一行都求出非 null 字符串
- **THEN** `EvaluateResult::Vec` 长度等于批次行数，第 i 个单元对应第 i 行

### Requirement: 消费方 SHALL 保持行对齐且不得静默跳行
按索引消费逐行求值结果（topic/key/subject/channel/field）的组件 MUST 保持「结果单元 i 即行 i 的目的地」语义：结果缺失时 MUST 返回指名的错误，MUST NOT 静默跳过该行，MUST NOT 因索引越界 panic。

#### Scenario: kafka topic 结果短于行数时报错不 panic
- **WHEN** kafka output 的 topic 表达式求值结果长度小于载荷行数（防御性场景：不变量被未来回归破坏）
- **THEN** write 返回指名 topic 与行号的错误，进程不 panic

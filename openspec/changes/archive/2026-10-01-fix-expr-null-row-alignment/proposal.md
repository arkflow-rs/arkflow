# fix-expr-null-row-alignment

## Why

`Expr::evaluate_expr`（`crates/arkflow-plugin/src/expr/mod.rs:66-73`）对列式（数组）求值结果做 `filter_map(|x| x.map(...))`——表达式的 null 值被静默丢弃，结果向量比批次的行数短，「求值结果按索引与行对齐」的隐式契约被打破。全部按索引消费该结果的 output 都会出错位：kafka topic 的 `FutureRecord::to(&v[i])`（`output/kafka.rs:290`）在向量变短时**索引越界 panic**（整条流挂死）；kafka key（`i < v.len()` 静默丢 key）、mqtt/nats/pulsar topic 与 redis 四变体的 key/channel/field（`.get(i)` 静默跳行）则是 null 之后的行被静默丢弃、之前的行挂到**错误的 topic/key** 上——静默数据腐坏，与 `fix-plugin-p2-batch` 刚收口的「null 单元 fail-closed」语义（`output/payload.rs`）同族但根在共享求值器。发现于该变更的 CR 二轮深查（2026-10-01）。

## What Changes

- `Expr::evaluate_expr` 数组路径：null 单元改为显式报错（错误指名表达式文本与行号），删除 `filter_map` 丢弃路径；恢复并显式化不变量——**`EvaluateResult::Vec` 的长度恒等于批次行数**。Scalar 路径既有 null 报错（"Null string value"）不变。**BREAKING**（行为变化）：表达式产出含 null 的列从「静默丢行/下游错位/panic」变为整批报错。
- kafka output topic 取值从 `&v[i]` 改为 `get(i)` + 显式报错，消除最后一个索引 panic 位（不变量成立后属防御性加固）。
- 消费方 `.get(i)` 静默跳行模式（mqtt `publish_all`、redis 四变体、nats、pulsar）在不变量恢复后不可达，保持现状不动（防御性改写列为 Non-goal，避免无谓 churn）。
- 回归测试：表达式产出 null → `evaluate_expr` 报错（含表达式与行号）；含 null 行的列式表达式不再缩短结果；消费方（kafka write）在 null topic 下报错而非 panic。
- 文档（en/zh）：`value_field`/topic/key 相关页面补一句「表达式值含 null 时整批报错」的语义说明。

## Capabilities

### New Capabilities
- `expr-row-routing`：`Expr` 逐行目的地（topic/key/subject/channel/field）求值契约——结果与行按索引对齐、null 值 fail-loud、消费方不得静默跳行。

### Modified Capabilities

（无——既有 specs 未覆盖 Expr 求值语义；kafka/mqtt/nats/redis/pulsar output 的行为变化全部由该新 capability 承载。）

## Impact

- **代码**：`crates/arkflow-plugin/src/expr/mod.rs`（求值器 null 路径 + 测试）、`crates/arkflow-plugin/src/output/kafka.rs`（topic 取值加固 + 测试）。
- **行为面**：kafka/mqtt/nats/pulsar output 与 redis output 的 topic/key/subject/channel/field 表达式——null 值场景由静默错位/panic 变为整批报错（可见的错误回归，用户可用 `COALESCE`/`filter_columns` 规避）。
- **文档**：受影响 output 页面（en + zh-Hans）补 null 语义说明。
- **依赖**：无新增。

## Non-goals

- 不为 null 表达式值提供默认值/回退机制（如 fallback 配置）——fail-loud 即目标语义。
- 不改写 `.get(i)` 消费方的防御分支（不变量成立后不可达，改写属无谓 churn）。
- 不动 sql processor 临时表 key 路径——它走自由函数 `evaluate_expr` 的原始 `ColumnarValue`，不经 `filter_map`，不受本缺陷影响。
- 不动 `output/milvus.rs`、`output/pgvector.rs` 的 `.flatten()`（`Option` 展平语义，无 null 丢行问题）。
- 不做 vrl 行级 null→默认值的修复（存量独立问题，另行立项）。
- 不做表达式方言/函数扩展。

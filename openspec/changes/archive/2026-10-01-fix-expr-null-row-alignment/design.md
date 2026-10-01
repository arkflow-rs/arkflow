# design — fix-expr-null-row-alignment

## Context

`Expr<T>`（`crates/arkflow-plugin/src/expr/mod.rs`）是各 output 逐行目的地（kafka topic/key、mqtt/nats/pulsar topic/subject、redis key/channel/field）的统一配置类型：`Value` 字面量或 `Expr` SQL 表达式，求值为 `EvaluateResult<T>`（Scalar 或逐行 Vec）。数组路径当前实现：

```rust
let x: Vec<String> = v.into_iter().filter_map(|x| x.map(|s| s.to_string())).collect();
```

`filter_map` 丢弃 null，使 Vec 长度 < 行数；消费方全部按「索引 i = 行 i」取值（kafka `&v[i]` panic、其余 `.get(i)` 静默跳行/错位）。`fix-plugin-p2-batch` 已把 `output/payload.rs` 的同类问题收口为「null 单元 fail-closed、payload 与行索引对齐」；本变更把同一语义落到共享求值器层，一次修复保护全部 Expr 消费方。

## Goals / Non-Goals

**Goals:**
- `EvaluateResult::Vec` 长度恒等于批次行数（显式不变量）。
- null 求值单元 fail-loud：报错指名表达式与行号。
- 消除 kafka topic 的索引 panic 位（防御性 `get` + 报错）。
- 回归测试钉住不变量；文档（en/zh）说明 null 语义。

**Non-Goals:**
- null 默认值/fallback 机制。
- 改写其余消费方的 `.get(i)` 防御分支（不变量下不可达）。
- sql processor 临时表 key（自由函数 `evaluate_expr` 原始 `ColumnarValue` 路径，无 filter_map）。
- milvus/pgvector 的 `Option` flatten（非 null 丢行）。
- vrl 行级 null→默认值（存量独立问题）。

## Decisions

1. **null 报错而非保留 null 占位**（如 `Vec<Option<String>>`）：消费方（topic/key 送达目的地）对 null 无语义——一行没有目的地只能是错误；改 `Vec<Option<T>>` 会波及全部消费方签名，收益为零。与 Scalar 路径既有「Null string value」报错、`payload.rs` 的 null 拒绝一致。
2. **错误信息带行号与表达式文本**：表达式可能很长，报错含表达式全文 + 首个 null 的行号即可定位（遍历时 enumerate，遇到首个 null 即返回）。
3. **kafka topic 取 `get(i) + ok_or_else`**：不变量成立后该分支不可达，但它是唯一的 panic 位，防御成本一行，值得保留为显式错误。
4. **不动 `.get(i)` 消费方**：mqtt/redis/nats/pulsar 的 `if let Some` 分支在不变量下死代码化，批量改写只增加 churn 与回归面；留给未来触碰这些文件时顺带清理。
5. **测试锚在求值器层 + 一个消费方**：求值器单测（null 报错、对齐不变量、既有 Scalar 语义回归）+ kafka write 的 null-topic 报错测试（mock producer 路径不可行则锚 `get_topic` 层）；不为每个 output 重复同构测试。

## Risks / Trade-offs

- **[行为变化] 依赖「null 被静默吞掉」的既有部署在升级后开始报错** → 这是目标语义（错误可见可用 `COALESCE(expr, 'default')` 规避）；BREAKING 已在 proposal 标注，文档写明规避方法。
- **[求值器是共享路径] 改动波及全部 Expr 消费方** → 变更面小（一处 collect 改逐行检查）；全插件测试套件 + 新增回归测试覆盖。
- **[错误只报首个 null]** 批内多个 null 时逐个报错轮次多轮 → 首错即返是流式处理的常规语义，不聚合全部 null 行。

## Migration Plan

无配置迁移。升级后含 null 求值结果的流从静默错位/panic 变为报错；用户以 `COALESCE` 或上游过滤规避。回滚即还原两处代码改动。

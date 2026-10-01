# Tasks — fix-expr-null-row-alignment

## 1. 求值器 null fail-loud

- [x] 1.1 `crates/arkflow-plugin/src/expr/mod.rs` 数组路径删除 `filter_map`：逐行 `enumerate`，null 单元返回 `Error::Process`（含表达式文本与行号），非 null 收集；`EvaluateResult::Vec` 长度恒等于行数
- [x] 1.2 求值器单测：含 null 列 → 报错含表达式与行号；全非 null 列 → Vec 长度 == 行数且按行对齐；既有用例（`expr/mod.rs` tests）全绿不回归（注意：`concat` 等 SQL 函数会吞 null 参数，测试须用裸列引用触发）

## 2. kafka topic panic 位消除

- [x] 2.1 `crates/arkflow-plugin/src/output/kafka.rs` topic 取值 `&v[i]` 改 `get(i).ok_or_else(...)`（错误指名 topic 配置与行号），`write` 与事务路径同改
- [x] 2.2 kafka 侧测试：null topic 表达式批次经 `get_topic` 断言报错（含表达式与行号）不 panic（`test_topic_expression_null_fails_loudly_not_panic`；防御分支在不变量下不可达，保留为代码级防护）

## 3. 文档

- [x] 3.1 en 文档：kafka/mqtt/nats/redis/pulsar output 页表达式表格后补「表达式对某行求值为 NULL 时整批报错（可用 COALESCE 规避）」
- [x] 3.2 zh-Hans 树同步 3.1；`pnpm docs:check` 通过

## 4. 收尾验证

- [x] 4.1 `cargo clippy --workspace --all-targets` 无新告警；`cargo test --workspace --all-targets` 全绿
- [x] 4.2 更新 `openspec/CODE_REVIEW_2026-09-29.md`：在插件层批次的修复说明中补记本缺陷（CR 二轮发现）已由 `fix-expr-null-row-alignment` 收口

## 1. Deferred ack 契约

- [x] 1.1 `crates/arkflow-plugin/src/processor/batch.rs`：缓冲结构改持 `(MessageBatchRef, Arc<dyn Ack>, rows)`；实现 `process_with_ack`——缓冲期返回 `Deferred`，flush 发射 `SingleWithAck(merged, ConcurrentAck(held_acks))`；`finish`/`on_tick` 复用同一 flush；`process`（无 ack）保留为无 ack 缓冲变体
- [x] 1.2 失败/关闭路径：缓冲中未发射的暂存 ack 被 abort（与 window 算子的取消路径做法对齐），不泄漏不误结算
- [x] 1.3 单测：Deferred 期间 ack 未结算；flush 后每个暂存 ack 恰结算一次；下游失败/abort 传导至每个暂存 ack；关闭时 abort

## 2. 归一合并与行数计数

- [x] 2.1 `flush` 换 `normalize_and_concat`（`component/batch_merge.rs`，必要时调整可见性）；类型冲突错误含列名与两侧类型
- [x] 2.2 触发条件按累计行数（`count` 语义 = 消息行数）；`timeout_ms` 语义不变
- [x] 2.3 单测：异键序同类型行不错列；并集缺列 null；类型冲突显式失败；`count: 3` + (2 行批 + 1 行批) 即触发
- [x] 2.4 既有 7 例内联测试同步（count 语义、None→Deferred 变化）

## 3. 文档与门禁

- [x] 3.1 元数据/文档（en/zh）：batch 处理器页说明 ack 延迟结算（at-least-once）、schema 归一合并、`count` 行数语义与背压交互
- [x] 3.2 门禁：`cargo test -p arkflow-plugin` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`cargo fmt --all`；`pnpm docs:check`
- [x] 3.3 契约复核：两个 delta spec 的每个 Scenario 均有测试证据；tasks 勾选

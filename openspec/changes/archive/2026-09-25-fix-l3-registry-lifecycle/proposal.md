# Proposal: fix-l3-registry-lifecycle

## Why

CR 发现 `add-kafka-l3-transactional-offsets` 的组注册表实现与其规格声明相悖：规格写明"输入 drop 后注册表项自动失效（weak upgrade 失败 → 显式报错）"，实现却持强 `Arc`——输入 drop 后 `group_metadata()` 仍返回死消费者的陈旧元数据，输出会拿它继续 `send_offsets_to_transaction`；同 group 的第二个输入还会静默覆盖第一个的槽。顺修 P2-1：`try_read().ok()?` 在输入 connect 短暂持写锁时假性失败，误报"无活输入"。

## What Changes

1. 注册表项改持 `Weak<RwLock<…>>`：输入 drop → upgrade 失败 → 输出走既有的"无活输入"显式配置错误（fail-closed），与规格一致。
2. `group_metadata` 改 async，用 `read().await` 协作等待——不再因锁竞争假性失败；调用点 await。
3. 规格文本无需变更（本变更使实现符合既有规格）。

## Impact

- `crates/arkflow-plugin/src/kafka_txn.rs`、`output/kafka.rs`（调用点）。
- kafka_eos 真 broker 回归 5/5（含 L3 端到端）。

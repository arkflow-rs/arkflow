# Proposal: fix-l3-assign-mode-validation

## Why

CR 发现未声明的组合限制：partitioned Job 的 Kafka 源走 assign_partition（显式分区指派），消费者用 assign() 不 subscribe()——永不加入 consumer group，group_metadata() 不可用；transactional_offsets: true 在此组合下 connect 期报晦涩错误。

## What Changes

assign_partition 在 transactional_offsets 开启时以明确配置错误拒绝（L3 需 subscribe 模式单读者输入），把失败从运行期 connect 提前到图构建期。

## Capabilities

### New Capabilities
（无）

### Modified Capabilities

- `exactly-once-output`: L3 需求补充「分区指派输入显式拒绝」场景。

## Impact

- `crates/arkflow-plugin/src/input/kafka.rs`。

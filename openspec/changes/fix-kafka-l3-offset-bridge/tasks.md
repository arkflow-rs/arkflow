## 1. 位点钳制与错误捕获

- [x] 1.1 `crates/arkflow-plugin/src/kafka_txn.rs`：组注册表条目扩展携带 `Arc<CommitFrontier>`；输入侧 ack 用的 frontier 实例注册进表；单测：注册/存活/取前沿快照
- [x] 1.2 `output/kafka.rs`：`transactional_offsets_for_batches` 推导后逐分区 `min(批内最大 next, 前沿 next)` + 输出实例 `last_sent` 单调钳制；单测：前沿滞后时只提交前沿、单调不回退、无元数据批次不受影响
- [x] 1.3 `output/kafka.rs`：delivery 回调记录事务性 offset 错误；`commit_transaction` 前检查，有错走既有 abort 路径返回错误；单测：注入回调错误 → 批失败重放、事务不提交
- [x] 1.4 `input/kafka.rs`：`KafkaAck::undo` 在 `transactional_offsets` 下跳过 `store_offset`；单测：undo 仅回退内存 frontier

## 2. 配对启动校验

- [x] 2.1 core 侧 connect 段校验挂点：声明 `transactional_offsets` 的组必须被某输出的 `offset_commit_group` 指名，缺失 `Error::Config` 启动失败；plugin build 侧挂声明
- [x] 2.2 单测：错配启动失败（错误文案含组名与两侧配置键）；正确配对启动通过

## 3. 集成、文档与门禁

- [x] 3.1 `kafka_eos.rs`（testcontainers）扩充：路由拓扑下崩溃无丢失（重放未结算部分）；offset 发送失败批重放；undo 不动 broker 位点
- [x] 3.2 `docs/docs/`（en）与 zh-Hans 对应页：L3 前沿钳制语义、尾部一批重复的界、配对校验的启动失败行为
- [x] 3.3 门禁：`cargo test -p arkflow-plugin` 全绿（含 EOS 套件）；`cargo clippy --workspace --all-targets` 无新告警；`cargo fmt --all`；`pnpm docs:check`
- [x] 3.4 契约复核：delta spec 每个 Scenario 有测试证据；tasks 勾选

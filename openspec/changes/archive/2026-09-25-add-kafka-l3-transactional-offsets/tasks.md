# Tasks: add-kafka-l3-transactional-offsets

- [x] 1.1 `kafka_txn.rs` 组注册表（元数据共享槽 + 主题列表 + weak 生命周期）。
- [x] 1.2 输入：`transactional_offsets` 配置、connect 发布组元数据、KafkaAck 跳过 store_offset。
- [x] 1.3 输出：`offset_commit_group` 配置 + build 校验（要求 exactly_once）+ schema 元数据；`transactional_offsets_for_batches`（元数据列 → TPL max next-offset）；事务内 `send_offsets_to_transaction` → `commit_transaction`。
- [x] 1.4 真实 broker 端到端测试：L3 提交后同组零重投递（kafka_eos 5/5 绿）。
- [x] 1.5 单测/元数据快照回归（docs inventory 再生成）。

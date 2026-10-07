## 1. core：逐行元数据批量助手

- [x] 1.1 `arkflow-core/src/lib.rs` metadata 模块新增 `attach_row_source_metadata`（逐行 partition/offset/key/timestamp/ext，列存在性规则 + nullable 语义；全部列一次 RecordBatch 重建），单元测试覆盖：纯行对齐、混批 null 填充、全缺列不出现、ext 逐行 MapArray 与 `with_ext_metadata_per_row` 输出等价
- [x] 1.2 既有 metadata 测试全绿；`arkflow-core` clippy/fmt 零新告警

## 2. Kafka input 批量 read

- [x] 2.1 `KafkaInputConfig` 新增 `batch_max_rows`（默认 1024）/`batch_max_bytes`（默认 8 MiB），构建期钳制 <1 为 1 并记 debug 日志；`build_client_config` 无关改动不涉及；schema 注册与 JSON Schema（`init()`）同步
- [x] 2.2 `KafkaAck` 增加 `segment_start` 字段：ack 位置语义不变（next=last+1）；undo 改为回退段首（`store_offset(first)` + `rewind_position(first)`），单条=段长 1 特例；现有 KafkaAck 单元/集成断言迁移
- [x] 2.3 read() 重构为「阻塞 recv 首条 + `stream().next().now_or_never()` 无挂起点排空（双上界）」：payload/元数据 owned 收集后释放 consumer 读锁，锁外解码组批；tombstone spawn 结算路径保持；排空中 retryable 错误保留已认领消息、致命错误立即上抛
- [x] 2.4 行对齐：快路径 `apply_codec_to_payloads` 整批解码（行数==payload 数按位对齐），不等时回退逐 payload 解码（skip 丢行不占位、fail 首错上抛）+ 按 payload→行集附元数据；构建期断言元数据行数==解码行数
- [x] 2.5 Ack 组装：按 `(topic, partition)` 分组段式 KafkaAck（段首 anchor_delivery、段内逐 offset acknowledge——frontier 只按单点 next-offset 记账），单组直出 / 多组 `VecAck`（`ConcurrentAck` 为 core `pub(crate)` 不可用；各 partition frontier 独立，顺序 ack 无跨分区等待）；L3 `transactional_offsets` 路径行为不变

## 3. 测试防线

- [x] 3.1 单元：批量组装纯函数测试——多行批元数据逐行对齐（offset/partition/topic/headers 逐行断言）、混批 nullable 列形状、`batch_max_rows/bytes` 上界截断、=1 逐条回退
- [x] 3.2 单元：取消安全防线——模拟 select! 丢弃（首条 pending 时 drop future 重发不认领；认领后组批无挂起点不被打断），对齐 `input-cancellation-safety` 既有框架思路（Kafka 无进程内 broker，用可注入的假消费流）
- [x] 3.3 集成：本地 kafka_eos/kafka_codec_test 启动后按用户决定中断，全量 testcontainers 验证（WAL/durability、event-time 多 partition、fan-out、L3 EOS）交由 PR CI 执行（CI 必绿后再归档）；段 undo/段内 tombstone 幂等的离线单测已补（`undo_rewinds_the_whole_segment_frontier`、`segment_acknowledgement_advances_like_per_message_acks`、drain tombstone 分类）
- [x] 3.4 性能：`kafka_batch_assembly_timing` release 实测（200k 行，含 key/timestamp/headers 元数据）：批组装 8,873,574 rows/s（22.5ms）vs 旧逐条 7×重建链 oracle 135,192 rows/s（1.48s），≈65.6×（批组装路径；端到端收益受 IO/下游摊薄）

## 4. 验证与文档

- [x] 4.1 门禁：本地已过——arkflow-core 全部 metadata 测试（1122 lib tests）、arkflow-plugin `input::kafka` 49 passed、触及 crate clippy 零告警、fmt clean、docs regen（component-inventory.json + config-schema.json 已更新）后 `pnpm docs:check` 通过（142 页/49 组件）；全量 `cargo test --workspace --all-targets` 与全 workspace clippy 交由 PR CI（用户决定）
- [x] 4.2 `docs/docs/components/0-inputs/kafka.md`（en）与 zh-Hans 对应页补 `batch_max_rows/batch_max_bytes` 行 + 说明区改写（默认值、钳制语义、=1 回退、混批 nullable 列形状、ingest_time 批粒度、ack 段语义、tombstone 批外结算）
- [x] 4.3 CHANGELOG `[Unreleased]` 已补条目；PLANNING 10.2 第二批「Kafka input 逐条出批」已勾销并留实测数字（8.87M vs 135K rows/s ≈65.6×）

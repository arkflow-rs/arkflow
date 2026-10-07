## 1. core：逐行元数据批量助手

- [ ] 1.1 `arkflow-core/src/lib.rs` metadata 模块新增 `attach_row_source_metadata`（逐行 partition/offset/key/timestamp/ext，列存在性规则 + nullable 语义；全部列一次 RecordBatch 重建），单元测试覆盖：纯行对齐、混批 null 填充、全缺列不出现、ext 逐行 MapArray 与 `with_ext_metadata_per_row` 输出等价
- [ ] 1.2 既有 metadata 测试全绿；`arkflow-core` clippy/fmt 零新告警

## 2. Kafka input 批量 read

- [ ] 2.1 `KafkaInputConfig` 新增 `batch_max_rows`（默认 1024）/`batch_max_bytes`（默认 8 MiB），构建期钳制 <1 为 1 并记 debug 日志；`build_client_config` 无关改动不涉及；schema 注册与 JSON Schema（`init()`）同步
- [ ] 2.2 `KafkaAck` 增加 `segment_start` 字段：ack 位置语义不变（next=last+1）；undo 改为回退段首（`store_offset(first)` + `rewind_position(first)`），单条=段长 1 特例；现有 KafkaAck 单元/集成断言迁移
- [ ] 2.3 read() 重构为「阻塞 recv 首条 + `stream().next().now_or_never()` 无挂起点排空（双上界）」：payload/元数据 owned 收集后释放 consumer 读锁，锁外解码组批；tombstone spawn 结算路径保持；排空中 retryable 错误保留已认领消息、致命错误立即上抛
- [ ] 2.4 行对齐：快路径 `apply_codec_to_payloads` 整批解码（行数==payload 数按位对齐），不等时回退逐 payload 解码（skip 丢行不占位、fail 首错上抛）+ 按 payload→行集附元数据；构建期断言元数据行数==解码行数
- [ ] 2.5 Ack 组装：按 `(topic, partition)` 分组段式 KafkaAck（段首 anchor_delivery），单组直出 / 多组 `ConcurrentAck`；L3 `transactional_offsets` 路径行为不变

## 3. 测试防线

- [ ] 3.1 单元：批量组装纯函数测试——多行批元数据逐行对齐（offset/partition/topic/headers 逐行断言）、混批 nullable 列形状、`batch_max_rows/bytes` 上界截断、=1 逐条回退
- [ ] 3.2 单元：取消安全防线——模拟 select! 丢弃（首条 pending 时 drop future 重发不认领；认领后组批无挂起点不被打断），对齐 `input-cancellation-safety` 既有框架思路（Kafka 无进程内 broker，用可注入的假消费流）
- [ ] 3.3 集成：既有 kafka testcontainers 测试全绿（含 WAL/durability、event-time 多 partition、fan-out 路径）；如覆盖不到段 undo/段内 tombstone 幂等，补最小用例
- [ ] 3.4 性能：`#[ignore]` release 计时测试 `kafka_batch_assembly_timing`（N 条模拟消息 → 含元数据 RecordBatch，oracle=旧逐条 7×重建路径），记录 rows/s 对比

## 4. 验证与文档

- [ ] 4.1 门禁：`cargo test --workspace --all-targets` 全绿；clippy 零新告警；fmt clean；`ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot` 后 `pnpm docs:check` 通过
- [ ] 4.2 `docs/docs/components/input/kafka.md`（en）与 zh-Hans 对应页补 `batch_max_rows/batch_max_bytes`（默认值、钳制语义、=1 回退、混批 nullable 列形状、ingest_time 批粒度、ack 段语义）
- [ ] 4.3 CHANGELOG `[Unreleased]` 补条目；PLANNING 10.2 第二批「Kafka input 逐条出批」条目勾销并留实测数字（引用 3.4 计时结果）

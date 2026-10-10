# Proposal: optimize-kafka-input-batching

## Why

热路径审计（`openspec/PLANNING.md` 第十节）把 Kafka input 的逐条出批列为「批粒度放大器」：`KafkaInput::read()` 在 `crates/arkflow-plugin/src/input/kafka.rs:413` 每次 `consumer.recv()` 只取一条消息，因此 Kafka→Kafka 路径上所有名义上"每批"的开销实际都以"每条"的频率发生：

- **7 次元数据重建/条**：`kafka.rs:483-533` 串行调用 `with_source/with_partition/with_offset/with_key/with_timestamp/with_ingest_time/with_ext_metadata`，每次 `with_*` 都完整重建一次 RecordBatch + Schema（`arkflow-core/src/lib.rs:576-667` 的 `add_*_column` 结构性分配，O(已有列数) 指针操作 ×7）；其中 `with_ext_metadata` 还把同一组 k/v 字符串按行复制（`lib.rs:711-723`）。
- **逐条 codec decode**：`kafka.rs:473-475` 经 `apply_codec_to_payload`（`codec_helper.rs:30-39`）单条解码——而 `Decoder::decode` 的签名本就接收多 payload（`arkflow-core/src/codec/mod.rs:30-31`），批量助手 `apply_codec_to_payloads`（`codec_helper.rs:50-59`）已存在且被 generate input 使用。
- **重复字符串与 Ack 对象**：每条消息 3 次 `topic().to_string()`（`kafka.rs:508/521/546`）+ 每条一个 `KafkaAck`。

内核侧没有任何"一次 read() 一行"的假设：source chain 是 lock-step 循环，`(batch, ack)` 二元组是原子单位（`arkflow-core/src/executor/task.rs:712,820`），event-time gate 明确为多 partition 批设计（`event_time_gate.rs:447-451`："Kafka may return rows from several physical partitions in a single read"），WAL replay 已按逐行 `__meta_offset/__meta_partition` 工作（`stream_adapter.rs:353-403`）。改造只在插件侧。

## What Changes

1. **read() 批量出批**：首条消息仍走阻塞 `recv().await`（保持取消安全契约——首个 await 前无认领副作用），随后用 rdkafka 0.38 `StreamConsumer::stream()` 的非阻塞 `now_or_never()` 排空已缓冲消息（无挂起点，认领原子），受新配置 `batch_max_rows`（默认 1024）与 `batch_max_bytes`（默认 8 MiB）约束。
2. **元数据一次重建**：新增 core `metadata` 逐行批量助手——`__meta_partition(UInt32)/__meta_offset(UInt64)` 逐行，`__meta_key(Binary, nullable)/__meta_timestamp(Timestamp(ns), nullable)` 按批内是否至少一行存在决定列是否出现（混批 null 填充，对齐 `normalize_and_concat` 的 nullable 惯例），`__meta_ext` 复用既有 `with_ext_metadata_per_row` 的 MapArray 构造；全部列合并为**一次** RecordBatch 重建。
3. **段式 Ack**：按 `(topic, partition)` 分组，每组一个携带 `[first, last]` 段的 KafkaAck（ack=next(last+1)，undo 回退到段首）；跨组用既有 `ConcurrentAck` 组合。tombstone 维持现有 spawn 结算（与段 ack 幂等共存）。
4. **行对齐保障**：整批一次 `apply_codec_to_payloads` 为快路径；解码行数 ≠ payload 数时（skip 模式丢行、多行 payload）回退逐 payload 解码并按 payload→rows 映射附着元数据。
5. **锁范围收窄**：payload/元数据以 owned 形式收集后释放 consumer 读锁，解码与批组装在锁外进行。
6. 配置 schema、`docs/docs/components/input/kafka.md`（en/zh）、CHANGELOG、PLANNING 第十节勾销同步更新；新增 `#[ignore]` release 计时测试（对齐 `protobuf_batch_decode_timing` 模式）。

## Capabilities

### New Capabilities
- `kafka-input-batching`: Kafka input 一次 read() 聚合多行出批的行为契约——批量边界配置、逐行元数据对齐、段式 Ack 与 frontier 语义、tombstone 结算、取消安全排空。

### Modified Capabilities

（无——`input-cancellation-safety` 的枚举对象是通道型 input，Kafka 不在其列；批量认领的取消安全场景由新能力 `kafka-input-batching` 自带，引用引擎契约原文。）

## Non-goals

- 不改其他通道型 input（mqtt/nats/pulsar/redis/websocket）——各自单独立项。
- 不动 codec 内部实现（JSON 双遍解析/零 schema 缓存是第二批独立项）。
- 不切换 `BaseConsumer`（保持 StreamConsumer 的自动唤醒与 rebalance 语义）。
- 不做 broker 在环的端到端基准（CI 基准以纯组装函数计时测试代替）。
- 不做逐 token/逐行流式发射语义；批内行序与 partition 有序性维持现状。
- 不改 `input-durability`/WAL/replay 的既有契约（批量 read 自动减少 WAL seq 数量，replay 逐行判定已兼容）。

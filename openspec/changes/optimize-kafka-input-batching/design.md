# Design: optimize-kafka-input-batching

## Context

`KafkaInput::read()`（`crates/arkflow-plugin/src/input/kafka.rs:404-563`）每次调用只认领一条消息：阻塞 `recv().await` → 单条 codec 解码 → 7 次串行 `metadata::with_*`（每次完整重建 RecordBatch）→ 每条一个 `KafkaAck`。内核 source loop（`task.rs:712`）以 lock-step 逐次驱动 read()，因此 Kafka→Kafka 下全部 per-batch 开销按 per-message 频率放大（PLANNING 第十节 10.1「批粒度放大器」）。

前置调研结论（已核实）：

- **内核兼容多行批**：`(batch, ack)` 是原子单位；event-time gate 逐行读 `__meta_partition` + `__meta_ext.topic` 分组切片（`event_time_gate.rs:1352-1407`），注释明确容纳多 partition 批；WAL 一个 read 批一个 seq；replay/覆盖判定逐行消费元数据（`stream_adapter.rs:138-210, 353-403`）。
- **取消安全契约**（`arkflow-core/src/input/mod.rs:262-275`）：首个 await 前不得有认领副作用。现 read() 的唯一真实挂起点是 `recv()`；其后 decode 虽是 async fn 但同步完成，不产生新挂起点。
- **rdkafka 0.38 无批量 API**：`StreamConsumer::stream()` 返回的 `MessageStream` 的 `poll_next` 用 `poll_queue(queue, Duration::ZERO)` 非阻塞轮询（`rdkafka-0.38.0/src/consumer/stream_consumer.rs:125-143`）——`next().now_or_never()` 可无挂起点取走**已缓冲**消息，Pending 时未认领任何消息。
- **现成积木**：`apply_codec_to_payloads(Vec<Bytes>)`（`codec_helper.rs:50-59`）批量解码；`with_ext_metadata_per_row`（`lib.rs:765`）逐行 MapArray；`ConcurrentAck`（`input/mod.rs:415-477`）注释明确供"独立 source records"组合；generate input 是多行批 + 单 Ack 的现成范本。
- **标量助手局限**：`with_partition/with_offset/with_key/with_timestamp` 是标量广播（`lib.rs:539-560`），批量需逐行数组构造。

## Goals / Non-Goals

**Goals:**
- read() 一次返回多行批（默认无额外等待延迟——只聚合已缓冲消息）。
- 每批 7 次 RecordBatch 重建 → 1 次；codec decode 每批 1 次（快路径）。
- Ack/frontier/checkpoint/WAL 语义与现状等价（at-least-once、连续 frontier、乱序 fan-out 容忍）。
- `batch_max_rows: 1` 时行为粒度回到逐条（除内部实现统一外）。

**Non-Goals:** 其他通道 input；codec 内部优化；BaseConsumer 切换；broker 在环基准；流式逐行发射。

## Decisions

### D1 排空机制：首条阻塞 + now_or_never 无挂起点排空

```
first = consumer.recv().await          // 唯一真实挂起点（取消安全边界）
loop {                                  // 同步循环，零挂起点 → 原子认领
    if rows >= batch_max_rows || bytes >= batch_max_bytes { break }
    match consumer.stream().next().now_or_never() {
        Some(Ok(msg)) => 收集, Some(Err(e)) => 按 retryable 分流, None => break
    }
}
```

- 排空循环内**不得**出现可挂起的 await（含"实际会 yield"的 codec/元数据步骤）——整个认领要么全部完成要么未被 select! 打断。首个 `recv().await` 被 drop 时不认领任何消息，契约保持。
- `retryable_receive_error` 复用：排空中途遇 retryable 错误时，已收集消息照常组批返回（错误留待下轮 read 的 recv 报出 `Disconnection` 触发重连）；遇致命错误立即返回（已认领消息的丢失窗口与现状 read() 出错路径一致）。
- 排空条数上限：`batch_max_rows`（默认 1024，clamp ≥1）；字节上限 `batch_max_bytes`（默认 8 MiB，按 payload 累计，clamp ≥1）。两者任一满足即停止——控制大 payload 下的内存上界。

### D2 元数据：core 新增逐行批量助手，一次重建

core `metadata` 新增（与现有 per-row ext 助手同层）：

```
attach_row_source_metadata(batch, rows: &[RowSourceMetadata]) -> Result<RecordBatch>
// RowSourceMetadata { partition: u32, offset: u64, key: Option<&[u8]>,
//                     timestamp: Option<SystemTime>, ext: HashMap<String,String> }
```

- 由调用方先以 `with_source("kafka")` + `with_ingest_time(now)` 完成两个标量列（或助手内联），`__meta_ext` 直接内联逐行 MapArray 构造——合计**恰好一次** RecordBatch 重建。
- 列存在性规则（与现状形状兼容）：`__meta_partition/__meta_offset` 恒存在（UInt32/UInt64 非空）；`__meta_key`（Binary）/`__meta_timestamp`（Timestamp(ns)）**当且仅当批内至少一行有值**才出现，出现即 nullable、缺失行填 null（对齐 `normalize_and_concat` 的并集 nullable 惯例，`batch_merge.rs:48`）。
- `__meta_ingest_time` 每 read() 取一次 `SystemTime::now()` 广播全批（批内粒度，文档写明）。

### D3 Ack：按 (topic, partition) 段式 KafkaAck + ConcurrentAck

- `KafkaAck` 增加字段 `segment_start: i64`（现单条路径即 `offset == segment_start`）。ack 语义不变：position = `offset+1`，frontier 连续推进、`store_offset` 存连续 next。
- **undo 回退到段首**：`store_offset(segment_start)` + `frontier.rewind_position` 逐格回退到 `segment_start`——整段重投。现单条 undo（`kafka.rs:1019-1079`）是段长为 1 的特例。
- **段内逐 offset acknowledge（实施修正）**：`CommitFrontier.acknowledge` 只登记单个 next-offset（pending 集合），不自动填段内区间——`anchor(5)+ack(8)` 会留下永不闭合的 gap 6。段 ack 因此在 ack_lock 临界区内对 `first..=last` 逐 offset acknowledge（段内连续，仅首 offset 可能 `Pending`；frontier 的连续 drain 保证一次推进到 `last+1`，`store_offset` 仍只写最终连续值）。
- `anchor_delivery`：每 partition 段锚定**首条** offset（与 WAL replay 的锚定语义一致，`kafka.rs:639-643`）；批内同 partition 连续（单消费组天然成立）时段 ack 即一次 `Advanced`。
- 跨 (topic, partition) 组：1 组 → 直接 KafkaAck；n 组 → `VecAck`（**实施修正**：调研设想的 `ConcurrentAck` 是 arkflow-core 的 `pub(crate)` 类型，插件不可见；`VecAck` 顺序 ack 各组——各 partition frontier 相互独立，不存在跨分区 gap 等待，顺序执行无死锁风险，失败反向补偿语义相同）。fan-out 由内核 `fanout_ack` 逐组分发，现有乱序容忍不变。
- **tombstone**：维持现状 spawn 后台结算（`kafka.rs:431-470`）。位于数据段内的 tombstone 位置会被段 ack 连续覆盖，双重结算经 frontier `AlreadyCovered` 幂等；段外/孤立 tombstone 仍自结算。**不引入新的等待路径**（spec：结算不得阻塞 source loop）。
- `transactional_offsets`（L3）：段 ack 同样跳过本地 `store_offset`，只推进 frontier 供事务 output 按连续段提交——语义不变。

### D4 行对齐：批解码快路径 + 逐 payload 回退

- 快路径：整批 payloads 一次 `apply_codec_to_payloads`；若解码行数 == payload 数，按位对齐附着元数据（JSON 单文档/无 codec binary/Avro/Protobuf 固定 schema 均为 1:1）。
- 回退（行数不等：skip 模式丢行、多行 payload、异构 schema 合并）：逐 payload `apply_codec_to_payload`，失败按 codec error 模式处理（fail → read() Err，与现状一致；skip → 计 skipped 不产生行），幸存 payload 的行集携带该 payload 元数据。元数据行数必须 == 解码行数，构建期断言。
- 无 codec 路径：`MessageBatch::new_binary(payloads)` 一次成 N 行（零额外工作）。

### D5 锁范围与所有权

- 现状：consumer 读锁横跨 recv+decode+元数据（`kafka.rs:404-410`）。改为：锁内完成「recv + now_or_never 排空」，payload `to_vec` 为 owned、topic/key/headers 提取为 owned 后**先释放读锁**，再在锁外 decode + 组批。`BorrowedMessage` 不出锁作用域。

### D6 配置与文档

- `KafkaInputConfig` 新增 `batch_max_rows: Option<u32>`（默认 1024）、`batch_max_bytes: Option<u64>`（默认 8 MiB）；JSON Schema 注册、`docs/docs/components/input/kafka.md`（en/zh）、CHANGELOG、PLANNING 10.2 第二批对应条目勾销。
- 计时测试：`#[ignore]` release 测试 `kafka_batch_assembly_timing`（纯组装函数：N 条模拟消息 → 含元数据 RecordBatch，对照旧路径 N×7 次重建的 oracle），模式对齐 `protobuf_batch_decode_timing`（`component/protobuf.rs:1372-1375`）。

## Risks / Trade-offs

- **混批 schema 形状变化**：key/timestamp 列在混批下带 null（现状是拆成两个不同 schema 的单行批）。下游按列名读取且 `partition_batch_by_key_hash` 对 null key 行为需实现期验证（缺列报错路径保持，null 行进入 hash 的行为写测试固定）。
- **ack 粒度变粗**：段内单条失败重试 → 整段 undo 重投（at-least-once 不变，重复量增大到段长；默认 1024 行上限内）。
- **吞吐反转风险**：低流量时批=1，新增开销仅为一次 now_or_never 探测（无消息即返回 None），可忽略；实施后以 release 计时测试 + 现有 kafka 集成测试（testcontainers）双验证，若 `batch_max_rows=1` 与默认档出现回归则复查 D2 内联实现。
- **rdkafka 内部队列语义依赖**：`MessageStream::poll_next(ZERO)` 的"Pending 时无认领"行为来自 0.38 源码核实；升级 rdkafka 时需回归取消安全测试（任务中含防线测试）。

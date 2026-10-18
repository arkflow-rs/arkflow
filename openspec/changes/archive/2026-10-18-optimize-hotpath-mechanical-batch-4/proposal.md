# Optimize Hot-path Mechanical Batch 4（Avro L2 批内 reader 复用 + gate 混合组拷贝定夺）

## Why

热路径审计（`openspec/PLANNING.md` 第十节）第二批候选闭环后留了两个尾巴：10.3 的"已阻塞项"Avro L2 与 10.2 的"观察后再定"项 event_time_gate 混合结果组拷贝。本次对两项分别做了源码复核与 release 实测：

1. **Avro L2（reader 复用）原判"上游 API 阻塞走不通"——复核推翻**。原结论（PLANNING.md:595、10.3）依据三条上游约束：`decode_internal` 私有、reader 只收借用型 `ResolvedSchema`、`ResolvedOwnedSchema` 不可被 reader 消费。这三条对 apache-avro 0.22（crates.io 最新版，2026-08-10）仍然成立——但它们只阻塞"把 reader **长期缓存进结构体**"（`ResolvedSchema<'s>` 借用 schema，无 owned-reader 入口）。**批内作用域复用不需要任何上游改动**：`GenericDatumReader::read_value` 接收 `&self`（reader/datum.rs:132），构造一次即可跨消息复用。热路径现状是 `SchemaRegistryCodec::decode` 每条消息经 `avro_read_value` 重建一次 `GenericDatumReader`（`crates/arkflow-plugin/src/codec/schema_registry.rs` decode 循环 → `avro_arrow.rs:46-53`），即**每消息重建一次 `ResolvedSchema` 名字表**（公开的 `avro::decode()` 同样每次 `ResolvedSchema::try_from`，decode.rs:72-75，不是出路）。A/B 实测（新增 `avro_reader_reuse_timing`，release）：w5 1.75M→2.48M rows/s（**+42%**）、w25 483K→545K（**+13%**）、w100 125K→134K（**+6.7%**）——reader 构建在窄 schema 上占解码成本近三成。

2. **gate 混合组拷贝（batch 2 归档 Non-goal"观察 profile 后再定"）——实测不显著，定夺不做**。新增 `event_time_gate_group_copy_timing`（release）给出判定数据：`filter_record_batch` all-true 走 arrow 零拷贝快路径（0.15µs/1024 行批），50% 掩码 gather 2.71µs/批；2 组分拆的总拷贝 ≈5.4µs/2048 行 ≈ **2.6ns/行**。对照内核端到端基线（linear-sql 676K rows/s ≈ 1.48µs/行），该拷贝端到端占比 **<0.2%**，且"每行跨组恰好拷贝一次"已是批分拆的理论下界（只有连续段可 slice 化，触发频率低）。复制 batch 2/3 的审计口径：收益不明确 → 实测后仍不明确 → 以证据结案，不加分支复杂度。

延续 batch 1-3 的先例，本 change 打包语义保持的机械项与证据结案项：每项独立可回退，既有测试全绿为门槛，release 计时测试量化收益。

## What Changes

- **Avro 批内 reader 复用**：`SchemaRegistryCodec::decode` 改两段式——pass 1 按消息序做 wire-format 解析与 schema 解析（与历史逐消息管线相同的错误中止顺序，`fetch_error_precedes_later_bad_wire_header` 钉死的语义不变），schema `Arc` 按 id 暂存；pass 2 按消息序解码，`AvroValueReader` 按 schema id 惰性构建**一次**、跨消息复用（交织 id 也不重建）。`avro_arrow.rs` 新增 `AvroValueReader<'s>`：借用 schema Arc、批生命周期；`avro_read_value`（单发路径）保留不动。**语义零变化**：输出逐字节等价（`multi_row_accumulation_matches_per_message_batches` 等既有等价测试）、错误顺序逐消息保持（两个错误顺序钉死测试保持绿）。
- **gate 混合组拷贝：无代码改动**。新增 `#[ignore]` 计时测试 `event_time_gate_group_copy_timing` 把判定证据留档（含裸 filter 微基准），PLANNING 10.2 该条目从"观察后再定"改为"已测得不显著（2.6ns/行，端到端 <0.2%），结案不做"。
- 新增 `#[ignore]` release 计时测试 `avro_reader_reuse_timing`（对齐 `debezium_decode_timing` 模式，复用路径内联为对照臂）。
- PLANNING 10.3 Avro L2 条目更新为"已闭环（批内复用，无需上游改动）"；10.2 第二批候选补勾划。

## Capabilities

### New Capabilities

（无——本 change 为既有行为的性能保持性改造与证据结案。）

### Modified Capabilities

- `schema-registry-integration`: 新增护栏性 requirement"Avro 解码的批内 reader 复用 SHALL 保持逐消息语义"——reader 每批每 id 构建一次且 SHALL NOT 改变输出等价、交织 id 分组或逐消息错误顺序（沿用 `refactor-batchf-structural-debt` 的先例：用 spec 钉住重构必须保持的不变量；两个错误顺序钉死测试即该契约的机器可执行形态）。

## Impact

- **代码**：`crates/arkflow-plugin/src/codec/schema_registry.rs`（decode 两段式）、`crates/arkflow-plugin/src/codec/avro_arrow.rs`（`AvroValueReader` + `avro_reader_reuse_timing`）、`crates/arkflow-core/src/executor/event_time_gate.rs`（`event_time_gate_group_copy_timing`）。
- **行为**：无用户可见变化；无配置面变化；无 YAML 示例影响。
- **性能**：schema_registry Avro 解码端到端提升（官方 benchmark `avro-decode-w5/w25/w100` 前后对比记录于 tasks/results）。
- **文档**：无（无用户可见面，不触发 docs 义务）。

## Non-goals

- 不向 apache-avro 上游提交 owned-reader 能力 PR：批内复用已消除每消息成本；**跨批长生命周期** reader 缓存仍需上游 API，留待上游演进。
- 不做 event_time_gate 连续段 slice 化：测得收益端到端 <0.2%，不值得增加掩码连续性检测与 slice 分支。
- 不改 Avro Arrow 映射、schema 缓存（L1 已闭环）、列式累积器（L3 已闭环）、Protobuf 路径（已随 `optimize-protobuf-batch-decode` 闭环）。
- 不改 gate 的分组/确认/事件时间语义（`event-time-processing` spec 全部场景不变）。

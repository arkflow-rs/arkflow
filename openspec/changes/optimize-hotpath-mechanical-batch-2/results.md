# Results（优化后数据与收益评估，2026-10-13）

环境与口径同 `baseline.md`（M1 Pro / 32G / rustc 1.97.1 / release LTO；计时测试 200k 行 3 轮最优；benchmark count=200000 warmup=1 runs=3 取最优）。基线 HEAD `66a2142e`，本 change 后 HEAD 见 git log（每项一 commit）。

## 计时测试：优化前 → 优化后

| 测试 / 路径 | 基线 | 优化后 | 变化 | 归因 |
| --- | --- | --- | --- | --- |
| protobuf encode 窄 4 列（vs 逐 cell 基线档） | —（新测试） | 1,549,093 rows/s（基线档 1,344,943） | **+15.2%** | D1（每列 FieldDescriptor 预解析） |
| protobuf encode 宽 26 列（vs 逐 cell 基线档） | —（新测试） | 197,099 rows/s（基线档 190,302；首跑 236,292 vs 206,636 = +14.4%） | **+3.6% ~ +14.4%**（两轮波动） | D1；encode 瓶颈大头在字段映射与 wire 序列化，名字解析占比随列宽摊薄 |
| json_decode_timing 4 列新管线 | 1,502,911 | 1,440,619 | -4%（噪声） | 本 change 未触碰 JSON 解码 |
| json_decode_timing 200 列新管线 | 30,840 | 31,171 | +1%（噪声） | 同上 |
| kafka_batch_assembly_timing 批组装 | 8,651,611 | 8,817,953 | +2%（噪声） | 本 change 未触碰批组装热路径 |
| protobuf_batch_decode_timing 列式 | 1,220,789 | 1,213,237 | -1%（噪声） | 本 change 未触碰 decode |

同批 `--ignored` 触发的 influxdb/pgvector live-service 测试在基线与复跑两轮均因无真实服务失败，与本结果无关。

## benchmark 套件：优化前 → 优化后

| 场景 | 基线 | 优化后 | 变化 | 归因 |
| --- | --- | --- | --- | --- |
| linear-sql（generate 前端） | 670,686 | 702,317 | **+4.7%** | D4（前端 N-1 次解码消除） |
| filter-project-sql（generate 前端） | 767,643 | 799,166 | **+4.1%** | D4 |
| groupby-sql（generate 前端） | 817,196 | 768,726 | -5.9%（噪声；SQL 侧重排波动） | — |
| codec-json | 2,380 | 2,437 | +2.4%（噪声） | — |
| json-decode | 2,610 | 2,617 | +0.3%（噪声） | — |
| state-backend | 397 | 379 | -4.5%（噪声） | —（未触碰） |
| avro-decode-w5 | 1,217,609 | 1,127,295 | -7.4%（噪声；未触碰路径） | — |
| avro-decode-w25 | 391,440 | 393,481 | +0.5%（噪声） | — |
| avro-decode-w100 | 108,467 | 109,771 | +1.2%（噪声） | — |

**噪声评估**：未触碰路径（json-decode/state-backend/avro 三档）在两轮间波动 ±0.3%~7.4%，构成本次测量的噪声带；generate 前端两场景 +4~5% 超出噪声下沿但幅度有限——benchmark 前端原先每 read 1000 次重复解码一个 2 字段模板（约 0.4ms/batch），相对 SQL 段占比小。D4 的更大意义在**度量保真**：前端不再重复计费解码工作，后续所有 generate 前端场景的数字自此可信。

## 各项收益与实现说明

| 项 | 落点 | 收益形态 | 备注 |
| --- | --- | --- | --- |
| D1 protobuf encode | `component/protobuf.rs` | 计时 +15%（窄）/ +4~14%（宽）；解析工作 ∝ 列数而非 cell 数 | 字节级等价测试钉死 |
| D2 Kafka output | `output/kafka.rs` | codec 路径每 payload 消一次冗余深拷贝；每条消息 tokio Mutex → std Mutex（push 段无 await）；回执 join_all 观察后按序报首错 | 投递行为不变 |
| D3 stdout | `output/stdout.rs` | 每行一次 syscall → 每批一次落盘（计数 writer 断言 200 行 1 次 write） | 批间可见性不变（每批显式 flush） |
| D4 generate input | `input/generate.rs` | 每 read N-1 次 clone+解码 → 1 次解码 + take 复制；benchmark 前端失真消除 | 等价测试 codec 开/关双分支 |
| D5 memory buffer | `buffer/memory.rs` | 写锁内 O(n) 求和 → O(1) 增量计数 | 并发采样断言计数==求和 |
| D6 partitioned 路由 | `executor/task.rs` | O(subtasks×rows) 掩码+filter → 单趟分桶 + 每非空子任务一次 take | 行序/归属语义不变 |
| D7 fanout_ack | `input/mod.rs` | 每分支 3 Arc 分配 → 槽位数组（≈n+3） | ack/hold/abort 语义逐字节不变 |
| D8 TrackingAck | `executor/commit.rs` | 每源批异步 Mutex 分配消除；settlement 槽位（AtomicBool+Drop 守卫） | 3 个新并发交错测试 |
| D9 窗口算子 | `window/operator.rs`+`firing.rs` | 键 clone 从堆分配变 Arc bump；membership 单探测；快照从全量克隆 ∝ 打开窗口数 → touched 槽位镜像 | 2 个新回滚等价测试；state_key 字节布局不变 |
| D10 event_time_gate | `event_time_gate.rs` | RowDecision 集合 SmallVec 零堆分配（常见基数 1-3）；物理分区切分每 distinct 一次身份构造 + 单趟分桶 | 输出保持原身份排序 |

**无劣化项确认**：全部既有测试面（window 96+2、routing 6、fanout、commit tracker、memory 并发、generate/stdout/kafka 单测、gate 36）不改断言通过；未触碰路径的计时波动均在噪声带内。设计文档中"输出保持首现序"与旧 BTreeMap 实现有出入——实际实现按 spec 场景保持**原身份排序**（对 distinct 组排序一次，排序面从行数降为分区数）。

## 与设计的偏差记录

1. **D8 实现形态**：std Mutex 跨 await 持有会死锁 current-thread 运行时（测试运行时即此类），undo/abort 无法"全程 guard 内"。落地形态为 settlement 槽位（AtomicBool + yield 协作竞争 + Drop 守卫释放），内层调用互斥语义与旧异步锁一致，且消除了每批的 Mutex 分配（设计目标本身）。
2. **D10b 排序语义**：见上，按 spec 的"identical order"落地为身份排序而非首现序。
3. **5.1 测试适配**：键类型 String→Arc<str> 波及 tests.rs 约 30 处构造点做 `.to_string()`→`.into()` 机械适配（断言语义不变）；stdout MockWriter 装箱、kafka 测试 1 处 std::sync::Mutex 构造适配同类。

## 附录：delta spec 场景 ↔ 测试对照（任务 6.3）

### protobuf-codec（ADDED：Encoding SHALL resolve each column's field descriptor once）

| 场景 | 覆盖测试 |
| --- | --- |
| Encoded output is byte-identical to the per-cell baseline | `component::protobuf::tests::encode_columnar_descriptors_are_byte_identical_to_per_cell_baseline`（新增：12 字段形态 + null→absent + 字段序） |
| Per-cell name resolution is eliminated | `component::protobuf::tests::protobuf_batch_encode_timing`（#[ignore] release 计时：解析工作 ∝ 列数的量化证据，窄/宽两档） |
| Unsupported types still error at build time | 既有 `unsupported_proto_kind_is_rejected`、`arrow_column_type_mismatch_names_the_field`（不改通过） |

### columnar-window-operators（MODIFIED：Keyed window aggregation state）

| 场景 | 覆盖测试 |
| --- | --- |
| ARC-shared key + single entry probe + state_key layout unchanged | 既有窗口套件整体（96 项不改通过）；`state_key` 持久化布局经既有 backend get/restore 断言（`fired_window_rolls_back_when_source_ack_fails` 等） |
| Failed accumulation leaves untouched windows untouched | `executor::window::tests::failed_accumulation_rollback_leaves_untouched_windows_bit_identical`（新增） |
| Fired-ack compensation rewinds only touched windows | `executor::window::tests::failed_fired_ack_rewinds_only_fire_touched_windows`（新增，含"本批未触碰、更早批次填充的窗口"形态 + 全量快照等价断言） |
| Persisted state key layout is unchanged | 既有 `legacy_integer_state_migrates_and_legacy_float_state_is_rejected`、`fired_window_rolls_back_when_source_ack_fails`（backend 字节级断言不改通过） |
| 其余聚合/恢复/宽化/null-only/legacy 场景 | 既有 window 96 项（不改通过） |

### event-time-processing（MODIFIED：classify current rows consistently；ADDED：physical partition split）

| 场景 | 覆盖测试 |
| --- | --- |
| Inline decision storage preserves classification | gate 既有 36 项分类测试（hold/release/late 策略/EOS 全路径不改通过——内联存储现于每次分类中行使） |
| A batch contains a future row and an old row 等 3 个既有场景 | `holds_rows_until_watermark_opens_window`、`late_rows_are_dropped_with_drop_policy`、`mixed_released_and_late_rows_keep_distinct_actions`、`finish_classifies_held_memberships_against_real_deadlines`（不改通过） |
| Split output equals the per-row baseline | `physical_partition_split_groups_by_topic_partition_and_null_fallback`（含 topic/分区/null 回退/身份排序）、`connector_neutral_sources_get_distinct_watermark_partitions`、task.rs gate-dispatch 测试（不改通过） |
| Identity construction does not scale with rows | 实现不变量（单趟 HashMap 借用键、每 distinct 一次构造）+ 上述身份等价测试；无行数敏感性测试（计时属 release 量化，同 protobuf 模式留档） |

## 验证状态

- `cargo test --workspace --all-targets`：全绿（redis_cluster 两轮因残留 testcontainers 容器占用 :16379 失败，`docker rm -f` 后通过——AGENTS.md 记录的已知环境项）
- `cargo clippy --workspace --all-targets`：无告警；`cargo fmt --all --check`：通过
- `pnpm docs:check`：通过（142 页 / 49 组件；无文档面变化，符合预期）
- `openspec validate --specs --json`：94 个 spec 全部有效；`openspec validate --changes`：本 change 有效

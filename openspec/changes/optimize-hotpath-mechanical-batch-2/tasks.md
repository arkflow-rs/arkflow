# Tasks: optimize-hotpath-mechanical-batch-2

顺序：插件侧低风险先行（阶段 1-2），内核侧后行（阶段 3-5），收尾验证（阶段 6）。每项一个独立 commit，可单独 revert；既有测试不改通过是每项的硬门槛。

## 1. 前置基线

- [x] 1.1 采集优化前基线：跑 `cargo test --release -p arkflow-plugin --lib -- --ignored --nocapture` 中既有计时测试（json_decode_timing / kafka_batch_assembly_timing / protobuf_batch_decode_timing）与 `cargo run -p arkflow --release --example benchmark`，记录到本 change 目录 `baseline.md`（含本机环境说明）
- [x] 1.2 确认全量测试起点为绿：`cargo test --workspace --all-targets` 与 `cargo clippy --workspace --all-targets`

## 2. 插件 crate（D1-D5）

- [x] 2.1 stdout（D3）：`output/stdout.rs` writer 改 `BufWriter<Box<dyn Write + Send>>`（builder 处包装），保留每批末尾显式 flush；既有 MockWriter 测试适配、断言不变，新增"批内多行一次落盘"断言（借助计数 writer）
- [x] 2.2 protobuf encode（D1）：`component/protobuf.rs` `arrow_to_protobuf` 提升每列 `FieldDescriptor` 到行循环外，逐 cell 改 `set_field(&fd, v)`；既有 round-trip / columnar-vs-oracle 测试不改通过
- [x] 2.3 新增 `#[ignore]` release 计时测试 `protobuf_batch_encode_timing`（对齐 `protobuf_batch_decode_timing` 模式：新路径 vs 每 cell set_field_by_name 基线，宽/窄两档），跑并记录数据
- [x] 2.4 memory buffer（D5）：`buffer/memory.rs` 写锁内 O(n) 求和改锁内增量 `usize` 计数，枚举全部突变点（push_front/drain/flush/clear/merge 回补）同步维护；新增"计数与全队列求和一致"断言测试（并发压力下）
- [x] 2.5 Kafka output（D2a）：删除 codec 分支冗余 `.map(|p| p.to_vec())`（write 与 send_in_transaction 两处）；`send_futures` tokio Mutex 改 std Mutex（push 段无 await），flush/close 先 drain 再 await
- [x] 2.6 Kafka output（D2b）：flush/close 回执观察改 `join_all` 按原序检查结果、报首个错误；既有 kafka.rs 离线测试不改通过；确认无跨 await 持锁
- [x] 2.7 generate input（D4）：`input/generate.rs` read() 改"1 行模板批构造一次 + `take` 复制到 batch_size"；新增等价测试：codec 开/关两分支下复制路径与逐条解码路径逐列逐行相等（含元数据列），count/interval/batch_size 语义测试不改通过

## 3. 内核 crate：路由与 ack（D6-D8）

- [x] 3.1 partitioned 路由（D6）：`executor/task.rs` `partition_batch_by_key_hash` 改单趟行号分桶（ranges→子任务映射预建一次）+ 每非空子任务一次 `take_record_batch`；null 键、空 key_field、key_group_range 归属语义不变；6 个既有 routing_tests 不改通过
- [x] 3.2 fanout_ack（D7）：`input/mod.rs` 分支标志打包进 `FanoutAckState`（`Box<[BranchSlot]>`，每槽 acknowledged/held），子 ack 携 `(Arc<state>, index)`，总分配 ≈3n+3 → ≈n+3；ack/hold/abort/重试语义不变，`branches<=1` 短路保留；既有 fanout 测试不改通过
- [x] 3.3 TrackingAck（D8）：`executor/commit.rs` tokio Mutex 改 std Mutex，ack 路径拆"guard 内检查/置位 + clone inner + drop guard + await"；undo/abort 全程 guard 内；新增并发交错测试（join! ack vs abort，断言内层 Ack 恰一次终态调用）；tracker 计数测试不改通过

## 4. 内核 crate：event_time_gate（D10）

- [x] 4.1 `RowDecision` 集合字段改 `SmallVec<[T; 4]>`（`smallvec = "1"` 入 `[workspace.dependencies]`，crate 以 `workspace = true` 引用），classify_row 常见基数零堆分配；决策结果与顺序不变，gate 既有测试不改通过
- [x] 4.2 `split_by_physical_partition_for_source` 改单趟 `HashMap<&str, usize>` 借用键分桶，每 distinct (topic, source) 仅构造一次 owned 身份（source 格式化一次），每分区一次 take gather，输出保持首现序；gate/task.rs 既有测试不改通过

## 5. 内核 crate：窗口算子（D9）

- [x] 5.1 窗口键类型：buffers/touched/legacy 与相关签名 `(i64, String)` → `(i64, Arc<str>)`，`extract_keys` 改 `Arc<str>::from` 单次分配；`state_key` 字节布局不变；window/ 全量既有测试不改通过
- [x] 5.2 accumulate 单趟：865/892/899 的 contains_key+entry 双查改单次 `entry()`（Vacant 记 touched），late-update 与 session 桥接路径同形处理；既有 accumulate 测试不改通过
- [x] 5.3 touched-only 快照：before-image 按操作独立抓取——accumulate 首触槽位抓镜像（含 Absent 标记）供 `restore_runtime` 错误回滚；fire 路径（on_watermark/on_tick/finish 及 process_internal 内 watermark 触发的 fire）在改动每个槽位前独立抓镜像，**含本批 accumulate 未触碰的槽位**（如更早批次填充、本批才被 watermark 触发 fire 的窗口），fired-ack 补偿的 `WindowRollback` 携带 fire 侧自身镜像；删除 process_internal 每批的全量 `runtime_snapshot` 克隆
- [x] 5.4 新增回滚等价测试：失败 accumulate 后未触碰窗口逐字节不受影响、fired-ack 补偿只回退 fire 改动的窗口（含本批 accumulate 未触碰、由更早批次填充的窗口）、恢复结果与全量快照路径一致

## 6. 收尾验证

- [x] 6.1 全量验证：`cargo test --workspace --all-targets` 全绿 + `cargo clippy --workspace --all-targets` 无新增告警 + `cargo fmt --check`；`pnpm docs:check`（无文档面变化，应通过）
- [x] 6.2 采集优化后计时数据（2.3 新增 encode 计时 + 1.1 同口径 benchmark 复跑），前后对比写入本 change 目录 `results.md`（含每项收益与噪声评估；无收益或劣化项如实记录并评估回退）
- [x] 6.3 核对 delta spec 场景全部有对应用例覆盖（spec 场景 ↔ 测试清单对照写入 results.md 附录），`openspec validate --specs --json` 通过

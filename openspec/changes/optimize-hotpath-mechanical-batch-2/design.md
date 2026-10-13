# Design: optimize-hotpath-mechanical-batch-2

## Context

第一批机械批（`optimize-hotpath-mechanical-batch`，2026-10-07）确立了模式：审计 → file:line 核实 → 语义保持的最小改动 → 等价测试 + release 计时测试量化。本 change 处理第二批候选中无需新设计的 10 项（设计重活留 Non-goal）。所有落点已核实（2026-10-11），其中 2 项审计断言修正：Kafka 事务往返为每批而非每条；event_time_gate 全 Emit 深拷贝已被 arrow-select 58.4 的 `IterationStrategy::All` 零拷贝切片消解。

约束：无依赖面变化（唯一新增 workspace 依赖见 D8 的取舍）；无配置/YAML/文档语义变化；所有改动必须被既有测试面（window tests ≈4.6k 行、routing_tests、commit.rs ack 测试、memory 并发测试）无修改通过，新增测试只加不改。

## Goals / Non-Goals

见 proposal。设计只回答"每项怎么改、为什么这样改、风险在哪"。

## Decisions

### D1 protobuf encode：提升 FieldDescriptor，不改值构造

现状（`component/protobuf.rs:627-788`）：每列在 644 行解析一次 `get_field_by_name` 却丢弃，行内逐 cell 走 `set_field_by_name`（名字哈希 + HashMap 查找 + FieldDescriptor Arc bump + BTreeMap 探测，prost-reflect 0.16.5 内部核实）。

**决策**：把每列的 `FieldDescriptor` 收进列计划 Vec（紧挨现有 644 行解析处），行循环改 `msg.set_field(&fd, value)`。值构造（`Value::Bool/Int64/…/EnumNumber`）逐字节不动。alternatives：①`set_field_by_number`——无额外收益，名字解析已付过；②仿 decode 的全列式 converter——encode 天然逐消息出 wire format，无批形态，过度设计。

等价性论证：`set_field` 与 `set_field_by_name` 最终写入同一 `BTreeMap<u32, ValueOrUnknown>`，编码输出按 field number 序列化，字节级一致；`typed_column` 的类型校验已在 build 期保证 value/field-kind 匹配，`set_field` 的 panic 路径不可达（decode 侧同论证已验证过一轮）。

### D2 Kafka output：删 to_vec + std Mutex + 并发观察回执

- `to_vec`（`output/kafka.rs:283,575`）：`arkflow_core::Bytes = Vec<u8>`（`arkflow-core/src/lib.rs:131`），codec 分支已是 owned，`.map(|p| p.to_vec())` 纯冗余，直接删。no-codec 分支经 `apply_codec_encode` 的 eager copy 保留（借用批生命周期跨 await，改动不划算）。
- `send_futures: Arc<Mutex<Vec<DeliveryFuture>>>`（tokio）：push 路径 lock 段内无 await 点（330-334），改 `std::sync::Mutex`，`lock().await` → `lock()`，消灭每条消息的异步互斥开销。flush/close 先 `drain`（sync）再 await，不持锁跨 await。
- flush 串行 `future.await`（218-231, 381）：改 `join_all` 后按原序检查结果——首错上报语义与现在"遇错即停"的差异：现在是 fail-fast 短路（后续 future 已在 librdkafka 后台继续投递，只是不被观察）；join_all 观察全部回执后报首个错误，投递行为本身不变，错误归因更完整。ack/flush 成功语义不变。

### D3 stdout：BufWriter 包装，保留每批显式 flush

现状 `writeln!(io::Stdout)` 经 LineWriter 每行触发一次 write syscall（flush 本身是每批一次，69-87）。

**决策**：writer 类型改 `BufWriter<Box<dyn Write + Send>>`（builder 处包装），显式 flush 保留在 write() 末尾——批间可见性语义不变，批内 1 次落盘。`from_utf8_lossy` 保留（有效 UTF-8 时是 `Cow::Borrowed` 零分配，非 UTF-8 payload 是 bytes codec 的合法输入）。MockWriter 测试改泛型/装箱即可，断言不变。alternative：逐 payload 原始字节写 + 手工拼 `\n`——省一次格式化检查但破坏 `write!`/`writeln!` 的可读性，收益微小，不取。

### D4 generate input：模板批构造一次 + take 复制

现状（`input/generate.rs:82-93`）：每条 clone 模板 String，codec 对 batch_size 条**完全相同**的 payload 重复解码 batch_size 次；benchmark 套件 linear-sql/groupby-sql/filter-project-sql 的前端正是这条路径（`benchmark.rs:72-105`），量出的前端吞吐失真。

**决策**：read() 内构造 1 行模板批（payload=模板 bytes，走既有 codec/无 codec 两分支解码一次），用 `arrow::compute::take` 以 `[0; batch_size]` 索引扩到 batch_size 行。等价性：行值全同 ⇒ 推断 union/类型宽化结果与逐条解码一致；行元数据列同批内常量，复制后与逐条一致。count/interval/batch_size 语义不动（复制发生在 read() 返回前）。alternatives：①只预计算模板 bytes 不复制批——省 clone 但留 N-1 次解码，benchmark 失真仍在；②`interleave`/`concat` N 个单行批——多份中间批，不如 take 一次 gather。

风险：take 对全部列物化拷贝（与逐条解码的物化同量级），收益 = 消灭 N-1 次解析；需一个等价测试：codec 开/关两分支下，复制路径输出与逐条路径逐列逐行相等。

### D5 memory buffer：增量行数计数

`write` 持写锁 O(n) 求和（`buffer/memory.rs:171-176`）。**决策**：守卫同一把写锁的普通 `usize` 字段，在 push_front（+len）、drain/flush 释放（−len）、clear（=0）、merge 失败回补（按实际入队）全部突变点同步维护；容量判断 O(1)。不加锁外原子计数（多一个一致性面，且所有突变点本就在锁内）。任务里须逐一枚举突变点并加"计数=实际求和"的 debug 断言测试。

### D6 partitioned 路由：单趟分组 + 每非空子任务一次 take

现状（`task.rs:3093-3138`）：哈希每批一次（保留），但每子任务建全批布尔掩码 + `filter_record_batch` 全批扫拷贝 → O(subtasks×rows)。

**决策**：单趟把行号按 `hash → key_group → 所属子任务`（ranges 映射预先建一次）分桶到 `Vec<Vec<u32>>`，空子任务跳过，非空子任务各做一次 `take_record_batch`。顺序等价：分桶按行序 append，take 按升序索引 gather = filter 的原序保留。null 键、空 key_field 整批克隆到子任务 0（3103-3107）、key_group_range 跨节点归属——全部既有 routing_tests（6 个）不改通过为门槛。alternative：一次 `filter` 用字典化掩码——不成立，多目标必须多份输出。

### D7 fanout_ack：分支标志打包进共享状态

现状 `input/mod.rs:96-142`：每分支 `Arc<AtomicBool>`×2 + `Arc<FanoutAckPart>` = 3n 分配（+state/Vec ≈3n+3）。

**决策**：`FanoutAckState` 内嵌 `Box<[BranchSlot]>`（每槽 acknowledged/held 两个 `AtomicBool`，一次分配承载全部分支），子 ack = `Arc<FanoutAckChild>{ state, index }`（每分支 1 个 Arc，`Arc<dyn Ack>` 胖指针所需，不可省），总分配 ≈n+3。ack/hold/abort/重试逻辑改为按 index 读槽位，完成判定（pending 计数 + 全分支标志检查）语义逐字节不变；`branches<=1` 直返 parent 的短路保留。现有 fanout 测试（`input/mod.rs:557-614`、`task.rs:4815`）不改通过。

### D8 TrackingAck：std Mutex + guard 外 await

现状（`commit.rs:416-447`）：每源批 `tokio::sync::Mutex<()>`（task.rs:839-840 每批构造）；它存在的唯一原因是 `inner.ack().await` 在 guard 内。

**决策**：改 `std::sync::Mutex`；ack 路径拆两段——guard 内检查/置位（用既有 completed/aborted/held 原子 + 必要的"acking 中"标记保证并发 ack/undo/abort 恰好一个生效），clone 出 `Arc<dyn Ack>` 后 drop guard，再 await `inner.ack()`。undo/abort 路径全在 guard 内完成（无 await）。tracker 计数与 `Delivery settlement SHALL NOT block the source loop` 等既有语义不变。新增一个并发交错测试（join! ack vs abort，断言内层 Ack 恰一次终态调用）。alternative：无锁状态机（AtomicUsize 编码四态）——状态转换复杂度不值一个每批一次的 Mutex 分配/竞争，不取。

### D9 窗口算子：Arc<str> 键 + entry 单趟 + touched-only 快照

- **键类型**：`extract_keys` 仍每行物化一次（从数组取值不可避免），改 `Arc<str>::from` 单次分配；buffers/touched/legacy 的 `(i64, String)` 键全改 `(i64, Arc<str>)`——BTreeMap 序 = str 字典序不变，`state_key` 字节布局（`window_start.to_be_bytes() ++ key.as_bytes()`，operator.rs:411-415）不变 ⇒ 持久化状态向后兼容。收益点在 accumulate：每 membership 3 次 `key.clone()` 从堆分配变引用计数 bump。
- **entry 单趟**：865/873/892/895/899 的 contains_key+entry 双查改单次 `entry()`（Vacant 即首次触达，顺带记 touched）；873 行 late-update 路径保留条件检查。session 桥接路径（792-829）同形处理。
- **touched-only 快照**：`runtime_snapshot()` 全量克隆（94-101，process_internal 每批调用于 1508；用途 = 错误回滚镜像 + fired-ack 补偿的 before/after，**不是 checkpoint**）。**决策**：before-image 抓取**按操作各自独立**：① accumulate 在首次触碰某 `(window,key)` 槽位时抓取该槽 before-image（AggregateBuffer 为扁平小结构，clone 廉价；vacant 槽记 Absent），错误回滚（`restore_runtime`）按该镜像集施加；② **fire 路径（on_watermark/on_tick/finish 及 process_internal 内 watermark 触发的 fire）独立抓取**——在 drain/缩减/删除每个槽位前抓其 before-image，**不依赖本次 accumulate 的镜像集**：被 fire 的窗口可能由更早批次填充、本批从未触碰（watermark 触发正是常态），其镜像在 accumulate 集里不存在。fired-ack 补偿的 `WindowRollback`（firing.rs:193+）携带 fire 侧自身抓取的 before/after。两类镜像施加结果与全量快照路径逐字节等价（未被该操作触碰的槽位本就未被其修改）。window/tests.rs 的回滚/恢复场景为安全网；新增场景：失败 accumulate 后未触碰窗口不受影响、fired-ack 补偿只回退 fire 改动的窗口（含本批 accumulate 未触碰的窗口）。
  alternatives：①COW/persistent map——依赖面与复杂度不值；②保留全量快照但延迟到出错才取——不可能，回滚需要变更前镜像。

### D10 event_time_gate：SmallVec 行内决策 + distinct 分区身份

- **classify_row**（968-1160）：`RowDecision` 的 4-6 个 `Vec` 改 `SmallVec<[T; 4]>`（窗口/闭窗/动作集合的常见基数 1-3，栈上内联，零堆分配）。**依赖取舍**：新增 `smallvec = "1"` 到 `[workspace.dependencies]`——arrow 生态内既有传递依赖，升级为直接依赖属常规做法，集中管理规则允许。决策结果与顺序逐字节不变。
- **split_by_physical_partition_for_source**（1331-1407）：每行 `to_owned` + 每行 `format!("__arkflow_source__:{id}")`（event_time.rs:46-58）改为：单趟扫描，`HashMap<&str, usize>` 以批内借用作键，每 distinct (topic, source) 仅构造一次 owned 身份与分区对象；行号分桶后每分区一次 take（替代每分区全批掩码+filter，顺带复用 D6 的 gather 形态）。输出分组顺序保持首现序。gate 既有测试 + task.rs gate-dispatch 测试不改通过。

## Risks / Trade-offs

- [内核四件（D6/D7/D8/D9/D10）并发正确性] → 既有测试面不改通过为硬门槛；D7/D8 各加并发交错测试；任一项测试改不动即回退该项（各项独立 commit，可单独 revert）。
- [D9 touched-only 快照的等价性] → 回滚/恢复语义由 ≈4.6k 行 window 测试覆盖；fire 侧独立抓取镜像（不与 accumulate 镜像集混用）消除"fire 了本批未触碰窗口却无镜像"缺口；新增"未触碰窗口不受失败影响"场景；AggregateBuffer 含 legacy 序列化批时 before-image 仍整槽克隆（该路径本就重，不在本 change 放大）。
- [D4 take 复制对宽表的物化成本] → 与逐条解码的物化同量级，净收益为解析消除；等价测试双分支（codec 开/关）覆盖。
- [D2 join_all 改变错误观察时点] → 投递行为不变（librdkafka 后台），仅首错归因更完整；flush 返回 Result 语义不变。
- [smallvec 新直接依赖] → 传递依赖升级为直接声明，无版本冲突面（arrow 已携带）；若评审否决，D10 退化为 scratch-buffer 复用重构（决策备胎，工作量 +1 天）。
- [Arc<str> 波及 window/ 内签名面] → 纯类型替换，编译器兜底；持久化字节布局不变，无迁移。

## Migration Plan

单项一 commit，顺序：插件侧低风险先落（D3→D1→D5→D2→D4），内核侧后落（D6→D7→D8→D10→D9）。每项落点全量相关测试 + clippy；收尾 `cargo test --workspace --all-targets` + release 计时测试采集前后数据。若评审要求拆分，D1-D5（插件 crate）与 D6-D10（内核 crate）可拆两 PR；单项可独立 revert，无部署迁移、无状态迁移。

## Open Questions

（无阻塞项——smallvec 依赖取舍与 D2 错误观察时点已在上文给出演练过的备胎，评审可推翻。）

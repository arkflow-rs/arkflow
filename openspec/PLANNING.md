# ArkFlow 战略规划与方向② Roadmap

> 沉淀于 2026-07-31 的代码库探索，2026-08-27 对齐 v1 分支实际进展，2026-09-12 对齐内核收口与未来方向探索（见第七节）。目的：**避免重复探索**——下次会话读本文件即可恢复全部战略上下文，不必重新调研现状。
> 维护规则：方向或现状发生变化时更新本文档；具体 change 落地后由 OpenSpec `changes/` 与归档后的 `specs/` 承载细节，本文只保留总纲。

---

## 一、现状画像（探索结论，2026-07-31）

版本 `0.5.0`，Rust 1.97，DataFusion 54.1。单二进制、配置式（YAML）、插件化的流处理引擎。

### 1.1 真实强项

| 维度 | 现状 | 证据 |
| --- | --- | --- |
| 插件覆盖 | 14 input / 12 output / 6 processor / 6 buffer / 2 codec | `crates/arkflow-plugin/src/{input,output,processor,buffer}/` |
| 列式数据模型 | Apache Arrow `RecordBatch`（`MessageBatch`），区别于 Vector/Benthos 的行式 JSON | `crates/arkflow-core/src/message_batch_tests.rs`、CLAUDE.md「Data Model」 |
| SQL 处理 | DataFusion 54，支持聚合/窗口/Join/UDF/临时表 | `crates/arkflow-plugin/src/processor/sql.rs`、`sql/` |
| Input 级 WAL | at-least-once、ack-gated cursor、crash recovery、S3 后端、节点隔离、segment reclaim、checksum 容错 | `openspec/specs/input-durability/`、`s3-wal-pipeline/`、`wal-manifest-write-coordination/`；`crates/arkflow-plugin/src/wal/` |
| 脚本能力 | Python UDF（PyO3）+ VRL 双脚本 | `processor/python.rs`、`processor/vrl.rs` |
| 外部认可 | CNCF Landscape 入选；`components list/show/schema` 命令支撑 IDE 自动补全 | README.md:28；commit `5c398b1` |

### 1.2 真实缺口（机会所在）

1. **AI/ML 完全空缺**。README 主打「无缝集成 AI 能力、加载执行机器学习模型、推理、异常检测、复杂事件处理」（`README.md:16-19`、`README_zh.md:16-17,31`），但：
   - `Cargo.toml` 无任何 ML 依赖（candle/ort/tract/tch 全无）
   - `git log` 全量搜索 ai/inference/onnx/tensor/anomaly **零命中**（历史从未实现）
   - processor 仅 6 种：`batch/json/protobuf/python/sql/vrl`，无原生 AI processor
   - → **宣传与实现脱节最严重处 = 最大差异化机会**
2. ~~**单节点、无分布式**~~ → 已突破：v1 分支落地分布式 Job 运行时（Hub–Agent 多 Compute Node，见第四节）。YAML Stream 仍为单节点形态。
3. ~~**无有状态计算**~~ → 已突破（v1）：keyed state、checkpoint/savepoint、事件时间/watermark 已落地 `arkflow-core/src/{state,checkpoint,event_time,job,job_runner}.rs`；端到端 exactly-once（Kafka L2 事务）已合入 main。
4. ~~**无 CDC**~~ → 已闭环：`debezium_json` codec（MySQL/PostgreSQL/MongoDB/SQLServer），2026-07-31 归档。
5. **可运维性弱**。有 `prometheus` 依赖但缺完整 metrics 导出方案；无 trace；无动态配置/管理面。
6. **processor 工具箱偏薄**。缺数据处理常用的 filter/mask/encrypt/http-lookup/schema-registry 等。
7. **文档落后于实现**。README 的 input/output 清单不全（漏 memory/multiple_inputs/pulsar、redis/sql/influxdb/pulsar output 等）。

### 1.3 生态位（2026-07-31 调研结论）

**重型有状态流——不要正面竞争**：
- **RisingWave**：92% Rust，分布式流式数据库，Snowflake 式存算分离（compute/storage 解耦，frontend/compute/meta/compactor 四类节点），状态作为数据库一等对象存对象存储；2026 路线图转向「**agentic AI** event streaming」。SQL + 物化视图为核心。
- **Arroyo**：85% Rust，**同样基于 Apache Arrow**（与 ArkFlow 同栈），分布式有状态，exactly-once、ms 级延迟，公开 benchmark 吞吐约 RisingWave 3-5x；**2025-04 被 Cloudflare 收购**，开源项目方向生变，仍 v0.15 未到 1.0。
- → 两者核心护城河正是「分布式 + 有状态 + EOS」。ArkFlow 投入分布式有状态 = 在强敌主场追赶，**必输**。

**轻量数据管道——ArkFlow 的真正赛场**：
- **Benthos / Bento**（现由 WarpStream Labs 维护）：配置式、声明式、插件丰富，但**官方定位 stateless**——无原生 exactly-once、无任意状态 checkpoint/snapshot，依赖上游 broker 的事务；CDC（MySQL/PG/Mongo）是近期才加、尚不成熟（端到端 EOS / schema 变更 / 删除处理有局限，见 bento#396）。
- **Vector**：Rust，偏可观测性数据路由，行式，无复杂 SQL/状态。

**关键差异化结论**：ArkFlow 与 Arroyo 的差异**不在数据模型**（都是 Arrow），而在「**单节点轻量 vs 分布式有状态**」。ArkFlow 的最佳生态位是 **「Benthos 的形态 + 生产级可靠性 + 列式 SQL」**：
- 对 Benthos 形成**跨代差异**：ArkFlow 已有 input WAL（at-least-once + S3），Benthos 是 stateless；方向②（EOS/状态/CDC/schema）正是踩在 Benthos 公开软肋上。
- 对 RisingWave/Arroyo 保持**轻量错位**：单二进制、配置式、不搞分布式，承接「比 Benthos 可靠、比 Flink/RisingWave 轻」的中等规模实时数据集成 / ETL 场景。

**外部认知信号**：HN / Medium 把 ArkFlow 定位为「实验性」新引擎（2025 初发布）——方向②正是摘掉「实验性」标签、走向生产可用的关键。issue #284（用户问「vs Arroyo」）也印证社区在拿它与重型引擎对标，需用「轻量 + 可靠」明确错位，而非硬比分布式。

> 来源：[RisingWave 2026 landscape](https://risingwave.com/blog/streaming-database-landscape-2026-complete-guide/)、[RisingWave 架构](https://docs.risingwave.com/get-started/architecture)、[RisingWave vs Arroyo](https://risingwave.com/blog/risingwave-vs-arroyo-rust-stream-processors/)、[Arroyo GitHub](https://github.com/ArroyoSystems/arroyo)、[Bento 文档（stateless）](https://warpstreamlabs.github.io/bento/)、[WarpStream 整合 Bento](https://www.warpstream.com/blog/fancy-stream-processing-made-even-more-operationally-mundane)、[Bento CDC 讨论 #396](https://github.com/warpstreamlabs/bento/discussions/396)

---

## 二、推荐方向清单（编号即后续 roadmap 引用）

| 编号 | 方向 | 差异化 | 可行性 | 与现状关系 |
| --- | --- | --- | --- | --- |
| **①** | **智能流处理（AI/ML）** —— 推理 processor、异常检测、向量入库、流式 embedding/RAG | ★★★（兑现 README，但 RisingWave 已转 AI、蓝海收窄） | 中（需选推理后端） | 全新战线 |
| **②** | **生产级端到端可靠性** —— CDC、schema registry、EOS、状态 checkpoint | ★★★（企业刚需，踩 Benthos 软肋） | 高（延续 WAL） | **直接延续过去一个月的全部投入** |
| ③ | 可运维性（metrics/traces 导出、管理 API、动态配置） | ★★（同质化但必备） | 高 | 补当前短板 |
| ④ | 开发者生态（WASM/外部 processor、流编排 DAG、更多 connector） | ★★（降扩展门槛，issue #88 WASM） | 高 | 长期生态 |

> **本文档聚焦方向②**（用户 2026-07-31 指示）。①③④ 的 roadmap 待后续展开。

---

## 三、方向② Roadmap：生产级端到端可靠性

### 3.1 总目标

把现有的「input 级、at-least-once、单节点 WAL」升级为「**端到端、可恢复、可对接企业数据源与数据契约**」的生产级可靠性，分 4 个可独立交付的 OpenSpec change 递进完成。

**战略对标**：这不是追赶 RisingWave/Arroyo（分布式有状态），而是**踩在 Benthos（stateless）的公开软肋上**——把「轻量配置式数据管道」做到生产级可靠，形成对 Benthos 的跨代差异、对重型引擎的轻量错位。边界：保持单节点，不引入分布式。

### 3.2 路线图与依赖（价值优先序，2026-07-31 用户确认）

```
Change 1  CDC Input (Debezium / MySQL binlog / PG WAL)     独立，复用 WAL source-commit
Change 2  Schema Registry 与 schema 演进治理                 独立，与 protobuf-codec 协同
   ‖   ← 1、2 独立可并行，先交付最直接的企业集成价值
Change 3  端到端 Exactly-Once（聚焦 output 幂等适配）         独立于 input 类型
Change 4  有状态 Processor 的 checkpoint 与恢复              依赖 Change 3 的 ack 链路；最重，最后
```

交付顺序：**1 → 2 → 3 → 4**（1、2 可并行）。理由：CDC + schema 是「可对接企业数据源 / 数据契约」最直接的价值且彼此独立，先交付；EOS 居中深化可靠性；状态 checkpoint 最重、最接近 RisingWave/Arroyo 领域，推到最后。每个 change 走 OpenSpec：`propose → apply → verify → archive`（delta 合并入 `openspec/specs/`）。

---

### Change 1 — CDC Input（Debezium / MySQL binlog / PostgreSQL WAL）

**Why**
无 CDC 是企业数据集成硬缺口（社区 issue #430/#274 侧面反映）；binlog/WAL 位点天然适配 input-WAL 的 source-commit 机制（位点 = commit point）。Benthos 的 CDC 新加且不成熟（端到端 EOS / schema 变更 / 删除处理有局限，见 bento#396），ArkFlow 借 WAL 可后发做得更稳。

**What Changes**
1. 新增 CDC input（先支持 Debezium 协议 JSON 或 MySQL binlog 直连其一）。
2. **位点管理**：把 binlog/WAL 位点作为 source-side commit，与 `input-durability` 的 ack-gated commit 复用。
3. 配套 example 与文档。

**Capabilities**
- 新增：`cdc-ingestion`
- 修改：`input-durability`（CDC 位点作为 source commit）

**Impact（初判，propose 时核实）**
- 新增 `crates/arkflow-plugin/src/input/cdc.rs`（或 `debezium.rs`）
- 注册于 `input/mod.rs`

---

### Change 2 — Schema Registry 与 Schema 演进治理

**Why**
`protobuf-codec` 目前单文件 schema，无多版本/兼容性治理；CDC + EOS 场景下数据契约稳定是刚需（CDC 的 schema 变更是已知难题）。社区 issue 也反映对 schema 能力的关注。

**What Changes**
1. 对接 Schema Registry（Confluent / Apicurio），按 schema id 解码。
2. **兼容性检查**（向后/向前兼容策略）。
3. 与 protobuf-codec、CDC 数据契约协同。

**Capabilities**
- 新增：`schema-registry-integration`
- 修改：`protobuf-codec`（支持 registry 解析）

**Impact（初判）**
- `crates/arkflow-plugin/src/processor/protobuf.rs`、`codec/`
- 新增 registry client（`reqwest`，已在依赖）

---

### Change 3 — 端到端 Exactly-Once（聚焦 Output 幂等适配）

**Why**
当前 `input-durability` spec 明确为 at-least-once：crash 后重放，output 可能收到重复消息（见 spec「Duplicate delivery after recovery」）。对 Kafka/JDBC 等支持事务或幂等的 sink，重复会导致错误结果，阻碍企业生产落地。难点在 **output 端的幂等适配**（每个 sink 不同），不在 ack 链路改造。

**What Changes**
1. 为 output 引入**幂等 / 事务写入**：按 sink 适配——Kafka 事务（事务 id 由 WAL seq 派生）、JDBC upsert（dedup key 列）等。
2. **复用现有 ack-gated cursor**（不新造两阶段 ack / epoch 机制，避免过度设计）：output 写成功 → cursor 前进 → source commit，重复时靠 sink 幂等吸收。
3. recovery 时，已 commit 序列的重复写入由 sink 幂等兜底。

**Capabilities**
- 新增：`end-to-end-exactly-once`
- 修改：`message-acknowledgment`（ack 携带写入 epoch 供 sink 去重，最小扩展）、`input-durability`（replay 与 sink 幂等协同）

**Impact（初判）**
- `crates/arkflow-plugin/src/output/{kafka,sql}.rs`（事务 / upsert 适配，主要工作量在此）
- `crates/arkflow-core/src/output/mod.rs`（幂等契约 trait）
- `crates/arkflow-core/src/wal/`（cursor seq 暴露给 sink 事务 id）

---

### Change 4 — 有状态 Processor 的 Checkpoint 与恢复（最重，最后）

**Why**
现状仅 window 内存状态，processor 无持久状态；crash 后窗口中间结果丢失，只能从 input WAL 重放原始流重新计算，代价高且对非幂等算子不可行。**注意**：此项最接近 RisingWave/Arroyo 的有状态计算领域，须严守「单节点、不引入分布式」边界。

**What Changes**
1. 可选 **state backend** 抽象（内存 + 嵌入式 `redb`，已存在于依赖；远期 RocksDB）。
2. **周期性 checkpoint**：processor 快照（state + 对应 WAL seq），与 input-WAL cursor 对齐。
3. **恢复**：startup 先恢复 processor 状态到 checkpoint，再从对应 cursor 重放（而非从零重算）。

**Capabilities**
- 新增：`processor-state-checkpoint`
- 修改：`input-durability`（checkpoint 与 WAL cursor 协调点）、window buffer specs（状态化）

**Impact（初判）**
- 新增 `crates/arkflow-core/src/state/` 或扩展 `temporary/`
- `crates/arkflow-core/src/processor/mod.rs`（stateful trait）、`stream/mod.rs`（checkpoint 协调）
- 依赖 Change 3 的 ack 链路

---

## 四、进度与状态（2026-08-27 对齐 v1 实际进展）

### 方向② 推进进度

| Change | 状态 | OpenSpec 位置 | 备注 |
| --- | --- | --- | --- |
| **1 CDC** | ✅ 全流程闭环 | `changes/archive/2026-07-31-add-cdc-debezium`；spec `debezium-cdc-parsing` 已合并 `openspec/specs/` | `debezium_json` codec，复用 Kafka input + ack-gated offset |
| **前置 refactor** | ✅ 全流程闭环 | `changes/archive/2026-07-31-refactor-codec-async`；spec `async-codec-contract` 已合并 | Codec trait async 化，**纯重构无行为变更**；为 schema_registry 解锁 reqwest async |
| **2 Schema Registry** | ✅ 全流程闭环 | `changes/archive/2026-07-31-add-schema-registry`；spec `schema-registry-integration` 已合并 | reqwest async + `SchemaResolver` trait；认证 HTTP mock 测试（wiremock） |
| **3 端到端 EOS** | ✅ 全流程闭环 | `changes/archive/2026-08-01-add-end-to-end-exactly-once`；spec `exactly-once-output` 已合并 | Kafka L2 事务 producer（opt-in `exactly_once`+`transactional_id`）；PR #1195 已合入 main |
| **4 状态 checkpoint** | ✅ 以超集形态落地 | `changes/add-distributed-stateful-streaming-runtime`（v1 分支，118/118 全 done） | 见下方「战略边界修订」——**未按原「单节点 Change 4」方案做，而是走了分布式 Job 运行时** |

### 战略边界修订（2026-08-27 记录）

原 Change 4 设想为「单节点 stateful processor + checkpoint」，严守「不引入分布式」。实际落地为 **`add-distributed-stateful-streaming-runtime`**（v1 分支，26 个提交，~10000 行）：

- 新增 `arkflow-core` 模块：`job.rs`（JobSpec/DAG/代际）、`job_runner.rs`（任务分发/locality）、`checkpoint.rs`（分布式屏障/保存/恢复）、`state.rs`（keyed state，embedded_kv）、`event_time.rs`（watermark）、`streaming_sql.rs`。
- 扩展 Hub–Agent 控制面管理 Job/Task/Checkpoint/恢复；Console Job workbench + 可视化 DAG 编排器（`add-visual-job-dag-orchestrator`）。
- 7 个 capability spec：`streaming-job-api`、`distributed-job-runtime`、`keyed-state-backend`、`checkpoint-recovery`、`event-time-processing`、`control-plane-fleet`/`control-plane-reconciliation`（扩展）。
- **突破了 3.1 节「保持单节点」的边界**：任务可分布在多 Compute Node 上。这是有意为之的路线调整（数据平台团队诉求），但与 1.3 节「不在分布式有状态领域与 RisingWave/Arroyo 正面竞争」的原始定位存在张力——定位叙事需要重新审视：ArkFlow 的差异点应表述为「轻量 Hub–Agent 编排 + 列式 Arrow + 声明式 Job」，而非重型流数据库。
- 现有 YAML 本地 Stream 运行时保持兼容，不自动转换为 Job。
- v1 分支尚未合入 main；PR #1219（`fix/distributed-job-review-remediation`，含其中 25 个提交）评审中。

### 已确认决策
- 方向② = 生产级端到端可靠性（对标 Benthos 软肋、延续 WAL 势能；详见 1.3 节）。
- 交付顺序 = 价值优先：**1 CDC → 2 Schema → 3 EOS → 4 状态**。（前三项已全部闭环）
- ~~状态 checkpoint（Change 4）守「单节点、不引入分布式」~~ → 已修订为分布式 Job 运行时（见上节）。
- Codec trait async 化（`refactor-codec-async`）作为 IO 类 codec 的前置，独立 change。
- Change 3 EOS 形态：`Output::write_batch` 默认方法（1 ack = 1 事务单元，默认实现等价逐条）；Kafka L2 事务 producer（opt-in `exactly_once`+`transactional_id`，**显式配置而非 node_id 派生**——output build 拿不到 durability 配置，transactional.id 与 WAL node_id 是不同身份概念）；SQL L1 复用现有 upsert（零代码）。L2 诚实边界：已 commit 跨重启重复靠业务幂等；L3（Kafka→Kafka `send_offsets_to_transaction`）留 future。

### 下一步（2026-09-12 对齐）

1. **合入 v1**：v1 领先 main 51 个提交、main 零领先（单向干净窗口）。评审整改已全部归档，`openspec/changes/` 仅剩 `archive/`。合并 v1 是一切新方向的前置，拖久合并风险增大。
2. **关闭过时 issue**：#901（InfluxDB）、#430 等 issue 对应能力已实现，需核实后关闭并回复贡献者。
3. **方向选择**：方向② 四项全部闭环后，从第七节候选方向中确认下一主线（推荐序：数据面可观测性 → 企业集成安全 → AI 轻量切口），确认后展开 roadmap 并按 OpenSpec 立项。

---

## 六、统一执行内核重建（2026-08-27 启动，进行中）

用户决策（2026-08-27）：**允许破坏性更新，以 Job 运行时为本体重建引擎，对标最先进流处理产品**。OpenSpec change：`rebuild-unified-streaming-engine`（v1 分支）。

### 已落地（29/30 任务，2026-08-28 第三批：执行器删除 + 收尾）

- **executor 内核**（`crates/arkflow-core/src/executor/`）：Envelope 通道、算子链融合、per-chain 事件循环（流水线并行 + 通道反压）、partitioned/broadcast 路由；12 项单测。
- **异步 barrier checkpoint**：barrier 随数据流动、多输入对齐（有界缓冲）、异步快照不停世界、BarrierCoordinator 复用现有 checkpoint.rs 契约。
- **事件时间门控**（`event_time_gate.rs`）：watermark 追踪、行级 Hold/Emit/Route/Update/Drop、held 行 FIFO 释放、延迟 ack、idle 刷新；接入源链事件循环。
- **列式窗口算子**：向量化 tumbling 分配、keyed 聚合状态（持久化/恢复）、watermark/processing-time 双触发、迟到策略。
- **有状态算子接线**（`stateful.rs`）：图构建期 StatefulOperator 包装（keyed counter 入 state backend 命名空间），无 backend 时构建拒绝。
- **StreamConfig 编译器**：确定性编译 + buffer 映射 + error_output 侧边；全量 examples 黄金测试通过。
- **三路执行全走内核**：① YAML `jobs`（Engine 直驱）；② streams（RuntimeManager::start → compile → run_job，真实二进制验证 generate→SQL→stdout）；③ Agent（spawn_kernel_job + KernelJobHandle 命令驱动快照，生命周期测试验证快照期间数据继续流动）。
- **内核指标**（`metrics.rs`）：per-chain 吞吐/平均延迟/在途/错误 + checkpoint 时长/失败 + watermark lag + 迟到计数。
- **性能基线**：generate→json_to_arrow→sql→drop（20 万行，batch=1000）：内核 528ms vs legacy 559ms（快 6%，`kernel_perf_baseline.rs`）。

### 待办（已清零，2026-09-12 核实）

- ~~5.6 双节点 smoke~~：已由自动化测试 `crates/arkflow-server/tests/two_node_job_smoke.rs` 闭环（Hub + 双 Agent、barrier checkpoint、kill/restart 恢复、旧启动代 fencing；2026-09-12 实测通过，随 `3499843` 引入、`b17690d` 加固）。

第三批（2026-08-28）完成：5.4 双执行器删除（-2966 行；迁移 partition-guard/Route-vs-Update 测试到 executor；WAL 重放迁入 WalInput 惰性队列；temporary 表经 StreamJobAdapter::build_resource 进入内核 Resource——修复了内核丢 temporary 的缺口）；4.5 examples 等价回归（inline 管道 + durability_example 含 WAL 重放路径）；2.6 故障注入（注入 snapshot 失败→checkpoint 失败但数据继续；恢复位置 ≤ checkpoint 位点）；3.5 sliding/session 窗口（多窗口隶属分配、会话按 gap 合并扩展，编译器按 legacy 字段名 interval/gap 映射）；5.2 指标接通（run_job_with_metrics → RuntimeMetrics：input/processing/output/error 计数）；6.2 CLAUDE.md 对齐；6.3 tasks 勾选。

### 关键设计决策

- 两套运行时统一方向：**Stream 编译为 JobSpec 走内核**（编译器 + adapter 保留 YAML 零改动），不是反向。
- WAL 是 input 的持久化属性（`StreamJobAdapter::WalInput` 包装），与 Job checkpoint（barrier + 状态快照）互补：checkpoint 优先、WAL 兜底。
- `Resource` 的 `RefCell` 仅构建期使用——图构建同步完成后释放，`run_job` future 因此可 Send。
- Agent 切换内核需要"命令驱动快照"接口（现在是 barrier 注入式），是 5.3 的前置。

---

## 五、Hub 平台路线（2026-08-02 制定，2026-08-27 更新进展）

### 5.1 当前进展（对齐 main @ 2f423ef 与 v1 @ cea1e8e）

Hub 已完成从本地健康接口到单 Hub、多 Compute Node 控制面的转型，且已延伸为分布式 Job 平台：

- `add-control-plane` ✅ 已合入（PR #1200，71/71）：本地 `ControlPlane`、资源 API 和 Console 基础。
- `make-control-plane-hub` ✅ 已合入（PR #1203，33/33）：节点注册、租约、Agent 心跳与报告、节点资源聚合、目标命令派发、配置转发、Hub Console。
- `rebuild-control-plane-system` ✅ 已归档（2026-08-02，37 项收口）：服务边界、资源 API、操作模型和 Console 架构。
- `add-control-plane-reconciliation` ✅ 已合入（PR #1204）：durable reconciliation 和 HTTP contract，SQLite storage actor 持久化（`arkflow-server/src/storage.rs`）。
- `harden-control-plane-fleet` ✅ 已合入（PR #1216）：fleet rollout 操作加固。
- **分布式 Job 运行时**（v1 分支）：Hub–Agent 扩展为管理 Job/Task/Checkpoint/恢复的流计算平台，Console 含可视化 DAG 编排器（详见第四节「战略边界修订」）。

核心职责划分：Hub 管理节点目录、聚合资源和期望操作；Compute Node Agent 管理本地执行、观测状态和命令结果。

### 5.2 剩余生产化缺口（对照 2026-08-02 清单修订；2026-09-13 按 v1 代码逐项核实）

1. ~~Hub 运行历史内存态，重启丢失~~ → ✅ SQLite 持久化已落地（PR #1204）。
2. ~~操作取消/超时/重试/重连 reconciliation 未闭环~~ → ✅ 已落地（#1204、#1216）；Job 级重平衡/恢复编排仍在 v1 演进。
3. 鉴权仍以全局 operator/node token 为主，尚无用户、角色和细粒度权限模型。（未动；RBAC 属阶段 4。注意阶段 3 的配置审批流隐含依赖多身份，建议排在 RBAC 之后避免返工）
4. ~~Agent 命令轮询与结果上报用 URL 查询参数携带 session token~~ → ✅ 已完成（2026-09-13 核实）：`bearer_auth` 走 `Authorization: Bearer`（`agent.rs` `bearer_auth`），Hub 优先读 header，查询参数仅保留为升级窗口兼容。
5. ~~配置缺少版本化发布/回滚~~ → ✅ Console 已有 draft/validate/diff/publish/rollback 工作流（`harden-console-configuration-workflow` 已归档）；批量发布/审批流未做。
6. ~~Console Overview/Runtime/配置编辑未补全~~ → ✅ 已补齐（含组件目录、可视化 Job DAG 编排器）。
7. ~~双节点端到端测试~~ → ✅ 已闭环：`crates/arkflow-server/tests/two_node_job_smoke.rs`（Hub + 双 Agent、barrier checkpoint、kill/restart 恢复、旧启动代 fencing）；Stream 层面亦已覆盖。
8. **新增**：v1 合入 main 前需完成 PR #1219 评审闭环（`repair-v1-review-defects` 已 38/38，待归档）；分布式 Job 的生产化验证（长稳、规模上限、状态后端除 embedded_kv 外的选项）未做。
9. **2026-09-13 新核实的阶段 2 真剩余**（5.5 清单大部分已落地，勿重复立项，已完成项见下）：
   - ~~**Job 操作零审计**~~ → ✅ `add-hub-job-audit-and-command-metrics` 已归档（2026-09-13）：job_start/stop/checkpoint/savepoint 接受与拒绝均落 `cp_audit_events`（接受审计 = 首次派发去重；checkpoint 触发审计在 HTTP handler，周期调度不进审计）；
   - ~~**命令延迟/失败率直方图缺失**~~ → ✅ 同上 change：`arkflow_command_duration_bucket/_count/_sum` + `arkflow_command_total`，固定低基数标签，重启归零；
   - ~~**Job 命令幂等元数据弱于 stream intents**~~ → ✅ 同上 change：`expires_at_ms`（serde 默认字段，零迁移）+ sweep 有界重试（上限常量 3，重试继承），仅覆盖 job_start/stop，工件触发走 command-lease 重放；
   - ~~`cp_audit_events` 无界增长~~（核实中附带发现）→ ✅ 同上 change：30 天 + 100k 双界清理接入周期 sweep；
   - ~~**Session token 会话期静态**~~ → ✅ `harden-agent-session-credentials` 已归档（2026-09-13）：绝对 TTL（默认 1h，`health_check.agent_session_ttl_ms`，无滑动续期）、过期 401 走既有 re-register 循环、`RegisterResponse.session_ttl_ms` 通告、Agent 收口 query 泄漏通道（Bearer-only；Hub 保留 legacy 回退 + deprecation 警告）、重连退避 equal-jitter。混布升级顺序约束（先 Hub 后 Agent）写入 spec 与部署文档；
   - ~~长稳与规模上限验证的阻塞项~~ → ✅ `bound-control-plane-storage-history` 已归档（2026-09-13）：`cp_outbox` 已处理行与终态 `cp_attempts` 接入 24h/4096 双界清扫（未处理行/active 行永不回收）、retention 六件套移出 1s reconcile tick 改独立 60s 维护任务、vestigial `cp_job_observations` 表定义删除。长稳与规模上限验证（item 8 后半）本身仍未做——现为阶段 2 现存唯一剩余项，且所有「表行数收敛」断言已可硬，随时可立项 verification-only change。

### 5.3 推荐交付顺序

```text
阶段 1  Hub 当前版本收口
        E2E Smoke Test、操作重协调、API/脱敏测试、Console Runtime/配置补齐
          ↓
阶段 2  Hub 生产基础
        持久化、幂等操作状态机、认证加固、指标与审计
          ↓
阶段 3  多节点运营
        节点生命周期、标签能力、批量操作、滚动发布、配置版本治理
          ↓
阶段 4  平台化扩展
        RBAC、OIDC、Secret Manager、告警、Webhook、GitOps、Hub 高可用
```

### 5.4 阶段 1：当前版本收口

- 完成一个 Hub + 两个 Compute Node 的端到端 Smoke Test。
- 补齐操作取消、超时、重试、节点离线和重连恢复测试。
- 完成所有资源 API 的集成测试，包括分页、非法过滤器、认证失败、脱敏和兼容健康接口。
- 完成 Console Overview、Runtime 详情以及配置 YAML/JSON 编辑、校验、diff、发布和回滚。
- 验证 Rust workspace、WAL 行为、Console 构建和 Hub 断连时本地数据面不受影响。

### 5.5 阶段 2：Hub 生产基础

> **2026-09-13 状态**：本清单大部分已落地（持久化、操作状态机、幂等键、Authorization Header、审计基础、兼容性信息、记录保留）。真剩余收敛为 5.2-9 的四项，其中「Job 操作审计 + 命令延迟/失败率直方图 + Job 命令幂等元数据」立为一个收尾 change；session token 短期化涉协议演进，单独立项。

建议新增独立 OpenSpec change `harden-hub-production-foundation`，重点包括：

- 使用 SQLite 或其他嵌入式存储持久化节点身份、配置版本、操作记录和审计事件。
- 固化 `Queued → Dispatched → Acknowledged → Running → terminal` 操作状态机。
- 所有命令支持幂等键、过期时间、重试次数和重连 reconciliation。
- 将 Agent session token 改为短期凭证，并通过 Authorization Header 传输。
- 增加节点标签、能力、版本、租约状态和协议兼容性信息。
- 增加 Hub/Agent、命令延迟、失败率、租约和资源聚合的 Prometheus 指标。
- 审计事件默认不记录 token、密码和未脱敏配置。

### 5.6 阶段 3：多节点运营

- 节点注册、禁用、排空、维护、驱逐和删除。
- 按节点标签或能力批量启动、停止、重启 Stream。
- 支持滚动发布、分批发布、失败自动暂停和回滚。
- 配置草稿、schema 校验、diff、审批、发布和版本回滚。
- 节点版本检查、Agent 升级状态和能力不兼容检测。
- 展示节点、Stream、输入、处理器、输出之间的拓扑关系。

### 5.7 阶段 4：平台化扩展

- 多用户、RBAC、团队/租户隔离和 OIDC/OAuth2。
- 外部 Secret Manager、Webhook、Slack/邮件告警和事件订阅。
- GitOps 配置同步和审计追踪。
- OpenTelemetry trace 与跨节点故障诊断。
- 在单 Hub 模型验证稳定后，再评估外部数据库和 Hub 高可用。

### 5.8 设计边界

Hub 的近期目标是成为可靠的单 Hub、多节点运营控制面，而不是立即演化为分布式调度系统。优先保证资源模型、操作一致性、配置治理、可观测性和安全边界，再考虑共识、高可用和跨 Hub 调度。

推荐 OpenSpec 顺序：

1. Hub E2E 收口与验证。
2. Hub 操作一致性与持久化。
3. 配置发布与批量节点运营。
4. RBAC、审计与告警。
5. Hub 高可用与外部存储。

---

## 七、未来方向探索（2026-09-12）

v1 评审整改全部归档、`changes/` 清空后的系统性探索。以下为候选结论，**尚未立项**，方向由用户确认后展开。

### 7.1 现状刷新（相对 2026-08-27）

- 统一内核自 2026-08-28 起是唯一运行时，其后 5 批提交全部为评审整改（kernel / event-time / control-plane / kafka-reconnect），2026-09-11/12 全部归档；5.6 双节点 smoke 已由自动化测试闭环。
- **v1 未合入 main**：`main..v1` 领先 51 提交、`v1..main` 为 0。
- 依赖盘点：`prometheus` 0.13、`redb` 2、`reqwest` 已在 workspace；**无任何 ML 推理库、无 OpenTelemetry、无 WASM 运行时**——①③④ 的基建半成品状态。

### 7.2 分布式模型的真实边界（2026-09-12 核实）

`ExecutionGraphBuilder::build_subgraph`（`crates/arkflow-core/src/executor/graph.rs:309`）注释明确：**assignment 不允许把一条边拆到两个节点，Hub placement 保证算子链共置**。因此当前分布式 = 「轻量 Hub 调度 + 按源分区/子任务水平扩展 + 分布式 barrier checkpoint 容错」：

- 水平扩展路径是源分区切分（如 Kafka 20 分区两节点各消费一半）与独立子任务；
- **没有跨节点 network shuffle**：单算子中间数据不出节点，跨节点数据交换走外部系统（如 Kafka 重分区）；
- 适用：多分区并行消费、独立子任务、多节点 IoT 就近采集；不适用：需要 shuffle 的重型有状态聚合/join——与 1.3 节「避开 RisingWave/Arroyo 主场」的定位自洽。

### 7.3 候选方向与推荐优先序

| 序 | 方向 | 要点 |
| --- | --- | --- |
| 0 | **v1 合入 main + 收口** | 51 提交积压，一切新方向的前置 |
| 1 | **③ 数据面可观测性** | 内核已有 `KernelMetricsSnapshot`、Hub 已有控制面 metrics spec（`control-plane-observability`），缺数据面 Prometheus 导出、单进程 `/ready`（issue #768）、OTel trace；`prometheus` crate 已就位，小投入补齐「生产可用」叙事 |
| 2 | **⑤ 企业集成安全（本次新发现）** | Kafka SASL/SSL **零实现**（issue #902，核实 `input/kafka.rs`/`output/kafka.rs` 无命中）——企业环境接不了带认证的 Kafka，是入场券级缺口；延伸：全组件 TLS 核查、Secret 引用机制（为 Hub 阶段 4 Secret Manager 铺路） |
| 3 | **① AI 轻量切口** | LLM/embedding processor（reqwest 调 OpenAI 兼容 API，列式批量天然友好）+ 向量库 output（qdrant/milvus/pgvector）；2026 行业主流叙事即 streaming + agentic AI（RisingWave 已全面转向，蓝海收窄但仍有时机）；README 宣传与实现的脱节是最大差异化机会 |
| 4 | **① 本地推理 + ④ 生态** | ONNX(ort)/candle 推理 processor（IoT 异常检测与 Modbus 场景契合）、WASM processor（issue #88）、公开 benchmark（issue #87，扩展 `kernel_perf_baseline.rs`） |
| 5 | **Hub 平台阶段 2 穿插** | ~~Authorization Header~~ 已完成(v1);~~session token 短期化/轮换~~ 已完成(`harden-agent-session-credentials`,2026-09-13);~~Job 操作审计~~、~~命令延迟直方图~~ 已完成;剩余长稳/规模验证(5.2-9)与 RBAC/OIDC |

推荐逻辑：0-2 在 1-2 个月内把项目从「内核先进」推进到「能进企业生产」；3-4 打开差异化叙事。方向②打下的 CDC/Schema/EOS 底座恰是方向①「给 AI 供可靠数据」的叙事衔接点——两条线是承接而非切换。

### 7.4 本次探索同步修复的文档缺口

- `docs/docs/configuration/1-top-level.md` 补 `jobs` 字段与 JobSpec 文档（此前零覆盖）；
- `docs/docs/control-plane/http-api-v1.md` 补 Job API 路由（此前零覆盖）；
- `docs/docs/concepts/7-distributed-jobs.md` 显式声明 7.2 节的链共置/无 shuffle 边界。

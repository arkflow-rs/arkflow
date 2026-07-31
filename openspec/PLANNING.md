# ArkFlow 战略规划与方向② Roadmap

> 沉淀于 2026-07-31 的代码库探索。目的：**避免重复探索**——下次会话读本文件即可恢复全部战略上下文，不必重新调研现状。
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
2. **单节点、无分布式**。无 cluster/consensus/raft 依赖；`crates/arkflow-core/test_distributed_wal/` 是 2025-09 建的**空目录**——曾起念但未做。Engine/Stream 各为单文件（`engine/mod.rs`、`stream/mod.rs`）。
3. **无有状态计算**。仅内存 window 状态（`buffer/{session,sliding,tumbling}_window.rs`），无 state backend、无 checkpoint/savepoint、无端到端 exactly-once（当前仅 at-least-once，见 `input-durability` spec「At-least-once delivery」）。
4. **无 CDC**。无 Debezium/binlog/PG WAL input。社区 issue #430（NoSQL output）、#274（S3 & SQL output）侧面反映企业集成诉求。
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

## 四、进度与状态（2026-07-31）

### 方向② 推进进度

| Change | 状态 | OpenSpec 位置 | 备注 |
| --- | --- | --- | --- |
| **1 CDC** | ✅ 全流程闭环 | `changes/archive/2026-07-31-add-cdc-debezium`；spec `debezium-cdc-parsing` 已合并 `openspec/specs/` | `debezium_json` codec，复用 Kafka input + ack-gated offset |
| **前置 refactor** | ✅ 全流程闭环 | `changes/archive/2026-07-31-refactor-codec-async`；spec `async-codec-contract` 已合并 | Codec trait async 化，**纯重构无行为变更**；为 schema_registry 解锁 reqwest async |
| **2 Schema Registry** | ✅ 全流程闭环 | `changes/archive/2026-07-31-add-schema-registry`；spec `schema-registry-integration` 已合并 | reqwest async + `SchemaResolver` trait；认证 HTTP mock 测试（wiremock） |
| **3 端到端 EOS** | ⏳ 待 propose | — | 聚焦 output 幂等适配 |
| **4 状态 checkpoint** | ⏳ 待 propose | — | 最重、推最后；守单节点 |

### 已确认决策
- 方向② = 生产级端到端可靠性（对标 Benthos 软肋、延续 WAL 势能；详见 1.3 节）。
- 交付顺序 = 价值优先：**1 CDC → 2 Schema → 3 EOS → 4 状态**。
- 状态 checkpoint（Change 4）保留完整内容、推最后；守「单节点、不引入分布式」。
- Codec trait async 化（`refactor-codec-async`）作为 IO 类 codec 的前置，独立 change。

### 下一步
propose Change 3（端到端 EOS）→ Change 4（状态 checkpoint）。

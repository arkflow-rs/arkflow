# ArkFlow 产品迭代分析（2026-11-22）

> **文档定位**：本文件是后续产品迭代的输入与问题登记簿，不是行为规格。行为契约以 `openspec/specs/` 为准；本文件登记的问题在立项后由 OpenSpec change 承载，归档后从本文件划掉，并在 CHANGELOG 留痕。
>
> **快照**：`main` @ `c6ff539b`（2026-11-22 20:24）。分析期间另有会话在推进 `complete-connector-retry-adoption`，工作树存在未提交改动；涉及插件层的结论以该快照为准。
>
> **验证边界**：本次实测 `cargo check -p arkflow-core --locked` 通过、`console` 108 个前端测试全绿；未运行全量 Rust 测试、未做分布式压力测试、未运行时复现 PA-P0-01（仅复核代码路径）。**2026-11-22 后续**：PA-P0-01 已深入复核否证并补回归测试，见 3.1。

---

## 0. 结论摘要

ArkFlow 的真实形态已经超出 `CLAUDE.md` 的“三 crate 单机 YAML 引擎”描述：它是 **统一列式执行内核 + 可选 Hub/Agent 控制面 + Web 控制台 + 双语双版本文档站** 的完整平台。

**一句话判断：“硬核软壳”。**

- **核**：内核、持久化、checkpoint/恢复、HA 控制面属一线水准。
- **壳**：插件边界、元数据一致性、构建依赖面、文档新鲜度仍有治理空间。
- **真正的风险不是核心正确性**，而是构建重量（1,021 个锁包、零 feature 开关）、单点维护者、以及“报告/规格比代码慢半拍”的文档债。

**v1.0 主要是稳定性承诺决策，不是代码里程碑。** 核心已够格；建议先完成迭代 1（稳定性与真相）再讨论 v1.0 发布窗口。

### 评分（相对“成熟商业流处理产品”参照系）

| 维度 | 评级 | 主要依据 |
| --- | --- | --- |
| 架构设计 | **A** | 统一执行内核、链融合、三种配置层级共用 JobSpec |
| 内核正确性 | **A-** | 连续 ack、barrier 对齐、state journal 不变量扎实；此前登记的 1 个 EOS 缺陷已于 2026-11-22 复核否证（见 3.1） |
| 可靠性 | **A-** | WAL/S3、checkpoint/savepoint、EOS L2/L3；恢复边界诚实 |
| 控制面 | **A-** | HA 选主、审批、pause/resume、backup/restore 完整；节点身份偏弱 |
| 安全 | **B** | 凭据分层好；共享节点信任、审计 best-effort、无 mTLS |
| 性能 | **B+** | 无状态路径强；状态/窗口/远端有已知成本 |
| 测试 | **A-** | ~2,600 测试 + 多类结构门禁；插件边界与 Docker 依赖 |
| 文档/过程 | **A-** | 104 specs / 208 归档 change / 双语；新鲜度成本高 |
| 构建/DX | **C+** | 1,021 包、88 GB target、零 feature |
| 可维护性 | **B+** | 0 生产 unsafe、clippy/fmt 全绿；57k 行内核、超大文件、单点 |
| 产品聚焦 | **B** | 多线叙事 + 三层配置，认知负担 |

---

## 1. 项目现状画像

| 项 | 实测 |
| --- | --- |
| 版本 | 0.6.0（tag `v0.6.0`） |
| 历史 | 2025-03-01 → 2026-11-22，约 21 个月，1,173 commits |
| 贡献结构 | 420 次人类提交来自同一维护者；748 次 dependabot；其他 5 次 |
| Rust 规模 | 236,432 行（core 87.5k / plugin 66.6k / server 81.2k / bin 1.1k） |
| 测试 | 约 2,600 个 `#[test]/#[tokio::test]`（core 1,035 / plugin 1,008 / server 547 / bin 6） |
| 组件 | 13 input / 14 output / 14 processor / 4 buffer / 4 codec / 1 temporary |
| 规格资产 | 104 份 capability spec、208 个已归档 OpenSpec change、54 个 example |
| 文档 | 当前版中英各 ~145 页；另有 0.2/0.3/0.5/0.6 历史版本文档树 |
| 控制台 | React 18 + Router 7 + TanStack Query 5 + Tailwind 4 + xyflow，12.5k 行 TS/TSX |
| CI | 11 个 workflow：fmt、clippy、nextest、coverage、console、docs、helm、docker、release、benchmark |
| 依赖面 | `Cargo.lock` 1,021 包；二进制 810 个 unique normal deps；插件 76 个直接依赖 |
| 构建产物 | 本机 `target/` 88 GB，debug 二进制 748 MB，**全 workspace 零 `[features]`** |

**部署形态**：单机 engine（YAML `streams`）、单机 Job（YAML `jobs`）、Hub + 多 Agent 分布式 Job（Hub API / Console）。三者共用同一内核。

---

## 2. 架构与强项（迭代时应保护的资产）

```text
streams: (YAML) ─┐
                 ├─ compile_stream / JobSpec ─► ExecutionGraphBuilder ─► chains（有界 flume 1024）
jobs:    (YAML) ─┘                                      │
                                                        ├─ source chain：Input + WAL + event-time gate
                                                        ├─ fused processor chains：Arrow / DataFusion / VRL / Python
                                                        ├─ window / join / stateful：StateBackend + StateJournal
                                                        └─ sink chain：Output.write_batch → Ack → CommitFrontier
                                                                 │
                                   barrier checkpoint（Aligner/Coordinator）─► 状态快照 + 源位点
                                                                 │
                                   split placement ─► remote shuffle（HMAC v2、key-group、receipt）
                                                                 │
Hub（SQLite/Postgres，lease+epoch）◄── heartbeat/report/commands ──► Agent（kernel）
        ▲
   Console（React）/ HTTP API / OIDC / RBAC / 审计 / 限流
```

必须继续保护的强项：

1. **不变量写在代码与测试里**：连续 ack 前沿（[commit.rs](crates/arkflow-core/src/executor/commit.rs)）、`Ack` 的 undo/abort/held 语义（[input/mod.rs](crates/arkflow-core/src/input/mod.rs)）、state journal 版本栅栏、barrier 对齐与 held-ack。
2. **可靠性边界诚实**：at-least-once 为默认，exactly-once 明确只覆盖事务 sink，并写清残留重复窗口（[delivery-semantics.md](docs/docs/build/delivery-semantics.md)、[exactly-once.md](docs/docs/build/exactly-once.md)）。
3. **控制面完整度高**：Postgres 租约选主 + epoch 写栅栏、Job pause/resume、原子升级、rollout + 审批、RBAC/OIDC、限流、webhook、backup/restore/migrate。
4. **可观测性贯通**：per-chain 指标、checkpoint 时长/失败、watermark lag、迟到计数、OTel 跨节点 trace、WAL flush 失败计数。
5. **结构性门禁**：`connector_error_contract`（错误变体扫描）、`registry_consistency`（builder/元数据同名）、`dependency_lock_hygiene`（arrow/datafusion/zstd 单版本）、docs inventory / YAML 片段深校验。
6. **自评审闭环**：2026-11-15 评审登记的 P0/P1 已有多条在当前代码中修复（见 §5“已修复，勿重复”）。

---

## 3. 问题登记簿

> 状态标记：`已复核` = 本次直接读代码确认；`代理发现` = 深潜代理报告且关键路径已抽查；`待运行复现` = 仅有代码路径证据。每项给出建议 OpenSpec change 名与可验证的验收标准。

### 3.1 P0（立即处理）—— 已清空：唯一登记项已否证（2026-11-22）

#### PA-P0-01 多输入链 EOS 忙循环 · 已否证（2026-11-22 深入复核 + 回归测试）

**结论**：登记的"反复重注册已关闭 reader + buffered 无界增长"机制在当前代码中不存在，不构成缺陷，原 `fix-multi-input-eos-spin` 提案取消。

**否证依据**：
- `InputChannelClosed` 分支消费该通道的 reader 后经 `continue`（[task.rs:1574](crates/arkflow-core/src/executor/task.rs:1574)）直接回到循环顶部，**跳过**了循环尾部的重注册（[task.rs:1665](crates/arkflow-core/src/executor/task.rs:1665)）——原登记误以为关闭分支会落到尾部重注册（原引用行号 1534/1663 处代码片段描述属实，但控制流组装错误）。
- 尾部重注册仅在 `Ok(envelope)` 路径可达；已关闭的通道永远不会再产生 `Ok` 事件。故每通道关闭事件 ≤ 1 次，reader 只减不增，循环必然收敛（reader 耗尽 → 干净 `finish_chain` 退出）。
- `buffered` 中 EOS 每输入至多 2 条（显式 `Envelope::Eos` + 关闭合成的隐式 EOS），有界；数据/水位缓冲已有 `max_buffered` 上界与显式轮次失败路径。

**语义本身正确**：EOS 是隐式 barrier，补齐对齐、`active_inputs` 剔除已结束输入，`Aligner` 行为与 checkpoint 语义一致。

**实证**：回归测试 `channel_close_during_in_flight_barrier_stays_idle_and_settles`（[task.rs](crates/arkflow-core/src/executor/task.rs) `task_loop_tests`，3 输入 + barrier 在飞 + 一通道关闭 + 幸存输入补齐对齐）锁定该语义。

**残留低危观察（降级 P2，不立项）**：
- `Aligner::observe` EOS 分支非幂等（重复 EOS 重复入 `buffered`）且无 `max_buffered` 检查（[barrier.rs:114](crates/arkflow-core/src/executor/barrier.rs:114)）；实践中每输入 ≤2，无 OOM 风险。
- `stale duplicate barrier` 拒绝路径不设 `failed_barrier`（[barrier.rs:148](crates/arkflow-core/src/executor/barrier.rs:148)）→ 该轮失败经 `failure_reporter` 上报为 `(None, error)`、不可归因；仅协议违规（重发已完成 checkpoint id）时触发。

**动作**：无代码动作；迭代 1 由 PA-P1-02 领头。

---

### 3.2 P1（下一迭代 / 多节点生产前必须）

#### PA-P1-01 构建依赖无裁剪（最大 DX 风险） · 已复核

**现象**：全 workspace 无 `[features]`；插件 crate 无条件依赖 DataFusion、`datafusion-table-providers`（duckdb/mysql/postgres/sqlite）、Ballista、`spiceai_duckdb_fork`、vendored rdkafka/SASL、PyO3、Pulsar、HDFS。`Cargo.lock` 1,021 包，debug 二进制 748 MB，target 88 GB。

**影响**：冷构建时间长、镜像体积大、CVE 面大、无法交付“最小发行版”，直接抬高贡献者与 CI 成本。

**动作（建议分两步）**：
1. 先加 feature 层，不改默认行为：`connectors-file`（duckdb/ballista）、`connectors-kafka`、`connectors-python`、`connectors-mongodb`、`connectors-hdfs`、`ai-vector`；默认全开以保持兼容，另提供 `engine-minimal`。
2. 下一主版本再把默认切到最小集（写 CHANGELOG 与升级指南）。

**验收**：
- CI 增加 feature 矩阵：`engine-minimal`、`engine-minimal+connectors-kafka`、全量，三者可编译并通过对应测试。
- 记录并公布冷构建时间、default/minimal 二进制体积基线；minimal 相比全量下降 ≥40%（先测基线再定目标）。
- `Cargo.toml` 的 `[features]` 有注释说明每个 feature 覆盖的依赖与组件。

**建议 change**：`add-cargo-feature-gating`。

#### PA-P1-02 文档真相：CLAUDE.md 与插件指南已过期 · 已复核

**现象**：
- `CLAUDE.md` 仍写 Rust 1.88（实际 1.97）、“three crates”（实际四个）、10 个 output（实际 14）、6 个 processor（实际 14）、join buffer 作为 buffer（实际被编译器拒绝）。
- 插件指南示例写 `Option<&String>`（实际 `Option<&str>`）、`crates/arkflow-plugin/src/mod.rs`（实际 `lib.rs`）——两处均无编译校验。
- Helm `appVersion: 0.5.0` 落后 workspace 0.6.0（[Chart.yaml:8](deploy/charts/arkflow/Chart.yaml:8)）。

**影响**：`CLAUDE.md` 是交给 AI 协作代理的权威指令，错误信息会直接误导后续开发；文档站示例漂移损害可信度。

**动作**：
1. 从 `AGENTS.md` + 当前代码重新生成 `CLAUDE.md`（crate 列表、版本、组件计数、命令、架构边界）。
2. 把插件指南的 Rust 片段变成可编译 doctest（或 `include!` 一个测试文件），并修正路径。
3. release workflow 增加 Helm `appVersion` 与 workspace 版本一致性检查。

**验收**：
- `cargo test --doc -p arkflow-core`（或新增 doc 片段检查测试）覆盖插件指南中的示例。
- CI 有一条“CLAUDE.md 与 registry/crate 布局一致”的检查，或至少一条生成脚本 + 失败提示。
- Helm lint/render 在版本不一致时失败。

**建议 change**：`fix-docs-truth-2026-11`。

#### PA-P1-03 组件元数据与 serde 字段漂移 · ✅ 已实施并归档（`add-component-metadata-conformance`，2026-11-23）

**现象**：
- Kafka input 的 `transactional_offsets`（L3 核心开关）不在自身 metadata schema 中，而 schema 是 `additionalProperties: false`，IDE 会拒绝合法字段（[kafka.rs:1539](crates/arkflow-plugin/src/input/kafka.rs:1539)）。
- Redis input 元数据声称“stream consumer groups”，实现只有 list/pubsub（[redis.rs:697](crates/arkflow-plugin/src/input/redis.rs:697)）。
- 在飞的 `complete-connector-retry-adoption` 只补了 `vector_search`/`milvus_search` 的 `retry_count`，未解决系统性漂移。

**影响**：IDE/schema 与真实配置面不一致；用户按文档配置会失败或按错误认知使用组件。

**动作**：
- 增加“serde 配置字段 ⊆ metadata schema”的 conformance 测试（可用 `schemars` 派生 schema，或维护显式字段清单快照）。
- 修正 Redis 描述；重生成 inventory / config-schema 快照。
- 把 `transactional_offsets` 等缺失字段补进 schema 与示例。

**验收**：
- 新增测试在“struct 有而 schema 无”时失败（含 allowlist 机制，允许刻意隐藏的内部字段）。
- 全部组件文档页与 README 组件清单重新生成且 docs-check 绿。

**建议 change**：`add-component-metadata-conformance`。

#### PA-P1-04 SQL output 标识符未转义 · ✅ 已实施并归档（`harden-sql-output-identifiers`，2026-11-23）

**现象**：MySQL 用反引号、Postgres 用双引号包裹列名，但不转义内嵌引号；表名直接拼接。列名可来自 JSON 推断（潜在不可信输入）。pgvector 已有 `escape_identifier`，SQL output 未复用（[output/sql.rs:242](crates/arkflow-plugin/src/output/sql.rs:242)、[vector_util.rs:197](crates/arkflow-plugin/src/vector_util.rs:197)）。

**影响**：含引号/反引号的字段名可破坏语句，构成语句注入面（表名由配置控制，风险较低；列名可由数据驱动）。

**动作**：统一使用 `escape_identifier`（按方言实现 backtick/double-quote 转义），表名也走同一转义；upsert 的 key 列同源处理。

**验收**：
- 测试覆盖字段名 `a"b`、``a`b``、含分号的表名；生成的 SQL 在两种方言下可解析/执行，注入尝试被当作普通标识符。
- pgvector 与 SQL output 共享同一转义实现（或至少同一测试用例集）。

**建议 change**：`harden-sql-output-identifiers`。

#### PA-P1-05 向量输出批量原子性缺失 · 已复核

**现象**：qdrant/milvus/pgvector 只实现 `write`，未覆写 `write_batch`，继承默认“逐条写、失败继续”语义（[output/mod.rs:41-60](crates/arkflow-core/src/output/mod.rs:41)）。qdrant 未配置 `id_field` 时每次重放生成新 UUID，产生重复点（[qdrant.rs:124](crates/arkflow-plugin/src/output/qdrant.rs:124)）。

**影响**：批中途失败留下半批；at-least-once 重放下向量库出现重复点，RAG 结果被污染。

**动作**：
- 为三个后端实现 `write_batch`：要么整批一个事务/一次请求，要么在文档中明确“部分批可见 + 必须配置 `id_field`”的降级语义。
- 无 `id_field` 时，用行内容哈希或确定性 UUID 派生存 id（需在 spec 中定义稳定性契约）。

**验收**：
- 故障注入测试：第 N 条失败时，断言要么整批不可见、要么返回明确的部分批错误并给出补偿指引。
- 同一批重放两次，配置 `id_field` 时结果幂等；未配置时行为符合文档（或直接 build 期拒绝）。

**建议 change**：`add-vector-output-batch-atomicity`。

#### PA-P1-06 S3 WAL 段永不回收 · 代理发现（代码检索确认无删除路径）

**现象**：sealed 段永久保留、manifest 永久列出；恢复时逐段下载再按 cursor 过滤（[s3.rs:1393](crates/arkflow-plugin/src/wal/s3.rs:1393)）。

**影响**：对象存储与恢复成本随历史无界增长；长稳部署会持续变慢。

**动作**：增加 retention 配置（按段数/时间/字节），只删除 cursor 之下的段；manifest 更新与删除的顺序必须保证崩溃安全；恢复只读必要段。

**验收**：
- 测试：写入 N 段、cursor 前进到 N-2，触发保留策略后仅保留必要段；恢复成功且不读取已删段。
- 文档写明默认保留策略与对象存储成本模型。

**建议 change**：`add-s3-wal-retention`。

#### PA-P1-07 Agent / 数据面身份与 mTLS · 已复核

**现象**：
- Hub 的 Agent 注册只凭 `node_id + 全舰队共享 node_token` 自证；任意持有者可冒用其他节点 id（[nodes.rs:263-280](crates/arkflow-server/src/hub/nodes.rs:263)）。
- Hub TLS 明确 `with_no_client_auth`，无 mTLS（[api/mod.rs:1092](crates/arkflow-server/src/api/mod.rs:1092)）。
- 数据面 v2 已有逐帧 HMAC，但密钥仍是 fleet 共享；mTLS opt-in 且默认关闭。

**影响**：一个节点沦陷 ≈ 全舰队凭据暴露；节点身份无法审计到具体个体。

**动作**：注册时由 Hub 签发 per-node 凭据（存 hash、可轮换/吊销），共享 token 仅保留为 bootstrap/break-glass；多节点部署默认要求 mTLS（Helm 默认开启，单机开发可关闭）；数据面改用节点级证书/密钥。

**验收**：
- 攻击测试：节点 A 的凭据不能以节点 B 的 `node_id` 注册/上报。
- Helm 多节点模板默认启用 mTLS；文档给出关闭开关与威胁模型。
- 审计能记录“哪个节点身份”执行了操作。

**建议 change**：`harden-node-identity-and-mtls`。

#### PA-P1-08 审计、身份写入与会话撤销的一致性 · 已复核

**现象**：
- 安全拒绝、身份变更、Job 操作审计均为 `let _ = record(...)`，存储故障静默丢失（[observability.rs:332](crates/arkflow-server/src/hub/observability.rs:332)）。
- `create_user` 先 `list_users` 查重再 `upsert_user`，未取 `identity_write_lock`；upsert 是 `ON CONFLICT DO UPDATE`，并发可覆盖既有用户（[identity.rs:228-260](crates/arkflow-server/src/hub/identity.rs:228)、[sqlite.rs:3376](crates/arkflow-server/src/storage/sqlite.rs:3376)）。
- 未绑定目录用户的 OIDC session `user_id` 为 NULL；之后绑定/禁用该 subject 不会回收旧 session（[identity.rs:600-645](crates/arkflow-server/src/hub/identity.rs:600)）。
- 审批只校验 principal id 不同；admin 同时拥有 `ManageUsers` 与 `ApproveRollouts`，可自建第二身份自批。

**影响**：安全审计可静默缺失；身份数据可被并发覆盖；OIDC 撤销不彻底；职责分离可绕过。

**动作**：
1. 安全关键审计改为 outbox/失败计数 + 告警，至少不能静默；
2. `create_user` 取 `identity_write_lock`，改用 `INSERT` 语义并处理冲突；
3. 用户绑定/禁用/删除时按 subject 追索并删除相关 session；JWKS 已知 kid 增加周期刷新；
4. 审批分离原则写入威胁模型：要么禁止同一 principal 兼任，要么文档明确“当前不防合谋”。

**验收**：
- 存储故障注入：拒绝请求仍返回 401/403，审计失败有 counter + 日志；
- 并发 create 同一 user_id：仅一个成功，另一个 409；
- 绑定/禁用 OIDC subject 后旧 session 立即 401；
- 审批自批测试按决定的策略通过/拒绝。

**建议 change**：`harden-identity-and-audit`。

#### PA-P1-09 standalone 的 Job 隔离策略 · 已复核

**现象**：任一本地 Job 失败会让整个引擎退出（[engine/mod.rs:212-225](crates/arkflow-core/src/engine/mod.rs:212)），而 Stream 是 per-stream 重启语义。

**影响**：单进程多 Job 部署时，一个坏配置/坏上游会拖垮同进程所有作业。

**动作**：新增 `job_failure_policy: fail_fast | restart | isolate`（默认 `fail_fast` 保持兼容）；`restart` 有指数退避与上限；`isolate` 仅标记该 Job 失败，其它继续。

**验收**：
- 测试：两个 local Job，一个启动即失败；`isolate` 下另一个持续运行；`fail_fast` 下进程按现状退出。
- 文档写明三种策略的适用场景与 v1.0 默认。

**建议 change**：`add-local-job-isolation-policy`。

#### PA-P1-10 控制面有界性与驱逐语义 · 已复核 / 代理发现

**现象**：
- `OperationStore` 按 id 字典序驱逐最旧，可能踢掉 Running 操作（[runtime.rs:76-80](crates/arkflow-core/src/runtime.rs:76)）。
- rollout/approval 目标数未设上限；Job 数量仅受 4 MB body 限制。
- `mark_stale` 把操作改为 `NodeUnavailable` 但不持久化（[nodes.rs:1126](crates/arkflow-server/src/hub/nodes.rs:1126)），重启后内存/持久层可能分叉。

**影响**：长稳运行后操作历史丢失、迟到结果 404；超大 rollout 可拖垮 Hub。

**动作**：按状态 + 时间驱逐（Running/Queued 永不驱逐）；rollout/approval 目标数设上限并在 API 返回明确错误；`mark_stale` 走 `persist_operation`。

**验收**：
- 测试：操作表满且存在 Running 操作时，Running 不被驱逐。
- 目标数超限返回 `invalid_rollout`/`too_many_targets`；重启后状态一致。

**建议 change**：`bound-control-plane-operations`。

#### PA-P1-11 数据面规模上限与可观测 · 已复核

**现象**：默认 `max_connections = 256`、单帧上限 256 MiB、每 quad replay 预算 256 MiB（[manager.rs:73-82](crates/arkflow-core/src/executor/remote/manager.rs:73)、[wire.rs:11](crates/arkflow-core/src/executor/remote/wire.rs:11)）。16×16 exchange 正好打满；溢出只记 warn，没有生产消费者（[manager.rs:980](crates/arkflow-core/src/executor/remote/manager.rs:980)）。

**影响**：高并行 split job 会先撞上隐式连接上限；问题只能从日志发现。

**动作**：在 plan/placement 期计算 quad 数并与 `max_connections` 校验（fail-closed 或显式告警）；把 `rejected_connections` 接入 metrics；文档给出“并行度 × 连接数”的 sizing 公式。

**验收**：
- 提交超限 placement 时得到明确错误（或 dry-run 警告）。
- `/metrics` 出现 `arkflow_data_plane_rejected_connections_total`。

**建议 change**：`bound-network-shuffle-capacity`。

---

### 3.3 P2（按需 / 后续迭代）

| ID | 问题 | 证据 | 建议 |
| --- | --- | --- | --- |
| PA-P2-01 | 状态后端仅 memory + redb；state apply 在 async ack 持 std Mutex 做 redb I/O；manifest 全量 JSON | [state.rs](crates/arkflow-core/src/state.rs)、[state_journal.rs:636](crates/arkflow-core/src/executor/state_journal.rs:636) | 评估 RocksDB/远程后端；状态路径 benchmark 锚点已存在，先量化再改 |
| PA-P2-02 | join 只限 `max_per_key`，不同 key 数无全局上限；窗口 `operation_lock` 持有到 sink ack | [join.rs:109](crates/arkflow-core/src/executor/join.rs:109)、[firing.rs:315](crates/arkflow-core/src/executor/window/firing.rs:315) | 增加全局 key/字节上限；评估窗口锁粒度或文档化吞吐上限 |
| PA-P2-03 | 水位在新增分区时允许回退，窗口侧用 max 钳制，语义需文档化 | [event_time.rs:306](crates/arkflow-core/src/event_time.rs:306)、[operator.rs:1636](crates/arkflow-core/src/executor/window/operator.rs:1636) | 写入 event-time spec 的边界说明与回归测试 |
| PA-P2-04 | Console 无时序图表、per-node 下钻、OpenAPI 生成；CORS 未放行 Authorization（必须同源代理） | [api/mod.rs:526](crates/arkflow-server/src/api/mod.rs:526) | 按企业需求排期；短期在部署文档强调同源 |
| PA-P2-05 | 存储 actor 单 mpsc + spawn_blocking 串行，Postgres 也失去并发 | [storage/mod.rs:1349](crates/arkflow-server/src/storage/mod.rs:1349) | 为 PG 单独实现并发池，或按命令类型分片 |
| PA-P2-06 | 插件注册是编译期，无动态加载/WASM；外部插件需自建二进制 | `lazy_static` 注册表 | 有明确生态需求再评估独立可选 crate |
| PA-P2-07 | 单点维护者风险：420 次人类提交来自一人 | git shortlog | CONTRIBUTING、good-first-issue、模块 owner、降低 CI 成本 |

---

## 4. 迭代路线图

### 迭代 1：稳定性与真相（1-2 周）

**目标**：消除已知“会打挂进程”的缺陷，让文档/元数据重新说真话。

**2026-11-22 更新**：原第 1 顺位 PA-P0-01 已复核否证（见 3.1，回归测试已随否证入库），本轮由 PA-P1-02 领头。

| 顺序 | 事项 | 验收 | 产出 |
| --- | --- | --- | --- |
| ~~1~~ | ~~PA-P1-02 文档真相~~ ✅ 已实施（PR #1392）：CLAUDE.md 指针化、插件指南 doctest 门禁、Helm appVersion 门禁 | CLAUDE.md/插件指南/Helm 一致 | `fix-docs-truth-2026-11` |
| ~~2~~ | ~~PA-P1-03 元数据一致性~~ ✅ 已实施并归档（PR #1401，change `add-component-metadata-conformance`）：注册表 schema 与 serde 配置结构体的机械门禁（字段双向覆盖、`required`、`default`、枚举取值），并修掉它在 13 个组件里查出的漂移 | conformance 测试红/绿可控（13 个真实突变场景逐一验证） | `add-component-metadata-conformance` |
| ~~3~~ | ~~PA-P1-04 SQL 标识符~~ ✅ 已实施并归档（PR #1401，change `harden-sql-output-identifiers`）：列名、upsert key、表名全部按方言加引号转义，表名逐段解析 | 注入用例通过（真实 PostgreSQL + MySQL 往返验证） | `harden-sql-output-identifiers` |

**退出标准**：以上 3 项归档；`cargo test --workspace --all-targets`、`pnpm docs:check`、console 五件套全绿；CHANGELOG 更新。

**2026-11-23 更新**：迭代 1 三项全部实施并归档。门禁落地后查出的漂移比原登记多——除 `transactional_offsets` 与 Redis 描述外，还有 pulsar input/output 的 `tls`、milvus_search 的 `concurrency`、http input 的幽灵键 `method`/`interval`、三处 serde 根本不接受的枚举取值、两处"名义默认值"、若干 `required` 漂移，全部已修。

### 迭代 2：产品化（1-2 个月）

**目标**：解决构建重量与多节点信任模型，让“可裁剪、可安全部署”成立。

| 顺序 | 事项 | 依赖 | 产出 |
| --- | --- | --- | --- |
| 1 | PA-P1-01 Cargo feature gating | 无 | `add-cargo-feature-gating` |
| 2 | PA-P1-07 节点身份 + mTLS | 无 | `harden-node-identity-and-mtls` |
| 3 | PA-P1-08 审计/身份一致性 | 身份模型 | `harden-identity-and-audit` |
| 4 | PA-P1-05 向量输出批量原子性 | 无 | `add-vector-output-batch-atomicity` |
| 5 | PA-P1-06 S3 WAL 段回收 | 无 | `add-s3-wal-retention` |
| 6 | PA-P1-09 local Job 隔离 | 无 | `add-local-job-isolation-policy` |
| 7 | PA-P1-10/P1-11 控制面/数据面有界性 | 无 | `bound-control-plane-operations`、`bound-network-shuffle-capacity` |

**退出标准**：minimal feature 构建进入 CI；多节点 Helm 默认 mTLS；安全项有攻击测试；长稳测试覆盖 WAL 回收与控制面驱逐。

### 迭代 3：规模化与体验（按需）

- PA-P2-01/02：状态后端与 join/窗口上限，先 benchmark 再改。
- PA-P2-04：Console 时序、下钻、OpenAPI。
- PA-P2-06/07：生态与维护者风险。

**明确不做（本轮）**：

- 不追 Flink/RisingWave 的全量 join/shuffle 语义；distributed jobs 的定位是“轻量 fleet 编排 + opt-in split placement”。
- 不在插件契约稳定前继续扩张 AI/向量组件面；先把 `write_batch`、retry、metadata 契约补齐。
- 不新增第四种配置层；`streams` 与 `jobs` 的边界用文档说清，暂不做语法转换器。
- 不做动态插件加载/WASM，除非出现明确生态信号。

---

## 5. 在飞工作与“已修复，勿重复”

### 在飞（2026-11-22 更新）

- 无在飞 change。`complete-connector-retry-adoption` 已随 #1388 实施、#1390 归档（HTTP/InfluxDB/MQTT output 与 Kafka input 重试收编、NATS 死 sleep 清理、共享退避封顶、`vector_search`/`milvus_search` 元数据补 `retry_count` 等已全部落地 main）；PA-P1-03 只保留“系统性 conformance”部分。
- `consolidate-connector-skeleton` 已归档（#1386）。

### 已在当前代码中修复（不要按旧评审重复立项）

2026-11-15 评审登记簿中的以下项，本次核实已落地：

- HTTP output 锁跨 send/重试循环（[http.rs:142](crates/arkflow-plugin/src/output/http.rs:142) 的 `send()` 在短临界区内 clone client，随后释放锁）；
- Pulsar `tls.ca_file` 从静默忽略改为 build 期显式拒绝（[pulsar/common.rs:112](crates/arkflow-plugin/src/pulsar/common.rs:112)）；
- expr 缓存键长度前缀化，消除字段名混叠（[expr/mod.rs:118](crates/arkflow-plugin/src/expr/mod.rs:118)）；
- WAL append 先序列化再占序号，消除永久 ack 缺口（[wal/mod.rs:534](crates/arkflow-core/src/wal/mod.rs:534)）；
- 窗口 journaled 持久化改为 touched 快照 + 定向查找，scan 仅在 stale 时触发（[window/operator.rs:150](crates/arkflow-core/src/executor/window/operator.rs:150)）；
- embedding 跨 chunk 维度不一致改为显式错误（[embedding.rs:320](crates/arkflow-plugin/src/processor/embedding.rs:320)）；
- `/jobs` 分页与 mutation 响应脱敏（[api/jobs.rs:119](crates/arkflow-server/src/api/jobs.rs:119)）；
- docs-check 已覆盖 `examples/**`（`.github/workflows/docs-check.yml`）。

> **流程风险**：`CODE-REVIEW-2026-11-15.md` §7 的“仍开放/部分修复”状态已落后于代码。建议每次迭代开始时先复核登记簿，再决定立项，避免重复劳动。

---

## 6. 产品定位与叙事建议

**建议的主叙事**：

> 单二进制、配置式、Apache Arrow/DataFusion 列式 SQL 的流处理引擎；默认 at-least-once，事务 sink 可 opt-in exactly-once；可选 Hub + Console 管理多节点 Job 舰队。

**应避免的叙事**：

- 不要把自己描述成 Flink/RisingWave 的“分布式有状态流计算平台”平替；当前分布式是“轻量编排 + opt-in split placement”，重 shuffle 不是目标。
- 不要在没有前置条件的情况下宣传“AI/ML 推理平台”；当前 AI 是 HTTP 调用的 embedding/LLM/vector 管道，价值在“可靠地把数据送进向量库”，不是模型运行时。

**三层配置的认知负担**：文档已有决策表（Stream / Local Job / Distributed Job），但用户仍容易在 `streams` 里撞到“sliding/join 被拒绝”的墙。建议：

1. 在 `--validate` 错误里直接给出“改用 Job DAG”的可复制片段（现在已有诚实指引，可再进一步）；
2. 文档首页放一张“我该用哪一层”的决策树；
3. 暂不做自动转换器，避免第四层抽象。

**v1.0 建议**：完成迭代 1 后再决定。v1.0 的承诺面（YAML/CLI/组件名/公共 API/metrics 标签）已在 [versioning.md](docs/docs/reference/versioning.md) 定义，代码侧已基本就绪；剩余是“不要带着已知 P0 和过期文档发版”。

---

## 7. 度量与门禁（迭代验收用）

| 维度 | 指标/门禁 | 现状 | 目标 |
| --- | --- | --- | --- |
| 构建 | 冷构建时间、default/minimal 二进制体积、feature 矩阵 | 未测 | 建立基线；minimal 体积/时间显著下降 |
| 依赖 | `Cargo.lock` 包数、重复 arrow/datafusion/zstd | 1,021 包；已有单版本门禁 | feature 化后 default 锁包数下降并留档 |
| 文档 | CLAUDE.md 一致性、插件指南可编译、Helm 版本一致 | 3 处漂移 | 3 条 CI 检查全绿 |
| 元数据 | serde 字段 ⊆ metadata schema ⊆ inventory | 登记 2 处漂移；门禁实测 30 处，已全修 | conformance 测试全绿（2026-11-23 达成） |
| 可靠性 | EOS 忙循环回归、checkpoint 失败归因 | 回归测试已入库（2026-11-22，见 3.1，缺陷本身已否证） | 无 |
| 安全 | 节点身份冒用测试、审计失败可观测、会话撤销 | 未覆盖 | 攻击/故障注入测试入库 |
| 控制面 | 驱逐不伤 Running、rollout 目标上限 | 未覆盖 | 测试入库 |
| 数据面 | 超限 placement 明确失败、拒绝计数导出 | 仅 warn | plan 期校验 + metrics |
| 性能 | benchmark 套件（已有） | 仅观测不设阈值 | 迭代前后对比，不设硬阈值 |

---

## 8. 使用方式与维护规则

1. **每次迭代开始**：先读 §3 与 §5，确认哪些项已被在飞 change 覆盖；只把“仍需做”的项转为 OpenSpec proposal。
2. **立项**：每项建一个 change，proposal 的 Non-goals 直接引用本文件的“明确不做”；tasks 复用本文件的验收标准。
3. **归档**：change 归档后，在本文件对应条目打 `✅ 已归档（change 名）`，不要删除（保留决策历史）。
4. **快照**：本文件基于 `c6ff539b`；若工作树已有并发改动，先 `git log -1` 更新快照号再引用行号。
5. **升级为长期文档**：迭代 2 完成后，可把仍有效的战略结论合并进 `openspec/PLANNING.md`，本文件转为历史快照。

---

## 附录 A：关键证据索引

- 内核：`crates/arkflow-core/src/executor/{graph,task,barrier,commit,job_runner_adapter,window,remote}/`
- 可靠性：`crates/arkflow-core/src/{wal,state,checkpoint,event_time}.rs`
- 控制面：`crates/arkflow-server/src/{hub,agent,api,storage}/`
- 插件：`crates/arkflow-plugin/src/{input,output,processor,codec,wal}/`
- 规格：`openspec/specs/`（104 份）；策略：`openspec/PLANNING.md`
- 评审历史：`CODE-REVIEW-2026-11-15.md`（注意 §7 状态已过期）
- 文档门禁：`docs/scripts/docs-check.mjs`、`docs/DOCUMENTATION.md`
- 结构测试：`crates/arkflow-plugin/tests/{connector_error_contract,registry_consistency}.rs`、`crates/arkflow/tests/{dependency_lock_hygiene,examples_validate,docs_snippets_validate}.rs`

## 附录 B：待运行复现 / 待确认

- ~~PA-P0-01 的实际运行时表现（CPU/内存曲线、checkpoint 失败日志）尚未复现~~ 已于 2026-11-22 复核否证（见 3.1）；最小触发场景的回归测试已入库（`channel_close_during_in_flight_barrier_stays_idle_and_settles`）。
- 分布式长稳与规模上限（连接数、每 quad 预算）未在本机压测；PA-P1-11 的 sizing 公式需要真实 16×16/32×32 exchange 数据。
- 本机全量 Rust 测试受 DuckDB/本地 C++ 构建与 Docker 依赖限制；相关结论以 CI 为准。

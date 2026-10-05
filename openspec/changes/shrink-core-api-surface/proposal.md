# Proposal: shrink-core-api-surface

## Why

v1.0 评审批次 C（PLANNING §9.3：批次 A–E 中唯一未落地项；发版路径「批次 E → 批次 C → v1.0」的最后一环）判定：arkflow-core 的 Rust 公共面远大于跨 crate 实际使用面，趁 v1.0 semver 冻结前、消费者只有自家 crate 的窗口一次性收缩。当前证据（2026-10-05 核实，HEAD `88dda9b2`）：

- 脚本审计（顶层 pub item ↔ 全部消费者 crate grep）：314 个顶层 pub item 中 **139 个在 arkflow-core 外零引用**，遍布 33 个文件（executor/remote.rs 24 个、job.rs 18 个、job_runner_adapter.rs 7 个……），对应评审「~685 pub item 修剪 + executor/wal 520 行 pub」的存量。
- 5 个组件 builder trait 的 `name` 参数全线 `Option<&String>`（`input/mod.rs:37`、`codec/mod.rs:42`、`processor/mod.rs:145`、`output/mod.rs:102`、`buffer/mod.rs:43`），arkflow-plugin 侧 60 处实现签名跟随——惯用 Rust 应为 `Option<&str>`，冻结后即成为永久公共 API 瑕疵。
- `Temporary` 公共 trait 的 `get(&self, keys: &[ColumnarValue])`（`temporary/mod.rs:42`）把 DataFusion `ColumnarValue` 泄漏进公共契约；全部实现（plugin `temporary/redis.rs:51`）与唯一生产调用方（plugin `processor/sql.rs:429`）实际只消费 UTF-8 字符串键。
- `Error` enum 的 `LockTimeout(String)`（`lib.rs:122`）与 `InvalidConfig(String)`（`lib.rs:125`）全工作区零构造——dead variants（且与既有 `Config`/`Timeout` 语义重复）。
- `HealthCheckConfig`（`config.rs:139`）名为 health check，实为进程级大杂烩：health 端点 + 控制 API（api_prefix/api_token/cors_origins）+ Agent 模式（hub_urls/node_id/node_token/两个 ttl）+ 数据面（data_port/data_host）+ observability——评审「命名大杂烩，改名拆分」。
- Hub 专用的 `resolve_candidate_payload`（`secret.rs:79`，唯一外部调用方 `arkflow-server/src/hub/placement.rs:1306`）住在 core，违背「core 不含 Hub 专属逻辑」的分层。

评审中 `Pipeline` 死 API 一项已在此前变更中删除（`pipeline/mod.rs` 现仅存 `PipelineConfig`），本变更不再涉及。

## What Changes

- **BREAKING（仅 crate 内部消费者，无用户可见影响）** pub 面修剪：139 个 core 外零引用的顶层 pub item 降为 `pub(crate)`（以工作区编译为准绳修正审计误差）。
- **BREAKING（同上）** 5 个 builder trait（Input/Output/Processor/Codec/Buffer）的 `name: Option<&String>` → `Option<&str>`；core 与 plugin 全部实现、调用点（`name.as_ref()` → `as_deref()` 等）同步。
- **BREAKING（同上）** `Temporary::get` 签名 `&[ColumnarValue]` → `&[String]`；sql processor 侧求值结果转字符串键，redis temporary 侧删除 `get_key` 的 ScalarValue 解包。
- 删除 dead Error variants `LockTimeout`、`InvalidConfig`（零构造，删除为纯减法）。
- `HealthCheckConfig` 改名拆分：Rust 类型改为 `NodeConfig`（`EngineConfig` 字段 `node: NodeConfig` + `#[serde(rename = "health_check")]`），内部分组为 `health`/`control_api`/`agent`/`data_plane` 子结构（serde flatten）+ 既有 `observability` 字段；**YAML/JSON 配置形状逐字节不变**（round-trip 测试与手写 schema 均不动）。
- `resolve_candidate_payload` 连同其私有辅助（`resolve_secret_only_at`/`resolve_secret_only_string`）与单测从 core `secret.rs` 迁至 arkflow-server（调用方 `hub/placement.rs` 就近）；`secret-references` spec 的行为需求零变化。

## Capabilities

### New Capabilities

- `core-api-surface`: arkflow-core 公共 API 面的最小性政策——builder trait 命名参数用 `&str`、公共 trait 不泄漏 DataFusion 类型、Hub 专属 helper 不住在 core、dead Error variant 及时清除、配置 Rust 类型名实相符（YAML 兼容不变）。

### Modified Capabilities

（无——全部为 Rust API 面与代码组织变更，用户可见行为、配置格式、schema 均不变）

## Impact

- `crates/arkflow-core/src/`：33 个文件的可见性收紧、`lib.rs` Error variants 删除、`config.rs` 类型改名拆分、`secret.rs` 函数迁出、`temporary/mod.rs` 签名变更、5 个 builder trait 签名变更。
- `crates/arkflow-plugin/src/`：约 60 处 builder 实现 `Option<&String>`→`Option<&str>`、`temporary/redis.rs` 与 `processor/sql.rs` 的 `Temporary::get` 两侧适配。
- `crates/arkflow-server/src/`：`hub/placement.rs` 调用点改本地模块、`lib.rs` 等 `HealthCheckConfig`→`NodeConfig` 引用更新、secret-dispatch 迁入代码与单测。
- `crates/arkflow/src/`：如引用受影响类型则同步。
- 不影响：YAML 配置格式、JSON schema、docs 页面、console、CLI 行为、CHANGELOG（无用户可感知变化）。

## Non-goals

- 不改 YAML/JSON 配置形状：`health_check` section 名与字段路径保持不变（含 `DeprecatedHubUrl` 哨兵行为）；section 级改名/嵌套重组是用户可见 breaking，成本收益不成比例，留待维护者 pre-v1.0 另行决策。
- 不做 Error 分类收紧（`Process` 412 处垃圾桶 → 专用 variants）：独立开放项，另行立项。
- 不动 trait 方法级（impl 块内 `pub fn`）可见性——随容器类型可见性收紧自然收窄，不做逐方法清扫。
- 不重排 `lib.rs` 模块导出结构（`pub mod` 列表保持）。
- 不触碰 executor 内部逻辑、行为与测试语义（可见性变更除外）。

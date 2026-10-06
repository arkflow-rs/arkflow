# Proposal: refactor-batchf-structural-debt

## Why

批次 F（P3 结构债，2026-10-05 归档）在四个重构 change 中显式划出三项 Non-goal，至今仍开放（PLANNING 9.3）：

1. **字符串命令协议**：Hub→Agent 命令的 `operation` 是裸字符串（`hub/wire.rs:159` `pub operation: String`），Agent 侧靠 `command.operation.as_str()` 字符串匹配分发（`agent/commands.rs:48` `starts_with("job_")`、`:57`/`:100`/`:195` 三处 `matches!`），payload 是无类型 `serde_json::Value` 手工 `.get("checkpoint_id")` 逐字段挖掘（`agent/commands.rs:64-66,518-521`）。拼写错误编译期不可见、新增操作要改多处散落的字符串字面量、payload 缺字段只能靠运行时字符串报错。
2. **CheckpointHook 大杂烩**：`arkflow-core/src/executor/task.rs:80-105` 一个结构体塞 9 个字段，混杂三类正交关注点——checkpoint 报告（reporter/failure_reporter/barrier_rx/state/task_id/finished_reporter）、事件时间（event_time_gate/partition）、指标（metrics）——task.rs 全部内部函数无论需要哪类都整体透传 `hook: &CheckpointHook`（task.rs 内 20+ 处签名），构造侧（kernel_handle.rs:808、job_runner_adapter.rs:2371）被迫填一堆无关 `None`。
3. **hub/ 伪拆分**：`refactor-server-file-splits` 把 `lib.rs`/`agent.rs` 拆成目录，但 `hub/` 的 18 个子模块全部 `use super::*`（nodes.rs、operations.rs、job_orchestration.rs 等各 1-2 处），父模块 hub.rs 的全部私有项（NodeRecord、常量、now_ms、persist_operation）对所有子模块可见，模块边界名存实亡——在任一子模块内引用父级符号既无声明也无从审计。

三项均为零行为变化的内部结构治理，是批次 F 的收尾。

## What Changes

- **命令协议强类型化**：`AgentCommand.operation: String` 改为封闭枚举 `AgentOperation`（serde `snake_case`，wire JSON 逐字节不变），Hub 侧全部 `"job_start".into()` 构造点改枚举；`HubOperation.operation` 同步；Agent 分发改 `match` 穷尽检查；四类 payload（job start/checkpoint/checkpoint_commit/configuration）从手工 JSON 挖掘改为 serde 反序列化的类型化结构体；未知操作（未来新版 Hub 下发）保留既有 fail-closed 行为（枚举携带 `#[serde(other)]`-等价的未知回退）。
- **CheckpointHook 拆解**：按关注点拆为 `CheckpointHook`（纯 checkpoint：reporter/failure_reporter/barrier_rx/state/task_id/finished_reporter）、`EventTimeBinding`（gate/partition）、metrics 独立字段，组成 `ChainHooks`；task.rs 内部函数签名按需收窄，构造侧显式组装。
- **hub/ 显式导入**：18 个子模块的 `use super::*` 全部替换为显式 `use` 清单；hub.rs 中仅供测试经 glob 可达的再导出改为子模块直接路径引用；删除不再需要的 glob 再导出。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `compute-node-agent`: ADDED requirement——命令操作集合封闭且类型化分发，wire 格式（JSON 字符串值、payload 结构）零变化，未知操作 fail-closed（既有行为的规格固化）。
- `unified-execution-kernel`: ADDED requirement——执行钩子按关注点拆解（checkpoint/事件时间/指标），全部语义（barrier 报告、watermark 记录、指标计数、完成上报）与既有测试原样保持。
- `control-plane-hub`: ADDED requirement——hub 子模块边界显式化（禁止 `use super::*` glob 依赖父模块私有项），内部重组不改变 `crate::hub::*` 公共导出面。

## Non-goals

- 不改命令 wire 协议的任何字节（含 `operation` 字符串值、payload JSON 结构、`PersistedOperation` 存储格式）——新版 Agent 与旧版 Hub 混布零影响。
- 不引入命令 schema 版本协商或协议演进机制（v2 wire format 等留待真实需求）。
- 不拆解 `hub/wire.rs` 本身或移动任何公共类型路径；不处理 `agent/`、`api/`、`storage/` 目录中残留的 `use super::*`（仅 hub/，与批次 F 划出的债严格对齐）。
- 不改 `Input::read`/生命周期等任何运行时行为；CheckpointHook 拆解不触碰 barrier/checkpoint/事件时间语义。
- 不做 console/docs 变更（无用户可见面）。

## Impact

- `crates/arkflow-server/src/hub/wire.rs`（AgentOperation 枚举 + AgentCommand/HubOperation 字段类型）、`agent/commands.rs`（分发 match + 类型化 payload 解析）、`agent/kernel.rs`（payload 结构体归位）、hub 侧全部命令构造点（`hub/{jobs,checkpoint,placement,lifecycle,operations,rollout,nodes}.rs`、`api/{jobs,configuration}.rs`、`storage/mod.rs` 及 sqlite/pg 持久化行的枚举往返）。
- `crates/arkflow-core/src/executor/task.rs`（CheckpointHook/ChainHooks/EventTimeBinding 定义与全部签名）、`kernel_handle.rs`、`job_runner_adapter.rs`、`executor/tests.rs`。
- `crates/arkflow-server/src/hub.rs` + 18 个子模块（显式导入；hub.rs 再导出面清理）。
- 验证：`cargo test --workspace --all-targets`、`cargo clippy --workspace --all-targets`、`cargo fmt --check` 全绿；wire 兼容性由既有 agent/hub 协议测试（含 storage 往返）守卫。

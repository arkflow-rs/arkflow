# Design: refactor-batchf-structural-debt

## Context

批次 F 收尾的三项结构债（见 proposal Why 节的 file:line 证据）。共同约束：**零行为变化、零 wire 字节变化、公共 API 面零变化**。三项彼此独立，按「先纯机械（hub/ 导入）→ 再类型化（命令协议）→ 最后内核（CheckpointHook）」的顺序实施，每步独立可验证。

## Goals / Non-Goals

**Goals:**

- 命令操作成为封闭枚举，分发点 `match` 穷尽检查，编译期发现拼写/遗漏。
- payload 解析收敛到类型化结构体构造器，错误消息逐字节保留。
- `CheckpointHook` 按关注点拆解，task.rs 内部签名按需收窄。
- hub/ 子模块对父模块的依赖显式声明，删除 glob 再导出链。

**Non-Goals:** 见 proposal Non-goals（wire 协议演进、agent/api/storage 目录的 glob 清理、core `ControlPlane::lifecycle(&str)` 签名等均不动）。

## Decisions

### D1 — `AgentOperation` 枚举：自定义 Serialize/Deserialize + `Unknown(String)` 回退

`hub/wire.rs` 新增：

```rust
pub enum AgentOperation {
    Start, Stop, Restart,                 // stream lifecycle（经 cp.lifecycle 执行）
    ValidateConfiguration, DiffConfiguration, ApplyConfiguration, RollbackConfiguration,
    JobStart, JobRestart, JobStop,
    JobCheckpoint, JobSavepoint, JobCheckpointCommit, JobSavepointCommit,
    Unknown(String),                      // 未来版本/异常值，保留原串
}
```

- **序列化**：手写 `Serialize`（`serialize_str(self.as_str())`）与 `Deserialize`（`deserialize_str` 后按已知串匹配，未命中进 `Unknown`）。不用 `#[serde(rename_all)]` 派生 + `#[serde(other)]`：`other` 只能是无数据变体，会丢原始字符串——而 `agent/tests.rs:3573` 断言 `"unknown Job operation job_teleport"` 需要原串，且 `Unknown` 回写存储时必须无损往返。
- **`as_str()`** 是唯一字符串出口：`cp.lifecycle(&id, op.as_str(), ..)`、`PersistedOperation.operation`（保持 `String`）、metrics 标签等沿用。
- **变体名 ↔ 字符串核对**：`JobCheckpointCommit` → `job_checkpoint_commit`，与既有字面量逐一对照（见 tasks 里的清单核对项）。
- 备选「保留 String 字段 + 分发前 FromStr 解析」被否：构造侧（hub 存储恢复、api、rollout、placement 等十余处）仍是裸字符串，拼写错误依旧编译期不可见。

**覆盖范围**：`AgentCommand.operation` 与 `HubOperation.operation` 改为枚举（`reconcile` 增加对应变体——`hub/operations.rs:47` 在 `HubOperation` 上使用）；`PersistedOperation.operation` **保持 String**，在 `persist_operation`（hub.rs:202）与重启恢复路径的单一边界经 `as_str()`/解析转换，SQL 与存储测试零变化。存储读到未知串 → `Unknown(s)`，与命令分发路径同语义。

### D2 — 类型化 payload：手写构造器，错误消息逐字节保留

`agent/commands.rs` 的 `execute_job_operation` 与 configuration 分支中散落的 `payload.get("checkpoint_id")` 收敛为四个结构体（放 `agent/commands.rs` 或 `agent/kernel.rs`，随实现归属）：

- `JobStartPayload { plan: JobPlan, assignments: Vec<TaskAttempt>, recovery_id, recovery_savepoint, recovery_required, split: SplitPlacementPayload }`（吸收既有 `parse_recovery_payload`）
- `CheckpointPayload { checkpoint_id: String }`
- `CheckpointCommitPayload { checkpoint_id: String, manifest_nodes: Vec<String>, planned_task_ids: Vec<String> }`
- configuration 三类（validate/apply 共用 `ConfigCandidate`；diff 的 `from/to`；rollback 的 `id`）保持现有内联解析或提为小结构体，随实际行数决定——不为两行解析造结构。

手写 `fn from_payload(value: &serde_json::Value) -> Result<Self, String>` 而非 serde 派生：现有错误消息（"missing checkpoint payload"、"missing Job task assignments" 等）被测试断言，serde 的默认消息必然漂移；手写 getter 链只是把挖掘从分发逻辑移进构造器，消息零漂移。分发函数只消费字段。

### D3 — CheckpointHook 拆解为三个内聚结构

`task.rs` 现结构 →：

```rust
pub(crate) struct CheckpointHook {        // 纯 checkpoint：6 字段
    reporter, failure_reporter, barrier_rx, state, task_id, finished_reporter
}
pub(crate) struct EventTimeBinding {      // 事件时间：gate + partition
    pub gate: Arc<Mutex<Option<EventTimeGate>>>,   // 保持非 Option（语义同今）
    pub partition: Option<u32>,
}
pub(crate) struct ChainHooks {            // hooks map 的值类型
    pub checkpoint: CheckpointHook,
    pub event_time: EventTimeBinding,
    pub metrics: Option<Arc<RuntimeMetrics>>,
}
```

- hooks map 类型 `BTreeMap<String, ChainHooks>`；`Default` 语义与今逐字段等价（gate = `Arc::new(Mutex::new(None))`）。
- task.rs 内部函数：跨关注点的循环体收 `&ChainHooks`；单关注点函数（如 `record_watermark_lag`、`abort_event_time_held`、barrier 快照路径）签名收窄到对应子结构。不为收窄而收窄——若某函数仅因一处字段引用就需拆参，保持 `&ChainHooks`。
- 构造侧 `kernel_handle.rs:808`、`job_runner_adapter.rs:2371` 显式组装三段；测试构造器（tests.rs `hook()` 助手）同步。
- 备选「保持单结构体、仅文档分节」被否：这正是要偿还的债。备选「每关注点独立 map」被否：三段生命周期强绑定（同一链同时建），拆 map 徒增查找。

### D4 — hub/ 显式导入：三步机械化

1. 每个子模块删除 `use super::*`，按编译错误补显式导入：父级类型/常量/函数走 `use super::{...}`，兄弟模块项走 `use super::<defining_module>::{...}`（如 `use super::nodes::parse_operator_credential`），crate 路径走 `use crate::...`。
2. hub.rs 的 `pub(crate) use` 中转再导出（checkpoint/nodes/operations/placement 四组）收缩为 hub.rs 自用 + 公共面（`pub use wire::{...}` 不动）；兄弟改直连定义方后删除中转。`#[cfg(test)]` glob 专用再导出（hub.rs:65-70）删除，测试文件改直接路径（`use super::checkpoint::job_state_format_version`）。
3. `use super::*` 消失后，clippy 的 unused-imports 守护导入清单不过量。

**红线**：本步 diff 只允许 `use` 语句与符号路径变化，任何逻辑行变更都是实现事故。测试文件的 glob 同步显式化（tests.rs/session_report_tests.rs/job_orchestration_tests.rs）。

### 实施顺序与依赖

hub/ 导入（纯机械）→ 命令协议（改 hub/wire.rs 与全部构造点，行级 diff 叠在已显式化的导入上更可审）→ CheckpointHook（core，完全独立）。每步跑 `cargo test -p arkflow-server` / `-p arkflow-core` 门禁。

## Risks / Trade-offs

- [枚举序列化引入 wire 字节漂移] → 新增全变体 + `Unknown` 的 JSON 往返单测（序列化串与既有字面量逐一断言相等）；既有 agent/hub 协议测试与 storage 往返测试作纵深。
- [payload 错误消息漂移破坏测试断言] → 手写构造器搬运原消息；跑 `agent/tests.rs` 全量（含 "missing checkpoint_id" 类断言）。
- [hub/ 大机械 diff 夹带行为变化] → 红线约束（D4）；diff 审查仅 `use`/路径行；`cargo test --workspace --all-targets`。
- [CheckpointHook 收窄签名造成可读性回退（参数变多）] → D3 的「不为收窄而收窄」规则；以 `&ChainHooks` 为默认。
- [`Unknown` 被静默吞掉形成新债] → 分发 `match` 对 `Unknown` 显式走既有 fail-closed 错误路径（错误文案含原串），与 `job_teleport` 测试同语义。

## Migration Plan

单 PR 三步提交（每步独立绿）。回滚 = revert 整个 PR；无数据/配置迁移（wire 与存储格式零变化）。

## Open Questions

（无——三项均有既有测试与明确不变量约束。）

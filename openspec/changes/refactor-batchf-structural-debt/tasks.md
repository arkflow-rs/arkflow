## 1. hub/ 显式导入（纯机械，先行）

- [x] 1.1 `hub/wire.rs`、`error.rs`、`command_metrics.rs`、`operator.rs`、`secret_dispatch.rs`：删 `use super::*`，按编译错误补显式导入
- [x] 1.2 `hub/nodes.rs`、`placement.rs`、`operations.rs`、`jobs.rs`、`checkpoint.rs`、`lifecycle.rs`、`observability.rs`、`rollout.rs`、`job_orchestration.rs`、`leadership.rs`：同上
- [x] 1.3 hub.rs 收缩中转再导出：兄弟模块改 `use super::<defining>::{...}` 直连；删除 `#[cfg(test)]` glob 专用再导出（hub.rs:65-70），`hub/tests.rs`、`session_report_tests.rs`、`job_orchestration_tests.rs` 改直接路径
- [x] 1.4 门禁：`cargo test -p arkflow-server` 全绿；`grep -rn "use super::\*" crates/arkflow-server/src/hub*` 零命中；diff 红线检查（仅 use/路径行）

## 2. 命令协议强类型化

- [x] 2.1 `hub/wire.rs`：新增 `AgentOperation` 枚举（含 `Reconcile` 与 `Unknown(String)`），手写 Serialize/Deserialize + `as_str()`；新增全变体 + Unknown 的 JSON 往返单测（序列化串与既有字面量逐字节断言）
- [x] 2.2 `AgentCommand.operation`、`HubOperation.operation` 改枚举；hub 侧全部构造点（`hub/{jobs,checkpoint,placement,lifecycle,operations,rollout,nodes}.rs`、`api/{jobs,configuration}.rs`、`storage/mod.rs` 两处 `job_start` 构造）改枚举变体；`persist_operation` 边界 `as_str().to_owned()`，存储恢复路径解析回枚举（未知串 → `Unknown`）
- [x] 2.3 `agent/commands.rs`：`execute_command`/`execute_job_operation` 分发改穷尽 `match`；`Unknown` 走既有 fail-closed 错误路径（文案含原串）；`cp.lifecycle` 传 `op.as_str()`
- [x] 2.4 类型化 payload：`JobStartPayload`（吸收 `parse_recovery_payload` + `SplitPlacementPayload` 装配）、`CheckpointPayload`、`CheckpointCommitPayload` 手写构造器，错误消息逐字节保留；分发函数只消费结构体字段
- [x] 2.5 测试同步：`agent/tests.rs`、`hub/tests.rs`、`api/tests.rs` 中 operation 构造改枚举（`job_teleport` 类未知操作用例改 `Unknown("job_teleport".into())` 并保留断言文案）
- [x] 2.6 门禁：`cargo test -p arkflow-server` 全绿（含 storage 往返、协议兼容测试）；`cargo clippy -p arkflow-server` 无新告警

## 3. CheckpointHook 拆解（core）

- [x] 3.1 `task.rs`：定义 `EventTimeBinding`（gate/partition）、`ChainHooks`（checkpoint/event_time/metrics），`CheckpointHook` 收窄为 6 个纯 checkpoint 字段；`Default` 语义与今逐字段等价
- [x] 3.2 task.rs 内部全部签名与调用点迁移（hooks map 值类型 `ChainHooks`；单关注点函数按 D3 规则收窄，其余收 `&ChainHooks`）
- [x] 3.3 构造侧迁移：`kernel_handle.rs`（CheckpointHook 装配处）、`job_runner_adapter.rs`（hooks map 构建）、`executor/tests.rs` 测试构造器与全部 `CheckpointHook::default()`/字面量构造
- [x] 3.4 门禁：`cargo test -p arkflow-core` 全绿（~1,111 测试）；`cargo clippy -p arkflow-core` 无新告警

## 4. 收尾验证

- [ ] 4.1 `cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`cargo fmt --all`
- [ ] 4.2 契约复核：三个 delta spec 的每个 Scenario 均有对应测试证据；tasks 勾选；PLANNING 9.3 批次 F「仍开放」三项划掉

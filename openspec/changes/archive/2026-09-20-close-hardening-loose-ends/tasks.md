# Tasks: close-hardening-loose-ends

## 1. 数据面认证收口（arkflow-core）

- [x] 1.1 `remote.rs`：给 `open_edge`、`open_edge_with_stream`、`open_edge_deferred` 加 `#[cfg(test)]`，doc 注明仅测试边界可用；`cargo build --workspace --all-targets` 确认无非测试调用者编译失败
- [x] 1.2 `remote.rs`：`bind_tcp` 在 `config.credentials == None` 时返回显式 `Error::Configuration`（或现有等价错误类型），不创建 listener socket；新增单测覆盖该错误路径
- [x] 1.3 迁移 `remote.rs:3402` 一带依赖 None-auth API 的内嵌测试到会话 API（参照同文件既有认证测试），保持断言不变
- [x] 1.4 迁移 `executor/tests.rs` 三个远程集成测试：`4564-4567`（barrier 对齐）→ `open_edge_deferred_for_job` + `register_inbound_for_session`；`4674`（下游失败）→ `open_edge_with_stream_for_session`；`4384`（fail-closed）随 manager 构造联动调整
- [x] 1.5 `cargo test -p arkflow-core executor` 全绿，确认无测试语义漂移（握手帧对断言透明）

## 2. Barrier 协调器契约（arkflow-core）

- [x] 2.1 `barrier.rs`：删除 `complete()` 中构造即弃的 `TaskAttemptSnapshot` 向量与 `let _ = self.participants.len()`；为 `BarrierCoordinator` 补类型级 rustdoc："职责止于全部参与者上报，manifest 持久化归 Agent/Engine 接线"
- [x] 2.2 `cargo test -p arkflow-core`（executor 全量）+ `cargo clippy -p arkflow-core --all-targets` 通过

## 3. Hub boot 失效去重（arkflow-server）

- [x] 3.1 逐行 diff `hub.rs:2491-2527`（register_after_auth）与 `2687-2723`（report）两块失效逻辑，确认同构范围；若有语义差异记录到本任务勾选说明（已核实：两块逐行同构，仅 node_id 来源不同）
- [x] 3.2 抽取同构核为私有方法（按 node_id 过滤 `job_start`、置 `NodeUnavailable` + `failure_class=recovery_required`、返回失效 job 集），两个入口改为调用它；入口差异化动作（队列命令丢弃 / 观察游标重置）保留原位
- [x] 3.3 `cargo test -p arkflow-server` 全绿——重点跑 boot 变更相关用例（`boot_change` / `node_unavailable` / `recovery_required` 命名的测试）

## 4. 收尾验证

- [x] 4.1 `cargo build --workspace --all-targets` + `cargo clippy --workspace --all-targets` + `cargo test --workspace --all-targets`（CI 等价命令）通过
- [x] 4.2 确认无 README/组件清单/example manifest/docs 变更需求（本变更不涉及注册与配置），`git diff --stat` 复核改动面与 proposal Impact 一致

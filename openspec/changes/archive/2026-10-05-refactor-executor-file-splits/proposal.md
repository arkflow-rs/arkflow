# Proposal: refactor-executor-file-splits

## Why

P3 结构债的 executor 部分（2026-10-05 核实）：`executor/remote.rs` 7,332 行（wire 帧/编解码/TCP 传输+TLS/认证握手/网络管理器五合一，实现 3.5k + 测试 3.8k）与 `executor/window.rs` 7,483 行（实现 3k + 测试 4.4k）是目录内最大两块单文件；另有三个零价值兼容壳（两个 no-op 网关参数壳、一个无调用链环 `run_job_with_metrics`）。纯文件级拆分，零行为变化。

## What Changes

- **remote.rs → `executor/remote/` 目录**：`wire.rs`（Quad/FrameKind/FrameHeader/WireSignal/WireDataMeta/TieredFrame）、`codec.rs`（DataEncoder/DataDecoder/Piece）、`transport.rs`（TcpEdgeTransport/帧封装/ConnectionFailure/DataPlaneTlsConfig）、`auth.rs`（DataPlaneCredentials/PeerExpectation/SessionAuth/HandshakePayload/JobSessionKey/EdgeSessionKey）、`manager.rs`（NetworkManagerConfig/RemoteAck/PendingBatch/PendingReceipts/RemoteEdge/NetworkManager）、`mod.rs`（重导出，保持 `crate::executor::remote::X` 外部路径不变）、`tests.rs`（测试模块整体搬移）。
- **window.rs → `executor/window/` 目录**：`aggregate.rs`（NumericKind/NumericValue/AggregateBuffer/LegacyAggregateBuffer）、`firing.rs`（WindowTrigger/WindowKind/WindowOperatorConfig/WindowRuntimeSnapshot/WindowRollback/WindowFiring/WindowFiredAck）、`operator.rs`（ColumnarWindowOperator 及全部 impl）、`mod.rs` + `tests.rs`。
- **兼容壳清理**：删 `run_graph_with_gate` 与 `run_graph_with_hooks_and_gate`（task.rs 两个 no-op，tests.rs 各一处调用改 `run_graph_with_hooks`）；删 `run_job_with_metrics`（job_runner_adapter.rs 无调用链环，`run_job` 直连 `run_job_with_metrics_started`）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

（无——纯内部结构移动，公共 API 与行为零变化）

## Impact

- `crates/arkflow-core/src/executor/remote.rs` → `remote/{wire,codec,transport,auth,manager,mod,tests}.rs`
- `crates/arkflow-core/src/executor/window.rs` → `window/{aggregate,firing,operator,mod,tests}.rs`
- `crates/arkflow-core/src/executor/task.rs`、`job_runner_adapter.rs`、`tests.rs`（兼容壳删除与调用点更新）
- 验证：`cargo test -p arkflow-core`（~1,111 测试）+ workspace clippy/fmt。

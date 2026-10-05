# unified-execution-kernel Specification

## Purpose
TBD - created by archiving change rebuild-unified-streaming-engine. Update Purpose after archive.
## Requirements
### Requirement: Unified execution graph

The engine SHALL compile every JobPlan into a single execution graph of vertices
(chains of operators) connected by bounded in-process channels, used identically
by the local engine and by Compute Node Agents.

#### Scenario: Chain fusion

- **WHEN** a JobPlan contains consecutive single-in single-out, non-stateful, non-window operators connected by forward edges
- **THEN** the builder places them into one execution chain with zero channel hops between them

#### Scenario: Agent subgraph equals kernel graph

- **WHEN** an Agent builds the local subgraph for its assigned task subset
- **THEN** the vertices and edges are built by the same `ExecutionGraphBuilder` code path used by the local engine

### Requirement: Pipelined vertex execution

Every vertex SHALL run its own event loop so that source reads, operator
processing, and sink writes of different batches overlap in time.

#### Scenario: Overlapping execution

- **WHEN** a source produces batch N+1 while a downstream chain is still processing batch N
- **THEN** the source read proceeds without waiting for the downstream chain to finish

### Requirement: Channel backpressure

Edges SHALL be bounded channels. When a downstream channel is full, the upstream
vertex SHALL await send and thereby propagate backpressure to its source.

#### Scenario: Full channel blocks upstream

- **WHEN** a bounded edge reaches capacity and its consumer stops receiving
- **THEN** the producing vertex blocks on send instead of buffering unboundedly

#### Scenario: Capacity is configurable

- **WHEN** a graph is built through the builder API with a non-default channel capacity
- **THEN** edges are constructed with that capacity, defaulting to 1024

### Requirement: Ordered delivery per edge

Data envelopes on one edge SHALL be delivered in send order; forward chains
SHALL preserve end-to-end batch order from source to sink. With a processor
worker pool (`pipeline.thread_num > 1`), the pool SHALL publish processed
outputs in submission order, and control events that generate data (such as
idle ticks) SHALL NOT overtake deliveries already submitted to the pool.

#### Scenario: Forward chain order

- **WHEN** a forward-only chain writes two batches to its sink
- **THEN** the sink observes them in source order

#### Scenario: Tick does not overtake pooled data

- **WHEN** a pooled chain receives an idle tick while earlier data deliveries are still being processed by workers
- **THEN** the tick's generated batches are sent downstream only after those earlier deliveries have been published

#### Scenario: Control fence completes while workers are healthy

- **WHEN** a barrier or watermark fence waits for the pool's in-flight results and every worker completes them
- **THEN** the fence returns without stalling, regardless of scheduling interleavings between the collector's notifications and the fence

### Requirement: Ack after sink write

A batch's source acknowledgement SHALL be triggered only after the terminal sink
of its chain writes successfully, matching the legacy at-least-once contract.

#### Scenario: Sink failure withholds ack

- **WHEN** a sink write fails for a batch carrying an ack
- **THEN** the ack is not invoked and the batch is retried or routed to the error path

### Requirement: Cancellation and drain

Vertex event loops SHALL stop on cancellation, drain their channels, forward EOS to downstream, and close owned components even on error paths. Every wait a chain performs on its own infrastructure — shipping a pooled delivery, flushing the worker pool before a control event, joining a retired pool, writing to its sink, and capturing a state snapshot — SHALL have a bounded wait or observe cancellation, so a chain SHALL NOT park silently with no error, no output, and no progress. A collector drain that exhausts its wait bound SHALL surface a failure for the chain (and therefore the checkpoint round) instead of reporting success, and an abandoned collector SHALL be joined or aborted before the chain's sink is closed, so no retired collector writes into a closed sink or publishes data after EOS. A sink write or state snapshot that exceeds its bound SHALL fail the chain (or the round) with an explicit timeout error naming the duration; a write cancelled by the timeout MAY have left partial effects on the external system, which at-least-once replay absorbs. The source-chain barrier branch's own infrastructure waits — capturing current source positions, snapshotting operator state, and shipping the barrier downstream — SHALL likewise observe cancellation: when the Job's cancellation token fires during those awaits, the barrier round SHALL fail per the existing round-failure semantics (previous valid checkpoint preserved, data plane proceeds) instead of hanging the branch on a downstream channel or a stalled snapshot.

#### Scenario: Cancellation closes components

- **WHEN** the Job's cancellation token fires
- **THEN** every vertex stops its loop, closes its operator/output, and the task set terminates without leaking spawned tasks

#### Scenario: A chain never parks without surfacing a failure

- **WHEN** a chain's worker pool, collector task, or upstream channel stops making progress
- **THEN** the chain either completes the wait or fails with an explicit error within its configured bound instead of blocking indefinitely

#### Scenario: A collector drain timeout fails the chain

- **WHEN** a chain's collector drain exceeds its wait bound and the chain proceeds to end-of-stream
- **THEN** the chain reports a drain failure instead of success, the abandoned collector is joined or aborted before the sink closes, and no downstream write or EOS-after-data ordering violation occurs

#### Scenario: A hung sink write fails the chain within a bound

- **WHEN** the chain's sink write (external system hung mid-write) exceeds its bound
- **THEN** the chain fails with an explicit timeout error naming the duration (settling its acknowledgements per the failure path), unblocking both the data plane and shutdown; any partial external effect is absorbed by at-least-once replay

#### Scenario: A hung state snapshot fails the round within a bound

- **WHEN** a state backend snapshot exceeds its bound during a barrier round
- **THEN** the snapshot fails with an explicit timeout error naming the duration, the round fails per the existing snapshot-failure semantics, and a late-completing background snapshot result is discarded without side effects

#### Scenario: Cancellation during the source-chain barrier branch unblocks the round

- **WHEN** the Job's cancellation token fires while the source chain is awaiting source positions, an operator snapshot, or barrier delivery inside the barrier branch
- **THEN** those awaits observe cancellation, the barrier round fails with an explicit error (previous valid checkpoint preserved), and the chain proceeds to its cancellation path instead of parking on the await

### Requirement: Worker pool failure SHALL fail the chain

A processor worker pool SHALL report every abnormal exit as a failure the chain observes, and a disconnected failure channel or an unexpectedly retired pool SHALL NOT be interpreted as a clean shutdown. When a pool is retired the chain SHALL cancel and join the remaining workers and collectors, SHALL settle every queued or in-flight delivery by acknowledging or aborting it, and SHALL keep its edge-ordering and checkpoint fences in force or fail the chain, so no delivery is silently discarded and no barrier is sealed over unsettled work.

#### Scenario: A worker panics

- **WHEN** a processor worker panics while handling a pooled delivery and the remaining workers or the collector exit as a result
- **THEN** the chain fails with an explicit error, the in-flight delivery is settled, and the checkpoint round is not reported as valid

#### Scenario: Every worker exits without recording a failure

- **WHEN** every worker and the collector exit and the failure channel disconnects without an error
- **THEN** the chain settles its queued deliveries and either continues with its ordering fences intact or fails explicitly, instead of dropping the pool and continuing

#### Scenario: Flush observes a stalled pool

- **WHEN** the chain flushes the pool before a tick, barrier, or watermark and a delivery cannot complete
- **THEN** the flush fails or returns within its bound so the chain reports the condition instead of stopping silently

### Requirement: Checkpoint frontier capture SHALL fail closed

A checkpoint round SHALL fail, not seal, when it cannot capture a consistent frontier: a chain whose source-position snapshot errors SHALL fail the round instead of seeding stale or empty positions, queued snapshot errors SHALL be consumed and reported before the round can complete successfully, and a round whose execution graph is failing SHALL be invalidated before its manifest is persisted rather than sealed with a missing participant.

#### Scenario: Source positions fail during a round

- **WHEN** `current_positions()` for a source chain returns an error during checkpoint capture
- **THEN** the round fails with that error instead of sealing a checkpoint whose positions are empty or from a previous round

#### Scenario: A queued snapshot error cannot be bypassed

- **WHEN** a chain's snapshot report and its error are both pending and the report arrives last but the error was queued for the same round
- **THEN** the round does not return success until the error queue has been drained, and the checkpoint does not persist as successful

#### Scenario: A failing graph does not seal a partial manifest

- **WHEN** a chain exits with an error during a checkpoint round and has not delivered a snapshot for that round
- **THEN** the round is invalidated before persist instead of being sealed with that participant missing

### Requirement: Stateful and join chains SHALL reject explicitly configured parallelism above one
Graph 构建 SHALL 对「用户显式配置了大于 1 的 processor 并行度」且「链上含 join 或有状态（keyed/windowed）算子」的组合构建期拒绝，错误信息 MUST 指名该链与约束原因（单链串行是 join 双侧锁序与 keyed 状态一致性的前提）。未显式配置并行度时该类链默认单并行度，行为不变。系统 MUST NOT 静默丢弃用户显式配置的并行度值。

#### Scenario: 显式并行度与 join 同链被拒绝
- **WHEN** 用户为含 join 算子的链显式配置 `__arkflow_processor_parallelism > 1` 并构建 Job 图
- **THEN** 构建返回错误，指名该链含 join 算子且必须单并行度，而非静默按 1 执行

#### Scenario: 未配置时默认单并行度不变
- **WHEN** 含 keyed/windowed 有状态算子的链未显式配置并行度
- **THEN** 该链以并行度 1 构建，与既有行为一致

### Requirement: Input channel closure SHALL be signaled by a dedicated error variant
源链读取循环因输入通道关闭而停止时 SHALL 通过专用 `Error` variant 表达（而非把该状态折叠进通用消息字符串），下游控制流 MUST 按该 variant 匹配，MUST NOT 对通用错误消息做字符串内容比较来识别该状态。

#### Scenario: 通道关闭被结构化识别
- **WHEN** 某链的输入通道关闭且事件循环处理该错误
- **THEN** 该状态以专用 variant 匹配并走既有流控分支，同族代码无需字符串比较


### Requirement: Internal module reorganization SHALL preserve the kernel's public execution surface
执行内核内部模块结构的重组（文件/目录拆分、内部可见性调整、测试专用兼容壳删除）SHALL 保持 `arkflow_core::executor` 对外导出的执行入口集合与语义不变：`run_job`、`ExecutionGraphBuilder`、`Envelope`、`Chain`、`StreamJobAdapter` 等公共项的路径与签名 SHALL 不因内部重组而变化，内核全部测试 SHALL 在重组后原样通过。

#### Scenario: 内部文件拆分后外部引用不受影响
- **WHEN** executor 内部实现从单文件重组为目录模块（如 `remote.rs` → `remote/`、`window.rs` → `window/`）
- **THEN** crate 外部对 `arkflow_core::executor::*` 公共项的既有 `use` 路径无需修改即可编译

#### Scenario: 测试专用兼容壳删除不改变执行路径
- **WHEN** 仅供测试使用的 no-op 兼容壳（如带 gate 参数但不使用 gate 的 `run_graph_*` 包装）被删除且测试改用真实入口
- **THEN** 生产执行路径与测试断言语义不变，全部内核测试通过

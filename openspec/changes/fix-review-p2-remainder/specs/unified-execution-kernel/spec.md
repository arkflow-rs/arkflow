# unified-execution-kernel Delta

## ADDED Requirements

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

## MODIFIED Requirements

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

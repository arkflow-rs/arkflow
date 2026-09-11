# Tasks: Rebuild Unified Streaming Engine Kernel

## 1. Executor contracts (envelope, channel, graph)

- [x] 1.1 Create `crates/arkflow-core/src/executor/` module with `Envelope` (Data/Barrier/Watermark/EOS), bounded channel edge type (flume, capacity configurable, default 1024), and cancellation wiring.
- [x] 1.2 Implement `ExecutionGraph`/`ExecutionVertex`/`ChainSpec` types and `ExecutionGraphBuilder` that compiles a `JobPlan` (all tasks or an assigned subset) into vertices + edges.
- [x] 1.3 Implement operator chain fusion rule: single-in single-out, non-stateful, non-window, forward partition → same chain; zero-channel hops inside a chain.
- [x] 1.4 Implement per-vertex `TaskRunner` event loop: recv → chain process → route downstream (bounded backpressure); handle EOS drain and cancellation; sink vertex writes then acks.
- [x] 1.5 Implement partitioned edge routing (key hash → downstream subtask channel) and broadcast edge routing.
- [x] 1.6 Unit tests: chain fusion decisions, envelope ordering per edge, backpressure blocks upstream when bounded channel is full, EOS propagates and shuts down downstream.

## 2. Async barrier checkpointing

- [x] 2.1 Implement in-process `BarrierCoordinator`: periodic barrier injection at sources, collection of `TaskCheckpointAck`, completion via existing `CheckpointCoordinator` (`checkpoint.rs`).
- [x] 2.2 Implement barrier alignment at multi-input vertices (buffer other inputs until all barriers arrive; bounded alignment buffer with fail-checkpoint cap).
- [x] 2.3 Implement per-chain async state snapshot (Arrow IPC keyed state + source positions + watermark) that does not block data flow.
- [x] 2.4 Wire ack semantics: source positions captured when the barrier passes the source; WAL cursor falls back for Jobs without checkpoints (checkpoint-priority recovery order documented and tested).
- [x] 2.5 Unit tests: barrier does not stall processing (data continues during snapshot), alignment correctness with two inputs, stale-generation barrier rejection, checkpoint completes only after all vertices ack.
- [x] 2.6 Failure-injection tests: snapshot failure → checkpoint Failed and data continues; recovery replays from checkpoint source positions (reuses `RecoveryPlan`).

## 3. Columnar window operator

- [x] 3.1 Implement `WindowOperator` (processor type `window`): vectorized tumbling window assignment via Arrow compute (`div_euclid` on Int64/Timestamp column), batch grouped by window (no per-row batch copies).
- [x] 3.2 Implement keyed window aggregation state: `(namespace=operator_id, key=window_start||key)` entries in `StateBackend`, aggregate buffers serialized via Arrow IPC.
- [x] 3.3 Implement watermark trigger (emit aggregate when watermark ≥ window_end) and processing-time trigger mode (interval-based emit, matches legacy buffer batching semantics).
- [x] 3.4 Wire late-event policy (Drop/Route/Update) via existing `event_time::WindowAction` decisions and route operators.
- [x] 3.5 Implement sliding and session window assignment after tumbling is proven.
- [x] 3.6 Unit tests: window assignment edge cases (negative timestamps, window boundary), aggregation correctness across batches, trigger timing in both modes, state restore produces identical aggregates, late events per policy.

## 4. StreamConfig compiler

- [x] 4.1 Implement `stream_compiler.rs`: deterministic `StreamConfig → JobSpec` mapping (input→source, processors→Map chain, error_output→error edge, durability→source WAL, temporary passthrough, id/version derivation).
- [x] 4.2 Map buffer plugins: `memory`→no-op, `tumbling/sliding/session`→WindowOperator processing-time mode, `join`→compile-time error with migration message.
- [x] 4.3 Add `jobs` field to `EngineConfig` (`#[serde(default)]`) with validation.
- [x] 4.4 Golden compilation tests for every `examples/*.yaml` shape; `--validate` accepts both streams and jobs (deep validation incl. job graph checks).
- [x] 4.5 Regression: every runnable example produces equivalent output under the unified kernel (window batching aligned to processing-time mode).

## 5. Runtime integration (local + Agent)

- [x] 5.1 Engine executes compiled streams + declared jobs through the unified kernel with an embedded BarrierCoordinator per checkpointed Job. (Streams compile and run via `RuntimeManager::start` → `run_job`; YAML `jobs` run end-to-end. Local interval checkpoints and Agent command-driven barriers use the same `KernelJobHandle` path.)
- [x] 5.2 `RuntimeManager`/`RuntimeEntry` carry Job runtime state; health/metrics endpoints report kernel counters (in-flight batches, watermark lag, checkpoint duration).
- [x] 5.3 Agent `JobRuntime::start` rebuilds local subgraphs via `ExecutionGraphBuilder::build_subgraph`; generation fencing and recovery flow (agent.rs) preserved. (`spawn_kernel_job` + `KernelJobHandle`; kernel snapshots preferred; lifecycle test in `arkflow-server/tests/kernel_job_lifecycle.rs`.)
- [x] 5.4 Remove `SingleComputeJobRunner` execution path; migrate its review-fix test suite (generation isolation, recovery validation, fence semantics) to the kernel.
- [x] 5.5 Remove `stream/mod.rs` executor (`Stream::run` etc.); `StreamConfig` type moves to the compiler module; deprecated buffer plugins emit compile warnings.
- [x] 5.6 Multi-node smoke: two-node Hub–Agent Job with barrier checkpoint + kill/restart recovery passes (`arkflow-server/tests/two_node_job_smoke.rs`).

## 6. Observability and docs

- [x] 6.1 Kernel metrics: per-vertex throughput/latency, channel backlog, checkpoint duration/failures, watermark lag, late-event counters (expose via RuntimeMetrics snapshot). (`executor::metrics` KernelMetrics/ChainMetrics + snapshot; Agent reports and Hub/local metrics endpoints expose the runtime counters.)
- [x] 6.2 Update CLAUDE.md / docs: single kernel, Stream-as-compiled-Job narrative, breaking changes and migration notes.
- [x] 6.3 Sync OpenSpec specs (`stream-backpressure`, `stream-runtime-control`, `input-durability` wording) to unified-kernel acceptance criteria.
- [x] 6.4 Performance baseline: throughput/latency benchmark comparing legacy Stream runtime vs unified kernel (generate→json_to_arrow→sql→drop, 200k rows batch=1000: kernel 528ms vs legacy 559ms, ratio 0.94 — kernel 6% faster; `kernel_perf_baseline.rs`, run with --ignored). kafka/window variants to extend.

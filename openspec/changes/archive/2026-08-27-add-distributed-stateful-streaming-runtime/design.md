## Context

ArkFlow currently builds each configured Stream as one local runtime. The runtime uses bounded Flume channels, a configurable number of local processor workers, sequence-based ordered output, and an optional input WAL. The Hub and Agent already provide durable desired state, node leases, generation fencing, rollout orchestration, and observed-state reporting for Fleet operations.

The new runtime must support data-platform workloads that need keyed state, event-time processing, failure recovery, and high parallelism. It must also avoid changing the established YAML Stream behavior while the distributed runtime is developed and validated.

## Goals / Non-Goals

**Goals:**

- Define a new Job runtime with explicit DAG, task, subtask, partition, key-group, and checkpoint identities.
- Provide deterministic event-time and watermark semantics for stateful operators.
- Provide a local embedded state backend with durable object-store checkpoints and savepoints.
- Use the existing single Hub/multiple Compute boundary to deploy, observe, recover, rebalance, and upgrade Jobs.
- Define a SQL-first Job API with Rust extension points and a stable runtime protocol.
- Keep existing YAML Streams, local CLI behavior, input WAL, and connector contracts operational.

**Non-Goals:**

- Rewriting the existing Stream runtime or converting every YAML Stream into a Job.
- Multi-Hub consensus, global scheduling, or cross-cluster federation.
- PostgreSQL wire compatibility, materialized-view serving, and full streaming-database semantics.
- Making object storage the per-record hot state database.

## Decisions

### 1. Add a parallel Job runtime instead of rewriting Stream

The existing `Stream` remains the compatibility runtime. A new Job layer owns distributed execution and can reuse Input, Output, Processor, Arrow batch, and plugin registration abstractions through adapters. This limits migration risk and allows correctness benchmarks to compare old and new paths.

Alternatives considered:

- Rewrite `Stream` in place: rejected because it couples compatibility changes with a new distributed protocol.
- Keep only the existing runtime and add remote lifecycle commands: rejected because lifecycle control does not provide task ownership, partition movement, or state recovery.

### 2. Use explicit partitioned DAG execution

The Job plan is compiled into immutable operator nodes and edges. Each stateful operator is divided into subtasks by key-group/partition. The Hub assigns task attempts to Compute nodes; Compute nodes own execution, local state, and checkpoint participation. Partition ownership is fenced by Job generation and task attempt ID.

The first scheduling model is deterministic assignment plus bounded rebalancing. It does not require a general-purpose scheduler or cross-node consensus.

### 3. Define event time as a runtime protocol

Sources declare the event timestamp field and watermark strategy. Each input partition reports watermark progress; an operator watermark is the minimum eligible upstream watermark. Window closure, allowed lateness, late-event routing, and idle partition handling are explicit Job settings. Processing-time behavior remains available for low-latency jobs that do not require deterministic event-time results.

### 4. Separate hot state, input durability, and recovery snapshots

The state backend exposes keyed get/put/delete, namespace, iteration, TTL, snapshot, and restore operations. A local embedded KV backend is the initial implementation for low-latency random access and disk spill. Checkpoints and savepoints are immutable manifests plus state files in object storage. Existing input WAL remains responsible for input replay and output acknowledgement; it is not reused as the operator-state format.

Checkpoint completion requires a consistent source position, watermark, in-flight barrier position, operator state snapshot, and task assignment epoch. Recovery restores the latest valid checkpoint and replays source data after the recorded positions.

### 5. Make SQL compile to a Job specification

SQL DDL defines sources, sinks, schema, key, timestamp, watermark, window, and recovery options. The compiler produces a validated Job specification and an explainable physical plan. Rust UDF/UDAF adapters are explicit plan nodes with declared determinism and state requirements. SQL compilation errors are returned before deployment; runtime failures are reported through the existing operation/convergence model.

### 6. Extend the Hub contract without removing existing resources

Hub persistence gains Job, JobVersion, TaskAssignment, Checkpoint, Savepoint, and JobObservation records. Job deployments use desired/observed/convergence state and generation fencing already used for Streams. Existing Node, Stream, Config, Operation, Audit, and Rollout routes remain valid. Job operations are added as versioned API resources rather than overloading Stream endpoints.

### 7. Use object storage as the production checkpoint boundary

Object storage is the shared recovery boundary across Compute nodes and supports node replacement, savepoints, and upgrades. Local state is treated as a cache/working copy and may be discarded after a valid checkpoint. The checkpoint manifest includes format version, Job version, operator IDs, key-group ranges, source positions, watermark state, and checksums.

Alternatives considered:

- Memory-only state: rejected because process failure loses state and limits window size.
- Direct object-store reads/writes for each state mutation: rejected because random update latency and request cost are incompatible with high-throughput keyed state.
- External distributed KV as the first backend: deferred because it adds an operational dependency before the state/checkpoint protocol is stable.

## Risks / Trade-offs

- **[Risk]** Two runtimes increase maintenance and documentation cost. → Keep shared component adapters narrow, define explicit compatibility boundaries, and add parity tests for supported connectors.
- **[Risk]** Incorrect watermark or checkpoint ordering can silently produce wrong results. → Make progress and barrier state observable, add deterministic replay tests, and fail closed on incomplete snapshots.
- **[Risk]** Local state can become too large for a Compute node. → Support disk-backed state, state-size metrics, TTL, bounded backends, and checkpoint-based reassignment before enabling aggressive rebalancing.
- **[Risk]** Object-store outages delay checkpoints and recovery. → Separate processing from checkpoint retries, enforce bounded checkpoint age, retain the last valid checkpoint, and expose degraded readiness.
- **[Risk]** State schema changes can make upgrades unrecoverable. → Version state namespaces and manifests, require savepoint compatibility checks, and block rollout when migration is unavailable.
- **[Risk]** High-throughput multi-node execution may expose network Shuffle bottlenecks. → Establish partitioned benchmark scenarios before promising scale targets and keep transport pluggable.

## Migration Plan

1. Introduce Job and state contracts behind new modules without changing YAML Stream startup.
2. Implement a single-Compute Job runner using the same input/output adapters and local checkpoint cycle.
3. Add Hub/Agent Job registration and observation while keeping existing Stream reconciliation unchanged.
4. Enable multi-Compute task assignment, checkpoint recovery, and bounded rebalancing behind an explicit distributed Job mode.
5. Add SQL compilation and Console/API workflows after the runtime protocol is validated.
6. Roll back by stopping Job deployments and restoring from the latest savepoint; existing Streams continue using their current lifecycle.

## Open Questions

- Which embedded KV implementation should be selected after state workload benchmarks: existing `redb`, RocksDB, or another Rust-native backend?
- Which object-store checkpoint format should be stabilized first: one manifest per Job checkpoint or a table-oriented state-file layout?
- Which initial operators are required for the first production slice: keyed aggregate, tumbling/sliding window, stream-stream Join, and async lookup?

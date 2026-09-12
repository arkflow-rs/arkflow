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

Vertex event loops SHALL stop on cancellation, drain their channels, forward EOS to downstream, and close owned components even on error paths. Every wait a chain performs on its own infrastructure — shipping a pooled delivery, flushing the worker pool before a control event, and joining a retired pool — SHALL have a bounded wait or observe cancellation, so a chain SHALL NOT park silently with no error, no output, and no progress.

#### Scenario: Cancellation closes components

- **WHEN** the Job's cancellation token fires
- **THEN** every vertex stops its loop, closes its operator/output, and the task set terminates without leaking spawned tasks

#### Scenario: A chain never parks without surfacing a failure

- **WHEN** a chain's worker pool, collector task, or upstream channel stops making progress
- **THEN** the chain either completes the wait or fails with an explicit error within its configured bound instead of blocking indefinitely

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


# unified-execution-kernel Specification

## ADDED Requirements

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

- **WHEN** a JobSpec sets a channel capacity
- **THEN** edges are constructed with that capacity, defaulting to 1024

### Requirement: Ordered delivery per edge

Data envelopes on one edge SHALL be delivered in send order; forward chains
SHALL preserve end-to-end batch order from source to sink.

#### Scenario: Forward chain order

- **WHEN** a forward-only chain writes two batches to its sink
- **THEN** the sink observes them in source order

### Requirement: Ack after sink write

A batch's source acknowledgement SHALL be triggered only after the terminal sink
of its chain writes successfully, matching the legacy at-least-once contract.

#### Scenario: Sink failure withholds ack

- **WHEN** a sink write fails for a batch carrying an ack
- **THEN** the ack is not invoked and the batch is retried or routed to the error path

### Requirement: Cancellation and drain

Vertex event loops SHALL stop on cancellation, drain their channels, forward EOS
to downstream, and close owned components even on error paths.

#### Scenario: Cancellation closes components

- **WHEN** the Job's cancellation token fires
- **THEN** every vertex stops its loop, closes its operator/output, and the task set terminates without leaking spawned tasks

## REMOVED Requirements

### Requirement: Recursive single-batch dispatch

The engine SHALL NOT execute a Job by recursively dispatching one batch through
the whole DAG before reading the next batch (legacy `SingleComputeJobRunner`
behavior).

*Rationale: replaced by pipelined vertices; see pipelined vertex execution.*

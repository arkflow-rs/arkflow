# columnar-window-operators Specification

## ADDED Requirements

### Requirement: Vectorized window assignment

Window assignment SHALL be computed with Arrow columnar operations over entire
batches (window start derived by integer division on the timestamp column), never
by rebuilding a RecordBatch per row.

#### Scenario: Batch-wide assignment

- **WHEN** a batch of 1000 rows passes a tumbling window operator
- **THEN** window assignment performs O(1) full-column computations and groups rows by window without 1000 batch copies

#### Scenario: Boundary and negative timestamps

- **WHEN** event timestamps fall exactly on a window boundary or are negative
- **THEN** assignment follows `div_euclid` semantics so every row lands in exactly one deterministic window

### Requirement: Keyed window aggregation state

Window aggregates SHALL be keyed by `(namespace=operator_id, key=window_start||key)`
in the state backend, with aggregate buffers serialized via Arrow IPC, and SHALL
survive restart by restore.

#### Scenario: Aggregate across batches

- **WHEN** two batches contain rows for the same key and window
- **THEN** the stored aggregate merges both batches and the emitted result reflects the union

#### Scenario: Restore reproduces aggregates

- **WHEN** a Job restarts from a checkpoint containing window state
- **THEN** subsequent watermark-triggered emissions include pre-restart partial aggregates

### Requirement: Dual trigger modes

Window operators SHALL support event-time triggering (emit when watermark passes
window end) and processing-time triggering (emit on interval), with
processing-time mode as the compatibility mode for compiled legacy streams.

#### Scenario: Watermark trigger

- **WHEN** the watermark advances to or beyond a window's end
- **THEN** the operator emits the aggregate for that window downstream

#### Scenario: Processing-time trigger

- **WHEN** a window operator runs in processing-time mode with a 5s trigger
- **THEN** it emits aggregates on the 5s cadence regardless of watermark state, matching legacy buffer batching behavior

### Requirement: Late event policy

Windows SHALL apply the configured late-event policy (Drop/Route/Update) using
the shared event-time decision function, routing to the declared route operator
when configured.

#### Scenario: Route policy

- **WHEN** an event arrives after its window closed beyond allowed lateness with policy Route
- **THEN** the event is forwarded to the configured route operator marked as a late event

### Requirement: Window kinds

Tumbling windows SHALL be the first-class implementation; sliding and session
windows SHALL follow the same assignment/state/trigger contracts.

#### Scenario: Sliding membership

- **WHEN** a sliding window has size 10m and slide 2m
- **THEN** one event contributes to the windows whose intervals contain its timestamp

### Requirement: Window semantics live in operators

`event-time-processing` window semantics SHALL be enforced by window operators
inside the execution kernel rather than by buffer-layer plugins.

#### Scenario: Legacy window buffer maps to operator

- **WHEN** a StreamConfig declares `buffer: tumbling_window` and is compiled
- **THEN** the resulting JobSpec contains a window operator in processing-time mode and no buffer plugin is instantiated

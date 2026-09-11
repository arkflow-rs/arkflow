## MODIFIED Requirements

### Requirement: Keyed window aggregation state

Window aggregates SHALL be keyed by `(namespace=operator_id, key=window_start||key)`
in the state backend, with aggregate buffers serialized via Arrow IPC, and SHALL
survive restart by restore. A window with zero value observations SHALL NOT emit
a fabricated aggregate row, and a fired batch's `sum`/`min`/`max` columns SHALL
use the widest numeric kind across the fired buffers (Int64 < Float32 < Float64)
so aggregates widen instead of truncating.

#### Scenario: Aggregate across batches

- **WHEN** two batches contain rows for the same key and window
- **THEN** the stored aggregate merges both batches and the emitted result reflects the union

#### Scenario: Restore reproduces aggregates

- **WHEN** a Job restarts from a checkpoint containing window state
- **THEN** subsequent watermark-triggered emissions include pre-restart partial aggregates

#### Scenario: Null-only windows never fabricate aggregates

- **WHEN** a window's rows all carry NULL values and the window fires next to aggregates that observed values
- **THEN** no `count=0` zero-sentinel row is emitted for that window and the other aggregates keep their numeric type

#### Scenario: Fired output keeps the widest numeric kind

- **WHEN** fired buffers of different numeric kinds land in one fired batch
- **THEN** the output columns use the widest kind and integer contributions widen losslessly instead of truncating float sums

### Requirement: Late event policy

Windows SHALL apply the configured late-event policy (Drop/Route/Update) using
the shared event-time decision function, routing to the declared route operator
when configured. A row membership whose window end is already behind the
operator's watermark frontier and whose buffer was already fired and cleaned
SHALL NOT be re-opened as a fresh aggregate; it SHALL be treated as a late
membership instead.

#### Scenario: Route policy

- **WHEN** an event arrives after its window closed beyond allowed lateness with policy Route
- **THEN** the event is forwarded to the configured route operator marked as a late event

#### Scenario: Released row never re-opens a fired window

- **WHEN** an unmarked row reaches the window operator after the operator's frontier already fired and cleaned its window
- **THEN** the membership is skipped as late and the window is not re-emitted as a fresh initial result

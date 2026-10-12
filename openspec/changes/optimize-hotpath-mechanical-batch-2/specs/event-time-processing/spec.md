## MODIFIED Requirements

### Requirement: Event-time gates SHALL classify current rows consistently
The gate SHALL evaluate current rows against a watermark that is consistent with the batch's recorded progress. Rows that become late because the batch advances the watermark SHALL receive the configured Drop, Route, or Update action, while future rows MAY remain held. At end-of-stream the gate SHALL classify each held row per window membership against that window's real lateness deadline derived from the last observed watermark; a sentinel end-of-stream watermark SHALL NOT by itself exclude memberships that are still within their deadline. Row decisions SHALL be produced without per-row heap allocations for the common bounded-membership case (bounded inline storage), and the produced decisions — memberships, actions, ordering, and error behavior — SHALL be identical to the heap-allocated representation.

#### Scenario: A batch contains a future row and an old row

- **WHEN** a first batch contains event times `[2100, 100]` for a `[0,1000)` window and the batch advances the watermark beyond that window
- **THEN** the old row is classified by the configured late-event policy and only the future row remains held for a later window

#### Scenario: End-of-stream does not truncate live sliding memberships

- **WHEN** the source ends while a held sliding row has one membership past its deadline and one membership still within its deadline
- **THEN** the still-live membership receives the row under the same Update/Drop/Route policy as mid-stream, and the emitted aggregates are not permanently truncated by the end-of-stream classification

#### Scenario: Inline decision storage preserves classification

- **WHEN** a batch is classified through the inline-storage decision path and through the previous heap-allocated path on identical input
- **THEN** every row's memberships, actions, and the batch's gate decision are identical, including end-of-stream and late-event policy branches

## ADDED Requirements

### Requirement: Physical partition splitting SHALL assign rows identically with per-distinct-partition identity

The event-time gate's physical partition split SHALL build each distinct partition's identity (topic or source-derived identifier) at most once per batch instead of formatting per row, SHALL group row indices in a single pass preserving each partition's first-appearance order, and SHALL materialize each non-empty partition's rows with one gather; row-to-partition assignment, the partition identities, and the output group order SHALL be identical to the previous per-row construction.

#### Scenario: Split output equals the per-row baseline

- **WHEN** a batch with rows spanning several topics and source identities is split through the per-distinct-partition path and through the previous per-row path
- **THEN** the partition identities, row membership per partition, and group order are identical

#### Scenario: Identity construction does not scale with rows

- **WHEN** a large batch maps to a small number of distinct partitions
- **THEN** partition-identity construction (including the source identifier formatting) happens once per distinct partition per batch, not once per row

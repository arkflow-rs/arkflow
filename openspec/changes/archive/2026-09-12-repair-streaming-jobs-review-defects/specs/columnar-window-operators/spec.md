## MODIFIED Requirements

### Requirement: Keyed window aggregation state

Window aggregates SHALL be keyed by `(namespace=operator_id, key=window_start||key)` in the state backend, SHALL survive restart by restore, SHALL NOT emit a fabricated aggregate row for a window with zero value observations, and SHALL use the widest numeric kind across the fired buffers (Int64 < Float32 < Float64) for the emitted `sum`/`min`/`max` columns. An aggregate buffer SHALL track integer and float observations independently, SHALL seed each representation's min/max only from that representation's own first observation, SHALL fold every observed contribution into the widened aggregate, and SHALL NOT derive a boundary from an untouched representation's default value. A buffer decoded from persisted state, including state written before the observation counters existed and buffers migrated from the legacy aggregate format, SHALL carry observation counters consistent with its accumulated `count` and min/max before the next observation is applied.

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

#### Scenario: Float observations precede integer observations

- **WHEN** a window's first delivery carries a Float64 value of 99.5 and a later delivery for the same key and window carries an Int64 value of 100
- **THEN** the emitted aggregate reports min 99.5 and max 100 in the widened float representation, with no `0` boundary fabricated from the integer side's unseeded default

#### Scenario: Restored buffer keeps its range before the next observation

- **WHEN** a window buffer is restored from persisted state that predates the observation counters and a new float observation arrives
- **THEN** the restored min/max range is preserved and the new value extends it instead of replacing it

#### Scenario: Legacy contribution survives migration

- **WHEN** a buffer in the legacy aggregate format carries a non-empty float sum next to an integer aggregate and is migrated to the typed format
- **THEN** the migrated buffer retains that float contribution and its accumulated min/max when it later merges with new observations

## ADDED Requirements

### Requirement: Window aggregate observations SHALL be counted per representation

The columnar window operator SHALL maintain a per-representation observation count for each aggregate buffer, SHALL derive min/max from each representation's own observations, SHALL keep the observation counts consistent with the buffer's accumulated count across observation, merge, migration, and decode, and SHALL reject a migration that cannot preserve an accumulated contribution instead of emitting a silently truncated aggregate.

#### Scenario: Migration cannot preserve a legacy float aggregate

- **WHEN** a legacy buffer reports a float aggregate whose min/max were stored as integer sentinels
- **THEN** the operator refuses to migrate that group and reports the failure rather than restoring it as a corrupted integer aggregate

#### Scenario: Observation counts stay consistent

- **WHEN** a buffer observes values, merges with another buffer, or is decoded from persisted state
- **THEN** the sum of its per-representation observation counts equals its accumulated count

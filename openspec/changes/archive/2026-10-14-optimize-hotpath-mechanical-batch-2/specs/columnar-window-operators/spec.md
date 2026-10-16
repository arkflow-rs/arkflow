## MODIFIED Requirements

### Requirement: Keyed window aggregation state

Window aggregates SHALL be keyed by `(namespace=operator_id, key=window_start||key)` in the state backend, SHALL survive restart by restore, SHALL NOT emit a fabricated aggregate row for a window with zero value observations, and SHALL use the widest numeric kind across the fired buffers (Int64 < Float32 < Float64) for the emitted `sum`/`min`/`max` columns. An aggregate buffer SHALL track integer and float observations independently, SHALL seed each representation's min/max only from that representation's own first observation, SHALL fold every observed contribution into the widened aggregate, and SHALL NOT derive a boundary from an untouched representation's default value. A buffer decoded from persisted state, including state written before the observation counters existed and buffers migrated from the legacy aggregate format, SHALL carry observation counters consistent with its accumulated `count` and min/max before the next observation is applied. Narrow-integer value columns (Int8/Int16/Int32/UInt*) SHALL be normalized to Int64 at most once per batch (before the row loop), never by re-casting the whole column inside the per-row loop. In-memory window keys SHALL materialize the key string once per row into an ARC-shared key so per-membership reuse is a reference-count bump, and each row's accumulation into a window SHALL probe the buffer map once (entry API), with the persisted `state_key` byte layout (`window_start.to_be_bytes() ++ key bytes`) unchanged. Error-rollback and fired-ack compensation images SHALL be captured as before-images of exactly the window slots a batch or firing touches (including an absent-marker for slots created by the operation) instead of cloning the whole open-window map per batch; restoration SHALL apply touched-slot images so the resulting state is identical to a whole-map snapshot-restore, and per-batch snapshot cost SHALL NOT grow with the number of open windows.

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

#### Scenario: Narrow integer value columns normalize once per batch

- **WHEN** a window accumulation batch carries an Int32 value column with many rows (valid and null mixed)
- **THEN** the column is cast to Int64 once for the whole batch, aggregates match per-row casting exactly (nulls skipped), and per-batch cost does not grow quadratically with row count

#### Scenario: Failed accumulation leaves untouched windows untouched

- **WHEN** an accumulation batch touches windows A and B and fails partway, with open windows C and D never referenced by the batch
- **THEN** restore rolls A and B back to their exact before-images (removing slots that did not exist before) and C and D keep their prior state bit-for-bit

#### Scenario: Fired-ack compensation rewinds only touched windows

- **WHEN** a fired batch's downstream acknowledgement fails and window state is compensated
- **THEN** only the windows that contributed to the fired batch are rewound or re-created to their pre-firing images, and other open windows are unaffected

#### Scenario: Persisted state key layout is unchanged

- **WHEN** state written by the previous implementation is restored after the change (and vice versa)
- **THEN** windows restore with identical keys and aggregates, because the persisted `window_start || key` byte layout did not change

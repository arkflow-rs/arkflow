## ADDED Requirements

### Requirement: One `write_batch` SHALL be one atomic Iceberg append
The `iceberg` output SHALL treat one `write_batch(&[MessageBatchRef])` call as a single Iceberg transaction: every input batch of the call is concatenated, encoded into Parquet data files, and committed to the table as one fast-append snapshot. The call SHALL return `Ok` only after the catalog has accepted the commit. Any failure SHALL return `Err` without a partial commit, so the source acknowledgement is withheld and the acknowledgement range is replayed. The single-message `write` path SHALL commit exactly one transaction for that message.

#### Scenario: Multiple input batches commit together
- **WHEN** `write_batch` receives three input batches and the second one fails to encode
- **THEN** no data file from the call is committed, the call returns `Err`, and the table contains no row from any of the three batches

#### Scenario: Commit completes before acknowledgement
- **WHEN** a `write_batch` call returns `Ok`
- **THEN** the rows of the call are visible in a new snapshot of the table before the kernel settles the source acknowledgement

#### Scenario: Empty call does not commit
- **WHEN** `write_batch` receives only empty batches, or every row is removed by replay filtering
- **THEN** the call returns `Ok` without creating a data file or a snapshot

### Requirement: The output SHALL hold no uncommitted state across calls
The output SHALL NOT buffer rows, keep data files open, or run commit timers across `write_batch` calls. After a call returns, the output SHALL hold no row that is not committed to the table. Commit cadence and file size SHALL follow the batches the upstream pipeline delivers. `close()` SHALL have nothing to flush.

#### Scenario: Close after successful writes
- **WHEN** `close()` is called after successful `write_batch` calls
- **THEN** no upload or commit is performed and `close()` returns `Ok`

#### Scenario: A parallel commit pipeline is not introduced
- **WHEN** two `write_batch` calls are made in sequence
- **THEN** the second call starts only after the first has committed, and at most one transaction is in flight

### Requirement: High commit rate SHALL produce a warning
The output SHALL emit a warning when ten consecutive commits complete within two seconds, at most once per minute, recommending a `batch` processor in front of the output. The warning SHALL NOT alter delivery.

#### Scenario: Burst of tiny commits warns once
- **WHEN** ten commits complete within two seconds and ten more complete within the following minute
- **THEN** exactly one warning that names the `batch` processor is emitted in that minute

#### Scenario: Normal cadence stays silent
- **WHEN** commits are spaced at least one second apart
- **THEN** no commit-rate warning is emitted

### Requirement: Build SHALL be side-effect free
Building the output from configuration SHALL NOT open a network connection or contact the catalog or storage. Catalog reachability, table existence, support for the table's partition spec, and watermark recovery SHALL run in `connect()`; schema compatibility is evaluated for every batch. A configured `codec` SHALL be rejected at build time with a configuration error.

#### Scenario: Configuration validates offline
- **WHEN** `arkflow --config <file> --validate` runs on a configuration that declares an `iceberg` output and no catalog is reachable
- **THEN** validation succeeds

#### Scenario: Codec is rejected at build time
- **WHEN** the output configuration sets a `codec`
- **THEN** the build returns `Error::Config` naming the `codec` field

#### Scenario: Missing table fails at connect
- **WHEN** `connect()` runs and the configured namespace or table does not exist
- **THEN** `connect()` returns `Error::Process` naming the namespace and table, and no `write_batch` call is accepted

### Requirement: Exactly-once SHALL be opt-in and validated at build time
`exactly_once` SHALL default to `false`, in which case the output delivers at-least-once and performs no replay filtering. When `exactly_once` is `true`, a non-empty `writer_id` SHALL be required.

#### Scenario: Missing writer id fails the build
- **WHEN** the output is configured with `exactly_once: true` and no `writer_id`
- **THEN** the build returns `Error::Config` stating that `writer_id` is required for exactly-once delivery

#### Scenario: Default delivery is at-least-once
- **WHEN** `exactly_once` is not configured and an acknowledged range is replayed after a crash
- **THEN** the replayed rows are written again and the table contains duplicates

### Requirement: Replay SHALL be deduplicated by source position
With `exactly_once: true` the output SHALL keep a committed-through offset watermark per `(topic, partition)`. For every row it SHALL read the Kafka source position from `__meta_ext.topic`, `__meta_partition`, and `__meta_offset`; a row whose offset is below the watermark of its `(topic, partition)` SHALL be dropped as already committed, and every other row SHALL be written. The watermark after the commit — each present `(topic, partition)` advanced to its highest written offset plus one, other entries preserved — SHALL be stored atomically with the appended data files in the same snapshot, in the snapshot summary property `arkflow.writer.<writer_id>.offsets`, whose value is a JSON object mapping `"<topic>#<partition>"` to the exclusive next offset. Rows expanded from one Kafka record share one position and SHALL be kept or dropped together; this requires such rows to reach the same `write_batch` call (see the preconditions in `exactly-once-output`).

#### Scenario: Commit then crash before acknowledgement
- **WHEN** a commit succeeds, the process restarts before the source acknowledgement, and the same records are replayed
- **THEN** the replayed call returns `Ok` without creating a snapshot and the row count of the table is unchanged

#### Scenario: Replay with different batch boundaries
- **WHEN** the records of a committed range are replayed grouped into differently sized batches
- **THEN** the row count of the table is unchanged

#### Scenario: Replay partially covered by the watermark
- **WHEN** a replay delivers offsets 100 to 200 of a partition and the watermark for that partition is 151
- **THEN** only offsets 151 to 200 are written and the watermark advances to 201

#### Scenario: One record expanded into several rows
- **WHEN** a Kafka record is decoded into three rows and its offset is below the watermark
- **THEN** all three rows are dropped

### Requirement: Source position columns SHALL be mandatory under exactly-once
With `exactly_once: true`, a `write_batch` whose rows lack any of the three position columns, or whose position values are null, SHALL fail with `Error::Process` naming the missing column and explaining that a processor dropped the `__meta_*` columns. Nothing SHALL be written or committed.

#### Scenario: SQL projection drops the position columns
- **WHEN** the rows reaching the output carry `__meta_partition` and `__meta_offset` but no `__meta_ext`
- **THEN** `write_batch` returns `Error::Process` naming `__meta_ext`, and no data file is uploaded

### Requirement: The watermark SHALL be recovered with a bounded lookback
At `connect()` the output SHALL recover its watermark from the most recent snapshot in the table's snapshot ancestry whose summary carries this writer's watermark property. The ancestry walk SHALL visit at most 1000 snapshots. When no carrying snapshot is found within the bound, the output SHALL start with an empty watermark and log a warning stating that replayed rows may be duplicated.

#### Scenario: Watermark found behind other writers' commits
- **WHEN** other writers committed fifty snapshots after this writer's last commit
- **THEN** `connect()` recovers this writer's watermark from its own snapshot

#### Scenario: Lookback exhausted
- **WHEN** no snapshot within the last 1000 carries this writer's watermark property
- **THEN** `connect()` succeeds with an empty watermark and logs a warning, and new rows are written normally

### Requirement: A failed commit SHALL invalidate the cached watermark
After any failed commit attempt the output SHALL discard its in-memory watermark and reload the table metadata before the next attempt.

#### Scenario: Commit accepted but reported as failed
- **WHEN** a commit request times out after the catalog accepted it, and the same rows are retried or replayed
- **THEN** the output reloads the table metadata, finds the rows below the recovered watermark, and writes no duplicate

### Requirement: Schema mapping SHALL be strict and table-defined
The table SHALL exist before the output connects; the output SHALL NOT create tables or evolve schemas. Schema compatibility SHALL be evaluated for every `write_batch` call. Columns SHALL be matched by name. Columns whose name starts with `__meta_` SHALL never be written. A batch column that is not a table column SHALL be an error naming the column. A table column absent from the batch SHALL be accepted when the column is optional and SHALL be an error when it is required. Values SHALL be converted only by lossless or overflow-checked casts; any other conversion SHALL be an error naming the column and both types.

#### Scenario: Unknown batch column
- **WHEN** a batch carries a column `debug_flag` that the table does not define
- **THEN** `write_batch` returns `Error::Process` naming `debug_flag`, and nothing is committed

#### Scenario: Metadata columns are not written
- **WHEN** a batch carries `__meta_partition`, `__meta_offset`, and `__meta_ext`
- **THEN** the committed data files contain no column whose name starts with `__meta_`

#### Scenario: Missing optional and required table columns
- **WHEN** the table defines an optional column `region` and a required column `id`, and the batch provides `id` but not `region`
- **THEN** the commit succeeds with `region` absent from the data file

#### Scenario: Missing required column
- **WHEN** the batch does not provide the required table column `id`
- **THEN** `write_batch` returns `Error::Process` naming `id`

#### Scenario: Unsupported type conversion
- **WHEN** a batch column has a type that cannot be converted losslessly to the table column's type
- **THEN** `write_batch` returns `Error::Process` naming the column and both types

### Requirement: Data files SHALL follow the table's partition spec
The output SHALL route rows into data files according to the table's partition spec. A partition transform the output cannot evaluate SHALL be rejected at `connect()` with `Error::Process` naming the partition field and the transform.

#### Scenario: Unsupported transform
- **WHEN** `connect()` runs against a table whose partition spec uses a transform the output does not support
- **THEN** `connect()` returns `Error::Process` naming the field and the transform, and nothing is written

#### Scenario: Rows land under their partition values
- **WHEN** rows with two different values of a supported identity partition column are written in one call
- **THEN** the data files carry the two partition values and a partition-pruned scan for one value returns only its rows

### Requirement: Errors SHALL follow the connector output contract
Failures SHALL be mapped as follows: upload or catalog transport failures, timeouts, and HTTP 429 or 5xx responses as `Error::Disconnection`; catalog unreachable or credentials rejected while connecting as `Error::Connection`; a write attempted before `connect()` succeeded as `Error::Connection` with a message; table missing, schema mismatch, unsupported partition transform, and missing position columns as `Error::Process`; invalid configuration as `Error::Config` at build time only. `Error::EOF` SHALL NOT appear in the output.

#### Scenario: Transient catalog failure during commit
- **WHEN** the catalog returns HTTP 503 to a commit request
- **THEN** the failure is classified as `Error::Disconnection`

#### Scenario: Schema mismatch is deterministic
- **WHEN** a batch column cannot be converted to the table column's type
- **THEN** the failure is classified as `Error::Process` and is not retried

#### Scenario: Write before connect
- **WHEN** `write_batch` is called before `connect()` has succeeded
- **THEN** it returns `Error::Connection` with a message stating that the output is not connected

### Requirement: Transient failures SHALL retry through the shared implementation
Transient failures SHALL be retried with the shared connector retry implementation and a total retry budget below the kernel's sink write timeout. Deterministic failures SHALL NOT be retried. A retried commit SHALL follow the failed-commit rule and reload table metadata first.

#### Scenario: Transient failure then success
- **WHEN** the first commit attempt fails with a transport error and the second succeeds
- **THEN** `write_batch` returns `Ok` and the table contains exactly one snapshot for the call

#### Scenario: Deterministic failure is not retried
- **WHEN** a commit attempt fails with a deterministic error
- **THEN** `write_batch` returns `Err` without another attempt

## MODIFIED Requirements

### Requirement: Pluggable WAL storage backend
The WAL SHALL support a configurable storage backend selected per stream via a `backend` setting. The `local` backend (the existing embedded store) SHALL be the default. An `object_store` (S3-compatible) backend SHALL be available as an opt-in alternative.

Remote/object-store backends SHALL be drivable from asynchronous engine contexts: constructing the backend and invoking its store operations from an async task SHALL neither panic (a backend that internally parks on a private runtime via `block_on` MUST be driven on a thread where that is legal) nor block an async runtime worker for the duration of its network I/O — the engine's WAL wrapper drives such stores' calls on the blocking pool. The embedded local backend runs inline on the caller by design: its commit latency is bounded (µs–ms) and redb's fcntl flock must not contend with the blocking pool on the database file.

#### Scenario: Local backend is the default
- **WHEN** a stream has `durability.enabled: true` with no `backend` field (or `backend: local`)
- **THEN** the WAL persists to a local embedded store exactly as before — process-crash recovery, single-node, no behavioral change

#### Scenario: Object-store backend is opt-in
- **WHEN** a stream has `backend: s3` (or another registered object-store backend)
- **THEN** the WAL persists segments and a manifest to the configured object store

#### Scenario: Object-store backend works through the engine's async paths
- **WHEN** a stream with `backend: object_store` runs append / cursor-advance / acknowledge / read-after-cursor / close through the WAL's async API from a tokio runtime
- **THEN** no "cannot start a runtime from within a runtime" panic occurs, the operations complete with their durable effects, and no async worker thread is parked for the duration of the store's network I/O

### Requirement: WAL cursor advancement precedes wrapped source commit

For a WAL acknowledgement wrapping a native source acknowledgement, the durable WAL cursor SHALL be advanced before the wrapped source commit is invoked, and an entry SHALL be reclaimable only once its wrapped source commit has succeeded. If cursor advancement fails, the source acknowledgement SHALL NOT run and the WAL acknowledgement SHALL return an error; if the source commit fails after the cursor advanced, the cursor SHALL be compensated and the unwound entry SHALL remain replayable. A WAL backend SHALL NOT delete an entry a rewind can still expose.

#### Scenario: Cursor failure prevents source commit

- **WHEN** the WAL store cannot persist the next cursor during acknowledgement
- **THEN** the wrapped Kafka or input acknowledgement is not invoked and the entry remains recoverable

#### Scenario: Source commit failure after cursor advancement

- **WHEN** the durable WAL cursor advanced for sequence N and the wrapped source acknowledgement then fails and compensates the cursor
- **THEN** sequence N is replayed after a restart, or the compensation reports an explicit failure instead of silently discarding the entry

#### Scenario: Rewind after an intervening reclaim

- **WHEN** the acked-prefix reclaim has removed entries below the cursor and a rewind moves the cursor back
- **THEN** the entries the rewind exposes are still readable and replay returns every entry the acknowledged frontier does not cover

#### Scenario: Both WAL backends keep the replay guarantee

- **WHEN** the local embedded WAL backend and the object-store WAL backend both advance and rewind a cursor across a failed wrapped acknowledgement
- **THEN** each backend exposes the same replayable range for the same sequence history

#### Scenario: Object-store rewind survives an intervening manifest flush

- **WHEN** the object-store backend advances its cursor to sequence N, the wrapped source commit fails, and a manifest flush runs before the cursor is compensated
- **THEN** the persisted manifest cursor SHALL NOT advance past `N - 1` (the failed sequence stays replayable: reads after the rewind include N), and once the source re-acknowledges through N the backend resumes advancing normally

## MODIFIED Requirements

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

## ADDED Requirements

### Requirement: Acked-prefix reclamation SHALL be replay-safe

A WAL backend MAY reclaim entries covered by the acknowledged cursor once their source commit has succeeded, but SHALL NOT reclaim an entry that a cursor rewind can still expose, SHALL preserve the per-partition ordering of the remaining entries, and SHALL NOT assign a sequence at or below any cursor that has ever been persisted.

#### Scenario: Reclaim stays behind the acknowledged frontier

- **WHEN** a WAL acknowledgement advances the durable cursor for sequence N and the wrapped source commit succeeds
- **THEN** entries up to N may be reclaimed and a restart replays only entries above N

#### Scenario: Sequence assignment after reclamation

- **WHEN** the local backend has reclaimed every entry up to the cursor and the process restarts
- **THEN** the next appended sequence is strictly greater than the persisted cursor and no previously acknowledged sequence is reused

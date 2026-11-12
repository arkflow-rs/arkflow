# Delta Spec: hub-backup-restore

## ADDED Requirements

### Requirement: Backup produces a consistent snapshot

The `arkflow-server backup --to <file>` subcommand SHALL produce a snapshot as a single SQLite database file containing every control-plane business table and the schema version current at backup time. Backups from a SQLite backend SHALL run `VACUUM INTO` through the storage actor so the snapshot window contains no concurrent writes; backups from a PostgreSQL backend SHALL read every table inside a single repeatable-read transaction and verify per-table row counts against the snapshot before finishing. The lease table SHALL be excluded from every snapshot regardless of source backend. The snapshot path SHALL be rejected if the file already exists.

#### Scenario: Snapshot of a live SQLite Hub

- **WHEN** an operator runs `backup --to` against a Hub whose storage backend is SQLite while the Hub is serving requests
- **THEN** the snapshot file is a valid SQLite database whose business tables equal the source row counts and which contains no lease row

#### Scenario: Snapshot of a live PostgreSQL Hub is cross-table consistent

- **WHEN** an operator runs `backup --to` against a PostgreSQL-backed Hub while writes are in flight
- **THEN** the snapshot reflects one repeatable-read point in time across all business tables and every per-table row count matches

#### Scenario: Refuses to overwrite

- **WHEN** the `--to` path already exists
- **THEN** the command fails without modifying the existing file

### Requirement: Restore into an empty target only

The `arkflow-server restore --from <file>` subcommand SHALL import a snapshot into the storage backend selected by the normal configuration scheme, into an empty database only: the command SHALL fail naming the offending tables when any business table already holds rows, and SHALL fail when the snapshot's schema version is newer than the running binary's. A snapshot older than the binary SHALL be upgraded through the existing open-time migration path before copying. Copying SHALL follow the table order with parents-first ordering for self-referencing tables, verify per-table row counts, and reset identity sequences so newly generated ids continue after the restored rows. The restore target SHALL be usable by a subsequently started Hub without further manual steps, with desired-state reconciliation replaying any unprocessed intents.

#### Scenario: Restore to an empty PostgreSQL target from a SQLite snapshot

- **WHEN** an operator restores a snapshot taken from a SQLite Hub into an empty PostgreSQL database
- **THEN** all business tables are populated with counts matching the snapshot, identity sequences continue after the restored rows, and the command succeeds

#### Scenario: Refuses a non-empty target

- **WHEN** the target database already contains rows in any business table
- **THEN** the command fails naming every non-empty table and writes nothing

#### Scenario: Refuses a snapshot newer than the binary

- **WHEN** the snapshot's schema version exceeds the binary's schema version
- **THEN** the command fails with an actionable error before any table is written

#### Scenario: Restored Hub converges the fleet

- **WHEN** a Hub is started on a restored database and Agents reconnect
- **THEN** nodes re-register and desired-state reconciliation replays toward the restored intents without duplicate-intent or fencing errors

### Requirement: Webhook subscriptions survive table copies

The shared table copy order used by migration and backup/restore SHALL include the webhook subscriptions table, so a SQLite-to-PostgreSQL migration or any backup/restore round trip preserves webhook subscriptions with their delivery state.

#### Scenario: Migration preserves webhook subscriptions

- **WHEN** an operator migrates a SQLite store containing webhook subscriptions to PostgreSQL
- **THEN** the subscriptions, their secrets as stored, and their pause state are present in the target with matching row counts

### Requirement: Backup and restore are offline CLI operations

Backup and restore SHALL be command-line subcommands of `arkflow-server` alongside `migrate`, returning a non-zero exit code with an actionable message on failure. Neither subcommand SHALL require the Hub to be running; restore SHALL be documented as requiring every Hub instance against the target database to be stopped first, and SHALL fail rather than proceed when the target cannot be opened exclusively.

#### Scenario: Backup works against a stopped Hub's database file

- **WHEN** an operator runs `backup --to` against the storage path of a stopped Hub
- **THEN** the snapshot is produced without contacting any running process

#### Scenario: Failure is actionable

- **WHEN** a backup or restore fails (bad path, locked database, version mismatch, count mismatch)
- **THEN** the command exits non-zero with a message naming the failing step and the artifact involved

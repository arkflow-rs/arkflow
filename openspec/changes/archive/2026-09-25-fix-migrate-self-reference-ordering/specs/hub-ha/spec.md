# hub-ha 变更（Delta）

## MODIFIED Requirements

### Requirement: One-shot migration from SQLite to PostgreSQL

The server binary SHALL provide a `migrate` subcommand (`--from sqlite:<path> --to postgres:<url>`) that copies every `cp_*` table in foreign-key order in bounded chunks, resets IDENTITY sequences to each table's max id, verifies per-table row counts, and exits non-zero on any mismatch. Self-referencing tables (`cp_config_versions`, `cp_intents`) SHALL copy rows parents-first: a row whose self-reference targets another row of the same table must not insert before its target; rows with dangling self-references keep their relative order and surface as copy errors. Migration requires the Hub to be stopped (documented); the tool SHALL NOT be required for fresh PostgreSQL deployments.

#### Scenario: Self-referencing chains copy parents-first

- **WHEN** the source contains config versions or intents whose self-reference targets a later-scanned row of the same table
- **THEN** the migration orders parents ahead of children and the copy completes without foreign-key violations

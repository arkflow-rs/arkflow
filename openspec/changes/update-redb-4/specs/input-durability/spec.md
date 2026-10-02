## ADDED Requirements

### Requirement: Storage backend upgrade preserves the WAL durability contract

The redb-backed local WAL store SHALL preserve its operator-facing durability contract across the redb 2→4 engine upgrade: append/replay/trim semantics, watermark advancement, and the exclusive-lock lifecycle around `wal.redb` behave exactly as before, with every newly created database using the current redb file format. A `wal.redb` written by a prior ArkFlow release (file-format v2) SHALL fail loudly at open — an explicit error naming the file — rather than being silently recreated, silently truncated, or corrupting reads.

#### Scenario: WAL round-trip and trim still work after the bump

- **WHEN** a stream appends entries beyond the persisted watermark, restarts, and replays to the highest contiguous acked sequence
- **THEN** the redb-backed store returns exactly the appended entries in sequence order, and trimming below the watermark frees them, as before the upgrade

#### Scenario: A legacy v2 file fails loudly at open

- **WHEN** the local WAL store opens a `wal.redb` created by a pre-upgrade ArkFlow release (redb file-format v2)
- **THEN** startup fails with an error identifying the offending file path and the storage-format boundary, and the file is left untouched on disk

#### Scenario: The exclusive-lock lifecycle is unchanged

- **WHEN** a stream closes and a subsequent stream reopens the same WAL path
- **THEN** the redb handle is released on close and the reopen succeeds, preserving the existing flock-based single-writer behavior

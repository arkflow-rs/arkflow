## ADDED Requirements

### Requirement: Manifest persists per-segment metadata and pending deletes
The object-store WAL manifest SHALL persist additive, backward-compatible per-segment metadata (`segment_meta` mapping segment filename to `first_seq`, `last_seq`, `size_bytes`, and `sealed_at_ms`) when sealing segments, as well as a `pending_deletes` list for crash-safe segment reclamation. Existing v1/v2 manifests lacking these fields SHALL remain readable without a manifest version bump. Startup recovery SHALL remain read-only and SHALL record decoded metadata (including object `last_modified` timestamps) for legacy manifest segments and LIST-fallback segments in memory so subsequent reclaim sweeps adopt them into `manifest.json` and reclaim them safely.

#### Scenario: Sealed segment metadata is persisted on seal
- **WHEN** `publish_sealed_segment` uploads a sealed segment and updates the manifest
- **THEN** `manifest.segment_meta` records that segment's `first_seq`, `last_seq`, compressed `size_bytes`, and `sealed_at_ms`
- **AND** the manifest remains readable as schema version 2

#### Scenario: Legacy manifest and LIST-fallback segments load read-only and adopt lazily
- **WHEN** the S3 WAL opens a namespace containing a legacy v1/v2 manifest without `segment_meta`, or a LIST-fallback segment absent from the manifest
- **THEN** startup `recover()` succeeds without issuing any manifest PUT, decodes those segments to populate sequence ranges, and queues their metadata in memory
- **AND** the next reclaim sweep adopts those segments and their metadata into the manifest so they become eligible for retention and skipped on future restarts

### Requirement: Recovery and replay reads skip segments behind the committed cursor
During startup `recover()`, `read_after_cursor()`, and `read_after()`, the S3 WAL backend SHALL consult `manifest.segment_meta` (and the in-memory `segment_ranges` cache) to skip issuing object-store `GET` requests for segments whose `last_seq` is at or below the committed cursor (or requested `after` sequence), as well as any segments listed in `manifest.pending_deletes`. Recovery SHALL initialize its highest-written-sequence watermark from `max(manifest.cursor, max_recovered_seq)` (advancing past `manifest.cursor` only from decoded segments so a missing unacked tail segment never creates an unrecoverable ack gap) and its next segment index past all manifest-known (including `pending_deletes`) and listed segment indices so skipping or deleting old segments never regresses sequence allocation or collides on filenames.

#### Scenario: Startup recovery only downloads necessary segments
- **WHEN** a WAL has retained acknowledged sealed segments (with `segment_meta.last_seq <= manifest.cursor`) alongside unacknowledged segments (`last_seq > manifest.cursor`) and is reopened
- **THEN** `recover()` and `read_after_cursor()` issue `GET` requests only for the unacknowledged segments (and any LIST-fallback or legacy segments absent from `segment_meta`), issuing zero `GET` requests for acknowledged or deleted segments
- **AND** all entries strictly above `manifest.cursor` are replayed in order

#### Scenario: Sequence allocation remains monotonic after full old-segment reclaim
- **WHEN** all older sealed segments have been reclaimed and the WAL is reopened
- **THEN** `next_seq_hint()` is strictly greater than `manifest.cursor` and every recovered segment's highest sequence
- **AND** the next sealed segment receives a strictly higher filename index than every previously created segment

## MODIFIED Requirements

### Requirement: Backward compatibility
The system SHALL maintain all existing WAL configuration options and behavioral semantics.

#### Scenario: Existing configs work unchanged
- **WHEN** a user uses an existing S3 WAL configuration
- **THEN** the system functions with the new pipeline implementation
- **AND** no configuration changes are required

#### Scenario: Recovery preserves manifest and LIST union while skipping covered segments
- **WHEN** a WAL is opened after a crash
- **THEN** recovery preserves the manifest GET + LIST union for unacknowledged and unindexed segments (including legacy and LIST-fallback segments)
- **AND** skips downloading segments whose `segment_meta.last_seq <= manifest.cursor` or that are listed in `manifest.pending_deletes`

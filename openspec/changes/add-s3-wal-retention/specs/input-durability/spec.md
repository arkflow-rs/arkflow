## MODIFIED Requirements

### Requirement: Segment reclaim
The object-store backend SHALL support configurable retention (`retention.enabled`, `retention.max_segments`, `retention.max_age`, `retention.max_bytes`) to reclaim (delete) sealed segment objects whose entries are all behind the committed reclaim floor.

Reclaim eligibility SHALL be governed by the following safety invariants:
- Only sealed segments in `sealed_segments` whose recorded `last_seq` is at or below the reclaim floor (`min(persisted_manifest_cursor, reclaimable_hwm)`, where `reclaimable_hwm` advances only after the wrapped source commit succeeds via `mark_committed(seq)` to `seq.saturating_sub(1)`, and is seeded from `manifest.cursor` on open) SHALL be eligible for deletion. Committing sequence `S` therefore makes every sealed segment with `last_seq <= S - 1` eligible while protecting the segment containing `S` itself until `S + 1` commits (or until restart).
- Unacknowledged segments, `active_segment`, and any entry reachable by a `rewind_cursor` / `Wal::undo` compensation SHALL NOT be deleted. While a rewind poison is active, reclamation SHALL pause.
- Deletion SHALL follow a crash-safe manifest-first order: eligible segments SHALL be deregistered from `sealed_segments` and recorded in `pending_deletes` via ETag-coordinated manifest write BEFORE the corresponding segment objects are deleted from object storage, and removed from `pending_deletes` only after deletion succeeds (or returns `NotFound`).
- When `retention.enabled` is `true` (the default) and none of `max_segments`, `max_age`, or `max_bytes` is configured, all eligible sealed segments SHALL be reclaimed. When one or more limits are configured, the backend SHALL retain only the contiguous newest suffix of eligible sealed segments satisfying all configured limits (stopping on the first violated limit when scanning newest-to-oldest) and SHALL reclaim all older eligible segments. When `retention.enabled` is `false`, no segments SHALL be deleted.
- Reclaim SHALL be best-effort, bounded per sweep, and decoupled from the segment-sealing loop so that slow or failing S3 deletes SHALL NOT block ingestion, delay segment sealing / acknowledgement gating, or stall graceful `close()`.
- A segment referenced by `sealed_segments` in the manifest but missing on the store SHALL still be ignored with a warning during recovery; a segment listed in `pending_deletes` SHALL be ignored quietly during recovery and its deletion retried on subsequent reclaim sweeps.
- Sequence assignment after reclamation SHALL remain strictly greater than `max(manifest.cursor, max_recovered_seq)` and segment indices SHALL NOT collide with previously reclaimed segments.

#### Scenario: Reclaimed segments are behind the committed floor under default retention
- **WHEN** N sealed segments are written, acknowledgements commit entries past segment N-2 (so the committed reclaim floor covers segments 1..=N-2), and retention runs with default settings
- **THEN** eligible sealed segments wholly at or below the committed floor are deregistered from the manifest and deleted from object storage
- **AND** unacknowledged segments (N-1 and N) and `active_segment` remain intact
- **AND** reopening the WAL replays only entries above the cursor without reading any deleted segment

#### Scenario: Configured max_segments, max_age, and max_bytes bounds retain a contiguous newest suffix
- **WHEN** `retention.max_segments`, `retention.max_age`, or `retention.max_bytes` is configured and eligible sealed segments exceed a configured limit
- **THEN** scanning newest-to-oldest stops at the first eligible segment that violates any configured limit, reclaiming that segment and all older eligible segments without leaving gaps in retained history
- **AND** eligible segments within all configured limits remain in object storage and the manifest
- **AND** unacknowledged segments are never deleted even if total segments or bytes exceed the configured limits

#### Scenario: Retention disabled keeps all sealed segments
- **WHEN** `retention.enabled` is set to `false`
- **THEN** sealed segments behind the committed floor are not deleted from object storage or removed from the manifest

#### Scenario: Reclaim respects rewind safety before source commit and for the latest committed sequence
- **WHEN** `advance_cursor(seq)` flushes the manifest cursor to `seq` before `mark_committed(seq)` has succeeded, or `mark_committed(seq)` commits `seq` without a later sequence committing, or a rewind compensation lowers the cursor
- **THEN** segments containing entries reachable by `rewind_cursor` or `Wal::undo(seq)` are NOT deleted
- **AND** rewinding and re-reading returns every unwound entry intact

#### Scenario: Crash between manifest deregistration and object deletion is self-healing
- **WHEN** a reclaim sweep moves an eligible segment into `pending_deletes` in the manifest and the process crashes before the S3 `DELETE` executes
- **THEN** reopening the WAL skips downloading that segment during recovery without logging a missing-segment data-loss warning
- **AND** the next reclaim sweep deletes the lingering object and clears it from `pending_deletes`

#### Scenario: Missing segment is ignored
- **WHEN** recovery reads a manifest whose `sealed_segments` references a segment that no longer exists on the store
- **THEN** recovery skips that segment without error (with a warning)

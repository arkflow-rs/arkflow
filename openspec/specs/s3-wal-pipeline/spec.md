# Capability: S3 WAL Pipeline

## Purpose

Enable high-performance S3 WAL backend by allowing segment encoding and PUT operations to execute concurrently with ongoing WAL writes, eliminating blocking on the append_batch() path. This optimization reduces latency in high-QPS scenarios while maintaining data durability and recovery semantics.

## Requirements

### Requirement: Concurrent segment encoding and PUT
The system SHALL allow segment encoding and S3 PUT operations to execute concurrently with ongoing WAL writes, eliminating blocking on the append_batch() path.

#### Scenario: Seal does not block subsequent writes
- **WHEN** a segment reaches its seal threshold (max_entries or max_bytes)
- **THEN** subsequent append_batch() calls continue without waiting for the S3 PUT to complete
- **AND** the encoded segment is queued for asynchronous PUT

#### Scenario: Pipeline processes multiple segments in flight
- **WHEN** multiple segments are sealed in rapid succession
- **THEN** the system processes PUT operations concurrently
- **AND** up to N segments can be in-flight (where N is configurable or derived from channel capacity)

### Requirement: Ordered segment sealing
The system SHALL maintain segment ordering guarantees even with concurrent PUT operations.

#### Scenario: Segments are PUT in correct order
- **WHEN** segment A is sealed before segment B
- **THEN** segment A's PUT operation is initiated before segment B's PUT operation
- **AND** the manifest reflects the correct segment sequence

### Requirement: Manifest writes are ETag-coordinated
The system SHALL coordinate concurrent manifest writes using ETag-based optimistic concurrency so that two writers reading the same manifest do not silently overwrite each other's updates. Manifest PUTs SHALL carry an `if-match` ETag and SHALL retry on `PreconditionFailed` up to the configured budget. This requirement applies even when `parallel_put.workers > 1`, where multiple `seal_active_segment` calls can race on the manifest.

#### Scenario: Concurrent seal callbacks do not lose cursor advancement
- **WHEN** two or more PUT worker `on_complete` callbacks concurrently invoke `seal_active_segment` (which writes the manifest)
- **THEN** the final stored manifest reflects the union of all their updates (cursor = max of all observed advances, sealed_segments = union)
- **AND** no individual writer's mutation is silently overwritten by another

#### Scenario: Manifest write retries until convergence
- **WHEN** a manifest write's ETag precondition fails because another writer has overwritten the manifest
- **THEN** the system re-reads the manifest, reapplies the caller's mutation against the new base, and resubmits the PUT with the new ETag
- **AND** the original caller observes the write as successful

#### Scenario: Retry budget exceeded surfaces an error
- **WHEN** the ETag-coordinated manifest write exhausts its retry budget without convergence
- **THEN** the caller receives an `Error::Process` describing the contention
- **AND** the system does not silently lose the cursor advancement

### Requirement: Manifest write closure avoids stale-base race
The system SHALL accept manifest mutations as closures that run against a freshly-read manifest within the coordination window, so the caller cannot apply its mutation to a `Manifest` snapshot read before the PUT and accidentally overwrite a concurrent writer's later update.

#### Scenario: Mutation closure receives the latest base on each retry
- **WHEN** a caller supplies a `|m| { ... }` closure that mutates the `Manifest`
- **THEN** each retry attempt invokes the closure against the manifest read at the start of that attempt
- **AND** the closure does NOT execute against a snapshot obtained outside the coordination window

### Requirement: Failure handling and recovery
The system SHALL handle PUT failures without data loss and maintain compatibility with existing recovery semantics. Manifest PUT failures arising from concurrent contention SHALL be resolved by the ETag-coordinated retry path rather than by surfacing an error to the caller.

#### Scenario: PUT failure is surfaced to caller
- **WHEN** an asynchronous segment PUT fails
- **THEN** the error is propagated to the appropriate error handling path
- **AND** unacknowledged data remains in the WAL for retry

#### Scenario: Recovery preserves data integrity
- **WHEN** the system restarts after a mid-PUT failure
- **THEN** recovery reads both sealed segments and any in-flight segment data
- **AND** the union of manifest and LIST fallback behavior is preserved

#### Scenario: Concurrent manifest contention is resolved silently
- **WHEN** two writers contend on the manifest and one's ETag precondition fails
- **THEN** the loser retries against the new manifest state
- **AND** the contention is absorbed by the retry path rather than surfacing as an error to the caller
- **AND** the recovery process remains unchanged (recovery still reads the manifest once and treats it as the source of truth)

### Requirement: Committed cursor tracks acknowledgements
The S3 WAL backend SHALL advance the committed cursor in response to acknowledgements. `advance_cursor(seq)` MUST record `seq` (it SHALL NOT discard it), and the manifest cursor SHALL be derived from the highest acknowledged sequence clamped to the highest sequence durably sealed to object storage (`max_sealed_seq`). The cursor SHALL NOT advance past `max_sealed_seq`, so an entry that has been acknowledged but not yet sealed remains replayable on restart (at-least-once). The backend's next-sequence hint SHALL be derived from the highest written sequence (`max(max_sealed_seq, active segment last_seq) + 1`), not from `cursor()+1`, so a restart never reuses a sequence number already present on the store.

#### Scenario: Acknowledged entries are not replayed after a clean restart
- **WHEN** entries seq 1..=N are ingested and sealed, and all of them are acknowledged via `advance_cursor`
- **THEN** after closing and reopening the WAL at the same namespace, `read_after_cursor()` returns no entries
- **AND** `next_seq_hint()` returns N+1

#### Scenario: Cursor does not advance past unsealed data
- **WHEN** an entry is acknowledged while it is still in the active (not yet sealed) segment, so `acked_seq > max_sealed_seq`
- **THEN** the committed cursor is clamped to `max_sealed_seq` and does not advance past the unsealed entry
- **AND** once that entry is sealed, a restart replays it because the cursor never advanced past it (no loss of sealed data; unsealed in-memory entries remain subject to the existing group-commit loss window)

#### Scenario: Next sequence hint does not reuse a sealed-but-unacked sequence
- **WHEN** entries are sealed up to sequence M but only acknowledged up to sequence K (K < M) and the WAL is reopened at the same namespace
- **THEN** `next_seq_hint()` returns M+1, not K+1
- **AND** the next append does not collide with the existing sealed sequences K+1..=M

### Requirement: Backpressure management
The system SHALL apply backpressure when the PUT pipeline is full to prevent unbounded memory growth.

#### Scenario: Channel full blocks append
- **WHEN** the PUT channel reaches capacity
- **THEN** append_batch() blocks until space is available
- **AND** the system does not accumulate unbounded in-flight segments

### Requirement: Backward compatibility
The system SHALL maintain all existing WAL configuration options and behavioral semantics.

#### Scenario: Existing configs work unchanged
- **WHEN** a user uses an existing S3 WAL configuration
- **THEN** the system functions with the new pipeline implementation
- **AND** no configuration changes are required

#### Scenario: Recovery semantics unchanged
- **WHEN** a WAL is opened after a crash
- **THEN** the recovery process (manifest GET + LIST union + segment decode) behaves identically to the current implementation

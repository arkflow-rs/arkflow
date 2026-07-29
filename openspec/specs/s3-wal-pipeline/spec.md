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

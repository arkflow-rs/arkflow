# Capability: WAL Manifest Write Coordination

## Purpose

Ensure that concurrent writers to the S3 WAL manifest (`manifest.json`) do not silently overwrite each other. The S3-backed WAL backend supports up to 8 parallel PUT workers (PR #1186) and three independent write paths (`flusher` loop, `on_complete` callbacks from PUT workers, and `advance_cursor` flush). Without coordination, two tasks reading the same manifest and writing back their locally-modified copies causes the later write to overwrite the earlier one, dropping cursor advancement. This capability introduces ETag-based optimistic coordination so that the manifest converges to the union of all concurrent writes.

## Requirements

### Requirement: Read manifest with current ETag
The system SHALL provide a manifest read operation that returns both the parsed `Manifest` value and the current object's ETag, so that subsequent PUTs can be conditioned on the same object version.

#### Scenario: Manifest exists
- **WHEN** the manifest object exists in the bucket
- **THEN** the read returns the parsed `Manifest` and the ETag from the stored object metadata

#### Scenario: Manifest absent
- **WHEN** the manifest object does not exist (fresh bucket or first run)
- **THEN** the read returns a `Manifest::fresh(node_id, stream_id)` value with no ETag

### Requirement: PUT manifest with ETag precondition
The system SHALL submit manifest PUTs with a conditional precondition — an ETag (`if-match`) when the manifest already exists, and create-if-not-exists (`PutMode::Create`) when it does not — and refuse writes whose precondition does not hold, so concurrent writers do not silently clobber each other.

#### Scenario: ETag matches stored value
- **WHEN** the caller provides an ETag that matches the stored manifest's current ETag
- **THEN** the PUT succeeds and the stored object is replaced with the new contents

#### Scenario: ETag does not match
- **WHEN** the caller provides an ETag that differs from the stored manifest's current ETag (because another writer has overwritten it)
- **THEN** the PUT is rejected with `PreconditionFailed`
- **AND** the stored manifest is left unchanged

#### Scenario: Concurrent first-write on a fresh manifest
- **WHEN** the manifest does not yet exist and two writers concurrently attempt the first write
- **THEN** the writers use create-if-not-exists (`PutMode::Create`)
- **AND** exactly one writer's PUT succeeds; the loser receives `AlreadyExists` and retries against the now-existing manifest, rather than overwriting the winner with an unconditional PUT

### Requirement: Retry on ETag precondition failure
The system SHALL retry a manifest write whose ETag precondition failed by re-reading the manifest, reapplying the caller's mutation against the freshly-read base, and resubmitting the PUT with the new ETag.

#### Scenario: Retry converges within budget
- **WHEN** two tasks concurrently mutate the manifest and contend on the same ETag
- **THEN** each task's mutation is observed in the final manifest state on a subsequent retry
- **AND** the retry budget (≤ 8 attempts, covering `parallel_put.workers` ≤ 8 in the fully-concurrent worst case) is sufficient to absorb normal contention

#### Scenario: Retry budget exceeded
- **WHEN** the retry budget is exhausted without a successful PUT
- **THEN** the write returns `Error::Process` with a message identifying the contention
- **AND** the error is propagated to the caller without further automatic retry

### Requirement: Caller's mutation runs against a fresh base each attempt
The system SHALL require the caller's mutation to be supplied as a closure that receives the freshly-read manifest each retry attempt, so that the caller does not compose a mutated `Manifest` value outside the coordination window and accidentally overwrite a more recent write.

#### Scenario: Mutation is applied to the latest base
- **WHEN** the caller submits a closure that mutates the `Manifest` (e.g., `m.cursor = X`, `m.sealed_segments.push(name)`)
- **THEN** on each retry attempt the closure is invoked against the most recently stored manifest, not against a stale snapshot

#### Scenario: Idempotent mutation
- **WHEN** the caller's mutation is safe to re-apply (e.g., setting `cursor` to `max(prev, X)` or appending a segment name with an existence check)
- **THEN** repeated retry attempts do not corrupt the manifest (no duplicate segments, no regressed cursor)

### Requirement: Tracing observes retry activity
The system SHALL emit tracing records at `debug` level for each retry attempt and at `warn` level when the number of retries reaches or exceeds 3, so operators can monitor contention frequency without changing the default log level.

#### Scenario: Single-attempt success stays quiet
- **WHEN** the manifest PUT succeeds on the first attempt
- **THEN** no new tracing records are emitted at `info` or higher level

#### Scenario: High-contention case produces a warning
- **WHEN** 3 or more retry attempts are required for one manifest write
- **THEN** a `warn`-level record is emitted with the attempt count and a stable correlation id

### Requirement: Coordination does not introduce process-level locks
The system SHALL achieve manifest write coordination using only ETag-based optimistic concurrency and SHALL NOT introduce a process-level mutex that would serialize all manifest writers and undo the throughput gains of `parallel_put.workers`.

#### Scenario: Parallel PUT workers remain concurrent
- **WHEN** `parallel_put.workers` is configured with value N > 1
- **THEN** the manifest write coordination does not serialize concurrent `seal_active_segment` calls across workers
- **AND** the throughput improvement from PR #1186 is preserved

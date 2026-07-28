# Capability: WAL Parallel PUT

## Purpose

Enable concurrent segment uploads to S3 via multiple PUT workers, increasing write throughput for high-volume scenarios while maintaining segment ordering guarantees.

## ADDED Requirements

### Requirement: Configurable parallel PUT workers
The S3 WAL backend SHALL support configuring the number of parallel PUT workers via a `parallel_put_workers` parameter. The system SHALL default to 1 worker (current behavior) and allow up to 8 workers.

#### Scenario: Default single worker
- **WHEN** a stream does not configure `parallel_put_workers`
- **THEN** segments are uploaded by a single PUT worker (current behavior)

#### Scenario: Multiple workers
- **WHEN** a stream configures `parallel_put_workers: 4`
- **THEN** segments are uploaded by up to 4 concurrent PUT workers

#### Scenario: Maximum workers limit
- **WHEN** a stream configures `parallel_put_workers: 16`
- **THEN** the system caps workers at 8 and logs a warning

### Requirement: Ordered segment completion tracking
The PUT worker system SHALL track segment completion by sequence number and ensure that manifest updates reflect the highest contiguous completed segment, even when segments complete out of order.

#### Scenario: Out-of-order completion
- **WHEN** segments 5, 6, 7 are uploading in parallel and segment 6 completes before segment 5
- **THEN** the manifest is not updated for segment 6 until segment 5 also completes

#### Scenario: Contiguous manifest advance
- **WHEN** segments 5 and 6 complete, then segment 7 completes
- **THEN** the manifest advances to segment 7 immediately (contiguous completion)

### Requirement: Priority-based PUT scheduling
When multiple workers are available, the PUT scheduler SHALL prioritize older segments (lower sequence numbers) to minimize manifest update delays. Segments SHALL be assigned to workers in ascending order by sequence.

#### Scenario: Older segments prioritized
- **WHEN** segments 5, 6, 7 are queued and all workers are busy
- **THEN** segment 5 is assigned to the next available worker before segments 6 and 7

#### Scenario: Worker assignment order
- **WHEN** 4 workers are available and 3 segments are queued
- **THEN** segments are assigned to workers in ascending order (5, 6, 7)

### Requirement: Backpressure per worker
Each PUT worker SHALL maintain independent backpressure via a bounded channel (default 16 segments per worker). When a worker's channel is full, segment encoding SHALL block for that worker queue only.

#### Scenario: Per-worker backpressure
- **WHEN** worker 1's channel is full but worker 2 has capacity
- **THEN** segments continue to be assigned to worker 2 while worker 1 processes its backlog

#### Scenario: Global backpressure when all workers full
- **WHEN** all 4 workers' channels are full (each at 16 segments)
- **THEN** segment encoding blocks until at least one worker has capacity

### Requirement: Parallel PUT worker validation
The system SHALL validate `parallel_put_workers` at configuration load time. Non-positive integers SHALL be rejected with a clear error message.

#### Scenario: Invalid worker count
- **WHEN** a stream configures `parallel_put_workers: 0`
- **THEN** configuration loading fails with an error indicating worker count must be positive

#### Scenario: Negative worker count
- **WHEN** a stream configures `parallel_put_workers: -2`
- **THEN** configuration loading fails with an error indicating worker count must be positive

### Requirement: Graceful worker shutdown
When the WAL is closed, all PUT workers SHALL finish uploading their currently-assigned segments before shutdown completes. Segments still in encoding queues SHALL be flushed to S3 or discarded per a configurable `shutdown_timeout`.

#### Scenario: Clean shutdown
- **WHEN** the WAL is closed and workers have 1-2 segments each in progress
- **THEN** shutdown waits up to `shutdown_timeout` for all segments to upload

#### Scenario: Timeout during shutdown
- **WHEN** the WAL is closed, workers have many segments, and `shutdown_timeout` elapses
- **THEN** shutdown completes with a warning, and in-progress segments may be lost

# async-checkpoint-barriers Specification

## Purpose
TBD - created by archiving change rebuild-unified-streaming-engine. Update Purpose after archive.
## Requirements
### Requirement: Barrier flows with data

Checkpoint barriers SHALL travel as control envelopes inside the data channels,
in order with data, from sources through every vertex to sinks.

#### Scenario: Barrier ordering

- **WHEN** a barrier is injected after batch N on a source edge
- **THEN** a downstream vertex receives batch N, then the barrier, then batch N+1

### Requirement: Alignment at multi-input vertices

A vertex with multiple input edges SHALL buffer data from other inputs after one
input's barrier arrives until barriers from all inputs align, then release the
buffered data.

#### Scenario: Two-input alignment

- **WHEN** input A delivers a barrier while input B still streams pre-barrier data
- **THEN** the vertex buffers B's data, snapshots when B's barrier arrives, and forwards B's buffered data afterwards

#### Scenario: Alignment buffer is bounded

- **WHEN** alignment buffering exceeds the aligner's fixed cap
- **THEN** the checkpoint fails with a bounded-alignment error and data flow resumes, rather than growing memory unboundedly

### Requirement: Snapshot does not stall processing

Vertices SHALL snapshot their keyed state and source positions asynchronously
while data continues to flow through unaffected edges.

#### Scenario: Data continues during snapshot

- **WHEN** a stateful chain snapshots its state upon barrier alignment
- **THEN** batches on other edges and post-barrier batches are processed without waiting for the snapshot to complete

### Requirement: Checkpoint completion semantics

A checkpoint SHALL complete only after every participating vertex whose event
loop is still running reports its `TaskCheckpointAck` (state snapshot reference,
source positions, watermark). Vertices whose event loop has already ended SHALL
be exempt from the required set and SHALL NOT receive further barrier
injections. Completion reuses the existing `CheckpointCoordinator` and manifest
contracts.

#### Scenario: All live vertices must ack

- **WHEN** one of three running vertices has not yet acknowledged
- **THEN** the checkpoint remains InProgress and no manifest is written

#### Scenario: Ended vertex is exempt

- **WHEN** a bounded source's chain ended at EOF while another source keeps flowing and a new barrier round starts
- **THEN** the round completes with reports from the live vertices and does not wait for the ended chain

#### Scenario: Stale barrier rejected

- **WHEN** an acknowledgement arrives with a different checkpoint id or generation than the in-flight barrier
- **THEN** the coordinator rejects it and the checkpoint is not completed by that ack

### Requirement: Recovery replays from barrier positions

On recovery, the engine SHALL restore per-chain state and seek sources to the
checkpoint's source positions, replaying only post-checkpoint data.

#### Scenario: No pre-checkpoint replay

- **WHEN** a Job restarts from checkpoint C with sources at later positions
- **THEN** sources are restored to C's recorded positions and state is restored from C's snapshots before any input is read

### Requirement: WAL fallback ordering

For Jobs without checkpoint configuration, input-WAL recovery SHALL remain the
recovery mechanism; for Jobs with checkpoints, checkpoint positions take
priority and WAL replay MUST NOT re-deliver data before the checkpoint position.

#### Scenario: Checkpoint-priority recovery

- **WHEN** a checkpointed Job with a WAL-backed source recovers
- **THEN** the source seeks to the checkpoint position and WAL entries before that position are not re-emitted into the pipeline


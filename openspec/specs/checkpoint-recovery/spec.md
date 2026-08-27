# checkpoint-recovery Specification

## Purpose
TBD - created by archiving change add-distributed-stateful-streaming-runtime. Update Purpose after archive.
## Requirements
### Requirement: Checkpoints SHALL capture a consistent Job position
Each completed checkpoint SHALL identify the Job version, task assignments, source positions, operator watermark state, in-flight barrier position, state snapshots, format versions, and integrity checksums.

#### Scenario: Complete a checkpoint
- **WHEN** all participating sources and stateful tasks acknowledge the checkpoint barrier and durable state files are verified
- **THEN** the checkpoint becomes the latest valid recovery point with a durable manifest

### Requirement: Incomplete checkpoints SHALL NOT be recoverable
The runtime SHALL retain the last valid checkpoint and SHALL exclude incomplete, corrupt, or checksum-invalid checkpoints from automatic recovery.

#### Scenario: A task fails during snapshot
- **WHEN** a task cannot finish its snapshot or its state checksum does not match
- **THEN** the checkpoint is marked failed, the previous valid checkpoint remains selected, and the Job reports degraded checkpoint health

### Requirement: Recovery SHALL restore deterministic state
Recovery SHALL restore state, source positions, watermarks, and task assignments from one compatible checkpoint or savepoint before processing new input.

#### Scenario: Compute node restarts
- **WHEN** a Compute node restarts after a failure
- **THEN** the Job restores from the selected checkpoint, replays the required source range, and reports recovery progress before becoming healthy

### Requirement: Savepoints SHALL support controlled upgrades
An authorized operator SHALL be able to create, inspect, restore, and delete savepoints subject to retention and state-compatibility checks.

#### Scenario: Upgrade from a savepoint
- **WHEN** a new compatible Job version is deployed from a savepoint
- **THEN** the runtime restores compatible state, preserves source progress, and refuses deployment if migration requirements are unmet


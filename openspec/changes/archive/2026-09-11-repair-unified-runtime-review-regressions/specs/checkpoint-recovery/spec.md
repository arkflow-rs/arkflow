# checkpoint-recovery Specification

## MODIFIED Requirements

### Requirement: Checkpoints SHALL capture a consistent Job position

Each completed checkpoint SHALL identify the Job version, task assignments, complete planned task membership, source positions, operator watermark state, in-flight barrier position, state snapshots, format versions, and integrity checksums. All source positions and state snapshots SHALL represent the same acknowledged cut. When execution chains fuse logical processors, the manifest SHALL retain a deterministic mapping from every planned task to its chain snapshot and SHALL NOT omit a planned task merely because it is not a chain entry.

#### Scenario: Complete a checkpoint

- **WHEN** all participating sources and stateful tasks acknowledge the checkpoint barrier and durable state files are verified
- **THEN** the checkpoint becomes the latest valid recovery point with a durable manifest whose task set exactly matches the complete planned assignment and whose source positions and state snapshots come from one acknowledged cut

#### Scenario: Local execution fuses stateless processors

- **WHEN** adjacent stateless processors are represented by one execution chain and local recovery persists a checkpoint
- **THEN** the manifest records every logical planned task through the chain mapping and the checkpoint passes exact task-set validation after restart

#### Scenario: A stateless Job uses a non-default state format

- **WHEN** local recovery is enabled for a Job configured with state format `N` greater than 1 but no task has state entries
- **THEN** the checkpoint manifest records format `N` rather than the empty-snapshot default format 1

### Requirement: Incomplete checkpoints SHALL NOT be recoverable

The runtime SHALL retain the last valid checkpoint and SHALL exclude incomplete, corrupt, checksum-invalid, or task-set-incomplete checkpoints from automatic recovery. A checkpoint SHALL NOT be completed from only the online subset of an expected assignment set.

#### Scenario: A task fails during snapshot

- **WHEN** a task cannot finish its snapshot or its state checksum does not match
- **THEN** the checkpoint is marked failed, the previous valid checkpoint remains selected, and the Job reports degraded checkpoint health

#### Scenario: An expected task is absent

- **WHEN** a checkpoint round contains no manifest entry for one planned task or assignment
- **THEN** the round remains pending or fails and no artifact from that subset is eligible for recovery

### Requirement: Recovery SHALL restore deterministic state

Recovery SHALL restore state, source positions, watermarks, and task assignments from one compatible checkpoint or savepoint before processing new input. Restored source cursors, task membership, state format, and execution-chain mappings SHALL be validated together.

#### Scenario: Compute node restarts

- **WHEN** a Compute node restarts after a failure
- **THEN** the Job restores from the selected checkpoint, replays the required source range, and reports recovery progress before becoming healthy

#### Scenario: Local checkpoint contains fused chain snapshots

- **WHEN** a local Job restarts with a checkpoint whose manifest maps all planned tasks to a fused chain snapshot
- **THEN** the runtime validates the complete mapping, restores the chain state and source position, and reads new input only after restore completes

### Requirement: Savepoints SHALL support controlled upgrades

An authorized operator SHALL be able to create, inspect, restore, and delete savepoints subject to retention and state-compatibility checks.

#### Scenario: Upgrade from a savepoint

- **WHEN** a new compatible Job version is deployed from a savepoint
- **THEN** the runtime restores compatible state, preserves source progress, and refuses deployment if migration requirements are unmet

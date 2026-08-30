## MODIFIED Requirements

### Requirement: Checkpoints SHALL capture a consistent Job position
Each completed checkpoint SHALL identify the Job version, task assignments, source positions, operator watermark state, in-flight barrier position, state snapshots, format versions, and integrity checksums. Source positions, watermarks, and operator state SHALL all be captured from one acknowledged checkpoint cut; data and state that are still pending downstream acknowledgement SHALL NOT be included in that cut.

#### Scenario: Complete a checkpoint at one acknowledged cut
- **WHEN** every planned source and stateful task reaches the same barrier, all pre-cut output/state transactions are resolved, and durable state files are verified
- **THEN** the manifest records the corresponding contiguous source positions, watermarks, committed state epoch, task assignments, barrier identity, format versions, and checksums as one valid recovery point

#### Scenario: Data acknowledgement is pending at barrier injection
- **WHEN** a source has delivered a record but its downstream output acknowledgement has not completed when the barrier is sealed
- **THEN** the record and its uncommitted state mutation are excluded from the checkpoint cut, and recovery replays it from the recorded source position

#### Scenario: Multi-input state is snapshotted at alignment
- **WHEN** all inputs of a stateful chain have delivered the checkpoint barrier while post-barrier data remains buffered
- **THEN** the chain captures an immutable committed-state snapshot before releasing buffered post-barrier data, while snapshot persistence MAY continue asynchronously

### Requirement: Incomplete checkpoints SHALL NOT be recoverable
The runtime SHALL retain the last valid checkpoint and SHALL exclude incomplete, corrupt, checksum-invalid, or assignment-incomplete checkpoints from automatic recovery.

#### Scenario: A task fails during snapshot
- **WHEN** a task cannot finish its snapshot or its state checksum does not match
- **THEN** the checkpoint is marked failed, the previous valid checkpoint remains selected, and the Job reports degraded checkpoint health

#### Scenario: A planned task does not participate
- **WHEN** the checkpoint manifest does not contain every task in the planned assignment set
- **THEN** the runtime refuses to seal it as Completed and retains the previous valid checkpoint for recovery

### Requirement: Recovery SHALL restore deterministic state
Recovery SHALL restore state, source positions, watermarks, and task assignments from one compatible checkpoint or savepoint before processing new input. Restored positions and watermarks SHALL be installed for the actual assigned source partitions, and all compatible configured source assignments SHALL remain active.

#### Scenario: Compute node restarts
- **WHEN** a Compute node restarts after a failure
- **THEN** the Job restores from the selected checkpoint, applies every compatible task assignment and partition watermark before source reads, replays the required source range, and reports recovery progress before becoming healthy

#### Scenario: Restored checkpoint is used before a new read
- **WHEN** a source has a persisted checkpoint position and a source connector has not yet started reading after restart
- **THEN** the connector installs the restored cursor and the runtime does not consume from its pre-recovery position

### Requirement: Savepoints SHALL support controlled upgrades
An authorized operator SHALL be able to create, inspect, restore, and delete savepoints subject to retention, stable state-namespace/operator identity, and state-compatibility checks. A target Job version MAY differ from the artifact version when an explicit migration or compatible state-format path exists.

#### Scenario: Upgrade from a compatible savepoint
- **WHEN** a new compatible Job version is deployed from a savepoint and all state namespaces, operator identities, and format migrations are supported
- **THEN** the runtime restores compatible state, preserves source progress, and records the target Job version as the active deployment

#### Scenario: Upgrade has no migration path
- **WHEN** a savepoint's state format or namespace cannot be read or migrated by the target Job version
- **THEN** deployment is blocked before task execution and the incompatibility is recorded

## ADDED Requirements

### Requirement: Checkpoint reports SHALL use the Job state format contract
Checkpoint aggregation SHALL validate state reports against the configured Job/backend state format. A stateless source or sink report with an empty compatibility snapshot SHALL NOT make a checkpoint invalid solely because its local report uses the default format.

#### Scenario: Stateful and stateless chains use one Job format
- **WHEN** a Job backend declares a non-default state format and the checkpoint includes stateless chains with empty snapshots
- **THEN** the stateful snapshots are checked against the Job format and the stateless reports are accepted without a false format-mismatch failure

### Requirement: Recovery compatibility SHALL be evaluated consistently
Hub authorization, Agent validation, repository validation, and runtime restore SHALL apply the same compatibility result for Job identity, generation, state namespaces, operator identities, format migration, task membership, and checksums.

#### Scenario: Hub and Agent validate the same compatible artifact
- **WHEN** the Hub authorizes a savepoint upgrade whose format migration is registered
- **THEN** the Agent accepts the same artifact under the same compatibility result and does not reject it merely because the Job version changed

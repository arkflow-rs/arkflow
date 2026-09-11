## MODIFIED Requirements

### Requirement: Checkpoints SHALL capture a consistent Job position

Each completed checkpoint SHALL identify the Job version, task assignments, complete planned task membership, source positions, operator watermark state, in-flight barrier position, state snapshots, format versions, and integrity checksums. All source positions and state snapshots SHALL represent the same acknowledged cut. When execution chains fuse logical processors, the manifest SHALL retain a deterministic mapping from every planned task to its chain snapshot and SHALL NOT omit a planned task merely because it is not a chain entry.

#### Scenario: Complete a checkpoint

- **WHEN** all participating sources and stateful tasks acknowledge the checkpoint barrier and durable state files are verified
- **THEN** the checkpoint becomes the latest valid recovery point with a durable manifest whose task set exactly matches the complete planned assignment and whose source positions and state snapshots come from one acknowledged cut

#### Scenario: Data acknowledgement is pending at barrier injection

- **WHEN** a source has delivered a record but its downstream output acknowledgement has not completed when the barrier is sealed
- **THEN** the record and its uncommitted state mutation are excluded from the checkpoint cut, and recovery replays it from the recorded source position

#### Scenario: Local execution fuses stateless processors

- **WHEN** adjacent stateless processors are represented by one execution chain and local recovery persists a checkpoint
- **THEN** the manifest records every logical planned task through the chain mapping and the checkpoint passes exact task-set validation after restart

#### Scenario: A stateless Job uses a non-default state format

- **WHEN** local recovery is enabled for a Job configured with state format `N` greater than 1 but no task has state entries
- **THEN** the checkpoint manifest records format `N` rather than the empty-snapshot default format 1

## MODIFIED Requirements

### Requirement: Compute nodes SHALL execute fenced task attempts
The runtime SHALL assign task attempts to authenticated Compute nodes and SHALL fence stale assignments using Job generation and task attempt identity. A checkpoint SHALL be accepted only when every planned task in the execution model participates in the same valid cut.

#### Scenario: A stale task assignment arrives
- **WHEN** a Compute node receives an assignment for an older Job generation or superseded task attempt
- **THEN** it does not start the stale task and reports the assignment as superseded

#### Scenario: A checkpoint task is missing
- **WHEN** one planned task does not produce a checkpoint manifest for a round
- **THEN** the checkpoint is rejected or remains incomplete and cannot be selected for recovery

### Requirement: Job lifecycle SHALL support recovery operations
The control plane SHALL support submitting, starting, stopping, restarting, cancelling, and observing Jobs without changing the lifecycle semantics of existing YAML Streams. Recovery SHALL restore source positions and every physical event-time partition watermark from one acknowledged checkpoint cut.

#### Scenario: Restart a failed Job
- **WHEN** an authorized operator requests a restart for a failed Job
- **THEN** the Hub creates a new fenced task attempt and the Compute nodes restore or initialize the Job according to its recovery policy

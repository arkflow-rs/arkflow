## ADDED Requirements

### Requirement: Job plans SHALL have stable distributed identities
The runtime SHALL represent a Job as a versioned DAG with stable operator, task, subtask, partition, and key-group identities.

#### Scenario: Compile a valid Job plan
- **WHEN** a valid SQL or Job specification is submitted
- **THEN** the system produces a versioned plan whose nodes, edges, partitions, and stateful operators have stable identities

### Requirement: Compute nodes SHALL execute fenced task attempts
The runtime SHALL assign task attempts to authenticated Compute nodes and SHALL fence stale assignments using Job generation and task attempt identity.

#### Scenario: A stale task assignment arrives
- **WHEN** a Compute node receives an assignment for an older Job generation or superseded task attempt
- **THEN** it does not start the stale task and reports the assignment as superseded

### Requirement: Job execution SHALL provide bounded backpressure
The runtime SHALL propagate input, operator, network, and output pressure through the Job DAG and SHALL expose a bounded state when a downstream task cannot make progress.

#### Scenario: A downstream task is unavailable
- **WHEN** a downstream task stops consuming data
- **THEN** upstream tasks stop or reduce dispatch within configured bounds and the Job observation reports the blocked edge and pressure reason

### Requirement: Job lifecycle SHALL support recovery operations
The control plane SHALL support submitting, starting, stopping, restarting, cancelling, and observing Jobs without changing the lifecycle semantics of existing YAML Streams.

#### Scenario: Restart a failed Job
- **WHEN** an authorized operator requests a restart for a failed Job
- **THEN** the Hub creates a new fenced task attempt and the Compute nodes restore or initialize the Job according to its recovery policy

# distributed-job-runtime Specification

## MODIFIED Requirements

### Requirement: Job plans SHALL have stable distributed identities

The runtime SHALL represent a Job as a versioned DAG with stable operator, task, subtask, partition, and key-group identities. Any fused execution-chain representation used for local recovery SHALL retain a deterministic mapping to every logical planned task so checkpoint membership remains complete.

#### Scenario: Compile a valid Job plan

- **WHEN** a valid SQL or Job specification is submitted
- **THEN** the system produces a versioned plan whose nodes, edges, partitions, and stateful operators have stable identities

#### Scenario: Persist a fused execution chain

- **WHEN** adjacent stateless processors are fused into one execution chain for a local Job checkpoint
- **THEN** the checkpoint records the chain identity and a mapping covering every logical task in the plan

### Requirement: Compute nodes SHALL execute fenced task attempts

The runtime SHALL assign task attempts to authenticated Compute nodes and SHALL fence stale assignments using Job generation and task attempt identity. A worker-pool processing failure SHALL retain the failed batch and acknowledgement until the configured error edge or terminal failure policy handles them.

#### Scenario: A stale task assignment arrives

- **WHEN** a Compute node receives an assignment for an older Job generation or superseded task attempt
- **THEN** it does not start the stale task and reports the assignment as superseded

#### Scenario: A processor fails in a worker pool

- **WHEN** a processor worker fails for a batch and the Job has a configured error or DLQ edge
- **THEN** the failed batch and its acknowledgement are routed through the same error-output path as the single-worker execution and are not discarded as a generic process error

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

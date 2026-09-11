## MODIFIED Requirements

### Requirement: Compute nodes SHALL execute fenced task attempts
The runtime SHALL assign task attempts to authenticated Compute nodes and SHALL fence stale assignments using Job generation and task attempt identity. A checkpoint SHALL be accepted only when every planned task in the execution model participates in the same valid cut. A worker-pool processing failure SHALL retain the failed batch and acknowledgement until the configured error edge or terminal failure policy handles them.

#### Scenario: A stale task assignment arrives
- **WHEN** a Compute node receives an assignment for an older Job generation or superseded task attempt
- **THEN** it does not start the stale task and reports the assignment as superseded

#### Scenario: A checkpoint task is missing
- **WHEN** one planned task does not produce a checkpoint manifest for a round
- **THEN** the checkpoint is rejected or remains incomplete and cannot be selected for recovery

#### Scenario: A processor fails in a worker pool
- **WHEN** a processor worker fails for a batch and the Job has a configured error or DLQ edge
- **THEN** the failed batch and its acknowledgement are routed through the same error-output path as the single-worker execution and are not discarded as a generic process error

### Requirement: Job execution SHALL provide bounded backpressure
The runtime SHALL propagate input, operator, network, and output pressure through the Job DAG and SHALL expose a bounded state when a downstream task cannot make progress.

#### Scenario: A downstream task is unavailable
- **WHEN** a downstream task stops consuming data
- **THEN** upstream tasks stop or reduce dispatch within configured bounds and the Job observation reports the blocked edge and pressure reason

#### Scenario: A worker pool drains under a slow downstream
- **WHEN** a processor chain runs with a worker pool (concurrency greater than one) and its downstream edge stays full because the consumer is slow
- **THEN** processed results accumulate only within a bounded window, the workers stop accepting new deliveries, and the backpressure reaches the source chain instead of growing memory without bound

## MODIFIED Requirements

### Requirement: Compute nodes SHALL execute fenced task attempts
The runtime SHALL assign task attempts to authenticated Compute nodes and SHALL fence stale assignments using Job generation and task attempt identity. A checkpoint SHALL be accepted only when every planned task in the execution model participates in the same valid cut. A worker-pool processing failure SHALL retain the failed batch and acknowledgement until the configured error edge or terminal failure policy handles them. A Job start SHALL register the spawned kernel with the Agent's task registry before the starting command can be aborted, so every started kernel remains addressable by observation, stop, and stop-all even when the command task is cancelled mid-start; if startup fails, the registration is removed and the kernel's cancellation token is cancelled.

#### Scenario: A stale task assignment arrives
- **WHEN** a Compute node receives an assignment for an older Job generation or superseded task attempt
- **THEN** it does not start the stale task and reports the assignment as superseded

#### Scenario: A checkpoint task is missing
- **WHEN** one planned task does not produce a checkpoint manifest for a round
- **THEN** the checkpoint is rejected or remains incomplete and cannot be selected for recovery

#### Scenario: A processor fails in a worker pool
- **WHEN** a processor worker fails for a batch and the Job has a configured error or DLQ edge
- **THEN** the failed batch and its acknowledgement are routed through the same error-output path as the single-worker execution and are not discarded as a generic process error

#### Scenario: A starting Job is aborted mid-start
- **WHEN** the Agent aborts a `job_start` command task after the kernel spawn began (for example because the session was cancelled or another command result delivery failed)
- **THEN** either the kernel is registered and reachable by stop/stop-all, or the kernel's cancellation token has been cancelled, and no running kernel is left unregistered and uncancellable

#### Scenario: A revoked Job leaves no zombie kernel
- **WHEN** the Hub fences a node and the Agent stops the Job while a start for the same Job is still in flight
- **THEN** the stop path cancels the in-flight start's cancellation token or awaits its registration, and no kernel from the superseded generation keeps running after the stop completes

## Purpose

Define durable Fleet resources, Agent observations, compatibility state, and bounded operational history.
## Requirements
### Requirement: Durable Fleet resource representation

The Hub SHALL expose Node, Stream, configuration version, Operation, Audit Event, Rollout, Job, Task, and Checkpoint resources with stable identifiers and explicit desired, observed, and convergence data where applicable. A new Agent process registration SHALL be treated as a new runtime boot even when it reuses the same logical node identity.

#### Scenario: Read a Fleet resource after Hub restart

- **WHEN** an operator requests a node, Stream, Job, task, checkpoint, operation, or rollout after the Hub has restarted
- **THEN** the response contains the same durable identity, state, generation, and latest outcome that existed before restart

#### Scenario: Agent process restarts

- **WHEN** an Agent registers after its process has restarted and its desired Jobs are still running
- **THEN** the Hub keeps the durable desired state but requires the new process boot to reconstruct missing local runtimes

### Requirement: Agent observation integrity

The Hub SHALL accept Agent observations only from the authenticated session and SHALL reject or ignore reports with an older boot identity or report sequence without changing the current observed snapshot. The Agent SHALL report the process boot identity established at registration, not a session credential that can make a fresh process appear to be the old runtime.

#### Scenario: Accept the first report of a new session

- **WHEN** an Agent registers a new authenticated session and submits report sequence 1 with the new session identity
- **THEN** the Hub resets that node's report cursor for the new session and accepts the report

#### Scenario: Reject a stale observation

- **WHEN** a node submits a report older than the stored boot and sequence cursor
- **THEN** the Hub acknowledges the request without regressing observed state, metrics, task assignments, or checkpoint progress

#### Scenario: Reconcile a fresh Agent process

- **WHEN** a fresh Agent process has an empty local Job runtime map but receives a durable desired-running Job whose old start operation is successful
- **THEN** the Agent's boot-identified report causes reconciliation to dispatch or recreate the missing Job instead of treating the old operation as sufficient

### Requirement: Node compatibility status

The Hub SHALL record each node's protocol version, software version, and capabilities and SHALL make compatibility status visible before dispatching a command that requires capabilities, including Job runtime and state-backend capabilities.

#### Scenario: Block an incompatible command

- **WHEN** a rollout or Job deployment targets a node that lacks a required capability or protocol version
- **THEN** the Hub does not dispatch an executable Attempt and records a stable compatibility failure for that node

### Requirement: Durable operational history

The Hub SHALL retain bounded queryable operation and audit history according to configured retention limits, without removing the current desired state or latest observed state when old history is pruned.

#### Scenario: Prune old history

- **WHEN** operation or audit history exceeds its configured retention bound
- **THEN** the Hub prunes only eligible historical records and continues serving current resource state, active intents, and latest Job checkpoint status

### Requirement: Completed checkpoints SHALL contain every planned task
The Hub and Agent SHALL compare a checkpoint manifest's task/assignment set with the complete Job plan before publishing it as completed. A manifest containing only currently online nodes SHALL not be considered a valid recovery artifact.

#### Scenario: One planned node is offline
- **WHEN** the Hub can dispatch checkpoint work only to a subset of the planned nodes
- **THEN** the checkpoint remains pending or failed and no completed artifact is published with missing task snapshots or source positions

#### Scenario: All planned nodes participate
- **WHEN** every planned task reports exactly one matching manifest entry for the same barrier generation
- **THEN** the Hub/Agent may aggregate, seal, and publish the completed checkpoint

### Requirement: Agent recovery SHALL restore the assigned partition watermark
When a Job task restores a checkpointed watermark, the Agent SHALL use the task's actual source partition identity from the manifest/assignment rather than defaulting to partition 0.

#### Scenario: Recover partition three
- **WHEN** a source task assigned to partition 3 restores watermark 10,000
- **THEN** partition 3 has progress 10,000, partition 0 is not synthesized, and late-event classification uses the restored real partition

### Requirement: Hub sustains the maximum fleet

The Hub SHALL remain functionally complete with a fleet at the node admission cap (`MAX_NODES`): every dispatched command SHALL reach a terminal state, the node registry SHALL stay queryable for every node, and each durable history store (terminal operations, processed outbox rows, terminal attempts, audit events, checkpoint records, events) SHALL converge under its bounded retention policy. After a Hub restart or a simultaneous loss and rebirth of the whole fleet, the Hub SHALL reconverge to the desired state without operator intervention, and fleet-wide re-registration SHALL be desynchronized by randomized reconnect backoff. The capacity envelope observed by the verification harness SHALL be recorded for capacity planning.

#### Scenario: Full-fleet operations complete

- **WHEN** a fleet at the admission cap runs steady Job churn and every dispatched command is awaited
- **THEN** each command reaches a terminal state within the assertion timeout and the node registry lists every node with its status

#### Scenario: Durable history converges after churn

- **WHEN** the fleet generates sustained reconciliation churn followed by a quiescent period
- **THEN** each bounded history store converges to its retention bound and does not grow further across subsequent sweeps

#### Scenario: Hub restart storm recovers

- **WHEN** the Hub process is restarted repeatedly while the full fleet keeps polling with expired session credentials
- **THEN** Agents re-register through randomized backoff without synchronized stampede, and the desired state reconverges after each restart

#### Scenario: Fleet rebirth recovers

- **WHEN** every Agent is stopped and restarted with the same node identity and boot id
- **THEN** each Agent re-registers, resumes its desired state, and the Hub reports no permanently non-terminal operations

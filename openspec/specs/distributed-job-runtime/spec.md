# distributed-job-runtime Specification

## Purpose
TBD - created by archiving change add-distributed-stateful-streaming-runtime. Update Purpose after archive.
## Requirements
### Requirement: Job plans SHALL have stable distributed identities

The runtime SHALL represent a Job as a versioned DAG with stable operator, task, subtask, partition, and key-group identities. Any fused execution-chain representation used for local recovery SHALL retain a deterministic mapping to every logical planned task so checkpoint membership remains complete.

#### Scenario: Compile a valid Job plan

- **WHEN** a valid SQL or Job specification is submitted
- **THEN** the system produces a versioned plan whose nodes, edges, partitions, and stateful operators have stable identities

#### Scenario: Persist a fused execution chain

- **WHEN** adjacent stateless processors are fused into one execution chain for a local Job checkpoint
- **THEN** the checkpoint records the chain identity and a mapping covering every logical task in the plan

### Requirement: Compute nodes SHALL execute fenced task attempts

Compute nodes SHALL execute fenced task attempts: each task attempt is bound to one (job, generation, node), stale generations are rejected, and a task's placement SHALL respect execution topology — colocated placement keeps every connected component on one node (an edge MUST NOT be split), while explicitly enabled split placement MAY place the endpoints of an ordinary data edge on different nodes when both nodes advertise the cross-node data plane. Side edges — error outputs and late-event routes — MUST remain co-located, and nodes without the data plane keep the co-location constraint. Command dispatch, generation fencing, and per-node assignment filtering are unchanged by the placement strategy.

#### Scenario: colocated placement rejects a split edge

- **WHEN** colocated placement assigns an edge's endpoints to different nodes
- **THEN** the assignment is rejected with "connected tasks must be co-located"

#### Scenario: split placement executes a cross-node data edge

- **WHEN** split placement places an ordinary partitioned data edge's endpoints on two data-plane-capable nodes
- **THEN** both assignments are accepted and each node's graph contains the remote edge

#### Scenario: split placement rejects a cross-node side edge

- **WHEN** split placement places an error or late-event route endpoint on a different node from its origin
- **THEN** the assignment is rejected before any task attempt starts

### Requirement: Job execution SHALL provide bounded backpressure
The runtime SHALL propagate input, operator, network, and output pressure through the Job DAG and SHALL expose a bounded state when a downstream task cannot make progress.

#### Scenario: A downstream task is unavailable
- **WHEN** a downstream task stops consuming data
- **THEN** upstream tasks stop or reduce dispatch within configured bounds and the Job observation reports the blocked edge and pressure reason

#### Scenario: A worker pool drains under a slow downstream
- **WHEN** a processor chain runs with a worker pool (concurrency greater than one) and its downstream edge stays full because the consumer is slow
- **THEN** processed results accumulate only within a bounded window, the workers stop accepting new deliveries, and the backpressure reaches the source chain instead of growing memory without bound

### Requirement: Job lifecycle SHALL support recovery operations

The control plane SHALL support submitting, starting, stopping, restarting, cancelling, and observing Jobs without changing the lifecycle semantics of existing YAML Streams. Recovery SHALL restore source positions and every physical event-time partition watermark from one acknowledged checkpoint cut. The distributed Hub SHALL persist the Job lifecycle and recovery pointer before reporting ready, and an externally reachable Hub SHALL require authenticated operator/node access. When reconciliation re-places a Job onto a node set that excludes a node holding a successful start for the current generation, that stale claim SHALL be superseded and the abandoned node SHALL receive a stop command when it becomes reachable again, so exactly one live runner remains. A stable placement SHALL NOT be disturbed by reconciliation alone; the only exception is the Job's explicit opt-in rebalance policy (see `resource-aware-placement`), which triggers a fenced re-placement after sustained resource pressure on its placed node.

Partial-node-failure re-placement SHALL be incremental for automatic placements: when only some of the previously dispatched nodes are offline (or rebalance-evicted), the reconciler SHALL keep the remembered dispatch order and replace only the failed slots in place, so every surviving node's task→node assignment stays byte-identical and its successful start and running kernel remain truthful. Replacement slots SHALL be filled from the ranked candidate set (shuffle-capable when the Job uses split placement); when no new candidate exists, a surviving node may occupy the failed slot; when no previous placement survives, the full ranked placement applies as before. A successful start SHALL only satisfy the dispatch-skip when the assignment task set it was dispatched with matches the currently computed assignment for that node; a mismatch SHALL supersede the stale start and re-dispatch. A node's failed Job observation at the current generation SHALL invalidate that node's successful start for re-dispatch, so a crashed kernel (for example after a remote edge exhausted its reconnect budget) restarts from a recovery artifact without an operator-driven generation bump. On the Agent, a re-delivered same-generation start whose per-node task set matches the running kernel's assignment SHALL remain an idempotent no-op, while a differing task set SHALL replace the kernel through the existing teardown-and-recover path.

#### Scenario: Restart a failed Job

- **WHEN** an authorized operator requests a restart for a failed Job
- **THEN** the Hub creates a new fenced task attempt and the Compute nodes restore or initialize the Job according to its recovery policy

#### Scenario: Hub restarts with durable storage

- **WHEN** the Hub process restarts after Jobs and checkpoint pointers have been persisted
- **THEN** it restores the lifecycle records before becoming ready and reconciliation resumes from the durable desired state

#### Scenario: External unauthenticated access is attempted

- **WHEN** a caller reaches a non-loopback Hub without valid operator or node credentials
- **THEN** the Hub rejects the request and does not mutate Jobs, leases, or operations

#### Scenario: Re-placement after a node blip does not duplicate the Job

- **WHEN** a placed node loses reachability, the Job is re-placed onto other nodes, and the original node later returns
- **THEN** the original node's current-generation start is marked superseded and it receives a stop command instead of being deduped back into the target set

#### Scenario: A stable placement is not disturbed

- **WHEN** every node of the current successful placement remains inside the reconciled target set and the Job has not opted into pressure rebalancing
- **THEN** no start operation is superseded and no stop command is dispatched

#### Scenario: Opt-in pressure rebalance fences the abandoned node

- **WHEN** a Job with the rebalance policy enabled trips its sustained-pressure trigger on the placed node
- **THEN** the re-placement supersedes the abandoned node's start and dispatches its stop command through the same fencing path as any other re-placement

#### Scenario: Partial node loss keeps surviving assignments byte-identical

- **WHEN** an automatically placed running Job loses one of several dispatched nodes and a replacement candidate is available
- **THEN** the reconciled task→node mapping is unchanged for every surviving node and only the failed node's tasks move to the replacement node

#### Scenario: No replacement candidate concentrates the failed slot

- **WHEN** a dispatched node fails and no new candidate node is available
- **THEN** the failed slot is filled by a surviving node (which then hosts both slots' tasks) instead of reshuffling every task across the reduced node set

#### Scenario: A stale successful start with drifted assignments is superseded

- **WHEN** the currently computed assignment for a node differs from the task set its successful start was dispatched with (for example after the Hub restarted without its order memory)
- **THEN** that start is superseded and a fresh start with the current assignment is dispatched

#### Scenario: A failed observation re-dispatches the node's start

- **WHEN** a node reports a failed Job observation at the current generation (for example its kernel ended after a remote edge exhausted its reconnect budget)
- **THEN** that node's successful start no longer satisfies the dispatch skip and the node receives a fresh start carrying a recovery artifact

#### Scenario: A matching same-generation start stays a no-op on the Agent

- **WHEN** an Agent with a live kernel at generation G receives a re-delivered start at G whose per-node task set equals the kernel's assignment
- **THEN** the start is an idempotent success and the kernel is not restarted

#### Scenario: A drifting same-generation start replaces the kernel on the Agent

- **WHEN** an Agent with a live kernel at generation G receives a start at G whose per-node task set differs from the kernel's assignment
- **THEN** the existing kernel is torn down and re-spawned with the new assignment through the recovery path instead of being reported as a healthy no-op

### Requirement: Job resources SHALL be connected before task execution
The unified runtime SHALL connect all temporary resources, inputs, and outputs required by a Job before spawning its task event loops. Partial startup SHALL close every resource already opened and return the startup error.

#### Scenario: Temporary processor resource is used
- **WHEN** a processor resolves a Redis or other temporary resource during its first `get` call
- **THEN** the resource has already completed `connect()` and the processor does not receive a false disconnection error

#### Scenario: Resource connection fails
- **WHEN** one resource fails to connect after another resource has connected
- **THEN** the runtime closes the connected resources in reverse order and reports the failure before accepting input

### Requirement: Stream processor concurrency SHALL be preserved
The unified execution graph SHALL honor the configured Stream processor worker count (`pipeline.thread_num`) without changing a single source task into a single Kafka partition assignment. Processor worker pools SHALL remain bounded, cancellable, and ordered where the legacy Stream contract requires ordered output; stateful/window state commits SHALL remain serialized by their execution epoch.

#### Scenario: Stream requests multiple processor workers
- **WHEN** a Stream config sets `pipeline.thread_num` greater than 1
- **THEN** the compiled runtime creates the configured number of eligible processor workers while retaining the source and sink topology and bounded backpressure

### Requirement: Partitioned edges preserve complete downstream ownership

For a partitioned edge, graph construction SHALL connect each upstream task to the complete eligible set of downstream subtasks. Dispatch SHALL select the downstream task using the `JobPlan` key-group ownership mapping rather than a same-subtask shortcut.

#### Scenario: A key maps to another downstream subtask

- **WHEN** a source subtask emits a keyed record whose planned key-group owner is downstream subtask 1 while the source subtask is 0
- **THEN** the record is delivered to downstream subtask 1 and its keyed state is not split into the source-indexed task

### Requirement: Partitioned routing remains stable across source partitions

Partitioned routing SHALL produce the same downstream task for the same key regardless of which physical source partition or upstream subtask delivered it.

#### Scenario: Same key arrives from two source partitions

- **WHEN** identical keys arrive through two physical source partitions assigned to different upstream tasks
- **THEN** both records are routed to the one planned key-group owner and update one logical keyed state namespace

### Requirement: Job detail reflects observed task attempt state

The Job detail task listing SHALL report the observed state of each task attempt as last reported by the executing compute node, and SHALL distinguish observed state from desired placement. When no observation exists for a task (for example before first dispatch), the listing SHALL fall back to the desired placement state and mark the entry as not observed. The listing SHALL NOT present desired placement state as if it were observed runtime state.

#### Scenario: Running task shows its observed state

- **WHEN** a compute node reports a task attempt as running and an operator opens the Job detail
- **THEN** the task entry shows the observed running state and is marked observed

#### Scenario: Undispatched task falls back to placement

- **WHEN** a Job detail is requested for a task that has never been reported by any node
- **THEN** the task entry carries the desired placement state and is marked not observed


# distributed-job-runtime Delta

## MODIFIED Requirements

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

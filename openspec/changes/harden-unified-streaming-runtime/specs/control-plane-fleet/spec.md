## MODIFIED Requirements

### Requirement: Agent observation integrity
The Hub SHALL accept Agent observations only from the authenticated session and SHALL reject or ignore reports with an older boot identity or report sequence without changing the current observed snapshot. A newly registered session SHALL receive a fresh report cursor; a reconnecting Agent SHALL be able to start its sequence at zero without being treated as stale under the previous session.

#### Scenario: Reject a stale observation
- **WHEN** a node submits a report older than the stored session identity and sequence cursor
- **THEN** the Hub acknowledges the request without regressing observed state, metrics, task assignments, or checkpoint progress

#### Scenario: Accept the first report of a new session
- **WHEN** an Agent registers a new authenticated session and submits report sequence 1 with the new session identity
- **THEN** the Hub resets that node's report cursor for the new session and accepts the report

#### Scenario: Old session reports after reconnect
- **WHEN** a delayed report from a previous session arrives after a new session is registered
- **THEN** the Hub rejects or ignores it without changing the new session's observed state

## ADDED Requirements

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

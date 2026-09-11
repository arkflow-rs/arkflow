## MODIFIED Requirements

### Requirement: Job lifecycle SHALL support recovery operations
The control plane SHALL support submitting, starting, stopping, restarting, cancelling, and observing Jobs without changing the lifecycle semantics of existing YAML Streams. Recovery SHALL restore source positions and every physical event-time partition watermark from one acknowledged checkpoint cut. When reconciliation re-places a Job onto a node set that excludes a node holding a successful start for the current generation, that stale claim SHALL be superseded and the abandoned node SHALL receive a stop command when it becomes reachable again, so exactly one live runner remains.

#### Scenario: Restart a failed Job
- **WHEN** an authorized operator requests a restart for a failed Job
- **THEN** the Hub creates a new fenced task attempt and the Compute nodes restore or initialize the Job according to its recovery policy

#### Scenario: Re-placement after a node blip does not duplicate the Job
- **WHEN** a placed node loses reachability, the Job is re-placed onto other nodes, and the original node later returns
- **THEN** the original node's current-generation start is marked superseded and it receives a stop command instead of being deduped back into the target set

#### Scenario: A stable placement is not disturbed
- **WHEN** every node of the current successful placement remains inside the reconciled target set
- **THEN** no start operation is superseded and no stop command is dispatched

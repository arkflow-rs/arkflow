## MODIFIED Requirements

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

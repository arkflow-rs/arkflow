## ADDED Requirements

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

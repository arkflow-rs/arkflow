## MODIFIED Requirements

### Requirement: Recovery SHALL restore deterministic state

Recovery SHALL restore state, source positions, watermarks, and task assignments from one compatible checkpoint or savepoint before processing new input. Restored source cursors, task membership, state format, effective state namespaces, and execution-chain mappings SHALL be validated together. A recovery-required start with no compatible artifact SHALL fail explicitly and MUST NOT silently initialize an empty backend. An in-process source reconnect SHALL resume from the source's acknowledged cursor — explicit assignments SHALL carry those offsets — instead of relying on `auto.offset.reset`, so an outage window is neither skipped nor fully replayed.

#### Scenario: Compute node restarts

- **WHEN** a durable stateful Job restarts after a failure and the start is recovery-required
- **THEN** the Job restores from the selected checkpoint, replays the required source range, and reports recovery progress before becoming healthy

#### Scenario: Recovery artifact is absent

- **WHEN** a recovery-required start has no completed compatible checkpoint or savepoint
- **THEN** the Agent returns a missing-recovery error and does not connect the source or start the kernel with empty state

#### Scenario: Local checkpoint contains fused chain snapshots

- **WHEN** a local Job restarts with a checkpoint whose manifest maps all planned tasks to a fused chain snapshot
- **THEN** the runtime validates the complete mapping, restores the chain state and source position, and reads new input only after restore completes

#### Scenario: Reconnect resumes at the acknowledged frontier

- **WHEN** an explicit-partition Kafka source reconnects after a `Disconnection` with acknowledged offsets in its frontier
- **THEN** the rebuilt assignment carries the frontier offsets, records produced during the outage are consumed, and records acknowledged before the outage are not reprocessed

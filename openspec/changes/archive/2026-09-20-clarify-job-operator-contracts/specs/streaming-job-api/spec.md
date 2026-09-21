## MODIFIED Requirements

### Requirement: Job validation SHALL precede deployment

The API SHALL validate SQL syntax, connector options, schema compatibility, time semantics, state requirements, namespace and state-budget configuration, and unsupported plan constructs before creating a running task attempt. Every validation entry point SHALL perform the same deep component/backend build checks as deployment, and unsupported legacy window joins or distributed multi-input Join operators SHALL be rejected explicitly rather than silently dropped or built through a single-input processor.

#### Scenario: SQL uses an unsupported stateful construct

- **WHEN** validation finds a construct without a supported runtime or state implementation
- **THEN** the API rejects the Job with an actionable validation error and does not mutate running tasks

#### Scenario: Job uses an unsupported distributed Join

- **WHEN** a Job declares a Join operator without a dedicated multi-input runtime
- **THEN** validation rejects the Job before it is persisted or deployed

#### Scenario: Validation uses an unknown component

- **WHEN** a Job references an unknown input, processor, sink, or unsupported local state backend
- **THEN** validation fails before the Job is spawned

#### Scenario: Legacy window contains a join

- **WHEN** a legacy tumbling or session window configures a join that has no equivalent compiled graph node
- **THEN** validation rejects the configuration with an explicit migration error

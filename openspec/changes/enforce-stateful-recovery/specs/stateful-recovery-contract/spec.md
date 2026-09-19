## ADDED Requirements

### Requirement: Stateful Jobs SHALL declare their durability contract

A Job containing a stateful operator or Window SHALL declare a state specification. Durable state SHALL be the default, SHALL declare a checkpoint policy, and SHALL use a stable working-state root. Ephemeral state SHALL require an explicit configuration value and SHALL be observable as non-recoverable.

#### Scenario: Stateful Job omits state

- **WHEN** validation compiles a Job with a stateful operator or Window and no state specification
- **THEN** validation rejects the Job before it is persisted or started

#### Scenario: Durable state omits checkpoint

- **WHEN** validation compiles a durable stateful Job without a checkpoint specification
- **THEN** validation rejects it with an actionable message naming the missing checkpoint contract

#### Scenario: Ephemeral mode is explicit

- **WHEN** a Job configures `state.durability: ephemeral`
- **THEN** validation accepts the Job only with an explicit warning/metric contract that node loss and restart do not restore operator state

### Requirement: Initial empty start SHALL be distinct from recovery

The control plane SHALL allow a new durable stateful Job to initialize from empty state once. After a successful placement, generation replacement, restart, or explicit recovery request, the control plane SHALL mark the start as recovery-required.

#### Scenario: First deployment has no checkpoint

- **WHEN** a new durable stateful Job has no prior successful start and no checkpoint artifact
- **THEN** the Job may start with an empty backend and reports an initializing state

#### Scenario: Replacement has no checkpoint

- **WHEN** a previously started durable stateful Job is re-placed or restarted and no compatible checkpoint exists
- **THEN** reconciliation rejects the start with a missing-recovery error and does not dispatch a fresh empty start

### Requirement: Recovery SHALL restore before source consumption

When a start is marked recovery-required, the Agent SHALL read and validate one compatible checkpoint or savepoint, restore state, source positions, and watermarks, and only then connect or read from sources. A missing, corrupt, incompatible, or task-incomplete artifact SHALL fail the start.

#### Scenario: Compatible artifact is restored

- **WHEN** a recovery-required start receives a complete compatible artifact
- **THEN** state and source positions are restored before the first source read and the Job reports recovery progress

#### Scenario: Artifact is filtered as incompatible

- **WHEN** all candidate artifacts fail Job version, namespace, state format, task membership, or checksum compatibility
- **THEN** the start fails with the compatibility reason instead of initializing empty state

### Requirement: State paths SHALL be stable and isolated

The runtime SHALL resolve a durable state root independently of the system temporary directory and SHALL isolate Job, version, generation, operator, and task identities. A stale generation MUST NOT write into a current generation's working state.

#### Scenario: Process restarts on the same node

- **WHEN** a Job process restarts with the same durable root and compatible generation
- **THEN** it reopens the same isolated working-state area or restores from its checkpoint without using a new unrelated temporary directory

#### Scenario: Two Jobs use the same backend

- **WHEN** two Jobs or operators use equal user namespace prefixes
- **THEN** their effective state namespaces remain distinct through Job and operator identity components

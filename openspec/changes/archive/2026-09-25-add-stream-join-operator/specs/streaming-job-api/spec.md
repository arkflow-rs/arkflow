# streaming-job-api 变更（Delta）

## MODIFIED Requirements

### Requirement: Job plans SHALL be validated before execution

The API SHALL validate SQL syntax, connector options, schema compatibility, time semantics, state requirements, namespace and state-budget configuration, and unsupported plan constructs before creating a running task attempt. Every validation entry point SHALL perform the same deep component/backend build checks as deployment. A `Join` operator SHALL be accepted when its configuration passes `JoinOperatorConfig::validate` and it declares exactly two inbound edges (edge declaration order fixes left = input 0, right = input 1); any other inbound arity SHALL be rejected with an error naming the required arity. Legacy window joins that have no equivalent compiled graph node remain rejected.

#### Scenario: Job uses an unsupported distributed Join

- **WHEN** a Job declares a Join operator whose inbound edge count is not two
- **THEN** validation rejects the Job before it is persisted or deployed with an error naming the two-edge requirement

#### Scenario: Job declares a well-formed Join

- **WHEN** a Job declares a Join operator with a valid config (left/right keys, window_ms) and exactly two inbound edges
- **THEN** validation accepts the Job and the join chain executes single-threaded with input-side tagging

#### Scenario: Legacy window contains a join

- **WHEN** a legacy tumbling or session window configures a join that has no equivalent compiled graph node
- **THEN** validation rejects the configuration with an explicit migration error

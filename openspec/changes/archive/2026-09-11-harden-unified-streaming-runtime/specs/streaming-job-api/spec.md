## MODIFIED Requirements

### Requirement: Job validation SHALL precede deployment
The API SHALL validate SQL syntax, connector options, schema compatibility, time semantics, state requirements, unsupported plan constructs, component construction, state backend construction, and graph construction before creating a running task attempt. The same deep-build validation SHALL apply to Jobs declared in engine configuration and to Stream configurations after compilation to JobSpec.

#### Scenario: SQL uses an unsupported stateful construct
- **WHEN** validation finds a construct without a supported runtime or state implementation
- **THEN** the API rejects the Job with an actionable validation error and does not mutate running tasks

#### Scenario: Configuration references an unknown component
- **WHEN** a submitted Job or compiled Stream references an unknown input, processor, sink, temporary, or state backend
- **THEN** validation fails before startup and identifies the configuration path and component that cannot be built

#### Scenario: Deep validation succeeds
- **WHEN** every declared component, local backend, graph edge, and state requirement can be constructed without consuming input or writing output
- **THEN** validation succeeds and the subsequent deployment uses the same component-building path

## ADDED Requirements

### Requirement: Generated configuration schema SHALL describe local Jobs
The generated Engine configuration schema SHALL expose the `jobs` property, its JobSpec fields, and the same additional-property and required-field rules used by deserialization and deep validation. A jobs-only configuration SHALL be representable by schema-driven clients.

#### Scenario: Generate a jobs-only configuration schema
- **WHEN** a client requests the Engine schema and builds a configuration containing only valid `jobs`
- **THEN** the schema accepts the `jobs` property and exposes its Job, operator, source, sink, state, checkpoint, and recovery fields

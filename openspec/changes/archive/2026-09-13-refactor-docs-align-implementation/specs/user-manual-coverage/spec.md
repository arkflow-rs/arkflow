# user-manual-coverage Delta

## ADDED Requirements

### Requirement: The manual SHALL provide a tutorial path from install to a durable pipeline
The documentation SHALL include a tutorial path that takes a new user from
installation through a first running pipeline to a pipeline with WAL-backed
durability. Each tutorial SHALL be task-oriented, runnable end to end with
commands and complete configuration, and SHALL state the observable outcome
the reader verifies at each step.

#### Scenario: A new user completes the tutorial path
- **WHEN** a reader follows the tutorials in order, copying commands and
  configuration as written
- **THEN** each tutorial ends in a stated, verifiable running state (output
  visible, durability behavior demonstrated) without requiring knowledge from
  component reference pages

#### Scenario: A tutorial configuration is invalid
- **WHEN** a tutorial's YAML configuration is validated by the example
  validation gate
- **THEN** an invalid configuration fails CI with the tutorial page and
  example file identified

### Requirement: The manual SHALL cover common tasks as how-to guides
The documentation SHALL include task-oriented how-to guides for the common
scenarios: consuming Kafka and writing to SQL, CDC ingestion with Debezium
and the Schema Registry codec, windowed aggregation, HTTP ingestion, and
rolling out jobs through the control plane. Each guide SHALL identify
prerequisites, provide complete commands and configuration, state the
expected result, and link to related concept and reference pages.

#### Scenario: A user performs Kafka-to-SQL delivery
- **WHEN** a reader follows the Kafka-to-SQL how-to guide
- **THEN** the guide provides a complete, validated pipeline configuration
  and the expected sink behavior, without requiring the reader to assemble
  configuration from component reference pages

#### Scenario: A guide skips prerequisites
- **WHEN** any how-to guide is reviewed
- **THEN** it lists its prerequisites (running services, versions, prior
  tutorials) before the first command

### Requirement: End-to-end cases SHALL be backed by validated examples
The documentation SHALL include end-to-end case studies, each backed by a
YAML file under `examples/` registered in
`docs/reference/example-manifest.json`. Each case page SHALL link its example
file, describe the scenario's source and sink, and state the expected data
flow and outcome. Case examples participate in the offline example-validation
gate.

#### Scenario: A case example is missing from the manifest
- **WHEN** a case page references an `examples/` file that is not registered
  in the example manifest
- **THEN** the documentation check fails naming the unregistered example

#### Scenario: A reader reproduces a case
- **WHEN** a reader runs a case's example configuration against the case's
  stated prerequisites
- **THEN** the observed data flow matches the case page's stated outcome

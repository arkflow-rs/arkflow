## ADDED Requirements

### Requirement: Iceberg output effectively-once boundary is scoped to source position
With `exactly_once: true` the `iceberg` output SHALL eliminate the duplicates caused by a crash between a successful table commit and the source acknowledgement, whether the records return through WAL replay or Kafka redelivery, by dropping replayed rows below a per-`(topic, partition)` offset watermark that is committed atomically with the data (see `iceberg-output`). The guarantee SHALL hold only under three preconditions: every row reaches the output with its Kafka source position columns; the path from the Kafka input to the output is an ordered single chain per partition; and the `writer_id` identifies exactly one consumer group. Outside these preconditions, and whenever `exactly_once` is `false`, the output delivers at-least-once. The documentation SHALL state this boundary.

#### Scenario: Crash after commit before acknowledgement
- **WHEN** the output has committed a range and the process crashes before the source acknowledgement
- **THEN** the replayed range produces no duplicate row in the table

#### Scenario: Position columns missing fails closed
- **WHEN** `exactly_once` is `true` and the rows reaching the output lack a source position column
- **THEN** the output fails with an explicit error instead of writing at-least-once

#### Scenario: Unordered topology is outside the guarantee
- **WHEN** rows of one partition reach the output out of offset order and the watermark has already passed a lower offset
- **THEN** the output drops those rows as already committed, which is why an ordered single chain is a precondition rather than a recommendation

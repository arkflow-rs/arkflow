## ADDED Requirements

### Requirement: Encoding SHALL resolve each column's field descriptor once

`arrow_to_protobuf` SHALL resolve each Arrow column's target `FieldDescriptor` once per encode call (before the row loop) and SHALL set values through the resolved descriptor; it MUST NOT resolve field descriptors by name per cell inside the row loop. The produced encoded messages SHALL be byte-identical to the per-cell name-resolution output, and the unsupported-type and type-mismatch error behaviors SHALL be unchanged.

#### Scenario: Encoded output is byte-identical to the per-cell baseline

- **WHEN** the same Arrow batch is encoded through the columnar-descriptor path and through the previous per-cell `set_field_by_name` path
- **THEN** every output message is byte-identical between the two paths, including field order, absent-field handling, and null-to-absence mapping

#### Scenario: Per-cell name resolution is eliminated

- **WHEN** a wide batch (many columns × many rows) is encoded
- **THEN** field-descriptor resolution work is proportional to the column count, not the cell count

#### Scenario: Unsupported types still error at build time

- **WHEN** a batch carries an Arrow type with no supported proto mapping
- **THEN** the encode call fails with the same informative error as before the change

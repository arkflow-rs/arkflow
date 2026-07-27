## ADDED Requirements

### Requirement: Type-mismatched fields error instead of silently dropping
When an Arrow column's datatype does not match the proto field's `Kind` during `arrow_to_protobuf`, the processor/codec SHALL return an error naming the field, the expected proto kind, and the actual Arrow datatype. It SHALL NOT silently skip the field.

#### Scenario: Mismatched integer width surfaces an error
- **WHEN** an Arrow `Int32` column is encoded into a proto `int64` field
- **THEN** the conversion returns an `Err` naming the field and both types, rather than silently omitting the field

### Requirement: Decoded messages share a stable schema
`protobuf_to_arrow` SHALL build its schema from the message descriptor's full field set with every field nullable, so every decoded message yields the same schema regardless of which fields are present. Concatenating per-message batches SHALL NOT fail with a schema mismatch.

#### Scenario: Absent field does not break concat
- **WHEN** message A has field `b` set and message B does not
- **THEN** both decode to the same schema (field `b` present, nullable), and concatenating the two batches succeeds with a null in B's `b` column

### Requirement: Null Arrow values map to the proto field's absence
During `arrow_to_protobuf`, a null Arrow value SHALL NOT be explicitly written into the encoded proto message; the field is left absent (which in proto3 reads back as the default value), rather than being read through the validity bitmap and explicitly set.

#### Scenario: Null row does not carry another row's value
- **WHEN** an Arrow column has a null in row j while another row carries a non-default value
- **THEN** the encoded proto message for row j decodes to the proto default, never to another row's value

### Requirement: Unsupported field types produce an informative error
When a proto field uses an unsupported kind (nested message, repeated, map, oneof, proto3 optional), the conversion SHALL return an error that includes the field name and `field.kind()`.

#### Scenario: Nested message field reports its kind
- **WHEN** a proto field of kind message is encountered during `protobuf_to_arrow`
- **THEN** the error message names the field and includes its kind

### Requirement: Component metadata schema matches runtime config
The `arrow_to_protobuf` component metadata JSON Schema SHALL declare `fields_to_include`, and the `protobuf_to_arrow` schema SHALL declare `value_field`. Both schemas retain `additionalProperties: false`, so valid runtime configs pass schema validation.

#### Scenario: fields_to_include passes schema validation
- **WHEN** the `arrow_to_protobuf` metadata is queried
- **THEN** `arkflow components show processor arrow_to_protobuf --format json` lists `fields_to_include` in the schema properties

#### Scenario: value_field passes schema validation
- **WHEN** the `protobuf_to_arrow` metadata is queried
- **THEN** `arkflow components show processor protobuf_to_arrow --format json` lists `value_field` in the schema properties

### Requirement: Test coverage pins the protobuf round-trip and edge cases
The protobuf codec/processor SHALL ship tests covering at least: an encode→decode round-trip across all supported scalar types (bool, int32, int64, uint32, uint64, float32, float64, string, bytes, enum), the `fields_to_include` option, the `value_field` option, the unsupported-type error path, and the null-value path.

#### Scenario: Scalar round-trip tests exist
- **WHEN** `cargo test -p arkflow-plugin protobuf` runs
- **THEN** encode→decode round-trip tests for every supported scalar type pass

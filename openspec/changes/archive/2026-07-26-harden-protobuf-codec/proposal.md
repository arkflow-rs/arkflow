## Why

The protobuf processor/codec (`component/protobuf.rs`, `processor/protobuf.rs`, `codec/protobuf.rs`) silently drops data and rejects valid configs:

- **Type-mismatched fields are silently dropped.** `arrow_to_protobuf` downcasts each Arrow column to the proto field's expected array type via `if let Some(...)`, so a mismatch (e.g. Arrow `Int32` column for a proto `int64` field) makes the downcast return `None` and the field is skipped with no error or log (`component/protobuf.rs:215-326`).
- **concat_batches fails at runtime on optional fields.** `protobuf_to_arrow` skips any field whose value is absent (`component/protobuf.rs:133-135`), so two messages with different fields present produce different schemas and `concat_batches` later fails with only "Batch merge failed" (`processor/protobuf.rs:136-138`, `codec/protobuf.rs:105-107`).
- **Null Arrow values are silently encoded as proto defaults.** `arrow_to_protobuf` reads `value.value(j)` ignoring the validity bitmap, so a null row becomes `false`/`0`/`""` in the encoded message (`component/protobuf.rs:217-318`).
- **Valid configs are rejected by the schema.** The component metadata JSON Schema omits `fields_to_include` (arrow_to_protobuf) and `value_field` (protobuf_to_arrow) while setting `additionalProperties: false`, so the IDE YAML server and `arkflow schema` reject configs the runtime accepts (`processor/protobuf.rs:244-271`).
- **No codec round-trip tests.** The codec's tests cover only config deserialization; there is no encode→decode round-trip, and `fields_to_include` / `value_field` / non-happy-path types are untested.

## What Changes

- **FIX**: A type mismatch between an Arrow column and its proto field SHALL return an error naming both types, not silently skip the field (`component/protobuf.rs:215-326`).
- **FIX**: `protobuf_to_arrow` SHALL build a stable schema from the descriptor's full field set (all fields nullable), so every message yields the same schema and `concat_batches` no longer fails on optional/absent fields (`component/protobuf.rs:116-191`).
- **FIX**: Null Arrow values SHALL be left unset in the encoded proto message (not silently coerced to the proto default) (`component/protobuf.rs:217-318`).
- **FIX**: Unsupported-field errors SHALL include `field.kind()` so users can see why a type is unsupported (`component/protobuf.rs:178-183, 319-324`).
- **FIX**: The component metadata JSON Schema SHALL declare `fields_to_include` and `value_field` so valid configs pass schema validation (`processor/protobuf.rs:244-271`).
- **CLEANUP**: Remove dead code, fix inconsistent `init()` visibility, drop the redundant `use serde_json;`, and clear the clippy warnings (`map_or(false,..)`→`is_some_and`, `.clone()` on `Copy` types, redundant `Ok(..)?`).
- **NEW**: Tests — codec encode/decode round-trip, `fields_to_include`, `value_field`, full scalar type coverage (bool/bytes/enum/uint32/uint64/float32 in addition to int64/double/string), the unsupported-type error path, and the null-value path.
- **DOCS**: Document the supported/unsupported field-type matrix (scalar fields supported; nested message, repeated, map, oneof, proto3 optional are NOT supported) in module docs.

No **BREAKING** public API changes.

## Non-goals

- **Adding support for nested message / repeated / map / oneof / proto3 optional fields** — out of scope; this change only documents the limitation and errors clearly when such a field is encountered.
- **Protobuf/prost-reflect crate version upgrade** — separate change.

## Capabilities

### New Capabilities
- `protobuf-codec`: The protobuf processor/codec's data-correctness contracts — no silent field drops, stable schemas for concat, explicit null handling, schema-valid metadata, and the test coverage that pins them.

### Modified Capabilities
<!-- None — there is no existing openspec/specs/protobuf-codec spec today. -->

## Impact

- **`arkflow-plugin`**: `component/protobuf.rs` (core conversion), `processor/protobuf.rs` (metadata schema + dead code), `codec/protobuf.rs` (visibility + import).
- **Dependencies**: None added or upgraded.
- **Behavioral**: Type mismatches now error instead of silently dropping fields; optional/absent proto fields no longer break concat; null Arrow values are left unset rather than coerced to defaults. These are correctness fixes, not API breaks.
- **Tests**: New round-trip and edge-case tests.

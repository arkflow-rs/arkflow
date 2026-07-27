## 1. arrow_to_protobuf correctness (D1, D3)

- [x] 1.1 Replace each `if let Some(value) = column.downcast_ref::<XxxArray>()` with `downcast_ref::<...>().ok_or_else(...)?` naming the field, expected Kind, and actual DataType → verify: unit test "mismatched Int32→int64 errors"
- [x] 1.2 Skip `set_field_by_name` when `column.is_null(j)` (null → unset, not default) → verify: unit test "null row leaves field unset"

## 2. protobuf_to_arrow correctness (D2, D4)

- [x] 2.1 Build the schema from `descriptor.fields()` (all nullable) instead of the message's present fields; absent values become null; remove the `is_none() { continue }` skip → verify: unit test "absent field does not break concat"
- [x] 2.2 Include `field.kind()` in the unsupported-type error messages → verify: unit test "nested message field reports its kind"

## 3. Component metadata schema (D5)

- [x] 3.1 Add `fields_to_include` to the `arrow_to_protobuf` metadata JSON Schema → verify: `arkflow components show processor arrow_to_protobuf --format json`
- [x] 3.2 Add `value_field` to the `protobuf_to_arrow` metadata JSON Schema → verify: `arkflow components show processor protobuf_to_arrow --format json`

## 4. Cleanups

- [x] 4.1 Remove the redundant empty-check dead code in `processor/protobuf.rs:120-122` → verify: `cargo build -p arkflow-plugin`
- [x] 4.2 Make `init()` visibility consistent (`pub(crate)`) across codec and processor → verify: `cargo build -p arkflow-plugin`
- [x] 4.3 Remove the redundant `use serde_json;` in `codec/protobuf.rs:30` → verify: `cargo build -p arkflow-plugin`
- [x] 4.4 Clear clippy warnings in `component/protobuf.rs` (`map_or(false,..)`→`is_some_and`, `.clone()` on Copy types, redundant `Ok(..)?`) → verify: `cargo clippy -p arkflow-plugin --lib`

## 5. Docs

- [x] 5.1 Add the supported/unsupported field-type matrix to the module docs of `component/protobuf.rs`, `processor/protobuf.rs`, and `codec/protobuf.rs` → verify: doc comment present

## 6. Tests

- [x] 6.1 Add a codec encode→decode round-trip test covering all supported scalar types (bool, int32, int64, uint32, uint64, float32, float64, string, bytes, enum) → verify: `cargo test -p arkflow-plugin protobuf`
- [x] 6.2 Add tests for `fields_to_include`, `value_field`, the unsupported-type error path, and the null-value path → verify: `cargo test -p arkflow-plugin protobuf`

## 7. Validation

- [x] 7.1 `cargo test -p arkflow-plugin` green → verify: command exit 0
- [x] 7.2 `cargo clippy -p arkflow-plugin --lib` introduces no new warnings vs. baseline → verify: command output
- [x] 7.3 `openspec validate harden-protobuf-codec --strict` passes → verify: command exit 0

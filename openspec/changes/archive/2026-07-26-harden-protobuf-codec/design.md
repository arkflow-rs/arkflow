## Context

Protobuf conversion lives in `component/protobuf.rs` (`protobuf_to_arrow`, `arrow_to_protobuf`), shared by the processor (`processor/protobuf.rs`) and codec (`codec/protobuf.rs`). Two directions:

- `arrow_to_protobuf`: iterate Arrow columns, downcast to the array type matching each proto field's `Kind`, set the field on a `DynamicMessage` per row, encode.
- `protobuf_to_arrow`: decode one message, iterate its fields, build a 1-row `RecordBatch`. Callers concat the per-message batches.

The concat step assumes every per-message batch shares one schema, but the code skips absent fields, breaking that assumption. The downcast step assumes the Arrow type always matches the proto `Kind`, and silently drops the field when it doesn't.

## Goals / Non-Goals

**Goals:**
- A type-mismatched column errors loudly instead of disappearing.
- Every decoded message produces the same schema (descriptor-driven, all fields nullable), so `concat_batches` always succeeds.
- Null Arrow values map to "unset proto field", not "proto default".
- The metadata schema matches the runtime's real config fields.
- Tests pin the round-trip and every edge.

**Non-Goals:**
- Supporting nested / repeated / map / oneof / optional fields (document + error only).
- Crate upgrades.

## Decisions

### D1 — Type mismatch is an error, not a silent skip
Replace each `if let Some(value) = column.downcast_ref::<XxxArray>() { ... }` with `downcast_ref::<...>().ok_or_else(|| Error::Process(...))?` naming the field, the expected proto `Kind`, and the actual Arrow `DataType`. A field that can't be converted is a configuration/data bug the user must hear about.

**Alternative considered:** *Coerce where possible (Int32→Int64).* Rejected — silent coercion hides data-shape bugs and has no correct bidirectional answer. Error and let the user cast explicitly upstream.

### D2 — Descriptor-driven stable schema in `protobuf_to_arrow`
Iterate `descriptor.fields()` (not the message's present fields) and build every column as nullable. An absent field value becomes a null in that column. Every message therefore yields an identical schema and `concat_batches` cannot fail on shape. This also removes the `if field_value_opt.is_none() { continue }` skip (`component/protobuf.rs:133-135`).

### D3 — Null Arrow values are left unset
In `arrow_to_protobuf`, check `column.is_null(j)` before `value.value(j)`; a null skips `set_field_by_name`, leaving the proto field unset (proto3's semantics for "absent"). This differs from today, which reads the default through the validity bitmap and explicitly sets the default.

### D4 — Richer unsupported-type errors
The `_ => Err(...)` arms include `field.kind()` (and the field name) so the message identifies the offending type.

### D5 — Metadata schema matches runtime
Add `fields_to_include` to the `arrow_to_protobuf` schema and `value_field` to the `protobuf_to_arrow` schema, keeping `additionalProperties: false`. Validated via `arkflow components show`.

### D6 — Document the field-type matrix
A short table in each module doc listing supported scalar `Kind`s and explicitly listing nested / repeated / map / oneof / optional as unsupported.

## Risks / Trade-offs

- **Type-mismatch errors may surface pre-existing bad pipelines** → this is the intended fix; users get a clear message instead of silently wrong data.
- **Descriptor-driven schema adds null columns for absent fields** → strictly more correct; downstream SQL/JSON already handle nullable columns.
- **Null→unset changes wire bytes** → proto3 doesn't serialize unset fields, so messages get smaller/cleaner; read-back value is unchanged (default).

## Migration Plan

1. D1 + D3 in `arrow_to_protobuf`; D2 + D4 in `protobuf_to_arrow`.
2. D5 metadata; D6 docs.
3. Cleanups (dead code, visibility, import, clippy).
4. Tests; `cargo test -p arkflow-plugin` green.
5. Rollback: revert the three files; no schema/trait change to undo.

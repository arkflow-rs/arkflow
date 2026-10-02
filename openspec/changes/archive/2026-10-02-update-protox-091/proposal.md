## Why

The dependency-refresh track planned `protobuf` 3.7.2 → 4.x, but a spike showed that path is infeasible: `protobuf-parse` has no 4.x release and 3.7.2 pins `protobuf =3.7.2` exactly, while every `protobuf` 4.x version is a prerelease-suffixed (`-release`/`-rc`) experimental line that switched to a new `google_protobuf` dependency at 4.36. Meanwhile ArkFlow uses the rust-protobuf stack only as a pure-Rust `.proto` parser whose output is serialized and re-decoded into `prost_types` — a needless byte-bridge between two protobuf runtimes that also keeps a third dependency family alive in the lock.

## What Changes

- Remove `protobuf`, `protobuf-parse` (and transitively `protobuf-support` 3.7.x) from the workspace; `Cargo.lock` drops the rust-protobuf 3.7.2 stack (the `protobuf` 2.28.0 copy pulled by `prometheus 0.13` stays — transitive, out of scope).
- Add `protox = 0.9.1` (pure-Rust protobuf compiler, same author as `prost-reflect`) whose dependency set is the exact prost stack already in the lock (`prost 0.14` / `prost-reflect 0.16` / `prost-types 0.14`) — no new duplicate crates.
- Rewrite `parse_proto_file` and `parse_proto_source` in `crates/arkflow-plugin/src/component/protobuf.rs` to call `protox::compile(inputs, includes)`, which returns a `prost_types::FileDescriptorSet` directly; delete the rust-protobuf→prost encode/decode conversion loop and the `use protobuf::Message` import.
- Operator-facing behavior is preserved: same error wrapper texts, same include-path fallback (a direct file input contributes its parent directory), same empty-descriptor guard, still pure-Rust (no `protoc` on PATH needed).

No config surface changes: `proto_inputs` / `proto_includes` / `message_type` / `on_error` are untouched, so no docs, README, or schema-regeneration impact.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `protobuf-codec`: adds one requirement — the parser replacement preserves operator-facing behavior (schemas accepted today still compile, invalid schemas still fail on the existing config-error path, the pure-Rust no-protoc property is kept).

## Impact

- **Code**: `crates/arkflow-plugin/src/component/protobuf.rs` only (`codec/protobuf.rs` and `processor/protobuf.rs` use `prost-reflect` directly and are unaffected).
- **Dependencies**: workspace `Cargo.toml` (`protobuf`, `protobuf-parse` out; `protox` in), `Cargo.lock` (rust-protobuf stack removed; `protox` + `protox-parse` added).
- **Runtime**: none beyond the dependency swap — decode/encode data paths (`DynamicMessage`) are unchanged.

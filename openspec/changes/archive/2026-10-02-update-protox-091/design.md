## Context

ArkFlow's protobuf support has two halves: the data path (`prost-reflect` `DynamicMessage` ↔ Arrow, in `component/protobuf.rs`'s conversion functions) and the schema path (compiling `.proto` sources into a `FileDescriptorSet` for `prost_reflect::DescriptorPool`). The schema path currently uses the rust-protobuf crates (`protobuf` 3.7.2 + `protobuf-parse` 3.7.2) purely as a parser: `protobuf_parse::Parser::pure().parse_and_typecheck()` produces rust-protobuf `FileDescriptorProto`s that are then `write_to_bytes()`-serialized and re-decoded as `prost_types::FileDescriptorProto` — a byte-bridge between two independent protobuf runtimes.

The dependency-refresh track originally planned `protobuf` 3.7.2 → 4.x. Spike findings (rsproxy sparse-index, verified against dependency metadata):

- `protobuf-parse` has no 4.x line at all; its latest 3.7.2 depends on `protobuf =3.7.2` (exact pin), so a 4.x `protobuf` cannot coexist without a dual stack.
- Every `protobuf` 4.x version is prerelease-suffixed (`4.36.2-release`, `4.36.0-rc.2`, …). In semver these are prereleases, so a `protobuf = "4"` requirement matches none of them; selecting one requires `=4.36.2-release` exact pins.
- From 4.36.1 the line swaps its dependency base to a brand-new `google_protobuf` crate — an experimental lineage, not a safe migration target.

## Goals / Non-Goals

**Goals:**

- Modernize the schema path while keeping it pure-Rust (no `protoc` on PATH).
- Remove the rust-protobuf stack (`protobuf`, `protobuf-parse`, `protobuf-support`) from the workspace and the lock.
- Introduce zero new duplicate protobuf/prost stacks in `Cargo.lock`.
- Preserve operator-facing behavior exactly: error texts, include-path fallback, accepted schema shapes, decode/encode data mapping.

**Non-Goals:**

- Bumping `prometheus` (which transitively keeps `protobuf` 2.28.0 in the lock) — separate track.
- Any change to the Arrow↔protobuf data mapping or codec/processor config surface.
- protobuf `editions` support: neither the old nor the new parser claims it; no behavior change.
- Collapsing the `vrl 0.36`-pulled `prost 0.13`/`prost-reflect 0.14` duplicate (independent of this swap; owned by the vrl dependency).

## Decisions

1. **Replace rust-protobuf with `protox 0.9.1` (not a 4.x bump).** The 4.x line is unreachable for us (no parser crate, prerelease-only versions). `protox` is a pure-Rust protobuf compiler by the `prost-reflect` author; `protox::compile(files, includes)` returns a `prost_types::FileDescriptorSet` directly, which deletes the encode/decode conversion bridge entirely. Version 0.9.1's requirements (`prost ^0.14`, `prost-reflect ^0.16`, `prost-types ^0.14`) match the exact stack already in our lock — resolution adds only `protox` + `protox-parse`.
   - *Alternative considered*: `prost-build` (needs `protoc` on PATH at runtime for user-supplied schemas — breaks the pure-Rust property and our tests/CI); staying on rust-protobuf 3.7.2 (leaves the dual-runtime bridge and a dead-end dependency).

2. **Keep our include-path fallback logic unchanged.** `parse_proto_file` already computes includes (explicit list, else per-input: the directory itself, or the file's parent). `protox` has the same protoc-style requirement that inputs resolve inside an include dir, so the existing fallback transfers as-is.

3. **Keep error wrapper texts as the contract.** Existing tests assert `Failed to parse the proto file`, `Failed to parse proto source`, and `No proto files found`. We keep wrapping `protox::Error` in those strings; only the underlying diagnostic detail changes. Same for the empty-input/empty-descriptor guards, which run before/around the compile call exactly as today.

4. **No lock-hygiene guard extension.** The dependency-lock-hygiene guard pins single stacks for `arrow*`, `datafusion*`, `zstd`. This swap reduces crate copies rather than adding any; the prometheus-owned `protobuf 2.28.0` remains and is tolerated (it was already present before this change).

## Risks / Trade-offs

- [protox's parser front-end differs from protobuf_parse's, so diagnostic details and edge-case syntax acceptance may differ slightly] → Both fail loudly on invalid input; our scenario pins the wrapper text and the fail-loud behavior, not the diagnostic detail. Accepted schemas in our test corpus (proto2/proto3 with scalar/enum/bytes fields, imports, packages) are pinned by existing round-trip tests.
- [protox is a smaller-maintenance project than rust-protobuf] → It tracks the prost ecosystem (its reason to exist is prost integration), and our exposure is one function call whose output type is `prost_types::FileDescriptorSet` — swappable again cheaply.
- [A future prost 0.15+/prost-reflect 0.17 bump moves protox too] → protox releases in lockstep with prost; the swap actually reduces future coupling because the parser and the runtime now share one dependency family.

## Migration Plan

Single PR on branch `deps/stage5-protox-091`: manifest edit + lock regen + the two-function rewrite + tests run green. Rollback is the revert of that PR; no persisted state, config, or wire format is touched.

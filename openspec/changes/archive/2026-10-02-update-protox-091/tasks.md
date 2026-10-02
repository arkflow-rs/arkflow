## 1. Manifest and lock

- [x] 1.1 On branch `deps/stage5-protox-091`: remove `protobuf` and `protobuf-parse` from workspace `Cargo.toml` and `crates/arkflow-plugin/Cargo.toml`; add `protox = "0.9.1"` to workspace deps and reference it from the plugin crate
- [x] 1.2 Regenerate `Cargo.lock` and verify: rust-protobuf 3.7.2 stack (`protobuf`, `protobuf-parse`, `protobuf-support`) gone; `protox` + `protox-parse` added against the existing prost 0.14 / prost-reflect 0.16 stack; no new duplicate stacks beyond the pre-existing vrl-owned prost 0.13 copy and prometheus-owned `protobuf` 2.28.0

## 2. Parser rewrite

- [x] 2.1 Rewrite `parse_proto_file` in `crates/arkflow-plugin/src/component/protobuf.rs` to use `protox::compile(inputs, includes)`, keeping the include-fallback computation, the empty-input guard, and the `Failed to parse the proto file` / `No proto files found` wrapper texts
- [x] 2.2 Rewrite `parse_proto_source` the same way (temp dir write stays; `Failed to parse proto source` wrapper text stays); delete the rust-protobuf→prost encode/decode conversion loops and the `use protobuf::Message as ProtobufMessage` import

## 3. Verification against the spec contract

- [x] 3.1 Run `cargo test -p arkflow-plugin` — every existing protobuf test (codec round-trip, skip/fail policies, descriptor tests, schema-registry) passes unmodified
- [x] 3.2 Run the dependency-lock-hygiene guard and workspace integration tests (`examples_validate`, `docs_snippets_validate`); confirm the delta-spec scenarios: 3.x-era schema still compiles and round-trips; invalid schema still errors with the pinned wrapper text
- [x] 3.3 `cargo clippy --workspace --all-targets` clean

## 4. Ship

- [x] 4.1 Commit, open PR, land on CI green
- [x] 4.2 Archive this change (`openspec archive`)

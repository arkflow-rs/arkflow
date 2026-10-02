## ADDED Requirements

### Requirement: Parser replacement preserves operator-facing behavior

The `.proto` → `FileDescriptorSet` parsing step SHALL remain pure-Rust (no `protoc` on PATH required) and SHALL preserve the behavior operators observed under the previous rust-protobuf-based parser: schemas that compiled before still compile with identical accepted field types, and failures surface on the existing `Error::Config` paths with the same wrapper texts.

#### Scenario: A 3.x-era schema still compiles and round-trips

- **WHEN** a proto3 schema accepted under `protobuf-parse` 3.7.2 (scalar fields, enums, bytes) is parsed via `parse_proto_file` or `parse_proto_source`
- **THEN** the resulting descriptor drives encode/decode exactly as before (values, nullability, and type mapping unchanged)

#### Scenario: An invalid schema fails on the existing error path

- **WHEN** a syntactically invalid proto source is parsed
- **THEN** the build fails with `Error::Config` whose message still contains `Failed to parse the proto file` / `Failed to parse proto source` (the wrapper text asserted by existing tests)

#### Scenario: The dependency swap adds no duplicate protobuf runtime

- **WHEN** `Cargo.lock` resolves after replacing `protobuf`/`protobuf-parse` with `protox`
- **THEN** the rust-protobuf 3.7.2 stack disappears from the lock, and `protox` reuses the existing `prost 0.14` / `prost-reflect 0.16` / `prost-types 0.14` stack without introducing another copy

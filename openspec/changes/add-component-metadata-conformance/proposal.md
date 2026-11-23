## Why

Component configuration schemas are hand-written `json!` literals maintained separately from the serde config structs they describe. The `component-config-honesty` requirement that a schema "SHALL 声明且仅声明该组件 serde 反序列化真实接受的字段" has no mechanical gate — only `config_example` is exercised (`crates/arkflow-plugin/tests/metadata_examples_validate.rs`) — so the two lists drift silently, and because the root schemas are `additionalProperties: false` the IDE rejects legal configurations and accepts illegal ones:

- **Legal fields rejected by the IDE** (struct has, schema lacks): `KafkaInputConfig.transactional_offsets` (`crates/arkflow-plugin/src/input/kafka.rs:76`) is absent from the schema properties (`kafka.rs:1583-1594`); `PulsarInputConfig.tls` (`input/pulsar.rs:83`) is absent (`input/pulsar.rs:516-521`); `PulsarOutputConfig.tls` (`output/pulsar.rs:61`) is absent; `MilvusSearchProcessorConfig.concurrency` (`processor/milvus_search.rs:93`) is absent (`milvus_search.rs:50-61`).
- **Ghost fields advertised**: the `http` input schema documents `method` and `interval` as "client mode" options (`input/http.rs:318-319`) and the description advertises polling a remote endpoint (`http.rs:311`), but `HttpInputConfig` (`http.rs:51-60`) only has `address`/`path`/`cors_enabled`/`auth` and the implementation is server-only.
- **Advertised defaults that do not exist**: the `kafka` input schema declares `start_from_latest` with `"default": false` (`kafka.rs:1587`) while the struct field has no `#[serde(default)]` (`kafka.rs:56`), so omitting it fails with `missing field`; same for the `http` output `method` (`output/http.rs:332` vs `output/http.rs:55`).
- **Requiredness drift**: `http` output `timeout_ms` is required by serde (`output/http.rs:57`) but not by the schema (`output/http.rs:339`); the nested `join` block of the window buffers and the `schema_registry` codec `auth` block are required by serde but carry no `required` list.
- **Description contradicts the implementation**: the `redis` input description claims "stream consumer groups" (`input/redis.rs:697`) while `Type` only has `Subscribe` and `List` (`redis.rs:64-67`).

## What Changes

- Add a structural conformance gate (`crates/arkflow-plugin/tests/metadata_conformance.rs`) that reads each component's config struct (and its flattened / nested types) from source and checks the registered schema against what serde actually accepts: field coverage in both directions, `required`, and `default`. Every registered component must be covered; deliberate exceptions go through two allowlists that are themselves checked for staleness.
- Fix every drift the gate finds: add the missing fields (`kafka` input `transactional_offsets`, `pulsar` input/output `tls`, `milvus_search` `concurrency`), remove the `http` input ghost fields and correct its description, correct the `redis` input description, mark `http` output `timeout_ms` required, and add the missing nested `required` lists (window buffers `join`, `schema_registry` `auth`).
- Honor the defaults the schema already promises — **behavior change**: omitting `start_from_latest` on the `kafka` input now means `false`, and omitting `method` on the `http` output now means `POST`; both previously failed with `missing field`.
- Remove the `http` input client-mode keys from the configuration surface and reject them explicitly at build time (the input is server-only; previously such keys were accepted by the schema and silently dropped by serde).
- Regenerate the committed inventory and config schema, update the affected component pages (EN and zh-Hans), the CHANGELOG, and strike the PA-P1-03 row in `PRODUCT-ITERATION-2026-11-22.md`.

## Capabilities

### New Capabilities

<!-- none — all work lands under existing capabilities -->

### Modified Capabilities

- `component-config-honesty`: add a requirement that the registry is guarded by a serde-field conformance gate (coverage, requiredness, defaults, allowlist hygiene), and a requirement that defaults advertised by a schema take effect when the field is omitted and that keys removed from a configuration surface are rejected explicitly.

## Impact

- **Files**: new test `crates/arkflow-plugin/tests/metadata_conformance.rs`; metadata in `input/{kafka,pulsar,http,redis}.rs`, `output/{pulsar,http}.rs`, `processor/milvus_search.rs`, `buffer/{session_window,tumbling_window}.rs`, `codec/schema_registry.rs`; struct defaults in `input/kafka.rs` and `output/http.rs`; generated `docs/reference/component-inventory.json` and `docs/static/config-schema.json`; component docs pages (EN + zh-Hans); `CHANGELOG.md`; `PRODUCT-ITERATION-2026-11-22.md`.
- **Behavior**: two omitted-optional-field configs become valid (documented defaults now apply); an `http` input config carrying `method`/`interval` fails at build with an explanatory error instead of being silently ignored.
- **Dependencies**: none — the gate parses source text with the standard library only, so `Cargo.lock` is untouched.
- **CI**: the gate joins `cargo test --workspace --all-targets`; the docs snapshot test fails until the inventory is regenerated.

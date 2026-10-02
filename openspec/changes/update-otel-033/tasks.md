## 1. Manifest and lock

- [x] 1.1 On branch `deps/otel-033`: bump the four workspace entries (`opentelemetry`/`opentelemetry_sdk`/`opentelemetry-otlp` 0.28→0.33, `tracing-opentelemetry` 0.29→0.34), resolve features (`http-json`, `reqwest-client`, `testing` 沿用), regenerate the lock
- [x] 1.2 Inspect the lock diff: family moved in lockstep; check whether a second `reqwest` copy appears (record, do not block); confirm no new duplicate stacks the hygiene guard covers

## 2. Compile-driven migration

- [x] 2.1 Rewrite the exporter config in `crates/arkflow-core/src/cli/mod.rs` `build_otel_layer`: replace `.with_export_config(ExportConfig {...})` with chained `WithExportConfig` trait methods (`.with_endpoint(...)` + explicit `Protocol::HttpJson`), until `cargo check --workspace --all-targets` is clean
- [x] 2.2 Fix remaining compile drift (e.g. `provider.tracer(...)` signature, test exporters in `executor/tests.rs`, `Value` construction) — keep edits mechanical and inside telemetry call sites only

## 3. Verification against the spec contract

- [x] 3.1 Run the tracing-related test suites unmodified (OTLP 导出与既有行为保持、跨节点 barrier trace 传播、关停冲刷场景对应的测试) plus full `cargo test -p arkflow-core` — every existing data-plane-tracing scenario passes
- [x] 3.2 Run workspace gates: dependency-lock-hygiene, `examples_validate`, `docs_snippets_validate`, `two_node_job_smoke`; `cargo clippy --workspace --all-targets` clean
- [x] 3.3 Confirm the delta-spec scenarios: OTLP config semantics (endpoint/service_name), W3C propagation round-trip, shutdown flush; assemble the semconv rename table for the PR description

## 4. Ship

- [ ] 4.1 Commit, open PR (body carries the semconv rename table as an operator-visible change), land on CI green
- [ ] 4.2 Archive this change (`openspec archive`)

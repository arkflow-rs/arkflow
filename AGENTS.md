# AGENTS.md

Guidance for AI agents working in the ArkFlow repository.

## What this is

ArkFlow is a high-performance Rust stream processing engine (Tokio + Apache Arrow/DataFusion): a single binary driven by hierarchical YAML config, with a plugin-based component system and a unified execution kernel.

## Workspace layout

- `crates/arkflow-core/` — engine abstractions and the unified execution kernel (`src/executor/`: envelope, graph, task, barrier, window, stream_compiler). Traits `Input`/`Output`/`Processor`/`Buffer`/`Codec`, `MessageBatch` (Arrow `RecordBatch` wrapper).
- `crates/arkflow-plugin/` — all plugin implementations (`input/`, `output/`, `processor/`, `buffer/`, `codec/`, `wal/`).
- `crates/arkflow/` — the main binary; component `init()` order lives in `main.rs`.
- `crates/arkflow-server/` — control plane (Hub–Agent distributed job runtime: `hub.rs`, `agent.rs`).
- `console/` — Vite web console for the control plane.
- `docs/` — Docusaurus docs site (pnpm).
- `openspec/` — spec-driven change workflow; `specs/` holds authoritative behavior specs, `PLANNING.md` the strategy context.

## Commands

```bash
cargo build --release                       # build
cargo test --workspace --all-targets        # what CI runs
cargo test -p arkflow-plugin <test_name>    # focused test
cargo clippy --workspace --all-targets      # lint before finishing
./target/release/arkflow --config <file> --validate   # validate a YAML config (deep-validates streams and jobs)
./target/release/arkflow components list    # discover registered components
./target/release/arkflow schema             # emit JSON Schema of the config
```

CI requires the protobuf compiler (`protoc` on PATH). Rust 1.97+ (`rust-version`). Note the README still advertises 1.88 in places — `Cargo.toml` is authoritative.

## Architecture rules

- **Single execution kernel**: all streams and jobs run through `crates/arkflow-core/src/executor/`. Legacy executors (`SingleComputeJobRunner`, linear Stream executor) are deleted — do not reintroduce per-stream ad-hoc runtimes. Streams compile via `stream_compiler.rs` → `JobSpec` + `StreamJobAdapter`; join buffers compile to an error.
- **Data model is columnar**: `MessageBatch` wraps Arrow `RecordBatch`. Source metadata columns are prefixed `__meta_` (`__meta_source`, `__meta_partition`, `__meta_offset`, ...).
- **Backpressure**: inter-chain edges are bounded flume channels (capacity 1024); producers must propagate backpressure, never buffer unboundedly.
- **Durability invariants**: WAL/Kafka acks advance only through the highest contiguous acked sequence/offset; stateful mutations stage until the processing ack commits; a fired window commits state before acking input. Read `openspec/specs/` (e.g. `input-durability`, `checkpoint-recovery`, `distributed-job-runtime`) before touching ack/state/recovery code.

## Adding a plugin (same pattern for all component kinds)

1. Implement the trait + builder trait (`InputBuilder`, `ProcessorBuilder`, ...) in `arkflow-plugin`.
2. Register via `register_*_builder()` in the plugin's `init()`.
3. Call that `init()` from the kind's `init()` in the plugin's `mod.rs` (wired from `crates/arkflow/src/main.rs`).
4. Add an example YAML under `examples/` and update the component docs; docs changes are CI-checked.

## Conventions

- Centralized dependency management: add versions only to `[workspace.dependencies]` / `[workspace.package]` in the root `Cargo.toml`; crates reference them with `workspace = true`.
- Errors: `thiserror` for structured types, `anyhow` for context, `Result<T, Error>` propagation; components are `async-trait`.
- Logging: `tracing` only.
- `flume` is pinned to `=0.11`.
- Prefer surgical changes over rewrites (openspec design rule).
- Large behavioral changes go through the openspec workflow (`openspec/config.yaml`: proposals need a Non-goals section; specs need WHEN/THEN scenarios).

## Gotchas

- Release profile is `lto = true, codegen-units = 1` — release builds are slow; use debug builds for iteration.
- Python UDF (PyO3) and VRL processors exist; don't break the `arrow-pyarrow` bridge when bumping Arrow versions.
- Both `README.md` and `README_zh.md` are CI-checked for doc accuracy — keep component lists in sync with actual plugins.
- The control-plane console (`console/`) is a separate Node/Vite app with its own package.json; run its checks from that directory.

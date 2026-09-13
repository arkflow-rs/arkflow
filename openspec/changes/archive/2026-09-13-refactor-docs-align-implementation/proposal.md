# Proposal: refactor-docs-align-implementation

## Why

The documentation's component reference is a hand-maintained approximation of a
machine-readable truth, and it has already drifted in ways CI cannot see:

- `crates/arkflow-plugin/src/processor/json.rs:156` registers `arrow_to_json`,
  but `docs/reference/component-inventory.json` has no entry for it — the
  component is invisible to docs readers.
- `docs/reference/component-inventory.json` lists processor `protobuf`, a name
  that exists nowhere in the code; the registered processors are
  `arrow_to_protobuf` and `protobuf_to_arrow`
  (`crates/arkflow-plugin/src/processor/protobuf.rs:239-247`). A user following
  the docs writes a config that fails to parse.
- `temporary` is documented as a component kind, but
  `crates/arkflow-core/src/component/mod.rs:38-42` defines only five kinds and
  `crates/arkflow-plugin/src/temporary/redis.rs:177` registers through a
  parallel plugin-local registry — so `arkflow components list`, the config
  JSON Schema, and the inventory generator all disagree about it.
- `docs/scripts/docs-check.mjs:42-65` only validates inventory-to-page
  consistency inside the docs tree; nothing links the inventory to the Rust
  registry, so all of the above passes CI today.

Beyond alignment, the manual lacks user-journey content: tutorials, task
how-to guides, and end-to-end cases with validated examples are thin, so new
users must reverse-engineer behavior from component reference pages.

## What Changes

- **Registry export becomes the reference source of truth**: a machine-readable
  export of the component registry (all `ComponentMetadata`: name, kind,
  description, config schema, example) replaces the hand-maintained
  `docs/reference/component-inventory.json`. The committed file becomes a
  generated artifact; a Rust snapshot test fails when it is stale and prints
  the regeneration command.
- **`temporary` is folded into the core component system** as the sixth
  `ComponentKind`, so it appears in `arkflow components list`, the config JSON
  Schema, and the export, and the plugin-local registry is retired.
  **BREAKING** (code-level only): `arkflow_plugin::temporary::register_temporary_builder`
  is removed in favor of the core registration path.
- **`arkflow components list --format json`** (and `components show` parity)
  exposes the same export to the console and external tools.
- **Docs-side validation switches to page-declared ownership**: every component
  page declares `components: [names]` in front matter; `docs-check` validates
  pages against the generated inventory bidirectionally (unknown name on a
  page → error; inventory name with no page → error).
- **Existing drift is fixed**: `arrow_to_json` documented, the ghost
  `protobuf` processor name removed, `temporary` pages aligned to the real
  registry.
- **The engine config JSON Schema (`arkflow schema`) is published as a site
  asset** with an IDE-completion how-to page, and its freshness is covered by
  the same snapshot mechanism.
- **User manual and cases**: tutorial path (install → first pipeline →
  durability), task-oriented how-to guides (Kafka→SQL, CDC with Debezium,
  windowed aggregation, HTTP ingestion, control-plane job rollout), and
  end-to-end cases backed by validated YAML files under `examples/` registered
  in `reference/example-manifest.json`.
- **Docs CI stays Rust-free**: the committed JSON is the seam — rust CI checks
  code↔JSON, docs CI checks JSON↔pages.

## Capabilities

### New Capabilities

- `component-registry-export`: machine-readable export of the component
  registry (kinds, names, metadata, config schemas) as both CLI output and a
  committed generated inventory; includes `temporary` as a first-class kind,
  the snapshot-enforcement contract, and publication of the engine config
  JSON Schema as a docs asset.
- `user-manual-coverage`: the user-journey content contract — a tutorial path
  from install to a running durable pipeline, task how-to guides for common
  scenarios, and end-to-end cases whose YAML examples are validated by CI.

### Modified Capabilities

- `documentation-reference-generation`: the coverage contract is now anchored
  to the registry export; page ownership is declared in page front matter and
  validated bidirectionally against the generated inventory.
- `documentation-quality-gates`: `docs:check` validates against the generated
  inventory and example manifests; a Rust snapshot test in the existing
  workspace test run enforces code↔inventory freshness; the docs CI job
  continues to run without a Rust toolchain.
- `documentation-accuracy`: reference and landing pages must use registry
  component type-names (no ghost names); landing parity requirement extended
  to codecs and temporary storage pages.

## Non-goals

- No changes to `versioned_docs/` (0.2.x / 0.3.x / 0.5.x stay frozen) and no
  new version snapshot in this change.
- No internationalization work (site remains en-only; README_zh stays as-is
  except where component names must stop contradicting the registry).
- No full generation of component reference pages (narrative, YAML samples,
  and behavior notes stay hand-written; only the inventory and designated
  blocks are generated).
- No console (`console/`) or control-plane API behavior changes; the console
  MAY later consume the new JSON export but that is not part of this change.
- No docs-CI Rust toolchain: `docs-check.yml` keeps running as a fast,
  Node-only job.

## Impact

- **Code**: `crates/arkflow-core/src/component/mod.rs` (sixth kind, export
  helpers), `crates/arkflow-core/src/cli/mod.rs` (JSON output, version string
  sourced from workspace metadata), `crates/arkflow-plugin/src/temporary/`
  (registration path), new snapshot test in `arkflow-plugin`.
- **Docs pipeline**: `docs/scripts/docs-check.mjs`,
  `docs/scripts/generate-component-inventory.mjs`,
  `docs/reference/component-inventory.json` (becomes generated),
  component pages gain `components:` front matter,
  `docs/package.json` scripts.
- **Docs content**: `docs/docs/` getting-started, new how-to guides and cases
  sections, `examples/` new case YAMLs, `reference/example-manifest.json`,
  landing pages where ghost names appear.
- **CI**: `.github/workflows/rust.yml` unchanged (snapshot test runs in the
  standard `cargo test --workspace --all-targets`);
  `.github/workflows/docs-check.yml` unchanged in shape (same two commands).
- **Compatibility**: generated inventory format gains fields
  (`description`, `config_schema`, …); consumers are in-repo only today.

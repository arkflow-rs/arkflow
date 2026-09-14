# Design: refactor-docs-align-implementation

## Context

The component reference in `docs/reference/component-inventory.json` is
hand-maintained while the authoritative data lives in the Rust registry:
`ComponentMetadata` (name, description, `config_optional`, `config_schema`,
`config_example`) is `Serialize`-able and populated for every registered
component via `register_*_metadata` calls. `docs-check.mjs` validates only
inside the docs tree, so registry↔docs drift (missing `arrow_to_json`, ghost
`protobuf` processor name, plugin-local `temporary` registry) passes CI.
`arkflow schema` already emits the full engine config JSON Schema but it is not
published anywhere. The manual lacks tutorial/how-to/case content backed by
validated examples.

Constraints: docs CI (`docs-check.yml`) must stay Node-only and fast; the
versioned trees (`versioned_docs/`) stay frozen; prefer surgical changes.

## Goals / Non-Goals

**Goals:**

- One machine-readable source of truth for the component reference, generated
  from the registry and enforced by tests in both CI halves.
- `temporary` becomes a first-class component kind in core.
- Page→component ownership declared on pages, validated bidirectionally.
- Fix all current name-level drift between docs and registry.
- Publish the engine config JSON Schema as a site asset with docs.
- User-journey content: tutorial path, task how-to guides, end-to-end cases
  with CI-validated YAML examples.

**Non-Goals:**

- No changes to `versioned_docs/`; no new version snapshot.
- No i18n; no full-page generation of component reference narratives; no
  console changes; no Rust toolchain in docs CI.

## Decisions

### D1: The committed JSON is the CI seam (no Rust in docs CI)

`docs/reference/component-inventory.json` becomes a generated artifact checked
into the repo. A Rust snapshot test asserts it equals the live registry dump
(fails with the regeneration command when stale) — this closes code→JSON in
rust CI. `docs-check` validates JSON↔pages — this closes JSON→docs in the
Node-only docs CI.

- *Alternative*: docs CI builds the binary and diffs against the registry —
  rejected: makes the docs job slow and duplicates the toolchain setup; the
  committed artifact is also reviewable in PRs.
- *Alternative*: no committed file, registry read at docs build time —
  rejected: same toolchain problem, plus unreviewable docs diffs.

### D2: Export shape and serializer

One library function in `arkflow-core` (e.g.
`component::export_registry() -> serde_json::Value`) serializes all five+
kinds as a sorted array of `{kind, name, description, config_optional,
config_schema, config_example?}`. Determinism comes from `BTreeMap` registries
(already sorted) and stable struct field order; pretty-print with
`serde_json::to_string_pretty` + trailing newline. The CLI
(`components list --format json`) and the snapshot test both call it, so the
CLI payload and the committed file cannot diverge.

The existing filename is kept to limit churn. The `doc` field is **removed**
from the JSON: page mapping moves to docs (see D4). The generated markdown
table (`component-inventory.md`, marker-based) gains a description column and
resolves links by joining the inventory with page front matter.

### D3: `temporary` joins core `ComponentKind`

Add `ComponentKind::Temporary` (as_str `"temporary"`), a `TEMPORARY_METADATA`
registry via the existing macro, and `register_temporary_metadata` /
`list_components_by_kind` support in core. `arkflow-plugin/src/temporary/`
switches from its private builder registry to `register_temporary_builder` +
`register_temporary_metadata`, and the private registry is deleted. The
stream config's `temporary` field keeps its semantics; only registration and
discovery change.

- *Alternative*: keep the parallel registry and special-case it in the
  exporter — rejected: leaves the component invisible to `components list`,
  the config schema, and IDE completion, and permanently special-cases every
  future consumer.

### D4: Page ownership lives in page front matter

Each component page declares what it documents:

```yaml
---
components: [arrow_to_json, json_to_arrow]
---
```

`docs-check` builds name→route from all component pages, then checks: every
inventory name has ≥1 page (else error), every declared name exists in the
inventory with matching kind (else error), and every component page declares
at least one name (else error). No mapping file, no naming-convention
exceptions list; paired processors (`arrow_to_json`/`json_to_arrow`) naturally
share `json.md`.

Front matter parsing stays dependency-free: a strict mini-parser for the two
supported forms (`components: [a, b]` and YAML list items) that errors loudly
on anything else — adding a YAML dependency to the docs pipeline for one key
is not warranted.

- *Alternative*: keep `doc` in the JSON (hand-editable section inside a
  generated file) — rejected: mixing generated and hand-edited content in one
  file recreates the drift this change removes.
- *Alternative*: convention `kind/name.md` + exception list — rejected:
  exceptions encode in the script what front matter expresses at the page.

### D5: Config schema published as a static asset

The snapshot test also dumps `build_config_schema()` to
`docs/static/config-schema.json`; a reference page ("IDE auto-completion")
links to it and shows editor setup. Freshness is enforced by the same
snapshot mechanism as D1 — one test, two artifacts.

### D6: Example YAMLs are validated in rust CI

A test in the binary crate walks `docs/reference/example-manifest.json` and
deep-validates each YAML offline (the same checks as `--validate`: parse,
stream ids, job specs, semantic validation). This makes the case-study
examples (and existing examples) release-blocking without touching docs CI.
Examples that cannot validate offline get an explicit, documented exclusion in
the manifest (`"validate": false` with a reason) rather than silent skipping.

### D7: User-journey content structure

- `docs/docs/tutorials/` — install, first pipeline (quickstart), and a
  durable-pipeline walkthrough; task-oriented, each ends with a running state
  the reader can verify.
- `docs/docs/how-to/` — task guides: Kafka→SQL sink, CDC with Debezium +
  Schema Registry, windowed aggregation, HTTP ingestion, control-plane job
  rollout; each with prerequisites, complete config, expected outcome,
  troubleshooting links (per `documentation-content-contract`).
- `docs/docs/cases/` — end-to-end cases, each backed by a new YAML under
  `examples/` registered in `example-manifest.json` (D6 validates them).
- `sidebars.ts` gains the three groups; the existing
  getting-started/components pages stay where they are (surgical; no mass
  route moves, so no redirect stubs needed).

### D8: CLI version string from workspace metadata

`cli/mod.rs` hardcodes `.version("0.4.0-rc1")` while the workspace is 0.5.0.
Switch to `env!("CARGO_PKG_VERSION")` so `arkflow --version` stops lying.
Surgical, in-scope because the same file gains the JSON output flag.

## Risks / Trade-offs

- [Thin metadata surfaces sparseness] → generated inventory exposes components
  whose descriptions/schemas are weak (`unit()` entries). Mitigation: drift-fix
  tasks include enriching the worst metadata; sparseness is visible, which is
  the point.
- [Example YAMLs fail offline validation] → some existing examples may not
  parse against the current config schema. Mitigation: fix them, or use the
  explicit `validate: false` exclusion with reason (D6); no silent skips.
- [Front matter mini-parser rejects valid-but-unusual YAML] → strict formats
  only, documented in `DOCUMENTATION.md`; the parser fails loudly naming the
  file, so authoring mistakes surface immediately.
- [Snapshot test writes files during `cargo test`] → the test never writes in
  check mode; regeneration only via an explicit env flag
  (`ARKFLOW_REGENERATE_DOCS=1`) documented in the failure message, keeping CI
  hermetic.
- [Breaking: plugin-local temporary registry removed] → code-level BREAKING
  for out-of-tree registrants; in-repo migration is part of this change, and
  the docs note the core path as the only supported one.
- [Inventory JSON gains fields] → consumers are in-repo scripts only today;
  format bump recorded in the JSON (`"version": 2`).

## Migration Plan

1. Core: sixth kind + registry export + CLI `--format json` + version string.
2. Plugin: temporary migration to core registration; snapshot test added;
   regenerate `component-inventory.json` and `static/config-schema.json`.
3. Docs scripts: docs-check against generated JSON + front matter ownership;
   add `components:` front matter to all component pages; regenerate the
   markdown table.
4. Drift fixes: `arrow_to_json` page content, ghost `protobuf` name removed
   everywhere (including landing docs), temporary pages aligned.
5. Manual: tutorials, how-to guides, cases + example YAMLs + manifest.
6. Full gates: `cargo test --workspace --all-targets`, `pnpm docs:check`,
   `pnpm build`, `--validate` on all examples.

Rollback: each stage is independently revertible; the committed JSON revert
restores the old hand-maintained contract (field additions are additive for
the old checker? — no: docs-check changes in step 3 assume the new fields, so
steps 3–5 revert together).

## Open Questions

- Which existing examples (if any) need the `validate: false` exclusion —
  resolved during implementation by running the validator.

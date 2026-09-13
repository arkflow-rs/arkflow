# Tasks: refactor-docs-align-implementation

## 1. Core: temporary as a first-class kind

- [x] 1.1 Add `ComponentKind::Temporary` (`as_str` "temporary") to `crates/arkflow-core/src/component/mod.rs`; add `TEMPORARY_METADATA` registry, `register_temporary_metadata`/builders plumbing via the existing macro, and include it in `ComponentKind::all()` and schema output
- [x] 1.2 Migrate `crates/arkflow-plugin/src/temporary/` (`redis.rs`, `mod.rs`) from the plugin-local builder registry to core `register_temporary_builder` + `register_temporary_metadata`; delete the private registry
- [x] 1.3 Update unit tests that assert five kinds / registry listings; `cargo test -p arkflow-core -p arkflow-plugin` green

## 2. Core: registry export and CLI JSON

- [x] 2.1 Add deterministic export (`component::export_registry()` → sorted array of `{kind,name,description,config_optional,config_schema,config_example?}`) with unit test for determinism and completeness
- [x] 2.2 Add `--format json` to `components list` in `crates/arkflow-core/src/cli/mod.rs` reusing the export serializer; update CLI tests
- [x] 2.3 Replace hardcoded `.version("0.4.0-rc1")` with `env!("CARGO_PKG_VERSION")`

## 3. Snapshot enforcement (rust CI half)

- [x] 3.1 Add snapshot test in `arkflow-plugin` (after full `init()`) comparing `docs/reference/component-inventory.json` (format `"version": 2`, no `doc` field) and `docs/static/config-schema.json` against live export; no writes without `ARKFLOW_REGENERATE_DOCS=1`; failure message prints the regeneration command
- [x] 3.2 Regenerate both committed artifacts and review the JSON diff
- [x] 3.3 Verify `cargo test --workspace --all-targets` passes with regenerated artifacts

## 4. Docs pipeline: generated inventory and page ownership

- [x] 4.1 Update `docs/scripts/generate-component-inventory.mjs`: read format-2 inventory, build name→route map from page front matter, render table with description column between existing markers; `--check` mode preserved
- [x] 4.2 Extend `docs/scripts/docs-check.mjs`: parse `components:` front matter (strict mini-parser, loud errors), bidirectional validation against the committed inventory (missing page / unknown name / kind mismatch / undeclared component page), drop the old `doc`-field checks
- [x] 4.3 Add `components: [...]` front matter to every page under `docs/docs/components/` (paired processors declare both names on the shared page)
- [x] 4.4 Regenerate the inventory table and confirm `pnpm docs:check` passes

## 5. Drift fixes in content

- [x] 5.1 Document `arrow_to_json` in `docs/docs/components/2-processors/json.md` with a valid configuration example
- [x] 5.2 Remove the ghost `protobuf` processor name from component pages, landing pages, `README.md`, `README_zh.md`; use `arrow_to_protobuf`/`protobuf_to_arrow` everywhere
- [x] 5.3 Align `4-temporary/redis.md` and related prose with the temporary-as-kind model; enrich thin registry descriptions/schemas found by the export (worst offenders at minimum)

## 6. Config schema asset and IDE docs

- [x] 6.1 Add `docs/static/config-schema.json` (from 3.2) reference page documenting IDE auto-completion setup; link from configuration docs
- [x] 6.2 Update `docs/DOCUMENTATION.md` and `docs/README.md` (yarn→pnpm, new generation/validation workflow, regeneration commands, front-matter contract)

## 7. Example validation gate (rust CI half)

- [x] 7.1 Add workspace test walking `docs/reference/example-manifest.json` deep-validating each YAML with `--validate` semantics; support explicit `validate: false` exclusions with required reason
- [x] 7.2 Run the validator; fix failing existing examples or record documented exclusions

## 8. User manual: tutorials, how-to guides, cases

- [x] 8.1 Create `docs/docs/tutorials/` (install → first pipeline → durable pipeline walkthrough) with verifiable end states; rework getting-started links to it
- [x] 8.2 Create how-to guides: Kafka→SQL, CDC with Debezium + Schema Registry, windowed aggregation, HTTP ingestion, control-plane job rollout
- [x] 8.3 Create `docs/docs/cases/` end-to-end case pages, each backed by a new validated YAML under `examples/` registered in `example-manifest.json`
- [x] 8.4 Add Tutorials/How-to/Cases groups to `docs/sidebars.ts` and cross-links from start-here/intro

## 9. Full verification

- [x] 9.1 `cargo test --workspace --all-targets` and `cargo clippy --workspace --all-targets` green
- [x] 9.2 `pnpm docs:check` and `pnpm build` green (no new broken-link warnings)
- [x] 9.3 Validate representative example YAMLs with `./target/release/arkflow --config <file> --validate`
- [x] 9.4 `openspec verify` passes; spec deltas match implemented behavior

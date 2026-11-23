## 1. Conformance gate

- [ ] 1.1 Add `crates/arkflow-plugin/tests/metadata_conformance.rs`: std-only source reader (comment/string/raw-string/lifetime aware) and config-struct/enum parser (`rename`, `default`, `skip`, `skip_deserializing`, `flatten`, `tag`, `rename_all`, `Option<_>`, multi-line generics), with unit tests for each tricky shape
- [ ] 1.2 Add the `(kind, name) → (file, config struct)` table with completeness (every registered non-unit component mapped, every row registered) and staleness guards (struct exists in file; struct deserialized in that file)
- [ ] 1.3 Implement rules R1–R5 per schema node (strict-node coverage, no ghosts, required both directions, default ⇒ optional), `flatten` inlining, nested recursion, union handling for enum-typed nodes
- [ ] 1.4 Add the `HIDDEN_FIELDS` and `BUILDER_REQUIRED` allowlists with the staleness pass
- [ ] 1.5 Mutation checks (record results in the PR description): temporarily drop a schema property, add a ghost property, flip a `required` entry, add a fake `default`, remove a table row, and make an allowlist entry stale — each must fail the gate with an actionable message; then revert

## 2. Metadata corrections (schema-side)

- [ ] 2.1 Add missing fields: `kafka` input `transactional_offsets`, `pulsar` input and output `tls`, `milvus_search` `concurrency`
- [ ] 2.2 `http` input: remove `method`/`interval`, rewrite the description and the `address`/`path` property descriptions for server-only operation; reject `method`/`interval` at build with an explanatory error (unit test)
- [ ] 2.3 `redis` input: correct the description (blocking list pops and pub/sub only)
- [ ] 2.4 `http` output: add `timeout_ms` to `required`
- [ ] 2.5 Nested `required`: window buffers `join` (`query`, `codec`) in `session_window` and `tumbling_window`; `schema_registry` `auth` (`type`)

## 3. Advertised defaults become real (struct-side)

- [ ] 3.1 `kafka` input: `#[serde(default)]` on `start_from_latest`; unit test that an omitted field deserializes to `false`
- [ ] 3.2 `http` output: `#[serde(default = "...")]` on `method` returning `POST`; unit test that an omitted method deserializes to `POST`

## 4. Generated artifacts, docs, registry

- [ ] 4.1 Regenerate `docs/reference/component-inventory.json` and `docs/static/config-schema.json` (`ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot`) and confirm the snapshot test passes without the flag
- [ ] 4.2 Review and update the affected component pages under `docs/docs/components/` and their zh-Hans mirrors (`i18n/zh-Hans/docusaurus-plugin-content-docs/current/components/`): `kafka` input (`transactional_offsets`, `start_from_latest` default), `pulsar` input/output (`tls`), `milvus-search` (`concurrency`), `http` input, `redis` input, `http` output (`method` default, `timeout_ms` required), `session_window`/`tumbling_window` (`join` required fields), `schema-registry` (`auth.type`)
- [ ] 4.3 Add a CHANGELOG `[Unreleased]` entry (Added: conformance gate; Changed: defaults now honored, `http` input rejects removed client keys, corrected metadata)
- [ ] 4.4 Strike the PA-P1-03 row in `PRODUCT-ITERATION-2026-11-22.md` with the change name

## 5. Gates

- [ ] 5.1 `cargo test -p arkflow-plugin --test metadata_conformance --test metadata_examples_validate --test docs_inventory_snapshot --test registry_consistency --locked` green
- [ ] 5.2 `cargo fmt --all -- --check` and `cargo clippy -p arkflow-plugin --all-targets -D warnings` green
- [ ] 5.3 `cd docs && pnpm docs:check` green (EN + zh-Hans pages, inventory up to date)

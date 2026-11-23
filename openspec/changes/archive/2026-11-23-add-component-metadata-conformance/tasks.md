## 1. Conformance gate

- [x] 1.1 Add `crates/arkflow-plugin/tests/metadata_conformance.rs`: std-only source reader (comment/string/raw-string/lifetime aware) and config-struct/enum parser (`rename`, `default`, `skip`, `skip_deserializing`, `flatten`, `tag`, `rename_all`, `Option<_>`, multi-line generics), with unit tests for each tricky shape
- [x] 1.2 Add the `(kind, name) → (file, config struct)` table with completeness (every registered non-unit component mapped, every row registered) and staleness guards (struct exists in file; struct deserialized in that file)
- [x] 1.3 Implement rules R1–R5 per schema node (strict-node coverage, no ghosts, required both directions, default ⇒ optional), `flatten` inlining, nested recursion, union handling for enum-typed nodes — rules R1–R6 per schema node; string-enum values (R6) and per-variant checks for serde's external/internal/untagged layouts were added during implementation
- [x] 1.4 Add the `HIDDEN_FIELDS` and `BUILDER_REQUIRED` allowlists with the staleness pass
- [x] 1.5 Mutation checks (record results in the PR description): temporarily drop a schema property, add a ghost property, flip a `required` entry, add a fake `default`, remove a table row, and make an allowlist entry stale — each must fail the gate with an actionable message; then revert — 13 real-file mutation cases, each failing with its named diagnostic; 10 unit tests of the gate's own

## 2. Metadata corrections (schema-side)

- [x] 2.1 Add missing fields: `kafka` input `transactional_offsets`, `pulsar` input and output `tls`, `milvus_search` `concurrency`
- [x] 2.2 `http` input: remove `method`/`interval`, rewrite the description and the `address`/`path` property descriptions for server-only operation; reject `method`/`interval` at build with an explanatory error (unit test) — plus corrected enum values serde never accepted (pulsar input `subscription_type`, kafka output `compression`) and the nats output `stream` ghost key — found by the gate after proposing
- [x] 2.3 `redis` input: correct the description (blocking list pops and pub/sub only)
- [x] 2.4 `http` output: add `timeout_ms` to `required`
- [x] 2.5 Nested `required`: window buffers `join` (`query`, `codec`) in `session_window` and `tumbling_window`; `schema_registry` `auth` (`type`)

## 3. Advertised defaults become real (struct-side)

- [x] 3.1 `kafka` input: `#[serde(default)]` on `start_from_latest`; unit test that an omitted field deserializes to `false`
- [x] 3.2 `http` output: `#[serde(default = "...")]` on `method` returning `POST`; unit test that an omitted method deserializes to `POST`

## 4. Generated artifacts, docs, registry

- [x] 4.1 Regenerate `docs/reference/component-inventory.json` and `docs/static/config-schema.json` (`ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot`) and confirm the snapshot test passes without the flag — regenerated twice; the snapshot test caught the second staleness (sql output `table_name` description)
- [x] 4.2 Review and update the affected component pages under `docs/docs/components/` and their zh-Hans mirrors (`i18n/zh-Hans/docusaurus-plugin-content-docs/current/components/`): `kafka` input (`transactional_offsets`, `start_from_latest` default), `pulsar` input/output (`tls`), `milvus-search` (`concurrency`), `http` input, `redis` input, `http` output (`method` default, `timeout_ms` required), `session_window`/`tumbling_window` (`join` required fields), `schema-registry` (`auth.type`)
- [x] 4.3 Add a CHANGELOG `[Unreleased]` entry (Added: conformance gate; Changed: defaults now honored, `http` input rejects removed client keys, corrected metadata)
- [x] 4.4 Strike the PA-P1-03 row in `PRODUCT-ITERATION-2026-11-22.md` with the change name

## 5. Gates

- [x] 5.1 `cargo test -p arkflow-plugin --test metadata_conformance --test metadata_examples_validate --test docs_inventory_snapshot --test registry_consistency --locked` green — green; `cargo test --workspace --all-targets --locked --no-fail-fast`: 2865 passed, 0 failed, 30 ignored
- [x] 5.2 `cargo fmt --all -- --check` and `cargo clippy -p arkflow-plugin --all-targets -D warnings` green — green workspace-wide
- [x] 5.3 `cd docs && pnpm docs:check` green (EN + zh-Hans pages, inventory up to date) — green: 145 pages, inventory up to date (the markdown inventory is regenerated too)

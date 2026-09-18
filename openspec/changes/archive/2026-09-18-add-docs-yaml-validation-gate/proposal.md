## Why

The documentation embeds 129 hand-written ```yaml code blocks across 58 pages
(85 of them on component pages — the pages users copy-paste from most), and
none of them are validated against the engine. `docs/scripts/docs-check.mjs:90`
scans only markdown links, and `docs/scripts/docs-check.mjs:99-100` explicitly
skips code blocks' content; `examples/` YAMLs are deep-validated through
`docs/reference/example-manifest.json` + `crates/arkflow/tests/examples_validate.rs:70-83`,
but inline doc snippets form a second, unguarded source of truth. When the
config schema evolves (e.g. #1227 added sql output upsert fields), doc snippets
can silently rot. Additionally, the 62 external links in `docs/docs` are never
checked (`docs-check.mjs` skips `http(s)` targets), so link rot is invisible.

## What Changes

- Establish a classification contract for every ```yaml block under
  `docs/docs/`: each block MUST declare one of
  - `full` — a complete ArkFlow config; validated as-is
  - `fragment` with a wrap kind — a snippet; a wrap template completes it
    into a full config before validation (wrap kinds: input, output,
    processor-list, top-level-section; stub complements use offline-free
    components such as `input/generate`, `output/drop`, `output/stdout`)
  - `foreign` with a stated reason — non-ArkFlow YAML (k8s manifests,
    prometheus scrape configs); only YAML well-formedness is checked
- Add a fast gate to `docs-check.mjs`: every yaml block must carry a
  recognized classification; unclassified blocks fail with file location
  (Node-side, no YAML parsing, no Rust toolchain — consistent with the
  existing docs-CI/Rust-CI seam)
- Add a deep gate as a workspace test in `crates/arkflow/tests/`: extract,
  wrap, and validate through the real parser
  (`arkflow_core::config::EngineConfig` + `validate_config`, mirroring the
  existing example-validation test); exclusions are explicit and reason-backed
  ("silent skipping is not permitted")
- Tag/fix the initial corpus: classify all existing blocks, repair whatever
  the first validation pass surfaces
- (Lower priority tail) Add a scheduled, report-only external-link check
  (weekly CI job, e.g. lychee) that files a report issue; external links
  SHALL NOT gate pull requests (network flakiness); current exposure is
  small — 62 links, 53 pointing at github.com/docs.rs

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `documentation-quality-gates`: add a requirement that inline YAML
  configuration blocks in maintained documentation pages SHALL be classified
  (full / fragment+wrap-kind / foreign+reason) and SHALL be validated by the
  gates (Node-side classification completeness; Rust-side deep validation
  through the real parser); add a requirement that external links SHALL be
  periodically checked report-only, not PR-gated.

## Impact

- `docs/scripts/docs-check.mjs` — new classification-completeness check
- `crates/arkflow/tests/` — new test (reuse patterns from
  `examples_validate.rs`; the deep gate runs in the existing
  `cargo test --workspace` CI, no new CI workflow for the yaml gate)
- `docs/docs/**/*.md` — one-time pass adding classification markers to ~129
  blocks and repairing failures the first pass finds
- No runtime/engine changes; no new Node dependencies required for the yaml
  gate (marker scan is text-level)
- Dependency note: legacy compatibility stubs under `docs/docs` also contain
  yaml blocks; the separate stub-retirement change should land first so this
  change does not spend markers on pages scheduled for deletion

## Context

`docs/docs` embeds 129 ```yaml blocks across 58 pages (85 on component pages).
A first-token survey classifies them into three buckets:

- ~75 component/option fragments: `input: {...}` (×30), `output: {...}` (×30),
  `- processor:` (×9), plus `buffer:`/`codec:`/`temporary:` — not complete
  configs; feeding them to the parser as-is fails.
- ~45 top-level snippets / near-complete configs: `streams:` (×12),
  `logging:` (×7), `durability:` (×8), `jobs:` — complete after merging into a
  minimal config.
- ~8 foreign YAML: k8s manifests, prometheus `scrape_configs:` — never
  ArkFlow configs.

Today no gate touches block content: `docs/scripts/docs-check.mjs:90` scans
markdown links only, and code-block content is not parsed. `examples/` YAMLs
are the covered source of truth (manifest +
`crates/arkflow/tests/examples_validate.rs:70-83`, which validates through the
real `EngineConfig::from_file` + `validate_config` path). Inline snippets are
a second, unguarded source of truth.

Constraints inherited from the `documentation-quality-gates` spec:
- The docs CI job SHALL stay Node-only; the committed artifacts are the seam
  to the Rust side. So the classification-completeness gate must live in
  `docs-check.mjs` (text-level, no YAML parser), and the deep gate in the
  workspace test suite (`cargo test --workspace` runs in CI already).
- Silent skipping is not permitted — exclusions carry explicit reasons.
- Semantic validation is offline-safe (kafka examples validate without a
  broker); only component `init()` needs external services. Wrap templates
  can therefore pass full semantic validation offline.
- `versioned_docs/` is already outside the docs-check walk; frozen snapshots
  stay out of scope.

## Goals / Non-Goals

**Goals:**
- Every ```yaml block in `docs/docs` is explicitly classified: `full`,
  `fragment` + wrap kind, or `foreign` + reason.
- Deep gate: wrapped snippets validated through the real engine parser in a
  workspace test; failures name file + block.
- Fast gate: `docs-check.mjs` fails on unclassified or unknown-vocabulary
  blocks without a Rust toolchain.
- One-time corpus pass tags all existing blocks and repairs surfaced rot.
- External links get a scheduled report-only check (no PR gate).

**Non-Goals:**
- No single-source restructuring: snippets stay inline; no MDX raw imports,
  no moving snippets into `examples/`.
- No validation of `versioned_docs/` snapshots.
- No PR gating of external links.
- No engine/runtime changes and no new Node dependencies.
- Legacy stub retirement is a separate change (should land first — stub pages
  contain yaml blocks and tagging them is wasted work).

## Decisions

### D1: Inline markers + test-time wrapping (not injection, not Node-side schema validation)
Alternatives:
- *Single-source injection* (snippets in files, MDX raw import): zero drift by
  construction, but 129-block migration churn, worse writing UX — and fragment
  files still need wrap templates to validate, so it relocates this design's
  core complexity rather than removing it. Rejected.
- *Node-side ajv against `config-schema.json`*: one toolchain, but a second
  validation truth (JSON Schema vs serde can drift) and no semantic
  validation. The Rust path already exists and CI runs it; strictly worse
  here. Rejected.

### D2: Strict explicit classification (no unmarked default)
Every block carries its classification in the fence metastring, e.g.
` ```yaml validate=full `, ` ```yaml validate=fragment wrap=input `,
` ```yaml validate=foreign reason="k8s manifest" `. Docusaurus renders the
metastring as metadata (title only comes from `title=`), so readers see no
change. Alternatives: YAML first-line comment (pollutes copy-paste content —
rejected); lenient default "unmarked = try as full config" (leaves foreign
blocks' intent implicit and weakens the fast gate — rejected). One-time cost:
tagging the existing corpus; ongoing cost: one marker word per block, with
the `docs-check` error message stating the fix.

### D3: Wrap kinds with offline-free stub complements
Vocabulary finalized during the corpus pass against the real schema
(`crates/arkflow-core/src/stream/config.rs`, `pipeline/mod.rs`): 7 wrap kinds
— `input`, `output`, `processors`, `durability`, `buffer`, `codec`, `engine`.
The originally sketched 4 kinds (`input|output|processor-list|top-level`)
were adjusted because `durability`/`buffer` live on the stream (not the
engine root), standalone `codec:` snippets merge under the stream's input,
and `buffer:` pages exist too. Wrapping is a serde_json deep-merge of the
parsed snippet into a stub config (`input/memory` — all fields optional;
`output/drop` — builder ignores config), not string concatenation.
Discovery: `EngineConfig` does not use `deny_unknown_fields`, so a merged
section could be silently dropped and still pass. The deep gate therefore
also asserts the re-serialized parsed configuration contains the fragment at
its wrap target. A second discovery: component pages use a
`- processor: {...}` nested list shape that does not match flat
`ProcessorConfig` (`processor/mod.rs:118-124`) — existing rot the corpus pass
(task 4.2) repairs. Known limitation: scalar field typos inside flattened
component configs survive when builders don't validate them; the gate
catches structural rot (wrong shape, wrong nesting, missing required
fields, unknown top-level sections).

### D4: Deep gate is a new workspace test, not an extension of examples_validate
New file (e.g. `crates/arkflow/tests/docs_snippets_validate.rs`): walks
`docs/docs`, extracts fences (fence-state scan mirroring `docs-check.mjs`'s
inFence logic), wraps per marker, calls `EngineConfig` + `validate_config`,
reports failures as `file: block-index: error`. `foreign` blocks get a YAML
well-formedness parse only. Rationale for a separate file: different concern
and traversal root than `examples_validate.rs`; shared machinery stays in
`arkflow_core`.

### D5: Marker vocabulary is mirrored, not shared
The vocabulary (3 markers, 4 wrap kinds) is defined once in
`docs/DOCUMENTATION.md` and implemented in both gates. Drift self-detects:
each gate fails on vocabulary it does not know, and both run in CI. A shared
generated artifact would add machinery disproportionate to a ~10-word
contract.

### D6: External links — scheduled report-only job, not a gate
Weekly CI job (prefer the `lychee` link checker with a checked-in config and
domain exclusions; fallback: minimal Node script with cached HEAD requests)
that reports results to a GitHub issue instead of failing PRs. Current
exposure is 62 links, 53 to github.com/docs.rs. Alternative (PR-gated
linkcheck) rejected for network flakiness in CI.

## Risks / Trade-offs

- [Marker friction for future writers] → `docs-check` error message includes
  the exact marker to add; `docs/DOCUMENTATION.md` documents the contract.
- [Wrap templates rot when top-level required fields evolve] → templates are
  localized in one test; schema evolution fails loudly there with a small fix.
- [Legitimate doc fragments that cannot pass semantic validation] → explicit
  `foreign`/reason markers provide the escape hatch without silent skips.
- [Metastring conflicts with Docusaurus parsing] → verified during
  implementation; documented fallback is comment-line markers.
- [Corpus pass touches ~58 files] → mechanical; can land in per-area commits;
  sequence after legacy-stub retirement to avoid tagging doomed pages.
- [Scheduled link job goes stale / reports ignored] → report issue
  auto-updates; exposure is small and concentrated on stable domains.

## Migration Plan

1. Land legacy-stub retirement (separate change) first.
2. Land this change as gates + marker vocabulary + initial corpus pass in one
   series so CI stays green (unclassified blocks fail `docs-check`; invalid
   blocks fail the Rust test — both fixed by the corpus pass).
3. Add the weekly external-link job last; independent of steps 1–2.
Rollback: revert the gates; no data or artifact migration is involved.

## Open Questions

- Metastring key naming (`validate=` vs `kind=`) — pick one at
  implementation; keep a single vocabulary everywhere.
- Do `input/generate` / `output/drop` accept zero required fields (D3 stub
  assumption)? Verify against the registry; substitute if not.
- How many of the ~45 top-level blocks are intentional non-runnable
  illustrations (expect a handful of reason-backed `foreign` tags)?
- lychee vs custom script for the link job — resolve when the tail task is
  picked up.

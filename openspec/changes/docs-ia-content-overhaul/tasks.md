## 1. Phase 1 — IA skeleton

- [x] 1.1 Create top-level directories under `docs/docs/` for the seven new
  sections (Processing Guides, Cookbook, Observability, Operations,
  Troubleshooting, Migration) and confirm the five existing section
  directories stay. Verify: `ls docs/docs/` lists 12 section dirs.
- [x] 1.2 Add a `_category_.json` to each of the 12 top-level sections with
  the canonical `position` (1–12) per `docs-information-architecture` spec
  and a `generated-index` landing `link` with a one-sentence description.
  Verify: each file has `position`, `label`, and `link.type=="generated-index"`.
- [x] 1.3 Re-number existing sections' `_category_.json` to canonical
  positions (Introduction=1, Getting Started=2, Concepts=3, Configuration=4,
  Components=5, SQL Reference=6). Verify: `grep -R '"position"' docs/docs`.
- [x] 1.4 Build the site green after skeleton creation. Verify:
  `cd docs && pnpm build` exits 0.
- [x] 1.5 Confirm no orphan Markdown under `docs/docs/` using a link/lint
  check (see 6.2) or manual sidebar audit. Verify: check reports zero orphans
  for pages other than the to-be-relocated performance page.

## 2. Phase 2 — Component template

- [x] 2.1 Write the canonical component template (sections per
  `docs-component-template` spec) as a reusable reference file under
  `docs/docs/components/_category_.json` description or a CONTRIBUTING-style
  note. Verify: template text exists and is linked from the Components
  landing page.
- [x] 2.2 Convert all input pages (`docs/docs/components/0-inputs/*.md`) to
  the new template (Status, When to use, Common fields, Full reference,
  multi-scenario Examples, Output schema, Error handling, Metrics, See also).
  Verify: each file has the mandatory headers.
- [x] 2.3 Convert all output pages (`docs/docs/components/3-outputs/*.md`) to
  the new template (with Input schema where applicable). Verify: headers
  present.
- [x] 2.4 Convert all processor pages (`docs/docs/components/2-processors/``)
  to the new template. Verify: headers present.
- [x] 2.5 Convert buffer, codec, and temporary-storage pages to the new
  template. Verify: headers present.
- [x] 2.6 Build green after component conversion. Verify: `cd docs && pnpm
  build` exits 0.

## 3. Phase 3 — Concept expansion & positioning

- [x] 3.1 Add a Concepts page on Time / event-time semantics in ArkFlow.
  Verify: `docs/docs/concepts/` contains the new page and it links to the
  windowing buffer docs.
- [x] 3.2 Add a Concepts page on Window semantics (tumbling/sliding/session)
  tying the buffer components to stream-processing theory. Verify: page
  exists and cross-links Buffer pages.
- [x] 3.3 Add a Concepts page on State in ArkFlow (in-memory window state,
  WAL durability as the persistence boundary, and the explicit no-cluster
  state stance per `openspec/PLANNING.md`). Verify: page exists and is
  consistent with `input-durability` / `exactly-once-output` specs.
- [x] 3.4 Add an Introduction "Why ArkFlow" comparison page against Vector,
  Benthos, Flink, RisingWave, aligned to the niche in `PLANNING.md`. Verify:
  page renders and each comparison claim is backed by a spec or capability
  reference.
- [x] 3.5 Rebuild green. Verify: `cd docs && pnpm build` exits 0.

## 4. Phase 4 — Evaluation-stage sections (first batch)

- [x] 4.1 Processing Guides: write VRL guide and Python UDF guide (two pages
  minimum). Verify: both pages have runnable example snippets.
- [x] 4.2 Processing Guides: write SQL-processing and Codecs/Joins guides.
  Verify: pages cross-link Components and SQL Reference.
- [x] 4.3 Cookbook: write 2 recipes reusing `examples/*.yaml` (e.g.
  eos-kafka pipeline, CDC via Debezium). Verify: each recipe references an
  existing example file by path.
- [x] 4.4 Cookbook: write 4 more recipes (log aggregation, IoT ingestion
  via MQTT/Modbus, stream-table enrichment, Schema Registry). Verify: 6
  cookbook pages total.
- [x] 4.5 Observability: write health-check, metrics (Prometheus), tracing,
  and logging pages, consistent with actual endpoints the engine exposes.
  Verify: any endpoint/field mentioned is verified against engine source.
- [x] 4.6 Troubleshooting: write FAQ, common errors, and debug-guide pages.
  Verify: pages render and link to relevant Concepts pages.
- [x] 4.7 Migration: write "From Benthos" and "From Vector" pages mapping
  their concepts to ArkFlow. Verify: each page has a config-conversion
  example.
- [x] 4.8 Migration: write "From Flink" and "From RisingWave" pages covering
  state/window/checkpoint differences. Verify: each page states ArkFlow's
  single-node positioning per `PLANNING.md`.
- [x] 4.9 Build green. Verify: `cd docs && pnpm build` exits 0.

## 5. Phase 5 — Absorb performance content

- [x] 5.1 Move `docs/performance/**` into `docs/docs/operations/` (e.g.
  `tuning/`), updating internal links. Verify: the moved file renders under
  Operations in the sidebar and `docs/performance/` no longer contains docs.
- [x] 5.2 Refresh the moved performance page(s) to fit the Operations
  section voice and link to relevant Concepts. Verify: page links resolve.
- [x] 5.3 Build green. Verify: `cd docs && pnpm build` exits 0.

## 6. Phase 6 — Automation & verification

- [x] 6.1 Implement a Node generator script (npm script in
  `docs/package.json`) that reads `arkflow components show <kind> <name>
  --format json` and regenerates the fenced `<!-- BEGIN AUTO -->` blocks in
  component pages. Verify: running the script updates only fenced regions.
- [x] 6.2 Add an orphan-page lint (Node or shell) that lists Markdown files
  under `docs/docs/` not reachable from the sidebar. Verify: lint exits
  non-zero when an orphan is introduced.
- [x] 6.3 Add a CI/docs-check that compares documented component inventory
  and per-component fields against `arkflow components list` /
  `components show --format json`, per `documentation-accuracy` delta.
  Verify: check passes on current docs and fails when a field is removed
  from docs.
- [x] 6.4 Final full build and link check. Verify: `cd docs && pnpm build`
  exits 0 with no broken-link warnings; orphan lint reports zero.
- [x] 6.5 Confirm consistency with `openspec/specs/documentation-accuracy`
  (no page re-introduces "stateless" or denies EOS, etc.). Verify: spec
  scenarios still hold against the rewritten pages.

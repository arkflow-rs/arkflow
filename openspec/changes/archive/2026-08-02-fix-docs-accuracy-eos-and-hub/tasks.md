# Tasks

## 1. Truth source capture

- [x] 1.1 Run `./target/release/arkflow components list` (build first if needed)
  and save the full input/processor/output/buffer/codec/temporary inventory as
  the authoritative component set for this change.
- [x] 1.2 Re-read the truth sources and extract the exact semantics to mirror:
  `openspec/specs/input-durability/spec.md`,
  `openspec/specs/exactly-once-output/spec.md`,
  `openspec/changes/make-control-plane-hub/specs/{control-plane-hub,
  compute-node-agent,fleet-control-console}/spec.md`, and
  `examples/eos-kafka.yaml`.

## 2. Intro accuracy (A1)

- [x] 2.1 Rewrite the `docs/docs/0-intro.md` delivery-semantics tip: remove
  "stateless" and the future-tense promise; state default at-least-once (WAL
  durability / replayable source) and opt-in exactly-once. Verify: page no
  longer contains "stateless" or lists WAL/EOS as upcoming.
- [x] 2.2 Add the shipped headline capabilities (CDC/Debezium, Schema Registry,
  WAL durability, EOS, control-plane Hub) to the `0-intro.md` feature/advanced
  sections with one-line pointers to their pages.

## 3. Delivery-semantics correction + EOS page (A4-EOS)

- [x] 3.1 Edit `docs/docs/components/0-inputs/delivery-semantics.md`: remove
  "Exactly-once delivery is not provided." and replace with a statement that
  exactly-once is opt-in via transactional outputs, linking to the new EOS
  page. Verify: the denial string and equivalents are gone.
- [x] 3.2 Create `docs/docs/components/exactly-once.md` with: the L2 config
  contract (`exactly_once: true` + required stable `transactional_id`), the
  "one ack range = one Kafka transaction / read_committed atomicity" model,
  the honest effectively-once boundary (post-commit pre-offset-commit crash
  window → downstream dedup; L3 future work), and a reference to
  `examples/eos-kafka.yaml`. Add a `sidebar_position` adjacent to
  `delivery-semantics.md`.
- [x] 3.3 Verify the EOS page renders in the local Docusaurus sidebar at the
  intended position (`cd docs && npm run build` or `npm run dev`).

## 4. README parity + features (A2, A3)

- [x] 4.1 Update `README.md` component lists (inputs/processors/outputs/buffers)
  to match the inventory from task 1.1; unify the SQL input name; add Memory,
  Multiple Inputs, Pulsar, InfluxDB, Redis, SQL outputs, Python processor.
- [x] 4.2 Update `README_zh.md` to match `README.md` exactly for the component
  inventory and feature set; reconcile the "智能分析" / Features divergence.
- [x] 4.3 Add the headline capabilities (CDC, Schema Registry, WAL durability,
  EOS, control-plane Hub) to both README Features sections.
- [x] 4.4 Bring `docs/docs/0-intro.md` component lists into the same set as the
  two READMEs (parity per the spec). Verify by diffing the three lists.

## 5. Control-plane consistency audit (A4-Hub)

- [x] 5.1 Build a checklist from every requirement in the three Hub specs and
  walk `docs/docs/control-plane.md` against it; record gaps (e.g.
  desired-vs-observed state, reconnect/resume, stale-node operator guidance).
- [x] 5.2 Walk `docs/docs/deploy/control-plane.md` against the same checklist;
  record gaps.
- [x] 5.3 Add only the missing content identified in 5.1/5.2 — do not rewrite
  accurate prose. Verify: no doc assertion contradicts a Hub spec requirement.

## 6. Verification

- [x] 6.1 Cross-check every requirement in
  `specs/documentation-accuracy/spec.md` against the edited files; confirm each
  WHEN/THEN holds (grep for the removed strings; confirm the component sets
  are equal).
- [x] 6.2 Build the docs site (`cd docs && npm run build`) to confirm the new
  page and edits render without broken links.
- [x] 6.3 Write `verification.md` recording the evidence: the
  `arkflow components list` output used, the grep proofs for removed claims,
  and the README/intro/zh parity diff.

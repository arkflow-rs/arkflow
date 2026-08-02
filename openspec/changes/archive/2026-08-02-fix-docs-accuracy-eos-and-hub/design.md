## Context

This is a **documentation-only** change. The engine contracts are settled and
authoritative in `openspec/specs/**`; the problem is purely that the
user-facing landing docs (`README.md`, `README_zh.md`, `docs/docs/0-intro.md`,
`docs/docs/components/0-inputs/delivery-semantics.md`, the control-plane pages)
contradict those specs. There is no code to design — only *where each fix
lives*, *what wording anchors the truth*, and *how to keep the two READMEs from
drifting again*.

The shipped capabilities that the docs must agree with (the "truth sources"):

- `openspec/specs/input-durability/spec.md` — WAL-persisted ingestion, at-least-once.
- `openspec/specs/message-acknowledgment/spec.md` — fallible Ack contract.
- `openspec/specs/exactly-once-output/spec.md` — `write_batch` transaction unit
  + Kafka transactional L2 + honest effectively-once boundary.
- `openspec/changes/make-control-plane-hub/specs/{control-plane-hub,
  compute-node-agent,fleet-control-console}/spec.md` — Hub/Agent/lease/console.
- `arkflow components list` — the authoritative component inventory.

## Goals / Non-Goals

**Goals:**

- Every delivery-semantics claim on a landing page is **true** as of the
  shipped specs.
- A reader can discover EOS, CDC, Schema Registry, WAL durability, and the Hub
  from the README/intro alone.
- `README.md` and `README_zh.md` agree on the component inventory and feature
  set, and both agree with `0-intro.md`.
- An invariant exists so future drift is detectable (encoded as the
  `documentation-accuracy` spec).

**Non-Goals:**

- Per-component reference rewrites, CLAUDE.md rescope, IA restructure,
  versioned docs, blog, OpenSpec spec edits (see proposal Non-goals).
- A build-time doc-generation pipeline (decided against — see D2).

## Decisions

### D1 — EOS page placement: a new `components/exactly-once.md`, linked from `delivery-semantics.md`

**Decision.** Add `docs/docs/components/exactly-once.md` (a top-level page
under `components/`, not nested under `0-inputs/` or `3-outputs/`), and have
`delivery-semantics.md`'s corrected EOS statement link to it.

**Alternatives considered.**
- *Fold EOS into `delivery-semantics.md`.* Rejected: that page is explicitly
  scoped to *input* delivery semantics ("Input Delivery Semantics", the
  ack-after-output rule, replayable-vs-non-replayable table). EOS is an
  *output*-side transaction contract; merging them overloads the page and
  buries the L2 boundary users need.
- *Put it under `3-outputs/exactly-once.md`.* Rejected: EOS spans input WAL +
  output transaction; nesting under outputs misrepresents it as "a Kafka
  output option" and hides it from readers browsing by concept. A concept
  page at `components/` level mirrors how `delivery-semantics.md` already sits
  as a concept page under `0-inputs/`.

**Anchor.** The page MUST state the honest boundary verbatim from the spec
(post-commit / pre-source-offset-commit crash ⇒ possible duplicates; downstream
dedup required; L3 `send_offsets_to_transaction` is future work) and MUST
reference `examples/eos-kafka.yaml`.

### D2 — README/intro parity: manual alignment + spec invariant, no generator

**Decision.** Edit `README.md`, `README_zh.md`, and `0-intro.md` by hand to the
same component inventory (sourced from `arkflow components list`) and the same
feature set, and encode the parity invariant in the `documentation-accuracy`
spec rather than building a generation step.

**Alternatives considered.**
- *Generate the component list from `arkflow components list` into all three
  files at build time.* Rejected: READMEs live at repo root (no Docusaurus
  build) and `0-intro.md` carries per-component prose; a generator would need a
  templating layer across two rendering contexts and would flatten curated
  descriptions into raw names. The cost exceeds the drift risk it prevents.
- *Single shared include file.* Rejected: GitHub-rendered root README and
  Docusaurus MDX have no shared-include mechanism that is cheaper than keeping
  the three lists aligned.

**Trade-off accepted.** Humans must keep three lists in sync. The
`documentation-accuracy` spec makes that a verifiable contract (a check that
the registered component set equals the union mentioned across the three docs)
rather than a hope. A future change can add automation if drift recurs.

### D3 — Intro wording anchor: replace "stateless" with the layered truth

**Decision.** Rewrite the `0-intro.md` tip to state, in order: (1) default
behavior is at-least-once via optional WAL durability (crash between read and
output does not lose data when durability is enabled, or when the source is
replayable); (2) exactly-once is available *opt-in* for transactional sinks
(Kafka L2); (3) single-node boundary is unchanged. Do **not** promise these as
future work — they ship today.

**Anchor.** The wording MUST NOT contain the word "stateless" as a description
of the engine, and MUST NOT list WAL/EOS as upcoming. This is a hard
requirement in the spec so it cannot regress.

### D4 — Hub audit method: spec-driven checklist, additions only

**Decision.** Build a checklist from each requirement in the three Hub specs
and walk `docs/docs/control-plane.md` and `docs/docs/deploy/control-plane.md`
against it. **Add only what is missing** (early read suggests: desired-vs-
observed state framing, reconnect/resume behavior, operator action on stale
nodes, console fleet navigation). Do not restructure or rewrite the existing
prose — `control-plane.md` already reflects the Hub architecture correctly
per the proposal grounding.

**Why additions-only.** The surgical-changes rule applies to docs too: these
pages were recently rewritten for the Hub and are accurate; rewriting them
again risks introducing new errors and inflates review surface.

## Risks / Trade-offs

- **[Drift recurs after this change]** → The `documentation-accuracy` spec
  encodes the invariants; future changes that add a component or capability
  must update the docs and can be checked against the spec. Imperfect but
  raises the cost of regression.
- **[EOS page overstates the guarantee]** → Highest doc risk: readers may read
  "exactly-once" and ignore the L2 boundary. Mitigation: the honest-boundary
  section is a required, prominent section of the page, restated in
  `delivery-semantics.md`, and mirrored from the spec's own
  "Effectively-once boundary is honestly scoped" requirement.
- **[Chinese translation lag]** → README_zh may fall behind README again.
  Mitigation: the spec requires parity at land time; reviewer checklist calls
  out the Chinese file explicitly.
- **[Hub audit uncovers a real spec/doc contradiction]** → If the audit finds
  the doc asserts something the spec does not support, treat it as an accuracy
  fix in-scope; if it finds the *spec* is wrong, stop and raise it rather than
  silently "fixing" the doc to match a possibly-wrong page.

## Migration Plan

Not applicable — documentation only, no runtime migration. Rollback is `git
revert`. Pages render through the existing Docusaurus build and GitHub README
rendering with no structural change (the one new file,
`components/exactly-once.md`, is auto-picked-up by the `autogenerated`
sidebar, weighted by a front-matter `sidebar_position`).

## Open Questions

- **Exact `sidebar_position` for the new EOS page.** It should sit near
  `delivery-semantics.md`. Resolved at implementation time by reading the
  existing positions in `docs/docs/components/0-inputs/` and picking an
  adjacent value; no design-level decision needed.
- **Whether to add a Chinese `0-intro.md`.** i18n of the Docusaurus tree is out
  of scope here (the README pair is the Chinese surface today); tracked as a
  possible separate change. Not blocking.

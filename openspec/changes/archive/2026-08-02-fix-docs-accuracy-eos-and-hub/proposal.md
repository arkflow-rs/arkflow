## Why

The user-facing documentation has drifted badly from the shipped engine. After
the reliability and control-plane work of the past weeks, several landing pages
make **factually false** claims that would mislead a technical selection:

- `docs/docs/0-intro.md:11` tells readers *"ArkFlow is **stateless**"* and
  promises transactional/state capabilities "in the future" — yet
  `openspec/specs/input-durability/spec.md` (WAL-persisted, at-least-once),
  `openspec/specs/message-acknowledgment/spec.md`, and
  `openspec/specs/exactly-once-output/spec.md` (Kafka transactional L2) are all
  shipped, and `examples/eos-kafka.yaml` exercises them.
- `docs/docs/components/0-inputs/delivery-semantics.md` ("At-least-once
  contract") states *"Exactly-once delivery is not provided."* — a direct
  contradiction of the shipped `exactly-once-output` capability. A reader who
  trusts this page will wrongly conclude EOS is impossible.
- `README.md` and `README_zh.md` ship a component list that is behind
  `docs/docs/0-intro.md`, which is itself informal: README lists ~10 inputs /
  6 outputs (`README.md:110-124`, `README.md:162-170`), omits Memory / Multiple
  Inputs / Pulsar / InfluxDB / Redis / SQL outputs and the Python processor,
  and gives the SQL input two different names ("Database" vs "SQL"). No
  landing page mentions CDC / Schema Registry / WAL durability / EOS / the
  control-plane Hub — the current version's headline features.
- `README.md` (English) and `README_zh.md` (Chinese) are out of sync: the
  Chinese "特性" list has an "智能分析" bullet the English "Features" lacks, the
  two share no single source of truth for the component list, and community
  sections differ.

These are accuracy bugs in the project's front door. They are fixed now because
the underlying capabilities have just landed and the docs are actively wrong,
not merely incomplete.

## What Changes

- **Rewrite the `0-intro.md` delivery-semantics claim** away from "stateless":
  describe WAL-backed at-least-once as the default and exactly-once (Kafka
  transactional L2) as opt-in, and stop promising these as future work.
- **Correct `delivery-semantics.md`** to state that exactly-once *is* available
  (opt-in Kafka transactional output) and link to the new EOS page instead of
  asserting it is not provided.
- **Add a user-facing exactly-once document** covering the L2 contract
  (`exactly_once: true` + `transactional_id`, one ack range = one Kafka
  transaction, `read_committed` atomicity), the honest effectively-once
  boundary (post-commit / pre-offset-commit crash window, downstream dedup,
  L3 as future work), and the `examples/eos-kafka.yaml` usage.
- **Align the README / README_zh component lists** with the registered
  components (the authoritative source surfaced by `arkflow components list`),
  unify the SQL input naming, and bring the two languages back into parity.
- **Add the headline features** (CDC/Debezium, Schema Registry, WAL durability,
  EOS, control-plane Hub) to the README and intro feature sections.
- **Audit the control-plane docs** (`docs/docs/control-plane.md` and
  `docs/docs/deploy/control-plane.md`) against the `control-plane-hub`,
  `compute-node-agent`, and `fleet-control-console` specs, filling any gaps
  (e.g. desired-vs-observed state, reconnect/resume, stale-node operator
  guidance). These pages already cover the Hub architecture; this is a
  consistency audit, not a rewrite.

## Capabilities

### New Capabilities

- `documentation-accuracy`: requirements that the user-facing documentation
  SHALL describe shipped engine semantics truthfully — covering the
  stateless/at-least-once/exactly-once claim, the EOS availability statement,
  component-list parity across README/intro and across languages, and
  feature-list coverage of shipped capabilities.

### Modified Capabilities

<!-- None. No code contract (Input/Output/Processor/Buffer/Codec, ack, WAL,
     control-plane) changes. Existing specs remain authoritative; this change
     only brings user docs into agreement with them. -->

(none)

## Non-goals

- **Rewriting per-component reference docs** under `docs/docs/components/**`
  beyond the two accuracy fixes (`0-intro.md`, `delivery-semantics.md`) and the
  new EOS page. Those pages are large and mostly accurate; a separate change
  can audit them.
- **The CLAUDE.md AI-instructions file.** It overlaps user docs but is a
  separate audience and concern (slimming / re-scoping it is its own task).
- **Information-architecture restructuring** (regrouping the sidebar, absorbing
  the loose `docs/control-plane-operations.md` and `docs/performance/*` into
  the Docusaurus tree). That is a structural change, not an accuracy change.
- **Versioned docs** (`docs/versioned_docs/**`). Historical snapshots are
  frozen.
- **Blog posts** (`docs/blog/**`).
- **The OpenSpec capability specs themselves** (`openspec/specs/**`). They are
  the source of truth this change defers to, not the target.
- **New features.** No engine, config, or API behavior changes — documentation
  only.

## Impact

- **Documentation files**:
  - `README.md`, `README_zh.md` — component lists, feature sections, parity.
  - `docs/docs/0-intro.md` — delivery-semantics tip rewrite, feature coverage.
  - `docs/docs/components/0-inputs/delivery-semantics.md` — correct the EOS
    statement and link out.
  - new `docs/docs/components/**` exactly-once page (placement decided in
    design).
  - `docs/docs/control-plane.md`, `docs/docs/deploy/control-plane.md` —
    consistency audit vs Hub specs, minor additions only.
- **Code**: none. No trait, config, or behavior change.
- **Dependencies**: none.
- **Verification**: every doc claim must trace to a shipped capability spec or
  to `arkflow components list` output; cross-checked in the change's
  `verification.md`.

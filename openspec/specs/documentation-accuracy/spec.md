# documentation-accuracy Specification

## Purpose

Keep ArkFlow's user-facing landing documentation truthful with respect to the
shipped engine. The front-door pages (`README.md`, `README_zh.md`,
`docs/docs/0-intro.md`, `docs/docs/components/0-inputs/delivery-semantics.md`,
the control-plane pages, and the new exactly-once page) SHALL NOT contradict
the authoritative capability specs in `openspec/specs/**` or the registered
component inventory surfaced by `arkflow components list`. These requirements
exist because the docs drifted badly after the reliability and control-plane
work landed; they encode the invariants so future drift is detectable.

## Requirements

### Requirement: Engine delivery-semantics claim is truthful
The introduction page (`docs/docs/0-intro.md`) SHALL NOT describe ArkFlow as
"stateless", and SHALL NOT list write-ahead-log durability, message
acknowledgment, or exactly-once output as future/upcoming capabilities. It
SHALL state the shipped semantics: at-least-once delivery is the default
(available via WAL durability on any input, and automatically when the source
is replayable), and exactly-once delivery is available opt-in for
transactional sinks.

#### Scenario: No "stateless" description
- **WHEN** the introduction page is rendered
- **THEN** it does not contain the word "stateless" as a characterization of
  the engine, and does not promise transactional or state-management
  capabilities as upcoming work

#### Scenario: Layered semantics stated
- **WHEN** a reader reads the introduction's delivery-semantics note
- **THEN** the note states at-least-once as the default and exactly-once as
  opt-in, consistent with `openspec/specs/input-durability/spec.md` and
  `openspec/specs/exactly-once-output/spec.md`

### Requirement: Delivery-semantics page acknowledges exactly-once
`docs/docs/components/0-inputs/delivery-semantics.md` SHALL NOT state that
exactly-once delivery is not provided. It SHALL state that exactly-once is
available opt-in via transactional outputs and SHALL link to the
exactly-once page.

#### Scenario: Contradictory statement removed
- **WHEN** the delivery-semantics page is rendered
- **THEN** it does not contain the assertion "Exactly-once delivery is not
  provided" or any equivalent denial of the shipped `exactly-once-output`
  capability

#### Scenario: Link to exactly-once page
- **WHEN** the delivery-semantics page discusses delivery guarantees beyond
  at-least-once
- **THEN** it links to the exactly-once page for the opt-in transactional path

### Requirement: Exactly-once page documents the honest L2 boundary
The exactly-once page SHALL document the Kafka transactional (L2) contract:
opt-in via `exactly_once: true` plus a stable, required `transactional_id`;
one ack range equals one Kafka transaction; downstream `read_committed`
consumers observe each batch atomically. It SHALL prominently document the
honest effectively-once boundary — that a crash after the producer commits
and before the source offset commits can still produce duplicates, requiring
downstream idempotency — and SHALL state that L3 true end-to-end EOS
(`send_offsets_to_transaction`) is future work. It SHALL reference
`examples/eos-kafka.yaml`.

#### Scenario: Configuration contract documented
- **WHEN** a reader opens the exactly-once page
- **THEN** it shows `exactly_once: true` and a required `transactional_id` and
  references `examples/eos-kafka.yaml` for a working configuration

#### Scenario: Honest boundary stated
- **WHEN** the page describes what exactly-once guarantees
- **THEN** it states the post-commit / pre-source-offset-commit crash duplicate
  window and recommends downstream dedup, mirroring the
  "Effectively-once boundary is honestly scoped" requirement of
  `openspec/specs/exactly-once-output/spec.md`

### Requirement: Component inventory parity across landing docs
For the primary configurable categories — inputs, processors, outputs, and
buffers — `README.md`, `README_zh.md`, and `docs/docs/0-intro.md` SHALL
enumerate the same set of components, and that set SHALL equal the components
registered in the engine (as surfaced by `arkflow components list`) for those
categories. The SQL input SHALL be named consistently ("SQL") across all three
documents. Any component type-name a landing doc mentions SHALL be a registered
component; "join" SHALL NOT be presented as a standalone buffer type (it is a
sub-configuration of the window buffers). Codecs and temporary storage are
covered by their own reference pages and the components listing; landing docs
MAY mention them but are not required to enumerate them fully.

#### Scenario: Registered primary components are listed
- **WHEN** a reader surveys the input/processor/output/buffer list in any of
  the three landing docs
- **THEN** every registered input, processor, output, and buffer is mentioned,
  including Memory and Multiple Inputs (inputs), Pulsar (inputs), the Python
  processor, and InfluxDB, Redis, and SQL outputs

#### Scenario: Cross-language and cross-page parity
- **WHEN** the input/processor/output/buffer lists of `README.md`,
  `README_zh.md`, and `docs/docs/0-intro.md` are compared
- **THEN** they enumerate the same component set per category, with no
  component present in one and absent in another, and the SQL input uses one
  name

#### Scenario: Join is not a standalone buffer type
- **WHEN** a landing doc describes the available buffer types
- **THEN** it lists memory, tumbling window, sliding window, and session
  window only, and does not list "join" as a peer buffer type

### Requirement: Feature coverage reflects shipped capabilities
The README (`README.md`, `README_zh.md`) and `docs/docs/0-intro.md` feature
sections SHALL mention the shipped headline capabilities: CDC via Debezium,
Schema Registry (Confluent wire-format Protobuf), WAL input durability,
exactly-once output, and the control-plane Hub. Mentions need not be deep but
MUST surface each capability's existence to a first-time reader.

#### Scenario: Headline capabilities discoverable
- **WHEN** a first-time reader scans the Features section of any landing doc
- **THEN** CDC/Debezium, Schema Registry, WAL durability, exactly-once, and the
  control-plane Hub are each identifiable

### Requirement: Control-plane docs agree with the Hub specs
`docs/docs/control-plane.md` and `docs/docs/deploy/control-plane.md` SHALL be
consistent with the `control-plane-hub`, `compute-node-agent`, and
`fleet-control-console` specs. In particular they SHALL cover node leases and
stale-node behavior, desired-versus-observed state, reconnect/resume, and the
operator experience for targeting a specific node — adding only what the specs
require that the pages currently omit.

#### Scenario: Stale-node operator guidance present
- **WHEN** an operator reads the control-plane docs
- **THEN** the docs explain that a stale node remains visible but cannot receive
  new commands, consistent with the `control-plane-hub` node-lease requirement

#### Scenario: No contradiction with Hub specs
- **WHEN** any assertion in the control-plane docs is compared to the three Hub
  specs
- **THEN** the doc does not assert behavior the spec does not support

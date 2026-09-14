# documentation-accuracy Delta

## MODIFIED Requirements

### Requirement: Component inventory parity across landing docs
For the primary configurable categories (inputs, processors, outputs, and
buffers), `README.md` and `README_zh.md` SHALL enumerate the same set of
components, and that set SHALL equal the components registered in the engine
(as surfaced by the generated component inventory) for those categories. The
SQL input SHALL be named consistently ("SQL") across both documents.
`docs/docs/intro.md` is a compatibility route: it SHALL link to the generated
component inventory as the authoritative component listing and SHALL NOT
mention component type-names that are absent from the registry. Any component
type-name a landing doc mentions SHALL be a registered component type-name;
"join" SHALL NOT be presented as a standalone buffer type (it is a
sub-configuration of the window buffers). Codecs and temporary storage are
covered by their own reference pages and the components listing; landing docs
MAY mention them but any type-name used SHALL be registered.

#### Scenario: Registered primary components are listed
- **WHEN** a reader surveys the input/processor/output/buffer list in either
  README
- **THEN** every registered input, processor, output, and buffer is mentioned
  under its registry type-name, including Memory and Multiple Inputs (inputs),
  Pulsar (inputs), the Python processor, and InfluxDB, Redis, and SQL outputs

#### Scenario: Cross-language parity
- **WHEN** the input/processor/output/buffer lists of `README.md` and
  `README_zh.md` are compared
- **THEN** they enumerate the same component set per category, with no
  component present in one and absent in another, and the SQL input uses one
  name

#### Scenario: Intro page defers to the generated inventory
- **WHEN** a reader opens `docs/docs/intro.md`
- **THEN** the page links to the generated component inventory as the
  authoritative listing, and every component type-name it does mention is
  registered

#### Scenario: Join is not a standalone buffer type
- **WHEN** a landing doc describes the available buffer types
- **THEN** it lists memory, tumbling window, sliding window, and session
  window only, and does not list "join" as a peer buffer type

#### Scenario: Landing docs use registry type-names
- **WHEN** a landing doc names any component
- **THEN** the name matches a registry entry exactly (e.g. the conversion
  processors are named `arrow_to_json`, `json_to_arrow`,
  `arrow_to_protobuf`, `protobuf_to_arrow`), and no unregistered name such
  as a bare `protobuf` processor appears

## ADDED Requirements

### Requirement: Component reference pages SHALL use registry type-names
Component reference pages SHALL document every registered component under its
exact registry type-name, and SHALL NOT present configuration examples using
type-names that are absent from the generated inventory. Every registered
component SHALL be documented by at least one reference page (via front-matter
ownership), including conversion processors such as `arrow_to_json`.

#### Scenario: A conversion processor is documented
- **WHEN** a reader looks for `arrow_to_json` in the processors section
- **THEN** a reference page documents it under that exact type-name with a
  valid configuration example

#### Scenario: A page uses a ghost type-name
- **WHEN** a component reference page presents a configuration example whose
  `type:` value is not in the generated inventory
- **THEN** the documentation check fails naming the page and the unknown
  type-name

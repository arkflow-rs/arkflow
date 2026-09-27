# Purpose

Define the machine-readable export of the ArkFlow component registry that backs the CLI, the generated documentation reference, and their CI enforcement.

## Requirements

### Requirement: The component registry SHALL be machine-readable from the CLI
The engine SHALL expose the full component registry — every registered
component's kind, type-name, description, optional-config flag, configuration
JSON Schema, and example configuration — through `arkflow components list
--format json`. The same serialization function SHALL back the CLI output, the
committed documentation inventory, and the snapshot test, so the three cannot
diverge. Output SHALL be deterministic (sorted by kind then name, stable field
order, trailing newline).

#### Scenario: A user lists components as JSON
- **WHEN** `arkflow components list --format json` runs
- **THEN** it prints one JSON document containing every registered component
  with its kind, name, description, `config_optional`, `config_schema`, and
  `config_example` when present, sorted by kind then name

#### Scenario: Temporary components are listed
- **WHEN** the JSON or text listing is produced after all plugin `init()`
  functions run
- **THEN** components registered as temporary (e.g. `redis`) appear with kind
  `temporary`, indistinguishable in structure from other kinds

### Requirement: Temporary SHALL be a first-class component kind
`ComponentKind` SHALL include `temporary` as a sixth kind with the same
registration, listing, metadata, and schema treatment as the existing five
kinds. The plugin-local temporary builder registry SHALL be removed;
temporary components register through the core registry. The engine
configuration JSON Schema and `components` CLI output SHALL include the
temporary kind.

#### Scenario: A temporary component registers through core
- **WHEN** the temporary plugin `init()` runs
- **THEN** its builders and metadata live in the core registry, and the
  plugin-local registry no longer exists

#### Scenario: Schema and listings include temporary
- **WHEN** `arkflow components list` or `arkflow schema` output is produced
- **THEN** the temporary kind and its components are discoverable through the
  same interface as every other kind

### Requirement: The committed documentation inventory SHALL be a generated artifact
`docs/reference/component-inventory.json` SHALL be generated from the
registry export (format version 2, without a hand-maintained page-mapping
field) and SHALL carry a version marker. A Rust snapshot test SHALL fail when
the committed file differs from the live registry dump, and the failure
message SHALL state the exact regeneration command. The test SHALL NOT write
files unless the documented regeneration flag is set.

#### Scenario: A component is added without regenerating
- **WHEN** a plugin registers a new component and `cargo test --workspace`
  runs without the committed inventory being regenerated
- **THEN** the snapshot test fails, naming the stale file and printing the
  regeneration command

#### Scenario: A contributor regenerates
- **WHEN** the documented regeneration command runs with the regeneration
  flag set
- **THEN** the committed inventory is rewritten from the live registry and
  the snapshot test passes on the next run

### Requirement: The engine configuration JSON Schema SHALL be published as a docs asset
The output of the engine config schema command SHALL be committed as
`docs/static/config-schema.json` and linked from a documentation page that
explains IDE auto-completion setup. The same snapshot mechanism as the
component inventory SHALL enforce its freshness.

#### Scenario: The schema changes
- **WHEN** the engine configuration schema changes and the snapshot test runs
- **THEN** the test fails until `docs/static/config-schema.json` is
  regenerated from the live schema

#### Scenario: A reader wants IDE completion
- **WHEN** a reader opens the configuration reference
- **THEN** the page links to the published JSON Schema asset and documents
  how to enable editor auto-completion with it

### Requirement: Component kind initialization SHALL propagate registration failures
每个组件 kind 的 `init()`（input/output/processor/buffer/codec/wal 等）SHALL 将注册结果持久化在进程级单次初始化存储中并向调用方传播：任一 builder 或元数据注册失败时 `init()` SHALL 返回该错误；重复调用 SHALL 幂等返回首次结果（成功或失败）而不重试注册。注册错误 SHALL NOT 被静默丢弃，SHALL NOT 使进程以残缺组件目录继续运行而对外报告初始化成功。初始化编排方（如 `arkflow_plugin::initialize`）SHALL 在首个 kind 失败时短路停止后续初始化并向上传播错误。

#### Scenario: 注册失败使启动失败
- **WHEN** 某个组件 builder 或其元数据注册返回错误（例如注册名冲突或元数据构造失败）
- **THEN** 该 kind 的 `init()` 返回该错误，编排初始化以配置错误失败，进程不进入组件残缺的运行状态

#### Scenario: 初始化幂等且失败可重复观察
- **WHEN** `init()` 首次调用失败后被再次调用
- **THEN** 第二次调用不重试注册，直接返回与首次相同的错误

#### Scenario: 成功路径保持幂等
- **WHEN** 所有注册成功且 `init()` 被多次调用
- **THEN** 注册表只构建一次，后续调用立即返回成功，组件目录完整

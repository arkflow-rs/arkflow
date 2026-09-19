# Purpose

Define local and CI quality gates that prevent broken ArkFlow documentation from being published.

## Requirements

### Requirement: Documentation quality checks SHALL run locally and in CI
The repository SHALL expose one documented validation command that checks page metadata/structure, sidebar reachability, internal links and anchors, reference coverage, and configured examples, and CI SHALL run the same command.

#### Scenario: A pull request has a broken internal link
- **WHEN** the documentation validation command runs
- **THEN** it fails with the source page and unresolved target so the pull request cannot pass the documentation gate.

#### Scenario: Documentation CI and local validation are compared
- **WHEN** CI executes the documentation check
- **THEN** it uses the repository's documented command and does not maintain a divergent duplicate implementation of the checks.

### Requirement: Documentation builds SHALL be release-blocking after baseline cleanup
The production site build SHALL be a required validation step, and known exceptions SHALL be explicit, owned, and reviewable rather than hidden by broad warning settings.

#### Scenario: A page references a missing anchor
- **WHEN** the production documentation build runs
- **THEN** the build or validation gate fails or reports an explicit tracked exception with the affected route and owner.

#### Scenario: A code/configuration example is invalid
- **WHEN** an example is marked as executable or validation-targeted
- **THEN** the checker runs the appropriate syntax/config validation and fails with actionable file and line information.

### Requirement: Code-to-inventory freshness SHALL be enforced by the Rust test suite
The workspace test run SHALL include a snapshot test that compares the
committed documentation inventory (`docs/reference/component-inventory.json`)
and the published config-schema asset (`docs/static/config-schema.json`)
against the live registry and schema. The test SHALL fail with the
regeneration command when either artifact is stale and SHALL NOT modify files
unless the explicit regeneration flag is set. The docs CI job SHALL NOT
require a Rust toolchain; the committed artifacts are the seam between the
two gates.

#### Scenario: Rust CI catches a registry change without regeneration
- **WHEN** a pull request changes component registration and runs
  `cargo test --workspace --all-targets`
- **THEN** the snapshot test fails naming the stale artifact and the
  regeneration command, blocking the pull request

#### Scenario: Docs CI stays Node-only
- **WHEN** the documentation check workflow runs
- **THEN** it performs inventory-to-page validation against the committed
  generated artifacts without compiling Rust

### Requirement: Example configurations SHALL be offline-validated
Every example registered in `docs/reference/example-manifest.json` SHALL be
deep-validated offline by a workspace test (parsing, stream/job graph checks,
and semantic configuration validation, matching `--validate` semantics).
Examples that cannot be validated offline SHALL carry an explicit
`validate: false` exclusion with a stated reason in the manifest; silent
skipping SHALL NOT be permitted.

#### Scenario: An example drifts from the config schema
- **WHEN** a configuration field is renamed or removed in the engine and the
  workspace tests run
- **THEN** the example validation test fails naming the affected example file
  and the validation error

#### Scenario: An example cannot be validated offline
- **WHEN** an example requires external state that offline validation cannot
  satisfy
- **THEN** its manifest entry records `validate: false` with a reason, and
  the validation test documents the exclusion rather than skipping silently

### Requirement: The example manifest SHALL cover every example YAML on disk
Every `*.yaml`/`*.yml` file directly under `examples/` SHALL be registered in
`docs/reference/example-manifest.json`; the documentation validation command
SHALL fail when a new example file is not registered, so examples cannot be
added without entering the offline-validation gate.

#### Scenario: A new example YAML is added without registration
- **WHEN** a contributor adds `examples/foo_example.yaml` without a manifest
  entry and the documentation validation command runs
- **THEN** the command fails naming `examples/foo_example.yaml` as
  unregistered

#### Scenario: An example cannot pass offline validation
- **WHEN** a registered example cannot be deep-validated offline (for
  instance a legacy configuration the compiler rejects by design)
- **THEN** its manifest entry records `validate: false` with a stated reason
  instead of leaving the file unregistered

### Requirement: Component builders and docs metadata SHALL be name-aligned
Each component kind SHALL expose its registered builder names, and the
workspace test run SHALL include a test that fails when a builder is
registered without matching docs metadata or docs metadata exists without a
registered builder, because either misalignment silently breaks
`components list`, the docs inventory, or the IDE schema.

#### Scenario: A plugin registers a builder without docs metadata
- **WHEN** a plugin's `init()` calls the builder registration but not the
  metadata registration, and the workspace tests run
- **THEN** the registry consistency test fails naming the kind and component

#### Scenario: Docs metadata exists without a builder
- **WHEN** metadata is registered for a component name that has no
  registered builder, and the workspace tests run
- **THEN** the registry consistency test fails naming the orphan metadata
  entry

### Requirement: Inline YAML blocks in documentation SHALL be classified and validated
Every fenced `yaml` code block in the maintained documentation tree (`docs/docs`) SHALL
carry an explicit classification: `full` (a complete ArkFlow configuration), `fragment`
with a wrap kind (a snippet completed by a wrap template before validation), or `foreign`
with a stated reason (non-ArkFlow YAML). The documentation validation command SHALL fail
on any unclassified block or unrecognized classification. Blocks classified as `full` or
`fragment` SHALL be deep-validated offline by a workspace test through the real engine
configuration parser (parsing plus semantic configuration validation, matching
`--validate` semantics), with wrap templates supplying offline-free complement
components; `foreign` blocks SHALL receive a YAML well-formedness check. Exclusions
beyond the declared classifications SHALL NOT be permitted.

#### Scenario: A contributor adds an unclassified yaml block
- **WHEN** a documentation page gains a `yaml` block without a
  classification marker and the documentation validation command runs
- **THEN** the command fails naming the page and block so the pull request
  cannot pass the documentation gate

#### Scenario: A doc snippet drifts from the config schema
- **WHEN** a configuration field is renamed or removed in the engine and the
  workspace tests run
- **THEN** the snippet validation test fails naming the documentation page
  and the validation error for the affected block

#### Scenario: A fragment snippet is completed for validation
- **WHEN** a block classified as `fragment` with a wrap kind is validated
- **THEN** the test wraps the snippet with the template for that kind using
  offline-free complement components and validates the completed
  configuration through the real parser

#### Scenario: A foreign yaml block is excluded
- **WHEN** a block is classified as `foreign` (for instance a Kubernetes
  manifest or a Prometheus scrape configuration)
- **THEN** only its YAML well-formedness is checked, and its stated reason is
  recorded rather than the block being skipped silently

### Requirement: External documentation links SHALL be periodically checked report-only
External links in the documentation SHALL be checked by a scheduled CI job on
a recurring cadence, and the job SHALL report its results (for example via an
issue) without failing pull requests; external-link results SHALL NOT gate
code or documentation changes.

#### Scenario: An external link rots
- **WHEN** the scheduled external-link check finds an unreachable external URL
- **THEN** the job reports the affected pages and URLs without blocking any
  pull request

#### Scenario: A pull request adds a temporarily unreachable external link
- **WHEN** a pull request adds an external link whose target is transiently
  unreachable and the documentation validation command runs
- **THEN** the documentation gate passes (external links are out of its
  scope) and the scheduled job surfaces the link if it stays unreachable

### Requirement: Localized documentation trees SHALL pass the same validation gate
The documented documentation validation command SHALL additionally validate every non-default locale tree (currently `docs/i18n/zh-Hans/`) using the same command, failure style, and CI wiring as the English tree. For each translated page the checker SHALL verify: (1) an English counterpart exists at the mirrored path under `docs/docs/`; (2) front-matter identity and the `components:` ownership list equal the English counterpart's; (3) file-relative internal links and anchors resolve inside the localized tree — the same resolution the production build performs with broken-markdown-links fatal — while locale-prefixed absolute routes (`/zh-Hans/docs/...`) MAY point at untranslated pages, which render English fallback content; and (4) every ` ```yaml ` fence is byte-identical to the corresponding fence in the English counterpart, including the classification metastring. The checker SHALL also pre-detect the symmetric case in the English tree: an untranslated English page using a file-relative link to a page that has a localized counterpart fails the gate, because the localized build registers that target under its localized source path. The Rust-side snippet validator SHALL remain scoped to `docs/docs/`; localized trees are snippet-valid by construction through the byte-fidelity rule.

#### Scenario: A translated page loses its English counterpart
- **WHEN** an English page is moved or deleted while its `zh-Hans` translation remains at the old mirrored path
- **THEN** the validation command fails, naming the orphaned localized page

#### Scenario: A translated page drifts in front matter
- **WHEN** a translated page's `id`, `slug`, `sidebar_position`, `sidebar_label`, or `components:` list differs from its English counterpart
- **THEN** the validation command fails, naming the page and the mismatched field

#### Scenario: A translated page links relatively to an untranslated page
- **WHEN** a translated page contains a file-relative internal link whose target file does not exist in the localized tree
- **THEN** the validation command fails with guidance to link the locale-prefixed absolute route instead, because the production build cannot resolve that link

#### Scenario: An untranslated English page links to a localized page
- **WHEN** an English page without a localized counterpart uses a file-relative link to a page that has a localized counterpart
- **THEN** the validation command fails with guidance to convert the link to its absolute `/docs/...` route, because the localized build cannot resolve that link

#### Scenario: A translated page has a broken internal link
- **WHEN** a translated page contains an internal link or anchor that does not resolve
- **THEN** the validation command fails with the source page and unresolved target

#### Scenario: Anchors are validated against the localized rendering
- **WHEN** a translated page links to an anchor on a localized target page
- **THEN** the anchor is checked against the localized headings of that page

#### Scenario: A translated page edits a YAML fence
- **WHEN** any ` ```yaml ` fence in a translated page differs in metastring or content from the corresponding fence in its English counterpart
- **THEN** the validation command fails, naming the page and the fence

#### Scenario: Localized validation runs in the same CI gate
- **WHEN** CI executes the documentation check on a pull request that touches only the localized tree
- **THEN** the localized-tree violations above fail the pull request through the same command and workflow as English-tree violations

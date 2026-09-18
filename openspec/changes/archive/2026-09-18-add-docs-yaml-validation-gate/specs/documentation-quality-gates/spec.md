## ADDED Requirements

### Requirement: Inline YAML blocks in documentation SHALL be classified and validated
Every fenced `yaml` code block in the maintained documentation tree (`docs/docs`) SHALL
carry an explicit classification: `full` (a complete
ArkFlow configuration), `fragment` with a wrap kind (a snippet completed by a
wrap template before validation), or `foreign` with a stated reason
(non-ArkFlow YAML). The documentation validation command SHALL fail on any
unclassified block or unrecognized classification. Blocks classified as
`full` or `fragment` SHALL be deep-validated offline by a workspace test
through the real engine configuration parser (parsing plus semantic
configuration validation, matching `--validate` semantics), with wrap
templates supplying offline-free complement components; `foreign` blocks
SHALL receive a YAML well-formedness check. Exclusions beyond the declared
classifications SHALL NOT be permitted.

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

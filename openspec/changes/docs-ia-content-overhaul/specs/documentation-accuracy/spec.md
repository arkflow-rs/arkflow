## ADDED Requirements

### Requirement: Documented component inventory matches the registered inventory
The set of components shown in the docs (`docs/docs/components/**`) SHALL
match the components surfaced by `arkflow components list` for the current
binary. No component registered in the binary SHALL be undocumented, and no
documented component SHALL be absent from the binary.

#### Scenario: Every registered component is documented
- **WHEN** `arkflow components list` is run against the current binary and
  its output is compared to the component pages under
  `docs/docs/components/**`
- **THEN** every registered input, output, processor, buffer, and codec has
  a corresponding documentation page

#### Scenario: No phantom component documented
- **WHEN** each component page under `docs/docs/components/**` is enumerated
- **THEN** every documented component `type` value appears in the output of
  `arkflow components list`

### Requirement: Documented config fields match the CLI-emitted schema
For every component, the documented Full reference fields SHALL be consistent
with the field metadata emitted by the CLI for the current binary (via
`arkflow components show` with `--format json`, and the equivalent section of
`arkflow schema`): field names, types, required-ness, and defaults SHALL not
contradict the CLI output.

#### Scenario: No undocumented field in the binary
- **WHEN** the CLI-emitted field set for a component is compared to the
  documented `## Full reference` table
- **THEN** every field present in the CLI output appears in the table (or is
  intentionally folded under a documented sub-structure)

#### Scenario: No phantom field in the docs
- **WHEN** the documented field set for a component is compared to the CLI
  output
- **THEN** no documented field is absent from the CLI-emitted schema unless
  the field is explicitly marked deprecated in the table

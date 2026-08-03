## ADDED Requirements

### Requirement: Component reference pages use the canonical template
Every component reference page under `docs/docs/components/` SHALL contain a
fixed set of named sections in order. The page begins with a one-line scenario
positioning statement, then these sections: Status; When to use; Common
fields; Full reference; Examples (with multiple scenarios); Output schema
(for inputs and codecs) or Input schema (for outputs where applicable); Error
handling; Metrics; and See also. A page MAY omit the Output schema / Input
schema section only when the component produces or consumes no schema worth
documenting, with the omission noted.

#### Scenario: Component page contains mandatory sections
- **WHEN** any `docs/docs/components/**/*.md` page is parsed
- **THEN** it contains the headers `## Status`, `## When to use`,
  `## Common fields`, `## Full reference`, `## Examples`, `## Error
  handling`, `## Metrics`, and `## See also`

#### Scenario: Examples section is multi-scenario
- **WHEN** the `## Examples` section of a component page is inspected
- **THEN** it presents at least two distinct usage scenarios as separate
  sub-sections or fenced blocks, not a single example

### Requirement: Component Status uses the defined taxonomy
The `## Status` line of every component page SHALL state exactly one of
`Stable`, `Beta`, or `Experimental`, reflecting the component's maturity.

#### Scenario: Valid status value
- **WHEN** the `## Status` line is read
- **THEN** it matches one of `Stable`, `Beta`, or `Experimental`

### Requirement: Generated config tables are fenced and non-editable
Every CLI-derived config table on a component page SHALL be wrapped between
matching HTML comment fences that mark the block as auto-generated. Content
inside those fences SHALL NOT be hand-edited and SHALL be regenerated only by
the generator script.

#### Scenario: Fence boundary present around generated table
- **WHEN** a generated Full reference table is rendered in a component page
- **THEN** it is enclosed by matching `<!-- BEGIN AUTO: ... -->` and
  `<!-- END AUTO -->` comment fences

#### Scenario: Fenced content matches CLI output
- **WHEN** the generator script is run for a component
- **THEN** the field set, field types, and defaults inside its fences match
  the JSON emitted by `arkflow components show <kind> <name> --format json`
  for the current binary

### Requirement: Full reference table columns are canonical
Every "Full reference" config table SHALL include the columns Field, Type,
Required, Default, and a `common?` indicator, so readers can scan required vs
optional fields uniformly across all components.

#### Scenario: Reference table has canonical columns
- **WHEN** a component's `## Full reference` table header is parsed
- **THEN** it contains the columns Field, Type, Required, Default, and a
  common/advanced indicator column

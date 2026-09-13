# documentation-reference-generation Delta

## MODIFIED Requirements

### Requirement: Component and configuration references SHALL have a canonical coverage contract
The documentation system SHALL define the supported component/configuration
inventory as a generated export of the engine component registry (see
`component-registry-export`) and SHALL detect entries that are implemented but
undocumented, documented but unknown, or missing required reference fields.
Each component reference page SHALL declare the component type-names it
documents in its front matter, and the documentation check SHALL validate
ownership bidirectionally against the generated inventory: an inventory name
with no declaring page is an error; a page declaring a name absent from the
inventory (or under a mismatched kind) is an error; a component page that
declares no component is an error.

#### Scenario: A new plugin is registered
- **WHEN** a supported input, buffer, processor, output, codec, or temporary
  component is added to the implementation and the inventory is regenerated
  but no page declares it
- **THEN** the documentation check reports the missing coverage with the
  component kind and type-name until a reference page declares it

#### Scenario: A stale component page remains
- **WHEN** a reference page declares a component type-name that is not
  present in the generated inventory, or declares it under the wrong kind
- **THEN** the check fails with the page, the declared name, and the expected
  inventory entry, pointing to the owning registry metadata

#### Scenario: A page stops declaring ownership
- **WHEN** a page under the components section carries no `components` front
  matter declaration
- **THEN** the documentation check fails naming the undeclared page

#### Scenario: Paired components share a page
- **WHEN** two registry components (e.g. the Arrow/JSON conversion pair) are
  documented on one page
- **THEN** the page declares both names in its front matter and the
  bidirectional check passes for both

## ADDED Requirements

### Requirement: The generated component table SHALL join inventory and page ownership
The generated component-inventory page SHALL be reproducible from the
generated inventory JSON joined with page front-matter ownership declarations,
including a description column sourced from registry metadata. Its generation
command and markers SHALL remain documented and verified by the documentation
check.

#### Scenario: A description changes in the registry
- **WHEN** a component's registry description changes and the inventory is
  regenerated
- **THEN** the generated table is stale until regenerated, and the
  documentation check reports the precise diff when run with `--check`

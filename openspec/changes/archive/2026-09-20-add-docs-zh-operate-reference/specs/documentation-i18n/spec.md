## ADDED Requirements

### Requirement: The operations and reference domain SHALL be translated
The Simplified Chinese locale SHALL include translations for the `operate/` section (including `operate.md`), the `control-plane/` section, the `reference/` section, the `develop/` section, the `deploy/` page, the `migration/` section, the `about/` section, and the root pages `index.md`, `intro.md`, `start-here.md`, and `contribute.md`, mirrored at the same relative paths under the `zh-Hans` docs tree. Each translated page SHALL satisfy the structural-fidelity contract and the localized-tree linking rules enforced by the `documentation-quality-gates` capability. The localized sidebar SHALL render the Operate, Control Plane, Reference, Develop, Migration, and About category labels in Simplified Chinese. With this requirement satisfied, every non-versioned, non-blog documentation page SHALL have a `zh-Hans` counterpart.

#### Scenario: An operator reads the operations pages
- **WHEN** a visitor opens any `zh-Hans` operate, control-plane, or deploy page
- **THEN** the prose renders in Simplified Chinese while configuration identifiers, paths, and YAML examples are unchanged from the English page

#### Scenario: A developer reads the extension and API reference pages
- **WHEN** a visitor opens any `zh-Hans` develop or reference page
- **THEN** prose renders in Simplified Chinese while code identifiers, signatures, and CLI examples remain verbatim

#### Scenario: Full-site coverage is complete
- **WHEN** the change is implemented
- **THEN** every Markdown page under `docs/docs/` outside `versioned_docs/` and blog has a counterpart at the mirrored path under the zh-Hans docs tree, and `pnpm docs:check` passes

#### Scenario: The localized build succeeds with full coverage
- **WHEN** the documentation site is built with both locales after this change
- **THEN** the build succeeds and every localized route renders Simplified Chinese content or explicit English fallback for never-translated trees

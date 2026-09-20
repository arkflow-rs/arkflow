## ADDED Requirements

### Requirement: The builder guides and SQL reference SHALL be translated
The Simplified Chinese locale SHALL include translations for the `build/` section (including the recipes index and all recipe pages), the `sql/` section and `sql.md` index, the `configuration/` section, the `how-to/` section, the `tutorials/` section, the `cases/` section, and the root pages `streaming-jobs.md` and `build-pipelines.md`, mirrored at the same relative paths under the `zh-Hans` docs tree. Each translated page SHALL satisfy the structural-fidelity contract and the localized-tree linking rules enforced by the `documentation-quality-gates` capability. The localized sidebar SHALL render the Recipes and SQL Reference category labels in Simplified Chinese.

#### Scenario: A Chinese-speaking builder follows a recipe
- **WHEN** a visitor reads any `zh-Hans` recipe, how-to, tutorial, or case page
- **THEN** the walkthrough prose renders in Simplified Chinese while YAML examples and configuration identifiers are unchanged from the English page

#### Scenario: A Chinese-speaking developer looks up a SQL function
- **WHEN** a visitor opens any `zh-Hans` SQL reference page, including the scalar-function reference
- **THEN** function names, signatures, and SQL keywords are verbatim while descriptions are rendered in Simplified Chinese

#### Scenario: Build-domain coverage is complete
- **WHEN** the change is implemented
- **THEN** every page in the listed sections has a counterpart at the mirrored path under the zh-Hans docs tree, and `pnpm docs:check` passes

#### Scenario: English pages linking translated pages use absolute routes
- **WHEN** an English page file-relative-links to a page that this change translates
- **THEN** that link uses its absolute `/docs/...` route so the localized build resolves, and `pnpm docs:check` passes

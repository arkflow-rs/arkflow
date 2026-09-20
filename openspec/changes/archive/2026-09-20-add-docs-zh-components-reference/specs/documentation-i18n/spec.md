## ADDED Requirements

### Requirement: The components reference SHALL be translated
The Simplified Chinese locale SHALL include translations for every page under `docs/docs/components/` (inputs, buffers, processors, outputs, temporary, codecs) and for the `components.md` section index, mirrored at the same relative path under the `zh-Hans` docs tree. Each translated page SHALL satisfy the structural-fidelity contract (front-matter identity, `components:` ownership, byte-identical YAML fences) and the localized-tree linking rules enforced by the `documentation-quality-gates` capability. The localized sidebar SHALL render the components section category labels in Simplified Chinese.

#### Scenario: A Chinese-speaking user reads a component page
- **WHEN** a visitor opens any `zh-Hans` component page (for example the Kafka input)
- **THEN** the prose, headings, and configuration-table descriptions render in Simplified Chinese, while configuration field names, type names, and YAML examples are unchanged from the English page

#### Scenario: Component reference coverage is complete
- **WHEN** the change is implemented
- **THEN** every Markdown page under `docs/docs/components/` and the `components.md` index have a counterpart at the mirrored path under `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/components/`, and `pnpm docs:check` passes

#### Scenario: The localized sidebar labels the components categories
- **WHEN** a visitor browses the docs sidebar under `zh-Hans`
- **THEN** the components section categories (inputs, buffers, processors, outputs, temporary, codecs) are labeled in Simplified Chinese

#### Scenario: The localized build succeeds
- **WHEN** the documentation site is built with both locales
- **THEN** the build succeeds with the translated component pages included in the `zh-Hans` tree

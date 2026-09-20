# Purpose

Define how the ArkFlow documentation site serves Simplified Chinese (`zh-Hans`) as a secondary locale with English fallback for untranslated pages, keep English as the canonical documentation locale with an explicit lag/drift policy, and establish the structural contract that every translated page must preserve relative to its English source (front-matter identity, `components:` ownership, and byte-identical YAML fences).

## Requirements

### Requirement: The site SHALL serve Simplified Chinese as a secondary locale with English fallback
The documentation site SHALL register `zh-Hans` as a secondary locale while keeping `en` as the default locale served at the site root (`/`). Localized routes SHALL live under the `/zh-Hans/` prefix, and any page without a Simplified Chinese translation SHALL render the default-locale (English) content at its localized route rather than producing a missing page.

#### Scenario: A visitor opens an untranslated page under the Chinese locale
- **WHEN** a visitor requests a `zh-Hans` route whose page has no translated file
- **THEN** the English content is rendered at that route with the localized chrome (navbar, footer), and the request does not 404

#### Scenario: The default locale keeps its routes
- **WHEN** the site is built with both locales configured
- **THEN** all existing English routes remain unchanged at the site root and `zh-Hans` content is served only under `/zh-Hans/`

#### Scenario: Locale switcher offers both locales
- **WHEN** a visitor uses the locale switcher on any docs page
- **THEN** both `English` and `简体中文` are offered and switching preserves the current page

### Requirement: Site chrome and the landing page SHALL be localized
The navbar, footer, announcement bar, tagline, and the landing page SHALL render Simplified Chinese when the `zh-Hans` locale is active, without altering the English rendering.

#### Scenario: A visitor switches the locale on the landing page
- **WHEN** a visitor switches the locale to `zh-Hans` while viewing the landing page
- **THEN** the landing page content, navbar, footer, and announcement bar render in Simplified Chinese

#### Scenario: English rendering is unchanged
- **WHEN** the default-locale site is built after the localization changes
- **THEN** the English chrome, landing page, and routes are identical in behavior to the pre-change site

### Requirement: Documentation search SHALL work in both locales
The local search integration SHALL be configured for both English and Chinese tokenization so that each locale build indexes its rendered content and returns results for queries in that locale.

#### Scenario: Searching under the Chinese locale
- **WHEN** a visitor searches in the `zh-Hans` locale using Chinese terms
- **THEN** the search returns hits from `zh-Hans` routes, including translated pages and English fallback pages

#### Scenario: Searching under the default locale
- **WHEN** a visitor searches in the default locale
- **THEN** search behavior covers the English tree as before the change

### Requirement: The first-mile documentation SHALL be translated
The Simplified Chinese locale SHALL include translations for the get-started section, the getting-started section, the concepts section, and the core build-concept pages (architecture, streams, and jobs). Pages outside this first-mile set MAY remain untranslated and fall back to English.

#### Scenario: A new Chinese-speaking user follows the quickstart
- **WHEN** a visitor reads the install and quickstart pages under `zh-Hans`
- **THEN** the full walkthrough, including explanatory prose, renders in Simplified Chinese

#### Scenario: First-mile coverage is complete
- **WHEN** the change is implemented
- **THEN** every page in the get-started, getting-started, and concepts sections and the architecture, streams, and jobs build pages exists under the `zh-Hans` docs tree

### Requirement: English SHALL remain the canonical documentation locale
English SHALL remain the single source of truth for documentation content. Translations MAY lag behind English edits; content staleness SHALL NOT fail any gate. Versioned documentation trees and blog posts SHALL NOT be translated. The policy SHALL be documented in the documentation contributor guide (`docs/DOCUMENTATION.md`).

#### Scenario: A pull request edits only an English page that has a translation
- **WHEN** a contributor updates an English page without updating its `zh-Hans` counterpart
- **THEN** the documentation gate passes; the translation is allowed to lag

#### Scenario: The drift policy is discoverable
- **WHEN** a contributor opens the documentation contributor guide
- **THEN** it states that English is canonical, which parts of the site are never translated, and how translations are expected to track English changes

### Requirement: Translated pages SHALL preserve structural fidelity with their English source
A translated documentation page SHALL keep the same front-matter identity (`id`, `slug`, `sidebar_position`, `sidebar_label` when present) and the same `components:` ownership list as its English counterpart, and SHALL NOT alter YAML code fences (neither the fence metastring carrying the validation classification nor the fence content). Title and description front matter are the translation surface and SHOULD be localized. Enforcement of this contract is defined by the `documentation-quality-gates` capability.

#### Scenario: A translator localizes only prose
- **WHEN** a translated page differs from its English source only in translated title, description, and prose content
- **THEN** the page is structurally valid and passes validation

#### Scenario: A translator edits a YAML example inside a translated page
- **WHEN** a translated page modifies a YAML code fence relative to its English source
- **THEN** the change is invalid and must be rejected in review and by the validation gate

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

### Requirement: Translation freshness SHALL be reportable on demand
The documentation tooling SHALL provide a report command that compares every translated `zh-Hans` page with its English source by the last git commit that touched each file, listing stale-candidate translations (English source committed more recently than its translation) and coverage statistics (translated pages vs total translatable pages, excluding versioned trees and blog posts per policy). The report SHALL be informational only: it SHALL NOT fail regardless of staleness findings, preserving the drift policy that content staleness never fails a gate.

#### Scenario: A maintainer asks which translations lag
- **WHEN** the report command runs after an English page is edited without updating its translation
- **THEN** that page's translation is listed as a stale candidate together with coverage statistics, and the command exits 0

#### Scenario: Staleness never fails the build
- **WHEN** the repository contains any number of stale translations
- **THEN** the report command, `pnpm docs:check`, and CI all pass; only the report output names the stale candidates

#### Scenario: Never-translated trees are excluded
- **WHEN** the report computes coverage statistics
- **THEN** versioned documentation trees and blog posts are excluded from both the numerator and the denominator

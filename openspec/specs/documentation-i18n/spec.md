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

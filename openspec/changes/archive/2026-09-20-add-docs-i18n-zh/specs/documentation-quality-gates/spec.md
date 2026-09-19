## ADDED Requirements

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

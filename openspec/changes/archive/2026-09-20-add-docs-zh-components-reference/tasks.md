## 1. Sidebar labels and section index

- [x] 1.1 Add components category labels (Inputs, Buffers, Processors, Outputs, Temporary, Codecs) to `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current.json`
- [x] 1.2 Translate `docs/docs/components.md` to `current/components.md` (index prose + card descriptions; `components:` front matter byte-identical)

## 2. Buffers (4 pages)

- [x] 2.1 Translate `components/1-buffers/*.md` (4 pages) into the zh tree; `pnpm docs:check` passes

## 3. Processors (6 pages)

- [x] 3.1 Translate `components/2-processors/*.md` (6 pages); `pnpm docs:check` passes

## 4. Inputs (13 pages)

- [x] 4.1 Translate `components/0-inputs/*.md` (13 pages); `pnpm docs:check` passes

## 5. Outputs (11 pages)

- [x] 5.1 Translate `components/3-outputs/*.md` (11 pages); `pnpm docs:check` passes

## 6. Temporary and codecs (5 pages)

- [x] 6.1 Translate `components/4-temporary/*.md` (1 page) and `components/5-codecs/*.md` (4 pages); `pnpm docs:check` passes

## 7. Final validation

- [x] 7.1 Coverage check: every `.md` under `docs/docs/components/` has a mirrored zh counterpart (40 files)
- [x] 7.2 `pnpm docs:check` and `npm run build` (full localized Docusaurus build) both pass

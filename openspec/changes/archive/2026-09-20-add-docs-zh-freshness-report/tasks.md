## 1. Report script

- [x] 1.1 Create `docs/scripts/i18n-freshness-report.mjs`: per translated page, compare git last-commit timestamps vs English source; print stale candidates + coverage stats; always exit 0; `--verbose` lists fresh pairs too
- [x] 1.2 Add `docs:i18n-report` script entry to `docs/package.json`

## 2. Documentation

- [x] 2.1 Add a freshness-report note to the i18n section of `docs/DOCUMENTATION.md` (command, what it reports, report-only policy)

## 3. Validation

- [x] 3.1 Run `pnpm docs:i18n-report` — output lists stale candidates (if any) and 128-page coverage; exits 0
- [x] 3.2 `pnpm docs:check` still passes; no other scripts affected

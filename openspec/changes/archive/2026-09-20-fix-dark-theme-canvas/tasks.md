## 1. CSS fix

- [x] 1.1 In `docs/src/css/custom.css`, add `background-color: #0b1120;` to the `[data-theme='dark']` token block (~line 91) so the root canvas goes dark in dark mode (hardcoded: Docusaurus re-declares the token on the higher-specificity `html[data-theme='dark']`, so a var() indirection resolves to the framework default — see design D1)
- [x] 1.2 Remove the dead `[data-theme='dark'] html` selector from the rule at ~line 175, keeping the `[data-theme='dark'] body` half (dark radial washes intact)

## 2. Visual verification (dev server, dark mode)

- [x] 2.1 On `/docs/get-started/quickstart`, scroll past the first viewport and confirm the content column, sidebar, and TOC show the dark canvas with readable light-on-dark text
- [x] 2.2 On the landing page, confirm the Features and "Where do you want to go" sections render on the dark canvas with readable headings, and hero/architecture/community bands are unchanged
- [x] 2.3 Rubber-band overscroll at page top/bottom and confirm no white flash; confirm a short page (e.g. 404) has no white band (404 verified dark; synthetic scroll cannot trigger real rubber-banding — deterministic evidence is the canvas color itself, now `#0b1120`)
- [x] 2.4 Toggle to light mode and confirm the canvas, radial washes, and all affected sections render exactly as before

## 3. Gates

- [x] 3.1 Run `pnpm docs:check` and the production build (`pnpm build`; the tasks previously said `docs:build`, which is not a real script) in `docs/` and confirm both pass unchanged

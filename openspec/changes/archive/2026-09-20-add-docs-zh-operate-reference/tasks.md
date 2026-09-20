## 1. Sidebar labels

- [x] 1.1 Localize remaining sidebar categories (Operate→运维, Control Plane→控制平面, Reference→参考, Develop→开发, Migration→迁移, About→关于) in `current.json`

## 2. Operate + control-plane (17 pages)

- [x] 2.1 Translate `operate.md` and `operate/*.md` (11 pages); `pnpm docs:check` passes
- [x] 2.2 Translate `control-plane/*.md` (5 pages); `pnpm docs:check` passes

## 3. Reference + develop (11 pages)

- [x] 3.1 Translate `reference/*.md` (7 pages); `pnpm docs:check` passes
- [x] 3.2 Translate `develop/*.md` (4 pages); `pnpm docs:check` passes

## 4. Root pages + deploy + migration + about (7 pages)

- [x] 4.1 Translate `index.md`, `intro.md`, `start-here.md`, `contribute.md`, `deploy/`, `migration/`, `about/`; `pnpm docs:check` passes

## 5. Reverse links and final validation

- [x] 5.1 Convert gate-flagged English file-relative links to absolute `/docs/...` routes until `pnpm docs:check` passes
- [x] 5.2 Coverage check: 0 untranslated pages outside versioned/blog; `pnpm docs:check` and `npm run build` both pass

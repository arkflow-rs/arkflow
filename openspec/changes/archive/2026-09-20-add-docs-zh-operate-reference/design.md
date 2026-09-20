## Context

Third coverage change of the zh-Hans rollout, after components (`add-docs-zh-components-reference`) and build/SQL guides (`add-docs-zh-build-sql-guides`). Same translation contract: page-for-page mirroring under `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/`, prose-only surface, `yaml` fences byte-identical, locale-aware link rules, `pnpm docs:check` plus one full localized `npm run build` as gates. 35 pages, ~2.7k lines.

## Goals / Non-Goals

- **Goal**: zh-Hans coverage for every remaining untranslated page (35), completing full-site coverage; remaining sidebar category labels localized.
- **Non-Goal**: versioned docs/blog (never translated); freshness tooling (follow-up proposal); English content changes beyond gate-flagged link-route conversions.

## Decisions

### D1: Three translation batches by domain
operate + control-plane (17 pages) → reference + develop (11) → root pages + deploy + migration + about (7). Gate after each batch.
*Alternative*: single pass — rejected: failures harder to attribute.

### D2: Glossary continuity with shipped pages
Reuse established terms: 控制平面(control plane), 作业(job)/分布式作业(distributed job), 部署(deploy), 可观测性(observability), 迁移(migration), 内核(kernel), 插件(plugin), 回滚(rollback), 灰度发布(rollout), 健康检查(health check). New domain terms (Kubernetes, Hub/Agent, Hub–Agent) keep English proper nouns; `develop/` content keeps Rust identifiers in code spans verbatim.

### D3: `index.md`/`intro.md` are user-facing landing prose
The docs landing pages are pure prose — translated fully; any absolute `/docs/...` links become `/zh-Hans/docs/...`, file-relative links into translated trees stay relative.

### D4: Reverse-link conversion mechanics unchanged
After each batch, `pnpm docs:check`; flagged English file-relative links into translated pages become absolute `/docs/...` routes with number prefixes stripped (route names verified against `docs/sidebars.ts` and prior build conventions).

## Risks / Trade-offs

- [control-plane pages reference fast-moving API surface] → prose-only translation; identifiers/paths verbatim; staleness accepted per policy and covered by the follow-up freshness report.
- [Landing-page tone differs from reference tone] → translators follow `build/architecture.md` punctuation/tone reference; landing copy kept marketing-neutral.

## Migration Plan

Additive-only plus mechanical link conversions; rollback = delete added files, revert link edits.

## Open Questions

(none)

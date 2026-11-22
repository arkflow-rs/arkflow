## Context

Three documentation surfaces state facts about the workspace without any machine check, and all three have drifted (evidence in the proposal). The docs site, by contrast, stays truthful through structural gates: registry snapshot tests (`docs_inventory_snapshot`), YAML-snippet validation through the real engine parser (`docs_snippets_validate.rs`), and `pnpm docs:check`. The gap is specific: the agent instruction entry (`CLAUDE.md`), the plugin guide's Rust snippets, and the Helm chart's `appVersion` have no equivalent gate.

Verified current state (2026-11-22):

- `CLAUDE.md` is a 700+ line hand-maintained file claiming Rust 1.88 (actual: 1.97), "three crates" (actual: 4, `arkflow-server` missing from its list), 12 inputs (actual: 13), and window `buffer:` YAML examples that predate the unified kernel's window compilation contract. `AGENTS.md` — maintained in-repo, current, and read by ZCode/other agents — already covers the same ground correctly.
- `docs/docs/develop/plugins.md` has 3 Rust fences (lines 21-43, 54-68, 78-86). Snippet 2 shows `Option<&String>`; the real `OutputBuilder::build` takes `Option<&str>` (`crates/arkflow-core/src/output/mod.rs:102`). The wiring text points to `src/mod.rs`; the crate root is `lib.rs` (`initialize()` at lib.rs:108 → kind `init()`s in `output/mod.rs` etc.). The snippets are otherwise faithful to `crates/arkflow-plugin/src/output/drop.rs`.
- `deploy/charts/arkflow/Chart.yaml` `appVersion: "0.5.0"` vs workspace `0.6.0`. The `kubernetes-helm-chart` spec already requires `appVersion` to track the engine release tag; nothing enforces it.

## Goals / Non-Goals

**Goals:**

- One authoritative agent instruction file; the other entry point defers to it and cannot drift.
- Plugin guide Rust snippets compile against the real API in CI, same philosophy as the YAML-snippet gate.
- Helm `appVersion` equals the workspace version at release time, enforced by the release pipeline.
- Spec coverage for all three so regressions are test failures.

**Non-Goals:**

- No rewrite or expansion of `AGENTS.md` content (it is current; its own maintenance is a separate concern).
- No feature gating / build-surface work (that is `add-cargo-feature-gating`, PA-P1-01).
- No docs-site content overhaul beyond `plugins.md`; the zh-Hans tree has no plugin-guide Rust snippets to sync (checked: `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/develop/plugins.md` mirrors the page and gets the same two-line drift fix, but no new mechanism).
- No changes to engine, plugin, or server runtime code.

## Decisions

### D1: CLAUDE.md becomes a pointer to AGENTS.md (not a regenerated second copy)

**Chosen**: replace `CLAUDE.md` content with a short file that directs agents to `AGENTS.md` (Claude Code resolves `@AGENTS.md` imports and follows links; the file keeps a minimal header + import reference).

**Alternative rejected**: regenerate `CLAUDE.md` from `AGENTS.md` + code. It was already regenerated at least once and drifted again immediately (Rust 1.88 → 1.97 happened after a regeneration). Two copies of facts that change on every release and every component addition is a structural drift generator; the spec therefore forbids restating facts in the entry file at all.

**Consequence**: all `CLAUDE.md`-only content that AGENTS.md lacks (e.g. the stale windowing YAML examples) is deleted, not migrated — it is either wrong (window examples) or already covered (component lists live in the generated inventory).

### D2: Plugin guide snippets are enforced via `#[doc = include_str!(...)]` doctests

**Chosen**: add a test-only file in `crates/arkflow-plugin` (e.g. `src/plugin_guide_doctest.rs`, `#[cfg(test)]`-gated module or a `tests/` integration test) whose doc comment is `#[doc = include_str!("../../../docs/docs/develop/plugins.md")]`; `cargo test --doc -p arkflow-plugin` then compiles every `rust` fence as a doctest. The three snippets are edited to be self-contained (inline the needed `use` items — mirroring `drop.rs:20-23`), with fence metadata `rust,no_run` (they define types; nothing to run) and `rust,ignore` only for fences that reference symbols outside the crate's public API (e.g. `ComponentMetadata::unit` in context). The two drifts are fixed as part of making them compile: `Option<&String>` → `Option<&str>`, `src/mod.rs` → `lib.rs` / `output/mod.rs`.

**Alternative rejected**: (a) `rustc`-spawning extraction test — heavier, reinvents the doctest harness for no gain; (b) string-containment checks ("guide must mention `Option<&str>`") — pins text without proving compilability; (c) marking snippets `ignore` — enforces nothing. The doctest route reuses the standard toolchain and fails on exactly the drift class that occurred.

**Risk**: doctest fences require self-contained snippets; if a future guide snippet legitimately needs unavailable context it gets `ignore` + an explanatory comment — the spec's compile requirement then applies to fences that claim to be real code, which is the honest boundary.

### D3: appVersion gate lives in the release workflow, not helm.yml

**Chosen**: `.github/workflows/release.yml` gains a step that compares `Chart.yaml` `appVersion` (trimmed of quotes) with the root `Cargo.toml` `version` and fails naming both values before any publish step runs. `Chart.yaml` is bumped to `"0.6.0"` in this change.

**Alternative rejected**: enforcing equality on every PR (helm.yml) — between releases the two values legitimately diverge only if a release is in progress; but the workspace version moves on `main` as soon as a release is cut, while `appVersion` moves with it in the same commit under this rule, so equality-on-main is actually maintainable; still, the release pipeline is the point where a stale chart becomes a shipped artifact, so the gate belongs there. PR-time drift remains visible in review.

### D4: zh-Hans mirror gets the same drift fix, no separate mechanism

The zh-Hans docs tree mirrors `plugins.md`; the two factual fixes (`Option<&str>`, wiring path) are applied there in lockstep per the repo's EN+zh 锁步 rule. The doctest mechanism only embeds the English file (the zh page links to the same code; the spec requirement targets the guide's canonical file).

## Risks / Trade-offs

- [Claude Code users lose CLAUDE.md-specific content] → The pointer keeps a visible header explaining where guidance lives; all current content is either duplicated from AGENTS.md or stale. Net information loss is negative (stale facts removed).
- [Doctest harness couples plugin crate to a docs/ file path] → `include_str!` with a relative path breaks loudly at compile time if the guide moves; the break itself is the gate working.
- [Snippets may rot differently in zh-Hans (not doctest-embedded)] → Accepted: the zh page carries the same two fixes now; long-term zh drift is a known docs-site-wide property, tracked by the i18n parity checks where they exist.
- [release.yml edit untested locally] → The gate is a two-line shell comparison; validated by rendering the workflow YAML and by a dry-run of the comparison snippet in the implementation PR description.

## Migration Plan

Single PR, no deployment ordering: docs + chart + workflow land together. Rollback is reverting the PR; nothing persists state. The CLAUDE.md pointer takes effect for agents on next read; no cache concerns beyond editor sessions.

## Open Questions

None — the registry entry's three action items all have verified, decided implementations.

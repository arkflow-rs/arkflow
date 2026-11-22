## 1. CLAUDE.md → AGENTS.md pointer

- [x] 1.1 Rewrite `CLAUDE.md` to a short pointer file: keep a one-paragraph header ("agent guidance lives in AGENTS.md — the checked-in authority"), add the `@AGENTS.md` import reference, delete all restated facts (crate list, Rust version, component counts, commands, architecture, windowing YAML examples)
- [x] 1.2 Verify no CI/docs gate references `CLAUDE.md` content expectations (grep workflows + `docs/scripts/`); adjust none found

## 2. Plugin guide snippets: fix drift, make self-contained, enforce compilation

- [x] 2.1 Fix the two drifts in `docs/docs/develop/plugins.md`: builder name parameter to `Option<&str>` (§2 snippet), wiring path to the real layout (`crates/arkflow-plugin/src/lib.rs` `initialize()` → kind `init()` in `crates/arkflow-plugin/src/output/mod.rs`)
- [x] 2.2 Make all three Rust fences self-contained: inline the `use` items from `crates/arkflow-plugin/src/output/drop.rs:20-23` into each snippet; set fence metadata (`rust,no_run` for type definitions; `ignore` + explanatory comment only where a fence genuinely references non-public context)
- [x] 2.3 Apply the same two drift fixes to the zh-Hans mirror `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/develop/plugins.md` (lines 51, 79), keeping surrounding prose in zh
- [x] 2.4 Add the doctest harness: a `#[cfg(doctest)]` marker struct in `crates/arkflow-plugin/src/lib.rs` whose doc comment is `#[doc = include_str!("../../../docs/docs/develop/plugins.md")]` (`cfg(doctest)`, not `cfg(test)` — doctest builds don't set `cfg(test)`)
- [x] 2.5 Run `cargo test --doc -p arkflow-plugin` until green; confirm the harness fails when a fence is deliberately broken (mutation check, then revert)

## 3. Helm appVersion truth

- [x] 3.1 Bump `deploy/charts/arkflow/Chart.yaml` `appVersion` from `"0.5.0"` to `"0.6.0"`
- [x] 3.2 Add the version-consistency gate to `.github/workflows/helm.yml`'s `publish` job (the only chart-publishing workflow): a step before `helm package`/`helm push` that compares `appVersion` (strip quotes) with both the root `Cargo.toml` workspace version and the `v`-stripped tag, failing with all three values on mismatch
- [x] 3.3 Validate the gate logic locally: extract the comparison into a shell snippet, run it against current files (must pass at 0.6.0/0.6.0 and fail with a temporary mismatch), include the transcript in the PR description

## 4. Gates and cross-checks

- [ ] 4.1 `cargo test --workspace --all-targets` green (the doctest harness rides the existing surface)
- [ ] 4.2 `cargo clippy --workspace --all-targets` and `cargo fmt --all -- --check` green
- [ ] 4.3 `cd docs && pnpm docs:check` green (plugins.md front matter/ownership unchanged; zh mirror in lockstep)
- [ ] 4.4 Confirm spec deltas match implementation: `documentation-accuracy` scenarios (pointer file, compilable snippets) and `kubernetes-helm-chart` appVersion scenario each map to a landed artifact
- [ ] 4.5 Update `PRODUCT-ITERATION-2026-11-22.md` §4 iteration-1 table marking the PA-P1-02 row done when the change archives (leave for archive PR if preferred)

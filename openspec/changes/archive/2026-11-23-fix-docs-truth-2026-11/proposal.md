## Why

The agent instruction file and the plugin guide contradict the code they describe, so both humans and AI agents are systematically misled at the entry points they trust most:

- `CLAUDE.md:110` claims "Rust 1.88 or later"; `Cargo.toml` requires `rust-version = "1.97"`.
- `CLAUDE.md:114` claims "three crates"; the workspace has four (`arkflow`, `arkflow-core`, `arkflow-plugin`, `arkflow-server`), and the crate list omits `arkflow-server` entirely.
- `CLAUDE.md:124` lists 12 input plugins; the registry (`docs/reference/component-inventory.json`) has 13 inputs, 14 outputs, 14 processors.
- `CLAUDE.md` §"Windowing Strategies" (line 676+) shows `buffer:` YAML with `type: "tumbling_window"`/`"sliding_window"` configs that no longer match the window compilation contract.
- `docs/docs/develop/plugins.md:60` shows the builder parameter as `Option<&String>`; the real trait is `name: Option<&str>` (`crates/arkflow-core/src/output/mod.rs:102`).
- `docs/docs/develop/plugins.md:91` says to wire `init()` in `crates/arkflow-plugin/src/mod.rs`; the crate root is `lib.rs` (`initialize()` at lib.rs:108), and kind `init()`s live in e.g. `output/mod.rs`.
- `deploy/charts/arkflow/Chart.yaml:8` pins `appVersion: "0.5.0"` while the workspace version is `0.6.0` — a live violation of the `kubernetes-helm-chart` requirement that "appVersion track the engine release tag", which no CI gate catches.

The component counts and window examples in `CLAUDE.md` are exactly the claims the machine-checked docs site already keeps truthful through `pnpm docs:check` and registry snapshot tests; the agent file never got the same treatment, and the plugin guide's Rust snippets have no compilation check at all (unlike the YAML snippets, which `docs_snippets_validate.rs` validates through the real parser).

## What Changes

- Replace the stale hand-maintained `CLAUDE.md` with a thin pointer to `AGENTS.md`, which is already the checked-in, CI-adjacent authority for agent instructions (crate layout, commands, architecture rules, gotchas — all current). This removes the dual-maintenance drift class instead of re-syncing a second copy that has already drifted.
- Rewrite `docs/docs/develop/plugins.md` Rust snippets to be self-contained and compilable, fix the two known drifts (`Option<&String>` → `Option<&str>`, `src/mod.rs` → the real wiring path), and enforce compilability with a doctest harness (`#[doc = include_str!(...)]` exercised by `cargo test --doc -p arkflow-plugin`), mirroring how YAML snippets are already validated through the real parser.
- Fix `deploy/charts/arkflow/Chart.yaml` `appVersion` to the workspace version and add a release-workflow gate that fails when `appVersion` and the workspace version diverge, turning the existing `kubernetes-helm-chart` prose requirement into an enforced check.
- Extend the `documentation-accuracy` spec with requirements covering the agent instruction file and compilable Rust guide snippets, so the next drift is a test failure instead of a discovery.

## Capabilities

### New Capabilities

<!-- none — all work lands under existing capabilities -->

### Modified Capabilities

- `documentation-accuracy`: add requirements that the agent instruction file (`CLAUDE.md`/`AGENTS.md` pair) states only verifiable facts about the workspace (crate list, toolchain floor, component surface) and that Rust snippets in the plugin guide compile through the real trait signatures.
- `kubernetes-helm-chart`: sharpen the existing "Chart publication and CI gates" requirement with a scenario enforcing `appVersion` ↔ workspace version consistency at release time.

## Impact

- **Files**: `CLAUDE.md` (rewritten to a pointer), `docs/docs/develop/plugins.md` (snippets + wiring path), `crates/arkflow-plugin` (new doctest harness file), `deploy/charts/arkflow/Chart.yaml` (appVersion), `.github/workflows/release.yml` (version-consistency gate), `openspec/specs/{documentation-accuracy,kubernetes-helm-chart}` (deltas).
- **No runtime code changes**: no engine, plugin, or server behavior is affected; the only Rust addition is a test-only doctest harness.
- **CI**: `cargo test --doc -p arkflow-plugin` joins the existing test surface (already covered by `cargo test --workspace --all-targets`); one new step in the release workflow.
- **Consumers**: AI coding agents (Claude Code reads `CLAUDE.md`; ZCode/other agents read `AGENTS.md`) get one authoritative instruction file; Helm users get a chart whose `appVersion` matches the images it deploys.

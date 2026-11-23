## ADDED Requirements

### Requirement: Agent instruction entry defers to the single authority

The agent instruction entry point (`CLAUDE.md`) SHALL NOT restate workspace facts (crate list, toolchain floor, component surface, commands, architecture rules) that `AGENTS.md` already carries; it SHALL defer to `AGENTS.md` as the single authoritative instruction file so the two cannot drift apart. Any agent-facing instruction file that states a workspace fact SHALL state only facts that match the workspace (`Cargo.toml` members, `rust-version`, the generated component inventory).

#### Scenario: Entry point carries no duplicated facts

- **WHEN** an agent reads `CLAUDE.md` at the repository root
- **THEN** it is directed to `AGENTS.md` for all guidance and the file restates no crate list, version, component counts, commands, or architecture rules of its own

#### Scenario: No stale toolchain or crate claims

- **WHEN** the workspace `rust-version` or the set of workspace members changes
- **THEN** no agent instruction file contradicts the new value, because neither file independently restates those facts

### Requirement: Plugin guide Rust snippets compile against real signatures

Rust code fences in the plugin development guide (`docs/docs/develop/plugins.md`) SHALL be exercised by the workspace test suite as doctests compiled against the crate's real public API, so that a trait-signature or wiring-path change that invalidates a snippet fails `cargo test` instead of shipping silent drift.

#### Scenario: Builder signature matches the real trait

- **WHEN** the guide shows a builder `build` implementation
- **THEN** the parameter types match the current trait definitions (e.g. the name parameter is `Option<&str>`), and any mismatch fails the doctest run

#### Scenario: Wiring path names a real file

- **WHEN** the guide names the file where a component's `init()` is wired
- **THEN** the named path exists in the repository (the plugin crate root is `lib.rs`; kind `init()`s live in e.g. `crates/arkflow-plugin/src/output/mod.rs`)

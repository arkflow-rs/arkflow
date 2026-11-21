# ci-hygiene-gates Specification

## Purpose
固化 CI 卫生门禁为可验证契约：cargo CI 命令一律 `--locked`（锁文件不得被 CI 静默改写）；clippy 纪律随 `[workspace.lints]` 清单走并经 `[lints] workspace = true` 继承；成员 crate 依赖 `rand` SHALL 经 `[workspace.dependencies]` 声明（锁卫生测试强制，rand 三代传递共存如实记录为不可收敛豁免）。源自 2026-11 全量代码审查（`CODE-REVIEW-2026-11-15.md`）批次 7（CI 卫生）。
## Requirements
### Requirement: CI cargo commands SHALL run with a consistent lockfile

Every CI workflow invocation of cargo build/clippy/test/coverage/benchmark SHALL pass `--locked`, so the lockfile can never be silently rewritten by a CI run: a manifest change without a matching `Cargo.lock` update fails the run instead of resolving fresh versions on the runner.

#### Scenario: Manifest change without lockfile update fails CI

- **WHEN** a pull request modifies a workspace `Cargo.toml` without committing the corresponding `Cargo.lock` update
- **THEN** the CI cargo command exits with a lockfile-consistency error instead of resolving new versions

#### Scenario: Dependabot lockfile updates stay green

- **WHEN** a dependency-refresh pull request updates both the manifest and `Cargo.lock`
- **THEN** the `--locked` cargo commands pass unchanged

### Requirement: Clippy discipline SHALL live in the workspace manifest

The clippy baseline SHALL be declared in the root `[workspace.lints]` and inherited by every workspace member via `[lints] workspace = true`; CI SHALL still deny warnings. Only lints that are clean across the entire workspace (including tests) SHALL be listed, so enabling them never breaks a build.

#### Scenario: Local clippy run enforces the same baseline as CI

- **WHEN** a contributor runs `cargo clippy --workspace --all-targets` locally without the CI `-D warnings` suffix
- **THEN** the lints declared in `[workspace.lints]` still fire, because every member crate inherits the workspace lints table

#### Scenario: Noisy lints are not enabled

- **WHEN** a lint candidate would flag existing workspace code (e.g. `unwrap_used` against thousands of test-module unwraps)
- **THEN** it MUST NOT be added to `[workspace.lints]` until the violations are cleaned up — the table stays buildable at all times

### Requirement: Member crates SHALL declare rand only through the workspace table

A workspace member's `[dependencies]`/`[dev-dependencies]` SHALL reference `rand` as `rand = { workspace = true }`, never with a direct version requirement. The lockfile MAY resolve multiple rand major versions when transitive dependencies pin incompatible ranges (as of 2026-11: `jsonwebtoken`/`nkeys`/`rsa` pin ^0.8, `arrow-avro` via datafusion pins ^0.9, `apache-avro`/`ballista`/`async-nats` pin ^0.10) — this coexistence is documented in the lock-hygiene test and MUST NOT be asserted away as a single-version failure.

#### Scenario: Direct rand declaration fails the lock-hygiene test

- **WHEN** a workspace member declares `rand = "0.10"` (or any direct version) outside `[workspace.dependencies]`
- **THEN** the `dependency_lock_hygiene` convention test fails, naming the offending manifest and line

#### Scenario: Transitive multi-major coexistence is accepted

- **WHEN** `Cargo.lock` resolves rand 0.8, 0.9 and 0.10 solely due to transitive dependency ranges
- **THEN** the single-version lock-hygiene assertions (arrow/datafusion/zstd) still pass and the convention test documents the pinned transitive dependents rather than failing

#### Scenario: Workspace-table declaration passes

- **WHEN** every member declares `rand = { workspace = true }` and version selection happens only in `[workspace.dependencies]`
- **THEN** the convention test passes


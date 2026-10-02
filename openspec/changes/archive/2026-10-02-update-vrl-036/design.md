## Context

The VRL processor (`crates/arkflow-plugin/src/processor/vrl.rs`) compiles operator-provided VRL source at processor build time via `vrl::prelude::*` / `compiler::Program`, executes per-row against `VrlValue`, and converts results back to Arrow. It is the only consumer of the `vrl` crate in the workspace. The `vrl-processor` spec (6 requirements) codifies the behavioral contract: string round-trip typing, runtime-error observability, full timestamp-unit coverage, and loud failure on unsupported result shapes. Stage 4 of the dependency track (#1290–#1293 merged): every stage lands behind a CI-green PR with the test suite as the regression net; the redis stage proved the value of that discipline when its E2E caught a real regression (RESP3).

## Goals / Non-Goals

**Goals:**
- vrl 0.30 → 0.36 with zero behavioral change, proven by the existing `vrl-processor` spec requirements and processor tests.
- Surface any upstream stdlib removals as the same operator-facing compile diagnostics the processor already produces (source-compilation failure is an actionable config error, not a panic).

**Non-Goals:**
- No new VRL features, config fields, or stdlib shims for functions removed upstream.
- No upgrade of the DataFusion/arrow ecosystem (that is the Stage-5+ train, blocked on ballista/table-providers releases).
- No change to the processor's position in the execution kernel or its error taxonomy.

## Decisions

**Single-version jump (0.30 → 0.36), not stepwise.** Intermediate versions offer no CI checkpoints we would keep; the processor's usage surface is narrow (`prelude`, `Program`, `VrlValue` conversions), so one compile-driven migration is cheaper than six. Alternative — stepping through each minor — rejected: six full workspace test cycles for a single-file consumer.

**Compiler-driven migration with the spec as acceptance gate.** Migrate until `cargo clippy --workspace --all-targets` is clean, then require the full `vrl-processor` requirement scenarios and processor unit tests to pass unchanged. If a stdlib function the tests rely on was removed upstream, the test fails and the migration adjusts the *test's* expression to an equivalent surviving function — but never the asserted behavior. Alternative — reading 6 changelogs up front — rejected: the compiler enumerates the actual breakage set faster and without omission.

**Keep feature set `["value", "compiler", "stdlib"]` unless 0.36 renamed a feature.** Renames surface immediately as cargo resolution errors; a rename gets a one-line manifest change, not a redesign.

**Behavioral drift watch: `Value::Bytes` → Arrow typing.** The string-round-trip requirement is the most likely place for silent upstream drift (Bytes/Utf8 handling changed repeatedly across VRL releases). If the processor tests for that requirement fail after migration, the processor adapts to preserve the *specified* behavior, not the new upstream default.

## Risks / Trade-offs

- [Stdlib function removed upstream breaks operator programs] → Accepted and surfaced: source compilation already fails with the VRL diagnostic returned to the operator; documented in the delta spec as the intended behavior.
- [Silent semantic drift in a stdlib function used by tests] → Mitigated by the spec-scenario tests asserting exact outputs (e.g., string round-trip typing), plus compile-time `VrlValue` type checks.
- [vrl 0.36 pulls a newer `chrono`/`bytes`/`regex` line that splits with the rest of the workspace] → Mitigated by the lock hygiene guard test pattern from Stage 0: after resolution, confirm no new duplicate major stacks (`arrow`/`datafusion`/`zstd` guarded already; extend only if the lock diff shows a meaningful new dupe).
- [Release profile build time] → vrl is proc-macro heavy; a new line may recompile a chunk of the tree. Acceptable: debug iteration only, CI caches already repaired (#1288).

## Migration Plan

1. Manifest bump + `cargo update -p vrl`; resolve feature renames if any.
2. Compile-driven fix loop in `processor/vrl.rs` until workspace clippy is clean.
3. Full processor test suite + `vrl-processor` spec scenarios green; lock hygiene guard green.
4. One PR; squash-merge on CI green, then archive this change.

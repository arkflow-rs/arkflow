# Tasks: repair-event-time-review-defects

## 1. Aggregate emission correctness

- [x] 1.1 In `fire_ready` (`crates/arkflow-core/src/executor/window.rs`): remove and skip zero-observation non-legacy buffers before the emitted/journal bookkeeping.
- [x] 1.2 Add `NumericKind::wider` and derive the fired batch's output kind as the widest kind across fired buffers instead of the first buffer's kind.

## 2. Admission guard

- [x] 2.1 In `accumulate`: read the operator's frontier once per batch; skip unmarked memberships whose `window_end <= frontier` when no buffer exists, excluding sessions and legacy payloads.

## 3. Regression tests

- [x] 3.1 `null_values_never_fabricate_or_truncate_aggregates`: NULL value row + float rows fire one row for the float key with Float64 sum 2.5 and no phantom row.
- [x] 3.2 `released_row_never_reopens_a_fired_and_cleaned_window`: after fire+cleanup, an unmarked row for the closed window is skipped and the window is never re-emitted.

## 4. Spec deltas (drafted in this change; verify at archive)

- [x] 4.1 `openspec validate repair-event-time-review-defects --strict --no-interactive` passes.
- [x] 4.2 Each delta's `### Requirement:` header matches its master spec verbatim.

## 5. Final verification

- [x] 5.1 `cargo fmt --all -- --check` and `git diff --check` are clean.
- [x] 5.2 `cargo test -p arkflow-core --lib` passes (352 tests).
- [x] 5.3 `cargo clippy -p arkflow-core --all-targets` reports no new warnings in touched files.

# Tasks: repair-kafka-reconnect-review

## 1. Reconnect resume

- [x] 1.1 In `KafkaInput::connect` (`crates/arkflow-plugin/src/input/kafka.rs`): build the explicit-partition assignment with `merged_restore_assignment` over `frontier.contiguous_positions()` instead of offset-less `add_partition`.

## 2. Regression tests

- [x] 2.1 `reconnect_assignment_uses_the_acknowledged_frontier`: seeded frontier + `connect()` → the assignment for the configured partition carries `Offset::Offset(42)`.
- [x] 2.2 `first_connect_keeps_configured_start_semantics`: empty frontier + `start_from_latest` → the assignment carries `Offset::End`.

## 3. Spec deltas (drafted in this change; verify at archive)

- [x] 3.1 `openspec validate repair-kafka-reconnect-review --strict --no-interactive` passes.
- [x] 3.2 Each delta's `### Requirement:` header matches its master spec verbatim.

## 4. Final verification

- [x] 4.1 `cargo fmt --all -- --check` and `git diff --check` are clean.
- [x] 4.2 `cargo test -p arkflow-plugin --lib` passes.
- [x] 4.3 `cargo clippy -p arkflow-plugin --all-targets` reports no new warnings in touched files.

## 1. Durable input and source acknowledgement

- [x] 1.1 Make `WalInput::read()` flush a newly appended entry before returning it, and add a crash/reopen regression for group-commit and periodic policies.
- [x] 1.2 Reconcile checkpoint-covered WAL entries into the durable cursor/frontier before replay filtering, preserving contiguous acknowledgement behavior for the first newly read entry.
- [x] 1.3 Advance the WAL cursor before invoking the wrapped source acknowledgement, with idempotent retry handling when either side fails.
- [x] 1.4 Make Kafka receive errors classify retryable broker/network failures as reconnectable and make all Kafka frontier waits wake and exit on close/cancellation.

## 2. Acknowledgement and processor-pool lifecycle

- [x] 2.1 Preserve and settle every sibling acknowledgement when a later processor fails after a multiple-output expansion; cover configured error output and terminal failure paths.
- [x] 2.2 Couple fired-window state finalization to source/WAL acknowledgement failure so a failed source commit retains or compensates the staged transaction.
- [x] 2.3 Make source reconnect backoff cancellation-aware and close the real stream adapter on all startup failure paths.
- [x] 2.4 Cancel, drain, and join processor workers/collector before chain resources close; propagate worker and collector failures through EOS drain.

## 3. Graph routing and legacy stream compatibility

- [x] 3.1 Connect partitioned edges to all eligible downstream subtasks and dispatch using the planned key-group owner; add cross-subtask same-key routing tests.
- [x] 3.2 Preserve legacy sliding row-count `window_size`/`slide_size` units or reject unsupported shapes explicitly, with compiler regression coverage.
- [x] 3.3 Preserve legacy tumbling/session payload schema and rows, and maintain buffer-before-processor ordering for accepted legacy configurations.
- [x] 3.4 Attach pre-window processor failures to the configured error output and verify subsequent deliveries continue.

## 4. Event-time and window recovery semantics

- [x] 4.1 Merge existing and newly discovered sliding-window exclusion markers across repeated watermark releases, including mixed open/closed memberships.
- [x] 4.2 Add an end-to-end regression proving source acknowledgement failure does not irreversibly finalize a fired window and replay remains exactly-once-oriented.

## 5. Agent command and heartbeat lifecycle

- [x] 5.1 Decouple long-running command execution from Agent heartbeat, report, and cancellation polling while retaining command idempotency and generation fencing.
- [x] 5.2 Convert checkpoint and aggregation errors into terminal failed command results with correlation metadata instead of dropping the session.
- [x] 5.3 Add Agent shutdown coverage for cancellation during an in-flight checkpoint and verify local WAL-safe cleanup.

## 6. Verification and documentation

- [x] 6.1 Add focused core, plugin, and server tests for all durable input, ACK, pool, compatibility, routing, window, and Agent scenarios.
- [x] 6.2 Update runtime documentation with the durable-read cut, cursor/source order, legacy compatibility, cancellation, partition routing, and Agent command guarantees.
- [x] 6.3 Run `cargo fmt --all -- --check`, `cargo test --workspace`, `git diff --check`, and `openspec validate repair-unified-runtime-review-followups --strict --no-interactive`; record unavailable external-service tests separately.

验证记录：`cargo fmt --all -- --check`、`git diff --check`、`openspec validate repair-unified-runtime-review-followups --strict --no-interactive`、`cargo test -p arkflow-core --lib` 和 `cargo test --workspace --lib` 通过。`cargo test --workspace` 中 `crates/arkflow-plugin/tests/kafka_eos.rs` 的 4 个测试因当前环境没有 Docker daemon socket（`/var/run/docker.sock`）不可执行；其余已执行的 workspace 测试通过。

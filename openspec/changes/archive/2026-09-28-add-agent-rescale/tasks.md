## 1. Core: compatibility parameterization + export

- [x] 1.1 Add `allow_task_set_mismatch` to `evaluate_recovery_compatibility` and `validate_state_snapshot_task_set` (checksum/job-id/format/version-direction/duplicate checks unchanged; only set-equality skipped); update all existing call sites to pass `false`
- [x] 1.2 Make `RescaleContext` and `redistribute` public, add `task_of_namespace` helper (mirrors `namespace_operator` percent-decoding), export from `arkflow_core::executor`
- [x] 1.3 `cargo test -p arkflow-core` green (local rescale tests unchanged prove zero regression)

## 2. Agent recovery path

- [x] 2.1 `validate_recovery_manifest` / `validate_recovery_snapshots` thread the rescale flag from `plan.spec.rescale`
- [x] 2.2 Recovery branch: on task-set mismatch with rescale declared, read ALL snapshots, redistribute each entry, filter to namespaces owned by this node's assignments, restore merged; identical task set keeps the existing assignment-filtered path untouched
- [x] 2.3 `recovery_record_is_valid` passes the rescale flag so Hub artifact selection accepts old-parallelism artifacts for rescale-declared specs
- [x] 2.4 Unit test: in-memory checkpoint store + parallelism-1 manifest + parallelism-2 plan with a single-node assignment — assert exactly the owned entries land in the node's backend, counterpart entries absent, values byte-identical; plus a no-rescale guard test asserting the explicit failure

## 3. Gates

- [x] 3.1 `cargo test -p arkflow-server` and full `cargo test --workspace --all-targets` green; clippy adds no new warnings

## 4. Docs

- [x] 4.1 Update the Job rescale/distributed deployment docs (en + zh-Hans): distributed path supports `rescale: true`, max_parallelism semantics, stop→change→start flow
- [x] 4.2 `pnpm docs:check` passes

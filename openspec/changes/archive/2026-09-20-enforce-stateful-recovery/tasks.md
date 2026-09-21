## 1. State contract and validation

- [x] 1.1 Add `StateDurability` and stable state-root fields with serde defaults, generated schema metadata, and config validation.
- [x] 1.2 Validate that stateful/Window Jobs declare state and that durable state declares a checkpoint; add initial-empty versus ephemeral validation tests.
- [x] 1.3 Add one effective state-namespace helper and use it for stateful graph construction and recovery compatibility.

## 2. Runtime and control-plane recovery

- [x] 2.1 Replace temporary local/Agent state roots with the configured stable root while preserving version/generation/task isolation.
- [x] 2.2 Make Hub reconciliation compute and persist `recovery_required`, reject missing compatible artifacts, and include the decision in `job_start` payloads.
- [x] 2.3 Make Agent and local runner fail before source connection when a recovery-required start has no valid artifact; report ephemeral and missing-recovery outcomes.

## 3. Verification and migration coverage

- [x] 3.1 Add validation tests for state/checkpoint combinations, namespace isolation, durable root reuse, initial empty start, and replacement without a checkpoint.
- [x] 3.2 Add Agent/Hub and local recovery tests proving restore precedes source reads and incompatible artifacts never degrade to empty state.
- [x] 3.3 Regenerate schema/docs snapshots and run the workspace tests and clippy.

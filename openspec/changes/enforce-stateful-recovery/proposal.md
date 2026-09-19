## Why

Agent startup currently creates state under the process temporary directory ([crates/arkflow-server/src/agent.rs:522-539](../../../crates/arkflow-server/src/agent.rs#L522-L539)) and starts a stateful Job with an empty backend whenever the Hub supplies no recovery id. Local Job state has the same temporary-root behavior ([crates/arkflow-core/src/executor/job_runner_adapter.rs:321-338](../../../crates/arkflow-core/src/executor/job_runner_adapter.rs#L321-L338)). A node replacement or restart can therefore report a successful fresh start while silently losing keyed/window state, violating the checkpoint contract.

Stateful execution needs an explicit distinction between an intentional first deployment from empty state and a recovery attempt that must restore a compatible checkpoint. This change makes state durability and missing-artifact behavior explicit and fail-closed.

## What Changes

- Add explicit state durability and root configuration for local and Agent Job state.
- Require checkpoint configuration for durable stateful Jobs and reject invalid state/recovery combinations during validation.
- Allow an empty state only for an initial deployment or an explicitly configured ephemeral mode.
- Make restart, replacement, and recovery without a compatible artifact fail with an actionable error instead of starting empty.
- Persist and validate the state namespace, Job version, task membership, and state format consistently across local and distributed startup.
- Report recovery-required, ephemeral-state, and missing-checkpoint outcomes through Job observations and metrics.
- Add crash/restart tests that prove state is restored before source consumption.

## Non-goals

- Implementing a new checkpoint storage backend or multi-Hub consensus.
- Changing the at-least-once delivery contract.
- Implementing transparent remote-edge reconnect.
- Migrating incompatible state formats automatically without an explicit migration path.

## Capabilities

### New Capabilities

- `stateful-recovery-contract`: Explicit durable/ephemeral state lifecycle and initial-empty versus recovery semantics.

### Modified Capabilities

- `checkpoint-recovery`: A stateful recovery attempt MUST NOT silently degrade to an empty state.
- `keyed-state-backend`: Durable working-state roots and namespace identity MUST be stable across restart.
- `streaming-job-api`: Stateful Job validation MUST require an explicit, compatible checkpoint/recovery contract.

## Impact

- Affected code: `crates/arkflow-core/src/job.rs`, local Job runner state construction, `crates/arkflow-server/src/agent.rs`, checkpoint selection, config schema, and recovery tests.
- Affected configuration: new state durability/root fields and stricter validation for stateful Jobs.
- Affected operations: existing Jobs that relied on temporary state or silent empty restart must opt into `ephemeral` or configure durable state/checkpoints.
- No new storage dependency is required; the existing Redb backend and checkpoint repository remain the implementations.

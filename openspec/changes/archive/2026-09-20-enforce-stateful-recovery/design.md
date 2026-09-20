## Context

The Job model already separates state, checkpoint, and recovery policy, but validation only rejects a checkpoint without a state section. Agent and local runners use temporary directories and treat a missing recovery id as a successful empty start. The Hub knows whether a Job has historical start operations and can therefore distinguish an initial deployment from a replacement, but that fact is not currently sent to the Agent.

The existing checkpoint repository and Redb backend are sufficient. The change needs a stable working-state root, an explicit durability mode, and one recovery-required bit carried in the start command so the first empty start remains possible without allowing a failed restart to erase state.

## Goals / Non-Goals

**Goals:**

- Make durable state the default for stateful/window Jobs.
- Require checkpoint configuration for durable stateful execution.
- Permit empty state only for an initial deployment or explicit ephemeral mode.
- Keep state paths stable across process restarts while isolating Job version/generation/task state.
- Reject missing or incompatible recovery artifacts before source consumption.
- Preserve existing checkpoint format and at-least-once replay semantics.

**Non-Goals:**

- A new state backend or checkpoint store.
- Automatic state migration.
- Multi-Hub consensus or exactly-once external side effects.

## Decisions

1. **Add `StateDurability` and an optional state root to `StateSpec`.**
   `durable` is the default and uses `state.root`, `ARKFLOW_STATE_ROOT`, or a stable `data/arkflow-state` fallback; `ephemeral` is explicit and may use a temporary root. A stable root is preferred over silently changing the existing generation layout because the checkpoint manifest still owns cross-node restore and generation-specific directories prevent stale writers from sharing files.

2. **Validate state requirements from the compiled Job shape.**
   Any explicit stateful operator or Window operator requires `state`. Durable state requires `checkpoint` unless the Job is explicitly ephemeral. A checkpoint without state remains invalid. This validation runs before Hub persistence and Agent startup.

3. **Carry `recovery_required` in the Hub start payload.**
   The Hub sets it for a stateful durable Job after the first successful placement, after a generation/version replacement, or when an explicit checkpoint is requested. Initial deployment may start empty. The Agent rejects a missing recovery artifact when the bit is set; it never infers recovery from a nullable id.

4. **Make recovery selection fail closed.**
   Hub reconciliation fails before dispatch when recovery is required but no completed compatible checkpoint/savepoint exists. Agent-side validation repeats the check for defense in depth. A filtered incompatible artifact is reported as the reason, not treated as an empty start.

5. **Use one effective state namespace helper.**
   Runtime state uses `job:<job_id>:state:<configured-prefix-or-default>:operator:<operator_id>:task:<task_id>`. The same helper is used by graph construction, checkpoint manifests, restore compatibility, and state-size accounting. The configured namespace is a logical prefix and never removes Job/operator isolation.

## Risks / Trade-offs

- [Risk] Existing deployments that relied on temporary state will fail validation or recovery. → Add an explicit `ephemeral` migration setting and report it prominently; do not silently preserve unsafe behavior.
- [Risk] A durable root on a local disk can still be lost with the node. → Checkpoints remain mandatory for replacement/restart recovery; document local disk as working state, not the recovery artifact.
- [Risk] The Hub may not know whether an old start actually processed data. → Treat any prior successful durable start as recovery-required; requiring an operator to create an initial checkpoint is safer than silently resetting state.
- [Risk] Namespace changes can make old artifacts incompatible. → Include the effective namespace in compatibility evaluation and reject artifacts without an explicit migration path.

## Migration Plan

1. Add the fields with serde defaults and update generated schema/docs.
2. Validate new durable state requirements while leaving existing colocated stateless Jobs unchanged.
3. Persist the Hub `recovery_required` decision in the start payload and add Agent defense-in-depth checks.
4. Migrate production Jobs by setting a durable root and ensuring at least one completed checkpoint before rolling a node.
5. Roll back by setting `state.durability: ephemeral` only for development; production rollback must restore the previous implementation together with its durable state root.

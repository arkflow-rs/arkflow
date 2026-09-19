## Why

The public Job schema advertises `join` as an operator kind, but the runtime still builds every non-window operator through the single-input Processor interface ([crates/arkflow-core/src/processor/mod.rs:75-113](../../../crates/arkflow-core/src/processor/mod.rs#L75-L113)); there is no distributed multi-input Join contract. At the same time, `StateSpec.namespace` is declared ([crates/arkflow-core/src/job.rs:193-209](../../../crates/arkflow-core/src/job.rs#L193-L209)) but runtime state construction hardcodes a different namespace, and the backend's byte limit is not exposed by Job configuration.

The API must not promise behavior the execution kernel cannot provide. This change makes unsupported Join plans fail explicitly, gives namespace its intended stable meaning, and exposes a bounded state-size contract.

## What Changes

- **BREAKING** Reject distributed multi-input `Join` operators during Job validation with an actionable unsupported-feature error until a dedicated multi-input runtime exists.
- Define one effective state namespace formula that includes Job/operator/task identity and treats the configured namespace as a logical prefix.
- Add optional `state.max_bytes`, wire it to the Redb backend, and reject invalid zero values before deployment.
- Report state budget exhaustion as a bounded processing error rather than allowing unbounded disk growth.
- Add schema, validation, namespace isolation, Join rejection, and state-budget tests.

## Non-goals

- Implementing a full multi-input streaming Join.
- Changing SQL/DataFusion joins that are compiled into a supported single-input processor shape.
- Adding transparent reconnect or side-edge network transport.

## Capabilities

### New Capabilities

- `job-operator-contracts`: Public Job operator support, state namespace, and state-size limits match runtime capabilities.

### Modified Capabilities

- `streaming-job-api`: Unsupported Join plans SHALL be rejected explicitly.
- `keyed-state-backend`: State namespaces and byte budgets SHALL be enforced by the backend/runtime.

## Impact

- Affected code: `crates/arkflow-core/src/job.rs`, graph/state construction, Redb backend configuration, generated schema/docs, and Job tests.
- Affected API: `StateSpec` gains `max_bytes`; Job configurations using distributed Join or previously ineffective custom namespaces receive corrected validation/runtime behavior.
- No new external service is required.

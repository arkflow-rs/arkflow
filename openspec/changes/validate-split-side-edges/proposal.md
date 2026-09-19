## Why

Split placement is intended to split only ordinary data edges, while error and late-event routes remain co-located. The Job validator checks the source-to-late-route relationship, but dynamic session-window graph construction later searches only local tasks ([crates/arkflow-core/src/executor/graph.rs:639-793](../../../crates/arkflow-core/src/executor/graph.rs#L639-L793)); a valid-looking placement can therefore reach Agent startup and fail because the window and route target are on different nodes.

The placement contract must reject every side-edge split before dispatch, producing a stable validation error instead of a partial Job start.

## What Changes

- **BREAKING** Treat every error-output and late-event route as a side edge during placement validation.
- Reject split assignments where any side-edge endpoint is on a different node.
- Reuse one side-edge relation for Job validation, Hub assignment validation, and Agent graph construction.
- Add session/tumbling window, error route, multi-parallelism, and two-node placement regression tests.
- Keep remote side-edge transport out of scope; invalid assignments are rejected until such a protocol exists.

## Non-goals

- Implementing remote late-event or error side edges.
- Changing ordinary partitioned data-edge routing.
- Changing the event-time late policy or payload schema.

## Capabilities

### New Capabilities

- `split-side-edge-placement`: Explicit placement validation for non-data side edges.

### Modified Capabilities

- `distributed-job-runtime`: Split placement MAY cross ordinary data edges but SHALL keep all side edges co-located.

## Impact

- Affected code: `crates/arkflow-core/src/job.rs`, `crates/arkflow-core/src/executor/graph.rs`, Hub assignment validation, and distributed placement tests.
- Affected users: some previously accepted split configurations will fail validation with an actionable placement error.
- No network protocol or new dependency is introduced.

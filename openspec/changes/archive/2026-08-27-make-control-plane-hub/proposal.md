## Why

The current implementation still embeds one local runtime registry inside the
control-plane service: `crates/arkflow-server/src/lib.rs:126-178` builds routes
from a single `ControlPlane`, while `crates/arkflow/src/main.rs:36-44` starts
that server beside the local Engine. This makes `arkflow-server` a local
monitor rather than a control-plane Hub and prevents one console from managing
multiple independent ArkFlow compute nodes.

## What Changes

- **BREAKING** Make `arkflow-server` a Hub with a durable node identity and an
  in-memory registry of connected ArkFlow compute nodes.
- Add a node Agent protocol for registration, authenticated heartbeats,
  capability/status snapshots, stream inventory, events, and metrics reports.
- Route lifecycle and configuration commands through a target node, returning
  Hub operation IDs that track dispatch, acknowledgement, execution, timeout,
  and disconnect outcomes.
- Add node leases, stale-node detection, reconnect/resume behavior, and
  explicit desired-versus-observed state at both node and Stream levels.
- Keep the existing HTTP resource API as the external Hub API, but change
  `/nodes`, `/streams`, `/operations`, `/events`, and `/metrics` to aggregate
  across nodes and require an explicit target for mutating commands.
- Refactor the `arkflow` binary so a normal compute node connects to a Hub and
  does not open a control-plane listener; retain an explicit standalone mode
  only for local development and compatibility.
- Rebuild the console around node selection, fleet health, node-scoped
  runtime/configuration views, and Hub operation status.
- Preserve WAL delivery semantics and health/readiness/liveness compatibility
  for each compute node.

## Capabilities

### New Capabilities

- `control-plane-hub`: Hub node registry, leases, aggregation, command routing,
  operation state, and security boundaries.
- `compute-node-agent`: ArkFlow node registration, heartbeats, reports,
  reconnect handling, and command execution protocol.
- `fleet-control-console`: Multi-node navigation and node-scoped operations in
  the control-plane console.

### Modified Capabilities

None. The previous control-plane work exists as an in-progress change rather
than a synced main specification; this change defines the Hub contract as the
new authoritative architecture.

## Impact

- Rust: `arkflow-server`, `arkflow-core::control_plane`, runtime reporting,
  `crates/arkflow/src/main.rs`, configuration, and workspace dependencies.
- HTTP/API: node registration/heartbeat/report endpoints plus node-scoped
  resource queries and command routing.
- Frontend: node selector, fleet overview, node status, and target-aware
  operation workflows.
- Deployment: Hub URL, node identity/credentials, lease settings, TLS/proxy,
  and standalone compatibility mode.

## Non-goals

- Distributed consensus, leader election, or automatic cross-node scheduling.
- Moving the stream data plane through the Hub; data stays between compute
  nodes and their configured inputs/outputs.
- A mandatory external database or hosted SaaS control plane.
- Arbitrary mutation of plugin internals outside validated node commands.

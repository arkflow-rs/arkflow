## Why

The existing control-plane console cannot reliably operate against both a local Engine and a Hub because the client assumes one response contract while the server exposes two related but different ones. For example, `console/src/api.ts:21-27` treats `GET /nodes` as a paginated fleet collection and places metrics under the versioned API prefix, while `crates/arkflow-server/src/lib.rs:155` exposes the local `/nodes` route as a single `NodeResource` and `:189` exposes `/metrics` outside the nested API router. Configuration publishing also does not require a successful validation result (`console/src/features.tsx:57-62`), which makes the current administrative workflow incomplete and unsafe.

The control plane is now used as both a single-node console and a multi-node Hub console, so these inconsistencies should be resolved before adding more administrative capabilities.

## What Changes

- Define one stable, versioned response contract for local and Hub system, node, stream, operation, event, and metrics endpoints, including pagination and node identity.
- Make the frontend select local versus Hub resources without guessing from response shape, and keep the selected `node_id` in all node-scoped requests and route state.
- Add complete operation feedback in the console, including dispatch state, progress, correlation ID, terminal errors, and cancellation refresh.
- Require successful configuration validation before publish, surface structured API errors, and keep node-scoped configuration actions consistent with the Hub routes.
- Add server and console contract tests for local mode, Hub mode, stale nodes, unknown resources, invalid configuration, and unavailable-node mutations.
- Preserve redaction guarantees for configuration and authentication data.

## Non-goals

- Changing the stream-processing runtime, plugin implementations, or agent wire protocol semantics.
- Introducing authentication providers or a new user/role management system.
- Redesigning the console visual language beyond states and feedback needed by the API contract.
- Removing the existing compatibility `/config` routes in this change.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `control-plane-console`: clarify the local/Hub resource contract, node-scoped navigation, safe configuration publishing, and operation feedback.
- `control-plane-service`: align versioned local and Hub HTTP responses, metrics routing, pagination, and standard errors.

## Impact

- `crates/arkflow-server/src/lib.rs` and `crates/arkflow-server/src/hub.rs`: HTTP routes, response adapters, validation and error handling.
- `crates/arkflow-server/src/agent.rs`: only where report/operation fields must satisfy the public contract.
- `console/src/api.ts`, `console/src/app.tsx`, and `console/src/features.tsx`: typed API client, node selection, polling, validation gating, and error presentation.
- Rust HTTP contract tests and `console/src/app.test.tsx`.
- No new runtime dependencies are expected.

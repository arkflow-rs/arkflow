## 1. Server contract normalization

- [x] 1.1 Add shared response helpers for paginated collections, local synthetic node resources, and typed problem envelopes in `arkflow-server`.
- [x] 1.2 Change local `/nodes` to return the canonical paginated node collection and retain a compatibility singleton route where needed.
- [x] 1.3 Mount versioned metrics under the configured API prefix while retaining the compatibility root metrics route.
- [x] 1.4 Align Hub node, stream, operation, event, and metric handlers with the canonical envelopes and preserve `node_id` on every node-owned resource.
- [x] 1.5 Ensure unavailable-node and invalid-page mutations return stable problem codes, correlation IDs, and non-success operation states.

## 2. Server verification

- [x] 2.1 Add Rust HTTP contract tests for local node collection shape, versioned metrics, unknown resources, and standard errors.
- [x] 2.2 Add Hub contract tests for pagination, stale-node mutation rejection, operation dispatch metadata, and duplicate stream IDs across nodes.
- [x] 2.3 Run `cargo fmt --check`, targeted `cargo test -p arkflow-server`, and clippy for the changed crate.

## 3. Console API and state model

- [x] 3.1 Update `console/src/api.ts` types and paths to use canonical envelopes, versioned metrics, typed errors, and node-scoped operations/configuration.
- [x] 3.2 Replace response-shape guessing in `App` with explicit resource loading, persist `node_id` in URL state, and preserve the last safe snapshot on refresh failures.
- [x] 3.3 Implement bounded operation polling with target node, correlation ID, terminal state, cancellation refresh, and permission/unavailable error handling.

## 4. Console administration safety

- [x] 4.1 Gate configuration publish on successful validation of the current content and track publish/rollback operations before refreshing state.
- [x] 4.2 Disable stale-node mutations, show node/operation availability feedback, and keep secrets out of rendered errors and URLs.
- [x] 4.3 Add console tests for local and Hub response contracts, node URL persistence, stale-node controls, validation-gated publishing, operation terminal states, and redacted errors.
- [x] 4.4 Run `npm test -- --run` and `npm run build` in `console`.

## 5. Integration verification

- [x] 5.1 Run the workspace test suite and verify the console API base works through the configured reverse proxy.
- [x] 5.2 Review compatibility routes and document any intentionally retained legacy behavior in the change design or server README.

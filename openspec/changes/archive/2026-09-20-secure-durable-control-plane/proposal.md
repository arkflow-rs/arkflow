## Why

The standalone Hub binary silently falls back to in-memory state when `ARKFLOW_HUB_STORAGE` is unset ([crates/arkflow-server/src/bin/arkflow-server.rs:29-34](../../../crates/arkflow-server/src/bin/arkflow-server.rs#L29-L34)), so Jobs, operations, and checkpoint indexes disappear on a Hub restart. Authentication also fails open: an unset operator token grants a legacy Admin principal and an unset node token accepts any registering node ([crates/arkflow-server/src/hub.rs:2188-2225](../../../crates/arkflow-server/src/hub.rs#L2188-L2225)). This is acceptable only for an explicitly local development server, not for a distributed control plane.

The control plane must fail safely at the deployment boundary: durable state and credentials are required when the server is reachable beyond an explicitly insecure local mode.

## What Changes

- **BREAKING** Require durable Hub storage for distributed/non-development startup; keep in-memory Hub only behind an explicit insecure-local mode or test constructor.
- **BREAKING** Require operator and node credentials when the Hub binds a non-loopback address.
- Replace implicit legacy Admin access with an explicit local-development authorization mode.
- Keep constant-time token comparison and return actionable startup/HTTP errors for missing or invalid credentials.
- Persist and reload Jobs, operations, desired state, leases, and checkpoint pointers before reporting Hub readiness.
- Add startup/auth matrix tests and restart tests proving control-plane state survives process restart.

## Non-goals

- Multi-Hub consensus, leader election, or active-active high availability.
- Replacing SQLite or the existing storage actor.
- Changing the Agent session-token protocol after successful registration.
- Data-plane authentication; that belongs to `harden-remote-data-plane`.

## Capabilities

### New Capabilities

- `secure-durable-control-plane`: Explicit startup safety, durable Hub state, and fail-closed operator/node authentication.

### Modified Capabilities

- `distributed-job-runtime`: Distributed control-plane startup SHALL not report ready with volatile state or unauthenticated network access.

## Impact

- Affected code: `crates/arkflow-server/src/bin/arkflow-server.rs`, `ServerConfig`, `HubConfig`, `crates/arkflow-server/src/hub.rs`, storage recovery, and server tests.
- Affected deployment: production startup must set storage and both credential classes, or explicitly select local insecure mode.
- Existing unit tests that construct `Hub::new` remain available as in-process test fixtures; the standalone binary behavior becomes safer.

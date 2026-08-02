## Why

The current implementation exposes control routes from `Engine` alongside the legacy health server (`crates/arkflow-core/src/engine/mod.rs:145-172`), so the backend still presents itself as a single-process health endpoint with a few Stream commands rather than as a control-plane service. The console has the same limitation: its shell only switches between four local views and its dashboard fetches only `/system` and `/streams` (`console/src/app.tsx:4-20`), which makes configuration, operations, events, topology, and multi-Stream administration secondary instead of first-class.

ArkFlow now needs a deliberate control-plane boundary: a backend that exposes system resources and desired/observed state, and a frontend that operates the whole deployment rather than monitoring one Stream at a time.

## What Changes

- **BREAKING** Move control-plane HTTP routing, request models, and server lifecycle ownership out of `arkflow-core::Engine` into the `arkflow-server` boundary; keep health/readiness/liveness as compatibility endpoints, not as the server's primary abstraction.
- Define a resource-oriented control-plane API for system identity, runtime nodes, Streams, components, configuration drafts/versions, operations, events, and metrics.
- Add operation IDs, desired/observed lifecycle state, filtering, pagination, correlation IDs, and consistent error/problem responses for administrative commands.
- Separate runtime orchestration from HTTP transport so the Engine can be embedded, tested, and managed by a dedicated control-plane service.
- Rebuild the Console as an operations application with a persistent application shell, system overview, runtime topology, Stream administration, configuration workflow, component catalogue, event/audit view, and settings/security status.
- Add shared loading, stale-data, empty, permission, conflict, and long-running-operation states; avoid treating periodic Stream polling as the primary UI model.
- Add deployment/startup integration that runs the backend control-plane service and frontend against the same API contract.
- Preserve existing Stream processing, WAL at-least-once semantics, health paths, and legacy configuration compatibility unless explicitly covered by the new API contract.

## Capabilities

### New Capabilities

- `control-plane-service`: Dedicated resource-oriented backend boundary, API contract, operations, events, and runtime orchestration integration.
- `control-plane-console`: Full administrative web console for system, runtime, configuration, components, operations, events, and security state.
- `control-plane-deployment`: Backend/frontend startup, proxy, production packaging, and protected deployment contract.

### Modified Capabilities

- None. The completed `add-control-plane` change established the initial implementation; this change replaces its health-centric architecture rather than modifying an existing main spec contract.

## Impact

- Affected Rust code: `arkflow-core::engine`, runtime/control models, `arkflow-server`, CLI startup, and workspace dependencies.
- Affected HTTP contract: routes under `/api/v1` will become resource-oriented and operation-aware; health endpoints remain compatible.
- Affected frontend: `console/src` will be reorganized into API, layout, feature, and shared state modules.
- Affected deployment: console proxy/container configuration and backend startup commands.
- Verification: Rust workspace tests, API contract tests, frontend typecheck/unit/build, and a local control-plane smoke test.

## Non-goals

- Multi-node consensus, leader election, or cross-node scheduling.
- Replacing the Stream data plane or changing WAL delivery guarantees.
- Building a hosted SaaS control plane or introducing a mandatory external database.
- Arbitrary mutation of live component internals that cannot be represented as validated configuration.

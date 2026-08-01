## Context

`add-control-plane` added useful primitives, but the transport, runtime, and health concerns remain coupled in `arkflow-core::Engine`. The current console is a single component with tab-like navigation and periodic aggregate polling. This change keeps the existing single-node deployment model while making the control plane a real product boundary with explicit resources and administrative workflows.

## Goals / Non-Goals

**Goals:**

- Make `arkflow-server` the owner of HTTP transport, API routing, authentication, request correlation, and response envelopes.
- Keep `arkflow-core` focused on Engine, Stream runtime orchestration, configuration, and domain services that can be invoked without HTTP.
- Expose a stable resource API with system/node identity, runtime inventory, desired/observed state, operations, events, configuration, components, and metrics.
- Give the console an information architecture suitable for operating many Streams and configuration versions.
- Preserve health endpoint compatibility and existing WAL/data-plane semantics.

**Non-Goals:**

- Distributed control-plane consensus or multi-node scheduling.
- A persistent database requirement; local configuration history remains file-backed.
- Arbitrary live mutation of plugin internals.

## Decisions

1. **Dedicated server boundary.** Move Router and HTTP handlers into `arkflow-server`, while exposing a cloneable `ControlPlane`/domain facade from core. The server depends on core; core does not depend on server. This avoids the current Engine-as-web-server coupling without introducing a dependency cycle.

2. **Resource-oriented API.** Use `/api/v1/system`, `/nodes`, `/streams`, `/operations`, `/events`, `/configuration`, and `/components`. Keep `/config`, `/status`, and health routes as compatibility aliases during migration. List endpoints return `{items, page, page_size, total}` where pagination is relevant.

3. **Desired versus observed state.** Runtime records expose `desired_state`, `observed_state`, transition timestamps, operation ID, and last error. Lifecycle commands create an operation record and return `202 Accepted` when work is asynchronous; idempotent requests reuse or report the active operation.

4. **Operation/event service.** RuntimeManager remains the execution primitive, but a control-plane operation store wraps it with IDs, progress, actor/correlation metadata, bounded retention, and event publication. The store is in-memory for operations/events and file-backed for configuration versions.

5. **Transport middleware.** `arkflow-server` applies authentication, correlation IDs, structured access logging, CORS policy, request limits, and standard problem responses once at the outer router. Request bodies and credentials are never logged.

6. **Console feature modules.** Split the React app into `app`, `api`, `features/overview`, `features/runtime`, `features/configuration`, `features/components`, `features/events`, and shared components. Use a query/cache layer with explicit stale/loading/error states; do not make each page own an independent five-second polling loop.

7. **Deployment contract.** The backend binary starts the Engine domain and `arkflow-server` together. The console is static assets served by Nginx/Vite, with same-origin `/api` proxying and an explicit production token/reverse-proxy configuration.

## Risks / Trade-offs

- **[Risk]** Moving handlers may temporarily break existing internal tests and compatibility clients → preserve old aliases and add contract tests before removing old routes.
- **[Risk]** Asynchronous operation state can diverge from the task handle → operation transitions are written from the same supervisor completion path and expose reconciliation timestamps.
- **[Risk]** More API resources increase frontend complexity → centralize typed client/cache and shared state components.
- **[Risk]** In-memory operations disappear on process restart → document this limitation and keep configuration versions durable; do not claim durable operation history.
- **[Risk]** A single-node server remains a local control plane → expose node identity and capability limits explicitly so future multi-node work has a clear seam.

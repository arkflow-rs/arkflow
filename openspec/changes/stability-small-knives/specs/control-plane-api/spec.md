## MODIFIED Requirements

### Requirement: Unified control HTTP server

The system SHALL serve health checks and control-plane routes from one configurable HTTP server and SHALL report listener binding failures to the Engine startup path. The server SHALL enforce a per-request timeout on control routes: a dispatched request whose handler does not produce a response within the bound SHALL be terminated with a standard error-envelope response carrying a stable timeout code, so a slow or stalled handler cannot hold server resources indefinitely. The bound covers handler execution only — from request dispatch to response production; it does not terminate response bodies that have already started streaming (the SSE event routes continue streaming after their headers are produced), and it does not bound the HTTP/1 header-read phase on the listener, which predates request dispatch. Bounding the header-read phase is a listener/proxy-level concern outside this middleware: deployments that require it SHALL enforce it with a reverse proxy or a listener with a header-read timeout; the engine documents this dependency rather than claiming coverage it does not implement.

#### Scenario: Server starts with configured address

- **WHEN** the Engine starts with control HTTP enabled and an available address
- **THEN** health routes and `/api/v1` routes are reachable on that address

#### Scenario: Server bind fails

- **WHEN** the configured address cannot be bound
- **THEN** Engine startup returns an error instead of reporting readiness or panicking in a detached task

#### Scenario: Request exceeds the timeout bound

- **WHEN** a control-route handler does not produce a response within the per-request timeout
- **THEN** the server responds with a non-2xx status whose body is the standard error envelope with a stable timeout code, and the connection is not left holding handler resources beyond the bound

#### Scenario: Streaming response is not cut by the timeout

- **WHEN** a handler produces its response within the bound and the body streams afterwards (for example the SSE event routes)
- **THEN** the body continues streaming without a server-side cut from this middleware

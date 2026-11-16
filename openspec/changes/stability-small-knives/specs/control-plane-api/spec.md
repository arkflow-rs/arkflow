## MODIFIED Requirements

### Requirement: Unified control HTTP server

The system SHALL serve health checks and control-plane routes from one configurable HTTP server and SHALL report listener binding failures to the Engine startup path. The server SHALL enforce a per-request timeout on control routes: a request that exceeds the bound SHALL be terminated with a standard error-envelope response carrying a stable timeout code, so a slow or stalled client cannot hold server resources indefinitely.

#### Scenario: Server starts with configured address

- **WHEN** the Engine starts with control HTTP enabled and an available address
- **THEN** health routes and `/api/v1` routes are reachable on that address

#### Scenario: Server bind fails

- **WHEN** the configured address cannot be bound
- **THEN** Engine startup returns an error instead of reporting readiness or panicking in a detached task

#### Scenario: Request exceeds the timeout bound

- **WHEN** a control-route request does not complete within the per-request timeout
- **THEN** the server responds with a non-2xx status whose body is the standard error envelope with a stable timeout code, and the connection is not left holding handler resources beyond the bound

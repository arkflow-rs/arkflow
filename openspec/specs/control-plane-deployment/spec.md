# Purpose

Define repeatable local and production deployment paths for the ArkFlow control-plane backend and Console.

# Requirements

### Requirement: Unified local and production startup

The project SHALL document and provide a repeatable startup path that runs the backend control-plane service and static console against the same API prefix. Production packaging SHALL support a protected same-origin reverse-proxy deployment.

#### Scenario: Start locally
- **WHEN** an operator runs the documented backend and frontend commands
- **THEN** the console reaches the backend through the configured development proxy and can discover system resources

#### Scenario: Deploy behind a reverse proxy
- **WHEN** the console and API are served through a protected TLS reverse proxy
- **THEN** /api, /metrics, and static assets use the documented routes and credentials are not exposed to the public listener

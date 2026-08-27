## Purpose

Define operator identity, resource-scoped authorization, and actor-aware control-plane auditing.

## Requirements

### Requirement: Authenticated operator principal

The control plane SHALL resolve every operator request to an authenticated principal, except explicitly configured health and liveness routes, and SHALL keep Agent authentication separate from operator authentication.

#### Scenario: Reject an unauthenticated mutation
- **WHEN** an unauthenticated request attempts to change a Stream, configuration, node mode, or rollout
- **THEN** the service returns 401 and does not create or mutate a control-plane resource

### Requirement: Resource-scoped RBAC

The service SHALL authorize actions by principal, role, action, and resource scope, including fleet, node, Stream, configuration, rollout, and audit read scopes.

#### Scenario: Deny an out-of-scope action
- **WHEN** an authenticated principal lacks permission for the target node or requested action
- **THEN** the service returns 403 and leaves desired state, operations, and audit state unchanged

### Requirement: Actor-aware audit

Every accepted or rejected control-plane mutation SHALL record the principal, action, target resource, correlation ID, outcome, and stable failure code when applicable. Audit records MUST NOT contain credentials, authorization headers, or secret configuration values.

#### Scenario: Audit a denied request
- **WHEN** a principal submits a validly authenticated but unauthorized mutation
- **THEN** an audit record identifies the principal, target, denied action, correlation ID, and reason without exposing secrets

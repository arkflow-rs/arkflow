## MODIFIED Requirements

### Requirement: Actor-aware audit

Every accepted or rejected control-plane mutation SHALL record the principal, action, target resource, correlation ID, outcome, and stable failure code when applicable. Audit records MUST NOT contain credentials, authorization headers, or secret configuration values. On a Hub configured with durable storage, an audit persistence failure MUST NOT be silently discarded: the Hub SHALL increment its closed-label audit write failure counter (`arkflow_audit_write_failures_total{category}`) and emit a warning log containing only scalar metadata (never credentials or secret payloads). Best-effort audit paths (such as authorization denials) SHALL still return their original security response (`401`/`403`) even when the audit write fails. A volatile Hub without durable storage has no audit store by design and SHALL NOT treat the absence of storage as an audit write failure on best-effort paths.

#### Scenario: Audit a denied request
- **WHEN** a principal submits a validly authenticated but unauthorized mutation
- **THEN** an audit record identifies the principal, target, denied action, correlation ID, and reason without exposing secrets

#### Scenario: Storage failure during security denial is never silent
- **WHEN** a Hub with durable storage denies an operator request (`401` or `403`) while audit persistence fails
- **THEN** the Hub still returns `401` or `403` to the caller, increments `arkflow_audit_write_failures_total{category="auth_denial"}`, and logs a warning without credential material

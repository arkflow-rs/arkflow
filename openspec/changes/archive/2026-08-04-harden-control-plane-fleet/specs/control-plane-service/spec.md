## MODIFIED Requirements

### Requirement: Operations and events

The service SHALL persist and retain bounded Intent, Attempt, operation, audit, and control-event records with resource identity, generation, timestamps, actor and correlation metadata, failure classification, retry information, and resulting observed state. A command acknowledgement SHALL NOT be represented as final operation success until observed convergence is confirmed. Historical records MAY be pruned according to retention policy, but active intents and latest resource state SHALL remain available.

#### Scenario: Observe a completed operation after restart
- **WHEN** a client requests /api/v1/operations/{id} after a Hub restart
- **THEN** it receives the durable terminal status, actor, timestamps, affected resource, generation, and resulting observed state

#### Scenario: Audit a control mutation
- **WHEN** an authorized or rejected operator mutation is processed
- **THEN** the operation/event history contains the actor, correlation ID, target, outcome, and stable failure classification without secrets

#### Scenario: Observe a retrying operation
- **WHEN** a temporary node or transport failure occurs
- **THEN** the operation remains associated with the desired Intent, exposes retry count and next retry time, and does not silently change desired state

### Requirement: Stable transport and security behavior

The server SHALL apply authentication, RBAC authorization, CORS policy, request correlation, request-size limits, and a consistent JSON problem envelope at the outer router. It SHALL NOT log request bodies, authorization values, or secret configuration values. Compatibility token configuration MAY resolve to a restricted compatibility principal during migration.

#### Scenario: Authorize a protected write
- **WHEN** an authenticated principal sends a lifecycle, configuration, maintenance, or rollout mutation
- **THEN** the service evaluates the principal's resource-scoped permission before creating any durable intent or operation

#### Scenario: Reject a forbidden write
- **WHEN** the principal is authenticated but lacks the required action scope
- **THEN** the service returns 403 with a correlation ID and does not mutate desired state

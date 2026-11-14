## ADDED Requirements

### Requirement: Audit reads SHALL enforce the existing permission and resource scope

The Hub audit listing route SHALL require the existing `ReadAudit` action and an applicable `audit` resource scope. An unfiltered listing SHALL require access to the complete audit collection; a listing for `resource_id` SHALL require access to that audit resource. Query filters MUST NOT broaden the permitted scope. The existing role matrix SHALL remain unchanged: admin and viewer possess `ReadAudit`, operator does not. Authentication failures SHALL return 401; authenticated unauthorized reads SHALL return 403 without returning audit records.

#### Scenario: Operator cannot read audit records
- **WHEN** an authenticated operator-role principal requests the audit list
- **THEN** the service returns 403 and no audit records

#### Scenario: Authorized viewer reads scoped history
- **WHEN** a viewer with an audit scope for a specific resource requests that resource's audit records
- **THEN** the service returns only that resource's history

#### Scenario: Scoped principal cannot list another resource or the whole collection
- **WHEN** a principal restricted to one audit resource requests another resource or an unfiltered listing
- **THEN** the service returns 403 and does not expose audit records

### Requirement: Audit queries SHALL filter before pagination

The audit route SHALL support optional exact-match `actor`, `action`, and `resource_id` filters and inclusive Unix-millisecond `from_ms`/`to_ms` time bounds, combined conjunctively in storage before the existing 1024-record response window and pagination. Page totals SHALL count the filtered records within that response window. An inverted time range SHALL return 400 with stable code `invalid_audit_query`. Omitting the additional filters SHALL preserve the existing ordering, page size defaults, page size bound and retention boundary.

#### Scenario: Combine audit filters
- **WHEN** an authorized caller filters by actor, action, resource, and time range
- **THEN** only records matching every filter are counted and paginated

#### Scenario: Filtered records remain reachable beyond the first page
- **WHEN** matching records occur after non-matching records in the retained audit history
- **THEN** filtering occurs first and the first filtered page includes those records with an accurate total

#### Scenario: Reject an inverted time range
- **WHEN** from_ms is greater than to_ms
- **THEN** the service returns 400 with code `invalid_audit_query`

### Requirement: Job operation metadata SHALL preserve the actual audit actor

Job operations dispatched directly on behalf of an authenticated operator SHALL carry that principal's id into accepted and rejected dispatch audit records. Background-only operations SHALL use the explicit `system` actor. Persisted operations and retries SHALL retain an already-recorded actor. Actor metadata MUST NOT contain the bearer credential and MUST NOT be inferred from a mutable global identity. Historical operation JSON without an actor SHALL remain readable.

#### Scenario: Managed user starts or stops a Job
- **WHEN** a managed principal triggers a Job start or stop and the operation is dispatched
- **THEN** the operation and corresponding audit record identify that principal rather than a fixed operator string

#### Scenario: Preserve actor across operation recovery and retry
- **WHEN** an attributed operation is persisted, restored, or retried
- **THEN** its actor remains the recorded originating principal

#### Scenario: Background work has an explicit system identity
- **WHEN** reconciliation dispatches an operation without an operator request or an attributed predecessor
- **THEN** the operation audit identifies system as the actor

#### Scenario: Read historical operations
- **WHEN** an operation JSON record created before actor metadata is deserialized
- **THEN** deserialization succeeds and no user identity is fabricated

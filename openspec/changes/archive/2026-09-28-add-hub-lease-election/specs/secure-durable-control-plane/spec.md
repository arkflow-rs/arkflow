# secure-durable-control-plane Delta

## MODIFIED Requirements

### Requirement: Hub readiness SHALL follow durable recovery

The Hub SHALL restore persisted Jobs, operations, desired state, leases, and checkpoint pointers before binding its listener or reporting ready. Storage recovery errors SHALL leave the server unavailable. When HA is enabled, readiness SHALL additionally require leadership: a standby Hub (one that does not hold the control-plane lease) SHALL report not-ready with its role, and SHALL NOT serve operator or agent APIs except health, liveness, readiness, and metrics.

#### Scenario: Storage recovery succeeds

- **WHEN** the configured store opens and persisted records are restored successfully
- **THEN** the listener binds and readiness can become healthy with the restored control-plane view

#### Scenario: Storage recovery fails

- **WHEN** opening or restoring the configured store returns an error
- **THEN** startup fails or readiness remains unavailable, and the Hub does not serve a partial in-memory state

#### Scenario: Standby reports not-ready with its role

- **WHEN** HA is enabled and the Hub does not hold the control-plane lease
- **THEN** readiness reports not-ready, includes the standby role, and mutating or reading operator/agent APIs return 503 instead of serving the standby's in-memory view

#### Scenario: Promoted Hub becomes ready after re-recovery

- **WHEN** a standby acquires the lease and completes durable re-recovery
- **THEN** readiness becomes healthy with the leader role before any reconcile tick dispatches operations

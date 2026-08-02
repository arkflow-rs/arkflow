## MODIFIED Requirements

### Requirement: Desired and observed lifecycle state

Each Stream resource SHALL expose independent desired state and observed runtime state, including desired generation, target configuration version, convergence state, transition timestamps, active Intent/Attempt identifiers, metrics, and bounded recent errors. Lifecycle commands SHALL update desired state through an idempotent Intent and SHALL return an operation representation whose terminal success requires observed convergence.

#### Scenario: Start a stopped Stream
- **WHEN** a client posts a start command for a stopped Stream
- **THEN** the API persists desired state running with a new generation and returns an operation whose initial state is converging or queued until the node reports that generation running

#### Scenario: Repeat an active command
- **WHEN** a client repeats the same lifecycle command while an equivalent Intent for the same generation is active
- **THEN** the API returns the existing Intent/operation without spawning a duplicate command attempt

#### Scenario: Show divergence
- **WHEN** desired state is running but the latest observed state is stopped or unknown
- **THEN** the Stream resource exposes both values and reports pending, applying, degraded, or unknown convergence rather than presenting the Stream as in_sync

### Requirement: Operations and events

The service SHALL retain bounded Intent, Attempt, and control-event records with resource identity, generation, timestamps, correlation metadata, failure classification, retry information, and resulting observed state. A command acknowledgement SHALL NOT be represented as final operation success until observed convergence is confirmed.

#### Scenario: Observe a converged operation
- **WHEN** a client requests an operation after the target node reports the desired generation and state
- **THEN** it receives terminal converged status, start/end timestamps, affected resource, generation, and resulting observed state

#### Scenario: Observe a retrying operation
- **WHEN** a temporary node or transport failure occurs
- **THEN** the operation remains associated with the desired Intent, exposes retry count and next retry time, and does not silently change the desired state

#### Scenario: Observe a superseded operation
- **WHEN** a newer desired-state mutation replaces an older one
- **THEN** the older operation is marked superseded with the newer generation and is not reported as an execution failure

### Requirement: Reconciliation HTTP contract

The service SHALL expose desired state, observed state, convergence state, generation, and Intent/Attempt identifiers in Stream and operation resources. The canonical lifecycle mutation SHALL be an idempotent desired-state write, and compatibility start/stop/configuration routes SHALL create the same durable Intent rather than requiring immediate node execution.

#### Scenario: Write desired state while a node is offline
- **WHEN** an authorized client writes `PUT /api/v1/nodes/{node_id}/streams/{stream_id}/desired-state` for an offline node
- **THEN** the service persists the desired generation, returns `202` with an Intent representation, and reports unknown or degraded convergence without claiming execution success

#### Scenario: Protect a stale desired-state write
- **WHEN** a client submits an `If-Match` generation older than the current desired generation
- **THEN** the service returns `412` with a stable `generation_conflict` problem and does not overwrite the newer intent

#### Scenario: Retry an idempotent request
- **WHEN** a client repeats a mutation with the same `Idempotency-Key`
- **THEN** the service returns the original Intent/operation representation without creating a second active Intent or Attempt

#### Scenario: Read divergent resource state
- **WHEN** desired state is running and the latest observed state is stopped or unknown
- **THEN** the Stream resource exposes both values, the generation pair, and a non-`in_sync` convergence state

### Requirement: Compatibility lifecycle routes

The service SHALL preserve versioned start, stop, restart, configuration apply, and rollback routes as compatibility adapters. Start and stop SHALL update desired lifecycle state; configuration routes SHALL update desired configuration version; restart SHALL create a one-shot action Intent with an action ID.

#### Scenario: Use the legacy start route
- **WHEN** a client posts to the existing node-scoped start route
- **THEN** the service creates or reuses the equivalent desired-state Intent and returns the same generation/convergence representation as the canonical desired-state route

#### Scenario: Confirm restart
- **WHEN** the Agent reports completion of the restart action ID and the Stream is observed running
- **THEN** the operation is marked converged even though the stable desired and observed lifecycle states are both running

### Requirement: Stable problem contract

The service SHALL return a consistent problem envelope containing a stable code, message, correlation ID, and applicable resource or generation details. It SHALL use `202` for accepted pending intent, `412` for stale generation, `409` for immediate resource conflicts, `422` for validation failures, and `503` for repository unavailability.

#### Scenario: Reject stale generation
- **WHEN** a desired-state write uses an old `If-Match` value
- **THEN** the response is `412` with `generation_conflict`, current generation details, and the request correlation ID

#### Scenario: Reject invalid configuration
- **WHEN** a configuration candidate fails schema or component validation
- **THEN** the response is `422` with path-aware validation issues and no desired configuration Intent is persisted

### Requirement: Stable resource and operation representations

The service SHALL use one resource-oriented representation for canonical and compatibility responses. A Stream representation SHALL contain `desired`, `observed`, and `convergence` objects plus `generation`, `observed_generation`, `intent_id`, `attempt_id`, `config_version`, `updated_at`, and bounded `recent_errors`. An operation representation SHALL contain Intent identity, latest Attempt identity, resource reference, generation, state, convergence, retry metadata, failure classification, and observed result.

#### Scenario: Read a resource during reconciliation
- **WHEN** a client reads a Stream while an Attempt is acknowledged but no matching report has arrived
- **THEN** the response shows the target generation and `convergence` as `applying` or `degraded`, and does not expose terminal success solely from the acknowledgement

#### Scenario: List operations deterministically
- **WHEN** a client lists operations with `page`, `page_size`, `node_id`, `resource_id`, `operation`, `state`, or `correlation_id` filters
- **THEN** the service returns a bounded page with `items`, `page`, `page_size`, and `total`, ordered newest-first by creation time and stable ID tie-breaker

### Requirement: Conditional mutation and cancellation semantics

The service SHALL treat `If-Match` as a desired-generation compare-and-swap guard and `Idempotency-Key` as request replay protection. A key SHALL be scoped to the authenticated principal and canonical resource/mutation shape; reusing it with a different mutation SHALL return `409` with `idempotency_key_reused`. `DELETE /api/v1/operations/{id}` SHALL cancel only an Intent that has not converged or been superseded, preserve desired-state history, and return the resulting operation state; it SHALL NOT claim that an already dispatched non-idempotent action was undone.

#### Scenario: Reuse an idempotency key with another mutation
- **WHEN** a client reuses an existing key for a different desired state or resource
- **THEN** the service returns `409` and leaves the original Intent unchanged

#### Scenario: Cancel before dispatch
- **WHEN** a client cancels an accepted Intent before an Attempt is dispatched
- **THEN** the Intent becomes `cancelled`, pending outbox work is suppressed, and the response identifies the cancelled generation

#### Scenario: Cancel after dispatch
- **WHEN** a client cancels an Intent after an Attempt was dispatched
- **THEN** the service records cancellation intent and returns the latest Attempt outcome separately; it does not report rollback or convergence unless a subsequent observed report proves the requested state

### Requirement: Separate operator and Agent HTTP contracts

The service SHALL keep operator routes and Agent routes distinct. Operator routes SHALL use operator authorization and correlation headers; Agent routes SHALL require node authorization, include `boot_id` and monotonic `report_seq` on reports, and carry generation, configuration version, Attempt ID, and expiry on commands. Agent acknowledgements SHALL be transport-level delivery results, while operator operation state SHALL be derived from durable Intent state and observed reports.

#### Scenario: Reject an Agent report on the operator contract
- **WHEN** a request without node credentials posts to `/api/v1/agent/report`
- **THEN** the service returns `401` and does not mutate observed or desired state

#### Scenario: Ignore an old Agent report
- **WHEN** an authenticated Agent submits a report older than the stored `(boot_id, report_seq)` cursor
- **THEN** the service acknowledges receipt without regressing observed state or creating a reconciliation transition

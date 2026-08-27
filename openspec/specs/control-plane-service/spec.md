## Purpose

Define the dedicated ArkFlow control-plane service boundary, resource API, runtime operations, and transport security behavior.

## Requirements

### Requirement: Dedicated control-plane service boundary

The system SHALL expose control-plane HTTP transport from arkflow-server through a domain facade, and arkflow-core::Engine SHALL remain usable without constructing an HTTP Router. Health, readiness, and liveness endpoints SHALL remain available for compatibility.

#### Scenario: Start the control-plane service
- **WHEN** the ArkFlow binary starts with control-plane settings enabled
- **THEN** it starts the Engine domain and the dedicated server using one configured listener

#### Scenario: Embed the Engine without HTTP
- **WHEN** a caller constructs and runs the Engine domain in a library context
- **THEN** no Axum Router or listener is required

### Requirement: Resource-oriented system API

The API SHALL expose versioned resources for system identity, runtime nodes, Streams, components, configuration, operations, events, and metrics. Collection responses SHALL provide stable envelopes and pagination metadata where applicable. `GET /api/v1/nodes` SHALL return a paginated node collection in both local and Hub mode, and every node-owned resource SHALL preserve `node_id`. Metrics SHALL be reachable under the same configured versioned prefix as the other control resources.

#### Scenario: Discover the control-plane resources
- **WHEN** a client requests /api/v1/system and /api/v1/nodes
- **THEN** it receives node identity, version, capabilities, health state, and runtime summary

#### Scenario: List many Streams
- **WHEN** a client requests /api/v1/streams?page=2&page_size=20
- **THEN** it receives only the requested page plus total and page metadata

#### Scenario: Read local nodes as a collection
- **WHEN** a client requests /api/v1/nodes against a local Engine
- **THEN** it receives one local node resource inside a paginated collection, rather than a singleton response

#### Scenario: Read metrics through the versioned API
- **WHEN** a client requests /api/v1/metrics
- **THEN** it receives aggregate and per-node metrics using the same authentication, correlation, and routing rules as other control resources

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

The service SHALL persist and retain bounded Intent, Attempt, operation, audit, and control-event records with resource identity, generation, timestamps, actor and correlation metadata, failure classification, retry information, and resulting observed state. A command acknowledgement SHALL NOT be represented as final operation success until observed convergence is confirmed. Historical records MAY be pruned according to retention policy, but active intents and latest resource state SHALL remain available.

#### Scenario: Observe a completed operation
- **WHEN** a client requests /api/v1/operations/{id} after a restart operation
- **THEN** it receives terminal converged status, start/end timestamps, affected resource, generation, and resulting observed state

#### Scenario: Correlate an administrative request
- **WHEN** a client sends an X-Correlation-ID
- **THEN** the response, operation record, event, and structured access log carry the same correlation ID

#### Scenario: Observe a retrying operation
- **WHEN** a temporary node or transport failure occurs
- **THEN** the operation remains associated with the desired Intent, exposes retry count and next retry time, and does not silently change the desired state

#### Scenario: Audit a control mutation
- **WHEN** an authorized or rejected operator mutation is processed
- **THEN** the operation/event history contains the actor, correlation ID, target, outcome, and stable failure classification without secrets

#### Scenario: Observe a superseded operation
- **WHEN** a newer desired-state mutation replaces an older one
- **THEN** the older operation is marked superseded with the newer generation and is not reported as an execution failure

#### Scenario: Target an unavailable node
- **WHEN** a Hub command targets a stale or offline node
- **THEN** the service returns a non-success operation state with a stable availability problem and does not claim execution success

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

### Requirement: Standard transport and security behavior

The server SHALL apply authentication, RBAC authorization, CORS policy, request correlation, request-size limits, and a consistent JSON problem envelope at the outer router. It SHALL NOT log request bodies, authorization values, or secret configuration values. Compatibility token configuration MAY resolve to a restricted compatibility principal during migration. Compatibility aliases MAY remain, but the versioned resource routes SHALL be the canonical contract.

#### Scenario: Reject an unauthenticated write
- **WHEN** a protected lifecycle or configuration write lacks valid credentials
- **THEN** the service returns a problem response with 401 or 403 and does not create an operation

#### Scenario: Reject an invalid resource request
- **WHEN** a client requests an unknown resource or invalid page/filter
- **THEN** the service returns a stable problem code, human-readable message, correlation ID, and appropriate HTTP status

#### Scenario: Authorize a protected write
- **WHEN** an authenticated principal sends a lifecycle, configuration, maintenance, or rollout mutation
- **THEN** the service evaluates the principal's resource-scoped permission before creating any durable intent or operation

#### Scenario: Reject a forbidden write
- **WHEN** the principal is authenticated but lacks the required action scope
- **THEN** the service returns 403 with a correlation ID and does not mutate desired state

### Requirement: Durable node maintenance mode

The service SHALL expose authenticated node maintenance actions that persist one of `active`, `draining`, or `maintenance`, append an audit event with actor and correlation metadata, and return the durable node representation. Reconciliation SHALL NOT dispatch new Attempts to a node in `draining` or `maintenance`, while already dispatched Attempts MAY settle.

#### Scenario: Drain an active node
- **WHEN** an authorized operator posts a drain action for an active node
- **THEN** the service persists `draining`, emits a `node_maintenance_changed` event, and prevents new command dispatches while preserving desired state

#### Scenario: Enter maintenance mode
- **WHEN** an authorized operator posts a maintenance action
- **THEN** the service persists `maintenance`, exposes the mode in node/status resources, and records the actor and correlation ID

#### Scenario: Resume a node
- **WHEN** an authorized operator deletes the node maintenance action
- **THEN** the service persists `active`, emits an audit event, and makes eligible current-generation Intents dispatchable again

#### Scenario: Reject an unauthorized maintenance action
- **WHEN** a request without valid operator authorization attempts to drain or maintain a node
- **THEN** the service returns `401` or `403` and does not mutate the node or append an audit event

### Requirement: Operational audit events

Operational mutations SHALL be represented in the same ordered event resource as reconciliation events, with event type, node identity, actor, correlation ID, previous mode, new mode, and outcome. Event payloads SHALL exclude authorization values and secret configuration content.

#### Scenario: Audit a drain transition
- **WHEN** a drain action succeeds
- **THEN** an ordered event is queryable by node and correlation ID and identifies the mode transition and successful outcome

#### Scenario: Audit a failed transition
- **WHEN** a maintenance action fails because the node does not exist or storage is unavailable
- **THEN** the API returns a stable problem and does not emit a successful state-transition event

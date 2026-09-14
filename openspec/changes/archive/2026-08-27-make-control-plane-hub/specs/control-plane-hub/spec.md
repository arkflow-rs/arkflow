## ADDED Requirements

### Requirement: Hub node registry

The Hub SHALL maintain a registry keyed by stable `node_id`, including node
capabilities, current status, last heartbeat, lease expiry, and the latest
reported system/runtime snapshot.

#### Scenario: Node registers

- **WHEN** a compute node presents a valid node identity and registration token
- **THEN** the Hub creates or resumes that node record and returns an
  authenticated session token plus the current report contract version

#### Scenario: Node lease expires

- **WHEN** a node misses heartbeats beyond its configured lease TTL
- **THEN** the Hub marks it stale and excludes it from healthy-node counts while
  retaining its last-known resources with a stale indicator

### Requirement: Agent report ingestion

The Hub SHALL accept authenticated heartbeat and full/delta report messages for
system identity, Streams, operations, events, and metrics, and SHALL reject
reports from unknown or invalid sessions.

#### Scenario: Node reports runtime state

- **WHEN** an authenticated node posts a report containing Stream snapshots
- **THEN** the Hub updates only that node's resources, records the report time,
  and makes the resources available to aggregated queries

#### Scenario: Invalid agent session

- **WHEN** a report uses an expired or mismatched session token
- **THEN** the Hub returns `401` with a stable problem code and does not mutate
  the node registry

### Requirement: Aggregated and targeted resources

The Hub SHALL aggregate node resources for read APIs and SHALL preserve
`node_id` on every node-owned Stream, operation, event, and metric resource.

#### Scenario: List fleet Streams

- **WHEN** an operator requests the Hub Stream collection without a node filter
- **THEN** the response contains resources from all registered nodes with
  stable pagination and node identity fields

#### Scenario: Resolve duplicate Stream IDs

- **WHEN** two nodes report a Stream with the same local ID
- **THEN** the Hub exposes two distinct resources keyed by `(node_id, stream_id)`
  and never routes a command by local ID alone

### Requirement: Node-targeted command dispatch

The Hub SHALL require a target node for mutating commands and SHALL track each
command through queued, dispatched, acknowledged, running, and terminal states.

#### Scenario: Dispatch a lifecycle command

- **WHEN** an operator submits a start command for a selected node and Stream
- **THEN** the Hub returns an operation ID, queues an idempotent command for that
  node, and exposes dispatch/acknowledgement timestamps

#### Scenario: Target node is unavailable

- **WHEN** a command targets a stale or offline node
- **THEN** the Hub does not claim execution success and returns an operation with
  `node_unavailable` or a conflict problem according to the command contract

### Requirement: Command idempotency and reconciliation

The Hub and Agent SHALL use a stable command idempotency key and SHALL reconcile
in-flight operations when a node reconnects.

#### Scenario: Duplicate command delivery

- **WHEN** the Agent receives the same command more than once
- **THEN** it executes it at most once and returns the existing command result

#### Scenario: Node reconnects with an in-flight operation

- **WHEN** a node reconnects and reports a local operation associated with a Hub
  operation ID
- **THEN** the Hub resumes that operation instead of creating a duplicate

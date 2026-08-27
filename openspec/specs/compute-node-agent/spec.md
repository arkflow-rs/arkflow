# compute-node-agent Specification

## Purpose
TBD - created by archiving change make-control-plane-hub. Update Purpose after archive.
## Requirements
### Requirement: Compute node registration

The ArkFlow compute process SHALL support Agent mode with configured `hub_url`,
stable `node_id`, node credentials, and protocol version, and SHALL register
before declaring its control-plane session ready.

#### Scenario: Agent starts

- **WHEN** a compute node starts in Agent mode
- **THEN** it registers with the Hub, receives a session, and begins heartbeat
  and report loops without opening a Hub listener

#### Scenario: Hub is temporarily unavailable

- **WHEN** registration or heartbeat cannot reach the Hub
- **THEN** the node keeps its local data-plane runtime policy, retries with
  bounded backoff, and exposes the disconnected state locally

### Requirement: Node heartbeat and report

The Agent SHALL periodically send authenticated heartbeat messages and full
reports containing node identity, capabilities, health, Stream snapshots,
operation snapshots, bounded events, and metrics.

#### Scenario: Healthy report

- **WHEN** the Agent sends a report within the lease interval
- **THEN** the Hub refreshes the node lease and the report's resources become
  queryable through the Hub

#### Scenario: Report does not leak secrets

- **WHEN** a node serializes configuration or capability data for a report
- **THEN** credentials and secret configuration values are redacted or omitted

### Requirement: Command polling and execution

The Agent SHALL poll for commands addressed to its node, validate expiry and
idempotency, acknowledge receipt, execute supported local ControlPlane actions,
and report terminal results with correlation metadata.

#### Scenario: Execute a start command

- **WHEN** the Agent receives a valid start command for a local Stream
- **THEN** it acknowledges the command, invokes the local runtime manager, and
  reports observed state and terminal result to the Hub

#### Scenario: Reject an expired command

- **WHEN** a command's expiry time has passed before execution
- **THEN** the Agent rejects it without changing the Stream and reports an
  explicit expired outcome

### Requirement: Reconnect and graceful shutdown

The Agent SHALL re-register after session loss and SHALL stop its polling loops
gracefully without interrupting WAL shutdown semantics.

#### Scenario: Reconnect after Hub restart

- **WHEN** the Hub restarts and the Agent reconnects
- **THEN** the Agent re-authenticates, sends a full report, and allows the Hub to
  reconstruct current node resources

#### Scenario: Process shutdown

- **WHEN** the compute process receives a termination signal
- **THEN** it stops accepting new commands, reports draining when possible, and
  shuts down local Streams using the existing WAL-safe lifecycle


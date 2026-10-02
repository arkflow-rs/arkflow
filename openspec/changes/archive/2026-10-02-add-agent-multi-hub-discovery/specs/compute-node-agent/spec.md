## MODIFIED Requirements

### Requirement: Compute node registration

The ArkFlow compute process SHALL support Agent mode with a candidate list of
Hub addresses declared as the `hub_urls` YAML sequence (empty or absent keeps
standalone mode), trimmed of trailing `/` and deduplicated in order, plus a
stable `node_id`, node credentials, and protocol version, and SHALL register
before declaring its control-plane session ready. The legacy `hub_url` key
SHALL be rejected at parse time with an error carrying the rename and
list-form migration hint. Registration SHALL be attempted against candidates
in order, advancing when a candidate is unreachable or returns a standby 503.
The Hub SHALL issue the per-session credential from a cryptographically secure
random source so a session token is not enumerable or predictable.

#### Scenario: Agent starts

- **WHEN** a compute node starts in Agent mode
- **THEN** it registers with the Hub, receives a session, and begins heartbeat
  and report loops without opening a Hub listener

#### Scenario: Session credentials are not enumerable

- **WHEN** two nodes register, and again after any node re-registers
- **THEN** each issued session token is an independent high-entropy random value with no observable sequence relationship

#### Scenario: Hub is temporarily unavailable

- **WHEN** registration or heartbeat cannot reach the Hub
- **THEN** the node keeps its local data-plane runtime policy, retries with
  bounded backoff, and exposes the disconnected state locally

#### Scenario: Registration against a standby advances to the next candidate

- **WHEN** the active candidate returns 503 with the `hub_standby` error code
- **THEN** no session is created on that Hub and the Agent attempts
  registration against the next candidate without waiting out the full
  reconnect backoff

### Requirement: Reconnect and graceful shutdown

The Agent SHALL re-register after session loss and SHALL stop its polling loops gracefully without interrupting WAL shutdown semantics. Reconnection backoff SHALL incorporate random jitter so simultaneous session loss across the fleet does not produce synchronized re-registration bursts at the Hub. Re-registration SHALL rotate across the configured candidate list: a standby 503 advances to the next candidate immediately (bounded only by a short fixed pause), while a full failed cycle across all candidates is what triggers the jittered exponential backoff; the candidate that last accepted a registration SHALL be preferred (scanned first) on subsequent reconnects.

#### Scenario: Reconnect after Hub restart

- **WHEN** the Hub restarts and the Agent reconnects
- **THEN** the Agent re-authenticates, sends a full report, and allows the Hub to reconstruct current node resources

#### Scenario: Process shutdown

- **WHEN** the compute process receives a termination signal
- **THEN** it stops accepting new commands, reports draining when possible, and shuts down local Streams using the existing WAL-safe lifecycle

#### Scenario: Session expiry behaves as session loss

- **WHEN** the Hub rejects an Agent request because the session credential has expired
- **THEN** the Agent treats the rejection as session loss, re-registers with its stable boot identity, resumes its loops, and redelivers cached terminal results through the existing command replay path

#### Scenario: Fleet-wide session loss does not stampede the Hub

- **WHEN** many Agents lose their sessions at the same moment, for example after a Hub restart
- **THEN** each Agent's retry delay is randomized within the bounded backoff window, desynchronizing re-registration attempts

#### Scenario: Leader loss rotates to another Hub

- **WHEN** the connected Hub stops serving (process death or demotion to standby) while another configured Hub holds the lease
- **THEN** the Agent re-registers against the new leader within a bounded window (lease TTL plus one candidate scan), keeps its boot identity, and resumes heartbeat, report, and polling loops

#### Scenario: Standby advance does not consume the full backoff

- **WHEN** registration fails with the `hub_standby` error code and another candidate exists
- **THEN** the next candidate is attempted after only a short fixed pause, and the jittered exponential backoff is reserved for a complete failed cycle

## ADDED Requirements

### Requirement: Agent SHALL report hub failover observability

The Agent SHALL include in its periodic report the currently connected Hub base address and a cumulative hub-failover counter, and SHALL log every candidate switch with its reason (`standby_advance`, `transport_error`, or `leader_hint`) and the from/to addresses.

#### Scenario: Failover is visible in reports and logs

- **WHEN** the Agent switches its active Hub address
- **THEN** a log line records the switch reason with the from/to addresses, and subsequent reports carry the new connected Hub address and the incremented failover counter

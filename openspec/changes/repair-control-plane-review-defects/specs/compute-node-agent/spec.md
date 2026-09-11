## MODIFIED Requirements

### Requirement: Compute node registration

The ArkFlow compute process SHALL support Agent mode with configured `hub_url`,
stable `node_id`, node credentials, and protocol version, and SHALL register
before declaring its control-plane session ready. The Hub SHALL issue the
per-session credential from a cryptographically secure random source so a
session token is not enumerable or predictable.

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

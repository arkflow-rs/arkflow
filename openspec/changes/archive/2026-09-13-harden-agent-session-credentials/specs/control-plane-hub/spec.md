## ADDED Requirements

### Requirement: Agent session credential lifetime

The Hub SHALL assign every issued agent session credential an expiry timestamp at registration, derived from a configurable session TTL (default one hour). The Hub SHALL reject any agent request whose session credential has expired with `401` and a stable problem code, and SHALL NOT mutate the node registry in response to an expired credential. Re-registration SHALL replace the session credential and its expiry. The registration response SHALL advertise the session TTL so agents can observe the credential lifetime, and Agents that predate the field SHALL continue to interoperate.

#### Scenario: Idle session expires

- **WHEN** an agent's session TTL elapses without re-registration and the agent sends its next request
- **THEN** the Hub rejects it with `401` and a stable problem code without mutating the node registry

#### Scenario: Re-registration rotates the credential

- **WHEN** an agent re-registers after session loss
- **THEN** the Hub issues a fresh session credential with a new expiry, and the previous credential stops authenticating

#### Scenario: Registration advertises the TTL

- **WHEN** a node registers
- **THEN** the registration response includes the session TTL, and agents built before the field existed continue to register and operate unchanged

#### Scenario: Expired session does not lose a terminal command result

- **WHEN** the session TTL elapses while a dispatched command is executing and the terminal result submission is rejected with `401`
- **THEN** the Agent re-registers and the terminal result reaches the Hub through the existing command replay path, where command idempotency records exactly one terminal outcome

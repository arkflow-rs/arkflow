## MODIFIED Requirements

### Requirement: Command polling and execution

The Agent SHALL poll for commands addressed to its node, validate expiry and idempotency, acknowledge receipt, execute supported local ControlPlane actions, and report terminal results with correlation metadata. The Agent SHALL present its session credential in the `Authorization: Bearer` header and SHALL also present it in the legacy query parameter while the transition window is open, so an Agent deployed against a Hub that still requires the query credential keeps polling. The Hub SHALL accept the credential from either transport and SHALL prefer the header when both are present.

#### Scenario: Execute a start command

- **WHEN** the Agent receives a valid start command for a local Stream
- **THEN** it acknowledges the command, invokes the local runtime manager, and reports observed state and terminal result to the Hub

#### Scenario: Reject an expired command

- **WHEN** a command's expiry time has passed before execution
- **THEN** the Agent rejects it without changing the Stream and reports an explicit expired outcome

#### Scenario: Agent upgraded before the Hub

- **WHEN** an Agent that presents its credential in both transports polls a Hub that still requires the query parameter
- **THEN** the Hub authenticates the session and the Agent receives and reports commands without a session rebuild

#### Scenario: Hub upgraded before the Agent

- **WHEN** an Agent that presents its credential only in the query parameter polls a Hub that prefers the header
- **THEN** the Hub authenticates the session from the query parameter and records a deprecation warning without failing the request

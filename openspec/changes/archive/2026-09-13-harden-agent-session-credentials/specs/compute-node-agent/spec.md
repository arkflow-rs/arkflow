## MODIFIED Requirements

### Requirement: Command polling and execution

The Agent SHALL poll for commands addressed to its node, validate expiry and idempotency, acknowledge receipt, execute supported local ControlPlane actions, and report terminal results with correlation metadata. The Agent SHALL present its session credential only in the `Authorization: Bearer` header and MUST NOT place the session credential in the request URL. The Hub SHALL continue to accept the credential from the legacy query parameter for Agents predating this change and SHALL prefer the header when both are present, recording a deprecation warning for query-parameter authentication.

#### Scenario: Execute a start command

- **WHEN** the Agent receives a valid start command for a local Stream
- **THEN** it acknowledges the command, invokes the local runtime manager, and reports observed state and terminal result to the Hub

#### Scenario: Reject an expired command

- **WHEN** a command's expiry time has passed before execution
- **THEN** the Agent rejects it without changing the Stream and reports an explicit expired outcome

#### Scenario: Upgraded Agent requires an upgraded Hub

- **WHEN** an Agent from this release polls a Hub from an earlier release that requires the query credential
- **THEN** the poll fails and the Agent retries through its normal reconnect loop, and mixed-fleet deployments upgrade the Hub before upgrading Agents

#### Scenario: Hub upgraded before the Agent

- **WHEN** an Agent that presents its credential only in the query parameter polls a Hub that prefers the header
- **THEN** the Hub authenticates the session from the query parameter and records a deprecation warning without failing the request

### Requirement: Reconnect and graceful shutdown

The Agent SHALL re-register after session loss and SHALL stop its polling loops gracefully without interrupting WAL shutdown semantics. Reconnection backoff SHALL incorporate random jitter so simultaneous session loss across the fleet does not produce synchronized re-registration bursts at the Hub.

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

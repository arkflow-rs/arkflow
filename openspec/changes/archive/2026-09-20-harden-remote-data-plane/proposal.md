## Why

The cross-node shuffle transport currently accepts unauthenticated TCP peers and trusts the `quad` in each frame; `TcpEdgeTransport::connect` opens a plain socket ([crates/arkflow-core/src/executor/remote.rs:549-579](../../../crates/arkflow-core/src/executor/remote.rs#L549-L579)) while the listener accepts peers without identity validation. A malformed Arrow IPC body can also panic while slicing `self.bytes[start..end]` ([crates/arkflow-core/src/executor/remote.rs:400-404](../../../crates/arkflow-core/src/executor/remote.rs#L400-L404)). The same path contains unbounded connection/failure/receipt queues and allocates from a peer-declared frame length, so a reachable peer can cause denial of service or inject control/data envelopes.

The existing network-shuffle specification already requires invalid frames to be rejected, end-to-end bounded backpressure, and fail-closed cleanup. This change makes those guarantees true for hostile, slow, malformed, and disconnected peers before split placement is treated as production-safe.

## What Changes

- **BREAKING** Require an authenticated data-plane handshake before accepting Data, Receipt, or Signal frames when network shuffle is enabled.
- Validate all frame, IPC-piece, Flatbuffer, and Arrow body lengths before slicing or allocating; protocol violations close only the affected connection and report a structured edge failure.
- Replace unbounded accepted-connection, failure, receipt, and pending-receipt paths with configured bounds and explicit overflow behavior.
- Add connection and read-idle limits, per-frame limits, and deterministic cleanup of registered quads and pending acknowledgements.
- Authorize the peer against the expected node, Job generation, and quad instead of trusting the frame header alone.
- Preserve the current fail-closed behavior after an established connection breaks; transparent reconnect is explicitly out of scope.
- Add adversarial protocol, authentication, resource-limit, and cleanup tests.

## Non-goals

- Implementing transparent reconnect after an established edge failure.
- Changing Envelope ordering, key-group routing, or the three-state receipt protocol.
- Providing multi-Hub high availability or changing checkpoint recovery semantics.
- Replacing the existing TCP transport with a new message broker.

## Capabilities

### New Capabilities

- `authenticated-network-shuffle`: Authenticated and authorized cross-node data-plane sessions with bounded protocol resources.

### Modified Capabilities

- `network-shuffle-data-plane`: Strengthen malformed-frame handling, boundedness, peer authorization, and connection cleanup requirements.

## Impact

- Affected code: `crates/arkflow-core/src/executor/remote.rs`, executor configuration, and remote-edge tests.
- Affected deployment configuration: network-shuffle peers must provide an authentication mechanism and resource limits when the feature is enabled.
- Affected operations: unauthenticated or unauthorized peers will be rejected; malformed or over-limit sessions will fail closed.
- No new runtime dependency is required if the initial implementation uses the existing configured secret and a framed challenge/MAC protocol; TLS/mTLS remains a separately selectable transport decision.

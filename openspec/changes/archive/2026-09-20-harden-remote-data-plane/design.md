## Context

The network shuffle implementation uses one TCP connection per quad and a fixed 24-byte frame header. The downstream side waits for a registered quad, decodes Arrow IPC data, and sends lifecycle receipts back over the same connection. The current implementation already has bounded local Envelope channels and fail-closed edge semantics, but the TCP boundary is not authenticated, an IPC body length can escape its piece, and several control paths are unbounded.

The implementation must preserve FIFO ordering, barrier/watermark/EOS placement, the existing Ack/Held/Released protocol, and the current behavior that an established connection failure fails the attempt closed. Tests also use `tokio::io::duplex`, so authentication and limits must be injectable without requiring real sockets.

## Goals / Non-Goals

**Goals:**

- Make malformed input return a protocol error rather than panic.
- Authenticate and authorize the peer before accepting a data or control frame.
- Bound every queue and pending receipt structure that can grow because of a peer.
- Enforce frame, connection, and idle-read limits before allocating or waiting indefinitely.
- Guarantee cleanup of quad registrations and pending acknowledgements on every connection exit.
- Preserve the current fail-closed and at-least-once recovery contract.

**Non-Goals:**

- Transparent reconnect after an established connection failure.
- Payload encryption or certificate management in this change; mTLS can replace the initial shared-secret authenticator later.
- Changes to Envelope routing, checkpoint barrier semantics, or receipt aggregation.

## Decisions

1. **Use a pluggable session authenticator with a shared-secret HMAC implementation initially.**
   The production TCP transport will send a handshake containing protocol version, source node, destination node, Job id, generation, quad, and a fresh nonce. The peer proves possession of the configured secret over the canonical handshake bytes. The manager verifies the expected node/job/generation/quad before registering the stream. Authentication is an explicit interface so duplex tests can use a deterministic test authenticator and a later mTLS transport can be added without changing graph wiring.

   A handshake-only secret exchange is sufficient to reject arbitrary network callers for the current threat model; the protocol will reserve an authenticated-session field for a future per-frame MAC. Certificate-based mTLS is an alternative, but would add certificate lifecycle and async TLS configuration to this focused hardening change.

2. **Add a handshake frame kind and reject all non-handshake frames before authentication.**
   The connection is one quad, so the handshake's quad must equal the expected quad. Receipt connections use the same authenticated session in the reverse direction. A missing, duplicated, stale, or mismatched handshake terminates the stream before it reaches the inbound registry.

3. **Make limits explicit in `NetworkManagerConfig`.**
   The config carries channel capacity, maximum accepted connections, maximum pending receipts per quad, maximum frame length, and read-idle timeout. `NetworkManager::new` retains a safe default for existing tests, while production Agent wiring constructs the config from validated deployment settings. A full accepted queue closes the new connection; a full pending-receipt limit fails the edge so it cannot silently lose an acknowledgement.

4. **Use one cleanup guard for inbound and outbound sessions.**
   The guard removes served quads, removes the outbound pending map entry, aborts outstanding branches, and reports a non-EOS connection death. All protocol errors and I/O failures go through the same exit path. The implementation will not rely on `catch_unwind` to make malformed input safe; parser slices and Flatbuffer/body lengths must be proven safe before indexing.

5. **Apply timeouts around each header and payload read.**
   A peer that sends only part of a frame cannot retain a task forever. The timeout is reset after each successfully read frame and is configurable for tests and deployments. TCP write backpressure remains asynchronous and bounded by the local channels.

## Risks / Trade-offs

- [Risk] A new required handshake makes mixed-version shuffle deployments fail to connect. → Advertise a data-plane protocol version during node registration and reject incompatible peers during placement; roll out the Hub/Agent/core versions together.
- [Risk] A too-small frame or receipt limit can fail legitimate large batches. → Validate limits at Job/Agent startup, expose them in diagnostics, and make them configurable with conservative defaults.
- [Risk] Authentication failure reports can be noisy during rolling restarts. → Classify handshake failures separately from task failures and include peer identity/quad in structured logs without logging the secret.
- [Risk] A bounded receipt queue can block downstream acknowledgement callbacks. → Keep the queue sized with the Envelope capacity and fail the edge on overflow; never drop Acked receipts silently.

## Migration Plan

1. Add the protocol/config types and parser tests while network shuffle remains disabled by default.
2. Deploy Hub/Agent/core versions that understand the handshake and advertise the same protocol version.
3. Require the data-plane secret for split placement; existing colocated Jobs remain unchanged and do not open a data port.
4. Enable split placement only after the malformed-frame, authentication-negative, slow-peer, and two-node recovery tests pass.
5. Rollback is to disable split placement and return to colocated mode; old peers cannot be mixed with the new authenticated data-plane mode.

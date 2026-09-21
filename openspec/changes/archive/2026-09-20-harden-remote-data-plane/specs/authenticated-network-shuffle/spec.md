## ADDED Requirements

### Requirement: Cross-node sessions SHALL authenticate before carrying envelopes

When network shuffle is enabled, a data-plane connection SHALL complete an authenticated handshake before either side accepts Data, Signal, or Receipt frames. The handshake SHALL bind the protocol version, source and destination node identities, Job identity, generation, and quad to a fresh challenge so a credential cannot authorize a different edge.

#### Scenario: Valid peer opens a quad

- **WHEN** a peer presents the configured credential, the supported protocol version, the expected node identities, Job generation, and the registered quad
- **THEN** the connection becomes an authenticated session and normal frames are accepted

#### Scenario: Unknown peer is rejected

- **WHEN** a connection presents no credential or an invalid credential
- **THEN** the listener closes it before registering a quad or delivering an envelope

#### Scenario: Stale generation cannot inject data

- **WHEN** a connection authenticates a valid node but names a Job generation that is not current for the registered quad
- **THEN** the session is rejected and no Data, Barrier, Watermark, or EOS reaches the local graph

#### Scenario: Frame arrives before handshake

- **WHEN** the first frame on a connection is Data, Signal, or Receipt instead of the handshake
- **THEN** the connection closes with a protocol/authentication error and leaves no registry or pending-ack entry

### Requirement: Peer-controlled protocol resources SHALL remain bounded

The data plane SHALL enforce configured upper bounds for frame payloads, accepted connections, idle reads, receipt queues, and pending receipt entries before allocating untrusted sizes or retaining peer-owned state. Reaching a bound SHALL apply backpressure or fail the affected edge explicitly; it MUST NOT silently drop a required acknowledgement or grow an unbounded queue.

#### Scenario: Oversized frame header is rejected before allocation

- **WHEN** a peer declares a frame payload larger than the configured maximum
- **THEN** the receiver closes the connection without allocating the declared payload buffer

#### Scenario: Slow peer is closed by idle timeout

- **WHEN** a peer sends an incomplete header or payload and makes no progress for the configured read-idle timeout
- **THEN** the connection closes, its quads are cleaned up, and outstanding branches are failed closed

#### Scenario: Receipt pressure reaches its limit

- **WHEN** a peer causes the pending receipt or receipt queue limit to be reached
- **THEN** the edge reports an explicit bounded-resource failure and does not silently discard Acked, Held, or Released state

### Requirement: Malformed Arrow IPC SHALL be rejected without a task panic

The decoder SHALL validate every IPC piece's Flatbuffer and body bounds before slicing. Any negative, overflowing, truncated, or inconsistent body length SHALL return a protocol error. A malformed frame SHALL terminate only the affected connection and SHALL execute the normal quad and pending-ack cleanup path.

#### Scenario: IPC body exceeds its piece

- **WHEN** a valid-looking Arrow message declares a body longer than the bytes in its IPC piece
- **THEN** decoding returns an error, the connection closes, and no worker task panics

#### Scenario: Truncated IPC piece is received

- **WHEN** the piece length or Flatbuffer length extends beyond the frame payload
- **THEN** the receiver rejects the frame before indexing and releases all session resources

### Requirement: Connection termination SHALL clean up both edge directions

Every normal close, protocol error, timeout, cancellation, and I/O failure SHALL remove the session's served quad registrations, remove stale outbound pending-receipt entries, abort unresolved branches, and report a non-EOS termination as an edge failure. A clean EOS SHALL not be reported as a mid-stream failure.

#### Scenario: Peer disconnects before EOS

- **WHEN** an authenticated peer disconnects while a served quad has not forwarded EOS
- **THEN** the local input closes, the affected attempt receives a failure, and no stale quad remains registered

#### Scenario: Kernel cancellation closes a session

- **WHEN** the Job kernel is cancelled while a remote session has pending data or receipts
- **THEN** the session exits, pending branches are aborted, and all registry entries are removed

# authenticated-network-shuffle Specification

## Purpose
Authenticated cross-node shuffle transport surface for ArkFlow's distributed data plane: remote edges and the data-plane TCP listener exist only through authenticated sessions with configured credentials, and the unauthenticated transport surface is confined to the test boundary. Created by syncing change close-hardening-loose-ends.

## Requirements

### Requirement: The unauthenticated transport surface SHALL be test-only

A `NetworkManager` without configured data-plane credentials SHALL NOT open remote edges or bind the production TCP listener outside the test boundary. The legacy unauthenticated edge constructors SHALL exist only within `cfg(test)` builds, and binding the data-plane listener without credentials MUST fail explicitly instead of serving unauthenticated frames.

#### Scenario: Credentials-less manager cannot bind a listener

- **WHEN** a `NetworkManager` is constructed without data-plane credentials and `bind_tcp` is called
- **THEN** the bind fails with an explicit configuration error and no listener socket is created

#### Scenario: Unauthenticated edge constructors are absent from non-test builds

- **WHEN** the workspace compiles a non-test target
- **THEN** the unauthenticated edge constructors (`open_edge`, `open_edge_with_stream`, `open_edge_deferred`) are not part of `NetworkManager`'s public API, and remote edges open only through the authenticated session APIs

#### Scenario: Test boundary keeps in-memory transports

- **WHEN** the executor's own test suite builds
- **THEN** the legacy constructors remain available so in-memory (duplex-stream) tests can exercise the frame codec and edge semantics without network secrets, while TCP-path tests use authenticated sessions

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

### Requirement: 数据面 SHALL 支持以舰队 CA 锚定的 mTLS

配置了 TLS 材料（节点证书、私钥、舰队 CA 三者齐备）时，数据面 SHALL 以 TLS 承载全部跨节点连接：入站连接先完成 TLS 握手——要求对端出示证书且链锚定舰队 CA——再进入既有 HMAC 会话握手；出站连接以同一 CA 校验服务端证书（ServerName 固定为 `arkflow-data-plane`，节点证书须含该 SAN）并出示自身证书。TLS 握手失败与 TCP 失败同预算（重连退避/fail-closed 不变）。未配置 TLS 时全部行为与现状逐字节一致；三份材料缺一 SHALL 以显式配置错误拒绝启动数据面。HMAC 会话握手与 TLS 并存：TLS 提供传输加密与"舰队 CA 签发证书"门禁，节点身份与作业/代数绑定仍由 HMAC 层认证。

#### Scenario: TLS 两端完成握手并传输帧

- **WHEN** 两个节点都以同一舰队 CA 签发的证书启用 TLS，一条远程边建立
- **THEN** TLS 与 HMAC 双层握手成功，数据帧照常往返，语义与明文路径一致

#### Scenario: 明文客户端连接 TLS 服务端被拒

- **WHEN** 一个未启用 TLS 的节点连接启用 TLS 的节点
- **THEN** 入站 TLS 握手失败，连接关闭，计入既有失败路径（不降级为明文）

#### Scenario: 非舰队 CA 签发的证书被拒

- **WHEN** 对端出示的证书链不锚定配置的舰队 CA
- **THEN** 握手失败，连接拒绝，无数据帧被接受

#### Scenario: 材料不齐拒绝启动

- **WHEN** 证书/私钥/CA 三份材料只配置了部分
- **THEN** Agent 启动以显式配置错误失败，数据面不绑定端口

#### Scenario: 未配置 TLS 行为不变

- **WHEN** 未配置任何 TLS 材料的部署升级
- **THEN** 数据面全部行为与升级前逐字节一致

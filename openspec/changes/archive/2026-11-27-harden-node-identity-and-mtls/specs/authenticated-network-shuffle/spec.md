## MODIFIED Requirements

### Requirement: Cross-node sessions SHALL authenticate before carrying envelopes

When network shuffle is enabled, a data-plane connection SHALL complete an authenticated handshake before either side accepts Data, Signal, or Receipt frames. The handshake SHALL bind the protocol version, source and destination node identities, Job identity, generation, and quad to a fresh challenge so a credential cannot authorize a different edge. When the Hub dispatches a split-placed Job, it SHALL provision an independent 32-byte per-edge secret for each directed cross-node pair `(source_node, destination_node)` and deliver to each node only the `edge_secrets` touching that node; `NetworkManager` SHALL verify the handshake MAC and derive per-connection `FrameCrypto` keys using that per-edge secret when registered (falling back to the node's configured `shared_secret` only when no per-edge secret is registered, and failing closed when neither exists). An Agent running data-plane mTLS (`ARKFLOW_DATA_PLANE_TLS_*`) MAY initialize `NetworkManager` with `DataPlaneCredentials::hub_provisioned(local_node)` without configuring a fleet-wide `ARKFLOW_DATA_PLANE_SECRET`, in which case only Hub-provisioned per-edge secrets can authorize data-plane edges.

#### Scenario: Valid peer opens a quad

- **WHEN** a peer presents the configured or per-edge credential, the supported protocol version, the expected node identities, Job generation, and the registered quad
- **THEN** the connection becomes an authenticated session and normal frames are accepted

#### Scenario: Third node in the same Job cannot spoof a peer edge

- **WHEN** a split-placed Job runs across `"node-a"`, `"node-b"`, and `"node-c"` with Hub-provisioned `edge_secrets`, and `"node-c"` attempts to open a handshake to `"node-b"` claiming `source_node = "node-a"`
- **THEN** `"node-b"` rejects the handshake before claiming the replay nonce or delivering any envelope because `"node-c"` does not possess the `"node-a->node-b"` edge secret

#### Scenario: Hub-provisioned shuffle works under mTLS without a fleet-wide shared secret

- **WHEN** two Agents configure `ARKFLOW_DATA_PLANE_TLS_{CERT,KEY,CA}` without `ARKFLOW_DATA_PLANE_SECRET` and receive a Hub split-placement `job_start` with `edge_secrets`
- **THEN** both Agents advertise `network_shuffle` and complete encrypted, per-edge-authenticated data-plane transfers

#### Scenario: Unknown peer is rejected

- **WHEN** a connection presents no credential or an invalid credential
- **THEN** the listener closes it before registering a quad or delivering an envelope

#### Scenario: Stale generation cannot inject data

- **WHEN** a connection authenticates a valid node but names a Job generation that is not current for the registered quad
- **THEN** the session is rejected and no Data, Barrier, Watermark, or EOS reaches the local graph

#### Scenario: Frame arrives before handshake

- **WHEN** the first frame on a connection is Data, Signal, or Receipt instead of the handshake
- **THEN** the connection closes with a protocol/authentication error and leaves no registry or pending-ack entry

### Requirement: 数据面 SHALL 支持以舰队 CA 锚定的 mTLS

配置了 TLS 材料（节点证书、私钥、舰队 CA 三者齐备）时，数据面 SHALL 以 TLS 承载全部跨节点连接：入站连接先完成 TLS 握手——要求对端出示证书且链锚定舰队 CA——再进入既有 HMAC 会话握手；出站连接以同一 CA 校验服务端证书（ServerName 固定为 `arkflow-data-plane`，节点证书须含该 SAN）并出示自身证书。若对端证书同时携带 `URI:urn:arkflow:node:<id>` SAN，入站握手 SHALL 校验 `<id>` 等于请求的 `source_node`、出站连接 SHALL 校验 `<id>` 等于目标 `destination_node`；仅含 `DNS:arkflow-data-plane` 的传统证书保持兼容并通过 per-edge/shared HMAC 层认证节点身份。TLS 握手失败与 TCP 失败同预算（重连退避/fail-closed 不变）。未配置 TLS 时全部行为与现状逐字节一致；三份材料缺一 SHALL 以显式配置错误拒绝启动数据面。HMAC 会话握手与 TLS 并存：TLS 提供传输加密与"舰队 CA 签发证书"门禁，节点身份与作业/代数绑定由 HMAC 层（及可选 SAN 绑定）共同认证。

#### Scenario: TLS 两端完成握手并传输帧

- **WHEN** 两个节点都以同一舰队 CA 签发的证书启用 TLS，一条远程边建立
- **THEN** TLS 与 HMAC 双层握手成功，数据帧照常往返，语义与明文路径一致

#### Scenario: 数据面拒绝 SAN 节点身份不匹配的证书

- **WHEN** 节点证书携带 `URI:urn:arkflow:node:node-a` 并尝试以 `node-b` 身份连接或接收目标为 `node-b` 的连接
- **THEN** 连接在 TLS/握手层 fail-closed 拒绝，无数据帧被接受

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

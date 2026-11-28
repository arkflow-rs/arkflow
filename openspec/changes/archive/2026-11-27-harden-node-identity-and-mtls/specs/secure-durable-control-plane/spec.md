## MODIFIED Requirements

### Requirement: Missing credentials SHALL fail closed

In secure mode, an absent operator token SHALL not create an implicit Admin principal, and an absent node token SHALL not authorize registration. Valid credentials SHALL continue to use constant-time comparison and the existing session-token checks. On a Hub with durable storage, per-node credentials (`arkn_`) SHALL fail closed on unknown, revoked, expired, or `node_id`-mismatched tokens, and the shared `node_token` SHALL fail closed once the target `node_id` holds an active per-node credential (unless `allow_shared_node_token_break_glass` is set). When Hub mTLS (`tls_ca`) is configured, `/api/v1/agent/*` routes SHALL reject requests lacking a verified peer certificate, and any peer certificate carrying a `URI:urn:arkflow:node:<id>` SAN whose `<id>` differs from the request's `node_id` SHALL fail closed with `401 Unauthorized`.

#### Scenario: Operator request omits a token

- **WHEN** a secure Hub receives an operator API request without credentials
- **THEN** it returns unauthorized and performs no mutating operation

#### Scenario: Node registers without a token

- **WHEN** a secure Hub receives a registration request without a valid per-node or bootstrap credential
- **THEN** registration is rejected and no node lease is created

#### Scenario: Valid credentials are supplied

- **WHEN** the operator or node presents a valid credential (and matching mTLS client certificate when configured)
- **THEN** the request passes the existing authorization and auditing path

#### Scenario: Agent route rejects request missing client certificate under Hub mTLS

- **WHEN** the Hub runs with `ARKFLOW_HUB_TLS_CA` configured (`agent_required`) and a caller posts to `/api/v1/agent/register` without presenting a client certificate
- **THEN** the Hub rejects the request with `401 Unauthorized` while `/health` and `/readiness` continue serving over HTTPS

#### Scenario: Agent client certificate SAN mismatch is rejected

- **WHEN** a node presents a CA-trusted client certificate bearing `URI:urn:arkflow:node:node-a` while registering or reporting as `node-b`
- **THEN** the Hub rejects the request with `401 Unauthorized` and audits `cert_node_identity_mismatch`

### Requirement: Hub SHALL support a TLS listener

配置了 TLS 证书与私钥（`ARKFLOW_HUB_TLS_CERT` / `ARKFLOW_HUB_TLS_KEY`，PEM 路径）时，Hub SHALL 以 TLS 承载全部控制面 HTTP 流量：监听器在传输层完成 TLS，路由、认证、readiness 语义不变。只配置其一 SHALL 以显式配置错误拒绝启动。当配置了 `ARKFLOW_HUB_TLS_CA`（`tls_ca`）时，Hub SHALL 以 `WebPkiClientVerifier` 校验客户端证书：默认模式 `agent_required` 在 TLS 握手层允许未出示证书的探测/控制台连接同时拒绝一切不受信任的证书，并在 `/api/v1/agent/*` 路由强制要求已验证的客户端证书；`required` 模式则在 TLS 握手层拒绝一切未出示有效客户端证书的连接。`tls_ca` 未配齐 `tls_cert`/`tls_key` 或 `tls_client_auth != none` 但缺少 `tls_ca` 时 SHALL 拒绝启动。Agent 侧 SHALL 支持 `ARKFLOW_AGENT_TLS_CA`（验证 Hub 服务端证书）与成对的 `ARKFLOW_AGENT_TLS_CERT` / `ARKFLOW_AGENT_TLS_KEY`（mTLS 客户端证书），后者缺一或无法读取/解析时 SHALL 在启动期显式失败。未配置任何 TLS 变量时监听与 Agent 客户端行为与现状逐字节一致。

#### Scenario: TLS Hub serves the API over https

- **WHEN** the Hub starts with a certificate and key configured and an operator requests `/readiness` over https
- **THEN** the TLS handshake succeeds and the readiness response matches the plaintext semantics

#### Scenario: Half-configured TLS refuses to start

- **WHEN** only one of the certificate or the key is configured, or `tls_ca` is set without `tls_cert` and `tls_key`
- **THEN** startup fails with an explicit configuration error before binding

#### Scenario: Agent connects to mTLS Hub with client certificate and CA

- **WHEN** the Hub configures `tls_cert`, `tls_key`, and `tls_ca`, and the Agent configures `ARKFLOW_AGENT_TLS_CA`, `ARKFLOW_AGENT_TLS_CERT`, and `ARKFLOW_AGENT_TLS_KEY`
- **THEN** the mTLS handshake succeeds and Agent registration, heartbeats, reports, and command polling succeed over HTTPS

#### Scenario: Partial Agent client TLS material fails startup

- **WHEN** an Agent sets `ARKFLOW_AGENT_TLS_CERT` without `ARKFLOW_AGENT_TLS_KEY` (or vice versa)
- **THEN** Agent startup fails with an explicit configuration error instead of silently omitting client authentication

#### Scenario: No TLS configuration keeps plaintext behavior

- **WHEN** no TLS variables are set
- **THEN** the Hub binds and serves exactly as before

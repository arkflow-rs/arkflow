# secure-durable-control-plane Specification

## Purpose
Secure and durable defaults for the Hub control plane: an externally reachable Hub requires durable storage and credentials at startup, readiness follows durable recovery, and missing credentials fail closed. Created by syncing change secure-durable-control-plane.
## Requirements
### Requirement: External Hub startup SHALL require durable storage and credentials

When the Hub binds a non-loopback address, startup SHALL require a configured durable storage path, operator credential, and node credential. The server MUST refuse to bind or report ready when any required item is missing. An insecure local mode MAY bypass these checks only when explicitly enabled and the bind address is loopback.

#### Scenario: External bind lacks storage

- **WHEN** the Hub is configured to bind a non-loopback address without durable storage
- **THEN** startup fails before the listener binds and explains how to configure storage

#### Scenario: External bind lacks credentials

- **WHEN** the Hub is configured to bind a non-loopback address without an operator or node credential
- **THEN** startup fails before the listener binds and does not expose unauthenticated APIs

#### Scenario: Explicit local development mode

- **WHEN** the Hub binds loopback, durable storage is omitted, and insecure-local mode is explicitly enabled
- **THEN** the server may start for development while marking the mode in diagnostics

#### Scenario: Insecure mode is attempted externally

- **WHEN** insecure-local mode is enabled with a non-loopback bind address
- **THEN** startup fails instead of weakening external authentication

### Requirement: Hub readiness SHALL follow durable recovery

The Hub SHALL restore persisted Jobs, operations, desired state, leases, and checkpoint pointers before binding its listener or reporting ready. Storage recovery errors SHALL leave the server unavailable. When HA is enabled, readiness SHALL additionally require leadership: a standby Hub (one that does not hold the control-plane lease) SHALL report not-ready with its role, and SHALL NOT serve operator or agent APIs except health, liveness, readiness, and metrics.

#### Scenario: Storage recovery succeeds

- **WHEN** the configured store opens and persisted records are restored successfully
- **THEN** the listener binds and readiness can become healthy with the restored control-plane view

#### Scenario: Storage recovery fails

- **WHEN** opening or restoring the configured store returns an error
- **THEN** startup fails or readiness remains unavailable, and the Hub does not serve a partial in-memory state

#### Scenario: Standby reports not-ready with its role

- **WHEN** HA is enabled and the Hub does not hold the control-plane lease
- **THEN** readiness reports not-ready, includes the standby role, and mutating or reading operator/agent APIs return 503 instead of serving the standby's in-memory view

#### Scenario: Promoted Hub becomes ready after re-recovery

- **WHEN** a standby acquires the lease and completes durable re-recovery
- **THEN** readiness becomes healthy with the leader role before any reconcile tick dispatches operations

### Requirement: Missing credentials SHALL fail closed

In secure mode, an absent operator token SHALL not create an implicit Admin principal, and an absent node token SHALL not authorize registration. Valid credentials SHALL continue to use constant-time comparison and the existing session-token checks.

#### Scenario: Operator request omits a token

- **WHEN** a secure Hub receives an operator API request without credentials
- **THEN** it returns unauthorized and performs no mutating operation

#### Scenario: Node registers without a token

- **WHEN** a secure Hub receives a registration request without the configured node credential
- **THEN** registration is rejected and no node lease is created

#### Scenario: Valid credentials are supplied

- **WHEN** the operator or node presents the configured credential
- **THEN** the request passes the existing authorization and auditing path

### Requirement: Hub SHALL support a TLS listener

配置了 TLS 证书与私钥（`ARKFLOW_HUB_TLS_CERT` / `ARKFLOW_HUB_TLS_KEY`，PEM 路径）时，Hub SHALL 以 TLS 承载全部控制面 HTTP 流量：监听器在传输层完成 TLS，路由、认证、readiness 语义不变。只配置其一 SHALL 以显式配置错误拒绝启动。未配置时监听行为与现状逐字节一致。Agents SHALL be able to reach a TLS Hub via an `https://` hub URL with no additional configuration.

#### Scenario: TLS Hub serves the API over https

- **WHEN** the Hub starts with a certificate and key configured and an operator requests `/readiness` over https
- **THEN** the TLS handshake succeeds and the readiness response matches the plaintext semantics

#### Scenario: Half-configured TLS refuses to start

- **WHEN** only one of the certificate or the key is configured
- **THEN** startup fails with an explicit configuration error before binding

#### Scenario: No TLS configuration keeps plaintext behavior

- **WHEN** no TLS variables are set
- **THEN** the Hub binds and serves exactly as before

### Requirement: 独立引擎控制平面 SHALL 应用启动护栏与 default-deny 认证
standalone 引擎的控制 API SHALL 在启动前校验绑定与凭据组合：绑定地址非回环且未配置 `api_token` 时 SHALL 拒绝启动，除非配置显式声明 `insecure_local`（沿用 Hub 既有语义）。运行期认证 SHALL default-deny：全部 API 端点（读与写）经统一认证中间件——配置了 `api_token` 时所有请求要求有效 Bearer token；未配置 token 时中间件对回环绑定直通，对非回环绑定仅在显式 `insecure_local` 声明下运行（该组合下全部 API 端点无认证可达，启动警告已指明后果）。handler 内散落的自查 SHALL 由中间件统一取代。两类豁免是**有意的**且不属于"全部 API 端点"：健康探针（`/health`、`/readiness`、`/liveness` 及其短别名）与顶层 `/metrics`（Prometheus 抓取端点，区别于 `/api/v1/metrics`）保持无认证——后者文档中已注明"勿暴露到非受信网络"。

#### Scenario: 非回环无 token 拒绝启动

- **WHEN** standalone 引擎配置 `health_check.address: "0.0.0.0:8080"` 且未设 `health_check.api_token`
- **THEN** 启动以明确的配置错误失败，文案指明非回环绑定必须设置 token 或显式 `insecure_local`

#### Scenario: 显式 insecure_local 可启动

- **WHEN** 非回环绑定 + 未设 token + 显式 `insecure_local`
- **THEN** 启动进行并输出显著警告（对齐 Hub 同语义的警告形态），运行期认证中间件对全部 API 端点放行无凭据请求（该行为是显式选择，由警告暴露后果）

#### Scenario: 设 token 后读端点同样要求认证

- **WHEN** 配置了 `api_token`，请求 GET `/api/v1/streams`（或 events/system/metrics 任一读端点）且不带 Bearer token
- **THEN** 返回 401；带正确 token 返回 200（读写一致，无裸读端点）

#### Scenario: 回环无 token 保持零摩擦

- **WHEN** 默认回环绑定且未设 token
- **THEN** 本地读写请求照常通过（本地开发体验不变）

#### Scenario: 错误 token 常量时间拒绝

- **WHEN** 携带错误 Bearer token 的请求
- **THEN** 返回 401，比较保持常量时间（既有 `ct_eq` 实现不变）


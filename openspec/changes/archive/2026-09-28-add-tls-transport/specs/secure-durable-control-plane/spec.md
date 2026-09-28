# secure-durable-control-plane Delta

## ADDED Requirements

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

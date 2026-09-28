## 1. Dependencies and data-plane TLS core

- [x] 1.1 Add `tokio-rustls` to workspace deps (arkflow-core) and `rcgen` as an arkflow-core dev-dep
- [x] 1.2 `DataPlaneTlsConfig::from_pem(cert, key, ca)` building a rustls connector (own cert, CA root, ServerName `arkflow-data-plane`) and acceptor (own cert, client certs required against the CA); material errors are explicit
- [x] 1.3 `NetworkManagerConfig.tls: Option<DataPlaneTlsConfig>` (validate: no partial states) + `bind_tcp` accepts then TLS-handshakes inbound connections (failures take the existing connection-failure path)
- [x] 1.4 `TcpEdgeTransport.tls` — outbound connect wraps the TCP stream with the connector; handshake failures share the existing retry/backoff budget

## 2. Plumbing

- [x] 2.1 `RemoteEdgeContext` carries the TLS config; `graph.rs` passes it into `TcpEdgeTransport`
- [x] 2.2 Agent env `ARKFLOW_DATA_PLANE_TLS_CERT/_KEY/_CA`: all three or none — partial = explicit startup error; build once and wire into the manager and edge contexts

## 3. Control-plane TLS

- [x] 3.1 `ServerConfig.tls_cert/tls_key`; `serve_hub` wraps the listener with a tokio-rustls acceptor via an axum `Listener` adapter when both are set; one-without-the-other fails startup
- [x] 3.2 Bin env plumbing `ARKFLOW_HUB_TLS_CERT`/`ARKFLOW_HUB_TLS_KEY`

## 4. Tests

- [x] 4.1 Data plane: two managers over loopback with fleet-CA certs exchange a frame end to end (TLS+HMAC); a plaintext client against a TLS server fails closed; a foreign-CA cert is rejected; default-off keeps every existing remote test green
- [x] 4.2 Hub: TLS listener serves a request with a self-signed cert (reqwest accepts with danger-accept-invalid-certs in the test); half-config fails startup; unconfigured keeps plaintext behavior (existing tests)
- [x] 4.3 `cargo test --workspace --all-targets` green; clippy adds no new warnings

## 5. Docs

- [x] 5.1 En + zh-Hans: data-plane mTLS config (fleet CA, SAN `arkflow-data-plane`, openssl generation example, rolling-enable window) and Hub TLS env vars
- [x] 5.2 `pnpm docs:check` passes

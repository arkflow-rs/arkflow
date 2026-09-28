## Why

两个平面都以明文传输：

1. **数据面**（跨节点 shuffle）：`remote.rs` 的 TCP 全明文（`TcpEdgeTransport::connect` at `remote.rs:883`、`bind_tcp` listener at `remote.rs:1824`），安全完全依赖全舰队单一 HMAC 共享密钥（`agent.rs` 数据面密钥缺省回退 `node_token`）——任何持密钥者可窃听与伪造任何节点的流量。
2. **控制面**（Hub HTTP）：`serve_hub` 用裸 `TcpListener`（`lib.rs`），文档要求"生产用外部反代终止 TLS"——Hub 自身无 TLS 能力。

`remote.rs` 已有 `RemoteStream` 字节流抽象（`remote.rs:855-857`，任何 AsyncRead+AsyncWrite 即实现），TLS 包装是窄改动。

## What Changes

- **数据面 mTLS**（可选，默认关闭=逐字节明文现状）：`NetworkManagerConfig` 增加 `tls: Option<DataPlaneTlsConfig>`（节点自身证书+私钥+舰队 CA 的 PEM 构建出的 rustls connector/acceptor）。配置存在时：入站连接先完成 TLS 握手（要求并校验对端证书链锚定舰队 CA）再进入现有 HMAC 会话握手；出站连接以同一 CA 校验服务端证书（SANS 固定名 `arkflow-data-plane`）并出示自身证书。HMAC 握手保留（传输加密+证书门禁叠加既有作业/代数绑定的会话认证）。证书不匹配/无法验证 = 连接失败，走既有 fail-closed 路径。
- **控制面 TLS**（可选）：`ARKFLOW_HUB_TLS_CERT`/`ARKFLOW_HUB_TLS_KEY`（PEM 路径）配置时 `serve_hub` 以 tokio-rustls 包裹监听器；未配置=现状。Agent 的 reqwest 客户端原生支持 `https://` hub_url（默认 TLS 特性）。
- **Agent 配面**：`ARKFLOW_DATA_PLANE_TLS_CERT`/`_KEY`/`_CA`（三者齐备启用；缺一报配置错误）→ 构建 `DataPlaneTlsConfig` → manager 服务端 + `RemoteEdgeContext` 携带 → `TcpEdgeTransport` 出站。
- 依赖：workspace 增加 `tokio-rustls`（rustls 0.23 系）；测试用 `rcgen` 生成自签舰队 CA 与节点证书（dev-dep）。

## Capabilities

### New Capabilities

（无——归属既有能力）

### Modified Capabilities

- `authenticated-network-shuffle`: 新增"数据面 TLS/mTLS"需求——配置语义、双向证书校验、握手失败 fail-closed、默认关闭零变化。
- `secure-durable-control-plane`: 新增"Hub TLS 监听"需求——证书配置、就绪语义不变、默认关闭零变化。

## Impact

- `crates/arkflow-core/src/executor/remote.rs`：`DataPlaneTlsConfig`、`bind_tcp` 入站握手、`TcpEdgeTransport` 出站握手、配置校验。
- `crates/arkflow-core/src/executor/graph.rs`：`RemoteEdgeContext` 携带 TLS 配置并传入 transport。
- `crates/arkflow-server/src/agent.rs`：环境变量→配置构建；数据面 bind 与边上下文接线。
- `crates/arkflow-server/src/lib.rs` + bin：Hub TLS 配置与监听包裹（axum Listener 适配）。
- 根 `Cargo.toml`：`tokio-rustls` workspace 依赖；`arkflow-core` dev-dep `rcgen`。
- 测试：TLS 管理器两端回环收发一帧；明文客户端对 TLS 服务端 fail-closed；默认关闭回归；Hub TLS 监听握手（自签证书）。
- 文档（en + zh-Hans）：两个平面的证书配置、舰队 CA/SAN 约定、默认行为。

## Non-goals

- 不做证书自动轮换/签发（操作员自管证书与 CA；更换证书=重启进程）。
- 不做 per-node 独立信任锚或 SPIFFE/x509-SVID 身份体系——节点身份认证仍由 HMAC 会话握手承担，TLS 层只做"舰队 CA 签发的证书"门禁。
- 不移除/弱化 HMAC 握手（mTLS 是叠加而非替代）。
- 不做 Hub→Agent 反向连接或 gRPC/QUIC 迁移。
- 不改 Agent↔Hub 的客户端证书认证（bearer 令牌不变）。

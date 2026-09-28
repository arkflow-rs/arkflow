# Design: TLS 传输（数据面 mTLS + 控制面 TLS 监听）

## Context

数据面协议栈：TCP → 帧（24B 头 + Arrow IPC/控制负载）→ HMAC 应用层握手（绑定 protocol_version/src/dst 节点/job/generation/quad/nonce）。字节流抽象 `RemoteStream`（blanket impl）使传输层可替换；出站唯一入口 `TcpEdgeTransport::connect`，入站唯一入口 `bind_tcp` 的 accept 循环。控制面 Hub 用 `axum::serve(TcpListener, ...)`。

## Goals / Non-Goals

**Goals**：数据面可选 mTLS（舰队 CA 锚定、双向校验）；控制面可选 TLS 监听；两者默认关闭且逐字节保持现状；TLS 失败走既有 fail-closed；证书材料全部来自操作员。

**Non-Goals**：见 proposal。

## Decisions

### D1：TLS 层叠加在 HMAC 之下，不替代

TLS 只提供"对端持有舰队 CA 签发证书"的门禁与传输加密；节点身份、作业绑定、代数围栏仍由应用层 HMAC 握手承担（它绑定的 quad/generation 信息证书里没有）。两层独立失败：TLS 握手失败=连接失败（fail-closed）；HMAC 失败=现状语义。证书不含节点 id 断言，因此不引入 SAN 按节点命名的运维负担。

### D2：固定 SNI/ServerName `arkflow-data-plane`

节点证书须含 SAN `DNS:arkflow-data-plane`（rcgen 测试证书与文档如此约定）；出站连接以 `ServerName::try_from("arkflow-data-plane")` 发起。理由：对端地址是 IP:port，证书验证需要稳定的服务名；按节点命名 SAN 在动态机群中不可运维；身份语义已在 HMAC 层，此名仅作链验证载体。

### D3：`DataPlaneTlsConfig` 双端一体

同一结构持有 `TlsConnector`（出站，带自身证书+CA 根）与 `TlsAcceptor`（入站，带自身证书+要求并校验客户端证书）。由三份 PEM（cert/key/ca）构建，`from_pem` 校验材料可解析。放在 `NetworkManagerConfig.tls` 与 `TcpEdgeTransport.tls`；`RemoteEdgeContext` 增加 `tls` 字段把它带到 graph 构造点（`graph.rs` 的 `TcpEdgeTransport{...}`）。启用时双向强制 mTLS（无"仅服务端验证"档位）——半开配置是运维陷阱。

### D4：入站握手在 accept 循环内、HMAC 会话握手外

`bind_tcp` 的 accept 分支：TLS 配置存在时先 `acceptor.accept(tcp)`，成功产物入 `AcceptedQueue`（类型不变——TLS 流满足 `RemoteStream` blanket impl）；失败计数入既有连接失败路径。出站：`TcpStream::connect` 成功后 `connector.connect(name, stream)`，失败计入既有重试/退避循环（与 TCP 失败同一预算）。

### D5：控制面 Listener 适配（axum 模式）

`serve_hub` 在 cert+key 配置时构建 rustls `ServerConfig` 与 `TlsAcceptor`，包一个实现 `axum::serve::Listener` 的 `TlsListener`（Stream 产出 TLS 流 + `Connected` 委托内层 remote_addr）——官方 tls-rustls 示例的适配写法。readiness/liveness/路由零变化（TLS 在传输层之下）。配置仅 env（bin），`ServerConfig` 增 `tls_cert`/`tls_key` 字段；只配其一=启动错误（防半开）。

### D6：证书材料与依赖

- workspace：`tokio-rustls = "0.26"`（携带 rustls 0.23、aws-lc-ring 后端默认）；`rustls-pemfile` 已在树（plugin 用于它处）。
- 测试：`rcgen`（dev-dep）生成舰队 CA + 两个节点证书（SAN=arkflow-data-plane），产物仅内存。明文/TLS 不匹配用例断言 fail-closed。
- 文档写明 SAN 约定与 openssl 生成示例命令。

## Risks / Trade-offs

- [TLS 握手延迟叠加重连预算] → 握手失败与 TCP 失败同计数，预算不变；重连预算默认 5 覆盖瞬时握手抖动。
- [证书过期导致全平面拒绝] → 操作员职责（Non-goal 声明轮换）；失败表现为显式握手错误而非静默。
- [ring/aws-lc 后端构建差异] → 使用 tokio-rustls 默认后端（ring 系），与插件已有 rustls 用法一致；CI 已编 rustls 相关依赖。
- [固定 ServerName 与现网证书不匹配] → 文档明示 SAN 要求；测试证书模板给出正确姿势。

## Migration Plan

全部可选、默认关闭：升级零变化。启用顺序：生成舰队 CA 与各节点证书 → Agent 配 `ARKFLOW_DATA_PLANE_TLS_*` → 滚动重启（重启窗口内明/TLS 节点互连失败属预期——全量启用后再恢复流量）。控制面独立启用。回滚=清空环境变量。

## Open Questions

（无）

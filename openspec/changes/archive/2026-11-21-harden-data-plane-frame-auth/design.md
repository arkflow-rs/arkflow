## Context

跨节点数据面（`crates/arkflow-core/src/executor/remote/`）现状：握手帧（`HandshakePayload`）以共享秘密做 HMAC 并有 nonce 重放缓存（`auth.rs`），但握手后的 Data/Signal/Receipt 帧只有 24B 明文帧头 + 负载（`wire.rs` / `transport.rs`），无任何认证或完整性保护；服务端仅在入站首帧校验握手 MAC（`manager.rs authorize_inbound`）。秘密来自 `ARKFLOW_DATA_PLANE_SECRET`，缺省回退全舰队共享的 `node_token`（`arkflow-server/src/agent/session.rs:45-48`）。mTLS（fleet CA，`transport.rs DataPlaneTlsConfig`）存在但 opt-in。C 批残留：`max_connections`/重连预算/宽限/单帧上限均为硬编码或仅结构体默认值、无运维入口；`codec.rs Piece::body` 对 IPC body `to_vec()` 全量二次拷贝；pending receipt 预算超限的 Err 上抛落点需语义确认。

约束：热路径（每帧 MAC 线性开销必须可忽略级）；零/极少新增依赖；测试面 `remote/tests.rs`（3864 行）大量使用明文 duplex 工厂，认证化改造必须给测试一条低成本路径；openspec 既有契约（去重、回执路由、fail-closed、背压）不得漂移。

## Goals / Non-Goals

**Goals:**

- 关闭 P0：伪造/篡改/重放帧不可达交付路径；秘密不再借用舰队注册凭证。
- 会话密钥派生（前向隔离每连接）+ 每帧认证 tag + 连接级单调 seq（防重放）。
- SLO 与资源上限参数化（重连预算、宽限、连接数、单帧上限），溢出可观测。
- IPC body 零拷贝；回执预算超限语义钉扎为交付级失败。

**Non-Goals:**

- 默认加密/机密性（mTLS 继续 opt-in）；Hub 分发秘密；跨版本协商；入站全局字节预算；放置/路由/checkpoint 语义变更。

## Decisions

### D1 每帧 HMAC（方案 A），否决 AEAD 与强制 mTLS

- **A（选定）**：会话密钥 + 每帧截断 HMAC。依赖零新增族（`hkdf` 与既有 `hmac`/`sha2` 同属 RustCrypto），热路径开销 GB/s 级，直接关闭 P0 的注入/篡改/重放面。
- 否决 **B（每帧 AEAD）**：机密性不是本 P0 的范围；新增重量级 crypto 依赖、256MiB 帧双份内存与更高 CPU。
- 否决 **C（默认强制 mTLS）**：每节点证书运维与轻量部署叙事冲突；且 TLS 不解决 node_token 共享秘密问题（身份绑定仍在应用层握手）。

### D2 握手 v2 与密钥派生

- `HandshakePayload` 增加 `server_nonce: Vec<u8>`（32B，仅 ack 携带）；ack 仍回显客户端 nonce（既有防 ack 重放语义不变）。
- 密钥：`HKDF-SHA256(ikm=shared_secret, salt=client_nonce || server_nonce, info="arkflow-data-plane/v2" || job_id || generation || quad || direction)`，输出每方向 32B。每连接独立、两方向独立；新旧连接密钥无关（nonce 换即换）。
- 版本常量 `DATA_PLANE_PROTOCOL_VERSION` 1 → 2；版本不匹配走既有协议错误（fail-closed，无降级）。

### D3 帧格式 v2 与集成点

- 帧格式：`[帧头 24B][payload len 字节][tag 16B]`；`header.len` 仍为 payload 长度（不含 tag）。tag = `HMAC-SHA256(k_dir, header || payload || seq_le)` 截断 16B。
- 连接级单调发送序号：每方向从 0 起、每帧递增；接收端要求严格递增，缺口/回退 = 协议错误。握手帧不计数（密钥尚未建立）。
- MAC 校验先于任何 payload 解析（parse-after-verify）。
- 集成点：`transport.rs` 的 `write_frame_with_limit` / `read_frame_with_limits` / `read_receipt_frame` 增加携带 `FrameCrypto`（会话密钥 + 各自方向计数器）的读写形态；`manager.rs` pump/serve/receipt 循环从握手结果构造 crypto。`mod.rs` 暴露的 `write_frame` 公共入口同步签名。
- 测试路径：`tests.rs` 增加认证化 duplex 工厂（两端各持 `SessionAuth`，一次握手后产出 crypto 对）；既有明文 `#[cfg(test)]` 帮助函数保留给 auth 层单测，集成测试全部走认证工厂。

### D4 秘密回退移除（fail-closed）

删除 `session.rs` 的 `.or_else(node_token)` 分支：无显式秘密 = 无凭证 = colocated-only（复用既有降级路径与告警）。理由：node_token 是注册凭证，共享面是全舰队；借作数据面秘密使单节点失陷升级为全舰队数据面失陷，违背数据面凭证「per-edge 会话隔离」的设计意图。

### D5 参数化与可观测

- `NodeAgentConfig`/env：`ARKFLOW_DATA_PLANE_MAX_CONNECTIONS`（256）、`ARKFLOW_DATA_PLANE_RECONNECT_ATTEMPTS`（5）、`ARKFLOW_DATA_PLANE_RECONNECT_GRACE_MS`（10s）、`ARKFLOW_DATA_PLANE_REGISTRATION_GRACE_MS`（10s）、`ARKFLOW_DATA_PLANE_MAX_FRAME_LEN_BYTES`（256 MiB）。全部可选、缺省 = 既有默认值；装配进 `NetworkManagerConfig`。
- 连接/握手许可溢出：manager 持有界 `AtomicU64` 计数 + 节流告警（替代「进无消费者通道」）；计数器供测试断言，Prometheus 导出留后续（数据面 metrics 布线另立项）。

### D6 IPC body 零拷贝

帧读取路径 payload 改为 `bytes::Bytes`（`Bytes::from(Vec)` 零拷贝接管），`DataDecoder::decode` 接收 `Bytes`，`Piece::body` 以 `bytes.slice()` + arrow-buffer `bytes` 特性的 `Buffer::from_bytes` 构建——整帧一次分配、body 零拷贝。`bytes` 已在依赖树（tokio 系）；workspace `arrow-buffer`/`arrow` 启用 `bytes` feature。Signal/Receipt serde 路径经 `Deref<Target=[u8]>` 无感。

### D7 回执预算超限 = 交付级失败

实施期核实预算超限 `Err` 的落点；契约钉扎为：仅该交付走发送失败路径（abort 分支、上游重投递、at-least-once 保持），attempt/Job 不失败。若现实现把该 Err 升级到 attempt 级，则改；以确定性测试钉扎（`pending_receipts_budget_exceeded_fails_delivery_not_attempt`）。

## Risks / Trade-offs

- [tests.rs 大面积改造] → 认证工厂收敛改造点；auth 层单测与端到端测试分层，先工厂后逐测试迁移。
- [混合版本舰队升级期数据面失败] → fail-closed 是正确行为；deploy 文档明确「停 Job → 同窗口升级 → 重启」与秘密前置配置两个动作；CHANGELOG 标注 BREAKING。
- [无密钥洪泛消耗 HMAC CPU] → tag 校验前无解析，单帧成本 ≤ 一次 HMAC；受连接上限与 TCP 背压约束，接受此残余面（与现状 TCP 洪泛同级）。
- [16B 截断 tag] → 128-bit 安全余量，业界标准（IPsec/AEAD tag 同量级）。
- [回执预算语义与现实现不符时改行为] → 修复方向与 spec 一致（交付级），风险局限在错误处理路径，测试钉扎。
- [`Buffer::from_bytes` 特性不可用或 arrow 版本约束] → 回退方案：payload 读入后仅对 body 区间做一次受控拷贝（仍优于现状的整负载二次拷贝），spec 的 MUST NOT 全量二次拷贝仍然满足。

## Migration Plan

1. 升级前：每个 Agent 显式配置 `ARKFLOW_DATA_PLANE_SECRET`（若此前依赖 node_token 回退）。
2. 停止分布式 Job（或接受升级窗口内 attempt 失败后由 fencing 重放置）。
3. 全舰队同窗口升级二进制；按需配置新增 env（缺省即旧行为）。
4. 回滚 = 降级二进制（v1 互通恢复）；配置项均为可选，无持久化格式变更。

## Open Questions

- 无阻塞项。D7 的实现落点在实施期核实后钉扎。

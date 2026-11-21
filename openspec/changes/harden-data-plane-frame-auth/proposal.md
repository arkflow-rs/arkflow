## Why

2026-11 全量代码审查的唯一 P0（CODE-REVIEW-2026-11-15.md #1）：跨节点数据面仅 HMAC 握手，握手后 Data/Signal/Receipt 帧无认证与完整性保护，秘密缺省回退全舰队共享 `node_token`。同网段攻击者可注入伪造 Data/Barrier 帧污染数据流与 checkpoint；单节点凭证失陷即全舰队数据面失陷。审查建议的 C 批 remote 残留（#21/#22/#23/#24）属同一子系统，并入同一轮协议设计。

## What Changes

- **BREAKING** 线协议 v2：握手交换双向 nonce，HKDF-SHA256 从共享秘密派生每连接每方向会话密钥；握手后每帧携带截断 HMAC tag（覆盖帧头+负载+连接级单调 seq），验证失败即确定性协议错误断连；版本不匹配维持既有协议错误（不降级）。mTLS 仍为机密性的 opt-in 层。
- **BREAKING** 移除数据面秘密对 `node_token` 的回退：未显式配置 `ARKFLOW_DATA_PLANE_SECRET` 的节点退化为 colocated-only（走既有降级路径），不再借用舰队注册凭证作数据面秘密。
- SLO 参数化：`reconnect_attempts` / `reconnect_grace` / `registration_grace` 从 `NetworkManagerConfig` 默认值暴露为 Agent 可配置项，隐式 SLO（约 30s 分区恢复窗口）文档化。
- 连接上限可配置 + 溢出可观测：`max_connections` 暴露为配置；accepted/handshake 溢出从"进无消费者通道"改为有界计数器 + 节流日志。
- 单帧上限可配置（默认维持 256 MiB）+ 数据帧 IPC body 解码零拷贝（消除 `body.to_vec()` 全量二次拷贝）。
- 回执预算超限语义钉扎：pending receipt 预算超限走单次交付的发送失败路径（abort 分支、at-least-once 重投递），MUST NOT 升级为 attempt/job 失败；核实现行为并测试钉扎，偏离则修正。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `network-shuffle-data-plane`：ADDED「跨节点帧 SHALL 逐帧认证且密钥按会话派生」「数据面秘密 SHALL 独立配置且不回退共享凭证」；MODIFIED「线协议帧编解码」（v2 帧 tag、单帧上限可配置、IPC body 零拷贝）、「远程边背压有界」（连接上限可配置、溢出可观测）、「接收端丢连在宽限期内挂起失败」与「远程边失败 fail-closed」（SLO 参数化）、「Ack 三态镜像回执」（预算超限语义钉扎）。

## Impact

- `crates/arkflow-core/src/executor/remote/`：`wire.rs`（帧格式 v2 + tag）、`auth.rs`（双向 nonce + HKDF 会话密钥 + 每帧 MAC）、`transport.rs`（带密钥的帧读写）、`codec.rs`（零拷贝 body）、`manager.rs`（seq 校验、计数器、配置暴露）、`tests.rs`（大量既有测试需过认证工厂）。
- `crates/arkflow-server/src/agent/session.rs`：秘密回退移除、新 env 解析与 `NetworkManagerConfig` 装配。
- 依赖：新增 `hkdf`（RustCrypto，与既有 `hmac`/`sha2` 同族）；评估 arrow-buffer `bytes` 特性用于零拷贝 `Buffer`。
- 文档：distributed-jobs 概念页与 deploy 运维页（en/zh）记录协议 v2、秘密配置与升级窗口要求。
- 兼容性：wire v2 不与 v1 互通，混合版本舰队的数据面边在握手版本检查处失败（已有 fail-closed 路径），升级需同窗口完成全部 Agent。

## Non-goals

- 不做默认加密/机密性：payload 保密仍由 opt-in mTLS 承担（每帧 AEAD 方案已评估否决：新依赖 + 热路径开销，机密性非本 P0 范围）。
- 不做 Hub 分发数据面秘密（控制面协议扩展）；独立 env 配置即闭环。
- 不做跨版本协商或灰度兼容：版本不匹配 fail-closed。
- 不做入站全局字节预算：单帧上限 + 连接数上限已界定最坏情况内存。
- 不改放置、key-group 路由、checkpoint 与 at-least-once 语义。

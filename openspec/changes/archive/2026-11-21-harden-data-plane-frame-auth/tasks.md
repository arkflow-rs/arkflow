## 1. 协议 v2：会话密钥与帧认证（P0 核心）

- [x] 1.1 `auth.rs`：`HandshakePayload` 增 `server_nonce`；`server_ack` 生成新鲜 nonce；HKDF-SHA256 派生每方向 32B 会话密钥（salt=双向 nonce，info 绑定 v2/job/generation/quad/direction）；协议版本常量升 2
- [x] 1.2 `wire.rs`：帧尾 16B tag 形态定义（`FRAME_TAG_LEN`、`FrameHeader::encode_fixed` 定长编码）；`FrameCrypto` 类型（方向密钥 + 收发单调计数器，seal/open）
- [x] 1.3 `transport.rs`：`write_frame_sealed` / `read_frame_sealed` / `read_receipt_frame`（带 `Option<&mut FrameCrypto>`）——发送侧附 tag 并推进 seq，接收侧 parse-after-verify + 严格递增校验；`read_dispatch`/`write_sealed_or_plain` 分发助手
- [x] 1.4 `manager.rs`：服务端 ack 后 `frame_crypto_server`（s2c→回执写、c2s→数据读）、客户端 `frame_crypto_client`（s2c→回执读、c2s 经 `HandshakeGate` 交给 pump）；replay/Data/Signal 全部密封；无凭证 legacy 路径保持明文（仅测试可达）
- [x] 1.5 auth 单测：`session_keys_are_direction_and_session_isolated`（同 session 两方向独立、跨 session 独立）、`frame_tag_binds_header_payload_and_sequence`（改头/改负载/改 seq/错长 tag 任一即败 + 方向不可互验 + 单调 seq）、`a_frame_sealed_under_one_session_fails_under_another`（跨会话重放必败）
- [x] 1.6 认证化测试工厂：`handshake_and_derive`（真 manager ack 派生）/ `fabricate_ack_and_derive`（模拟服务端）；迁移 duplicate_handshake、receipt_channel、authenticated_protocol_violation、late_connection_error 四测；新增端到端 `a_forged_frame_from_wrong_credentials_is_rejected_end_to_end`（错秘密握手即断连、无信封入管道）与 `a_frame_captured_from_another_session_is_rejected_end_to_end`（旧会话帧在新会话 job 通道报 auth 失败）

## 2. 秘密回退移除与运维参数化

- [x] 2.1 `session.rs`：删除 `node_token` 回退分支（match 重构）；无 `ARKFLOW_DATA_PLANE_SECRET` 时 colocated-only 降级 + 显式告警
- [x] 2.2 `resources.rs apply_data_plane_env_limits`：五个 env（MAX_CONNECTIONS / RECONNECT_ATTEMPTS / RECONNECT_GRACE_MS / REGISTRATION_GRACE_MS / MAX_FRAME_LEN_BYTES）→ `NetworkManagerConfig`，缺省 = 既有默认值；畸形值启动失败、越界值走 colocated-only 降级
- [x] 2.3 单测：`data_plane_env_limits_are_optional_and_override_only_present_keys`（缺省逐位默认 + 覆盖 + validate 通过）、`data_plane_env_limits_fail_loud_on_malformed_values`（畸形报错 + 空白视为未设）；受影响旧测试迁移（agent tests + two_node_job_smoke 显式 env 秘密 + 新增无秘密降级场景）

## 3. C 批残留：上限、可观测、零拷贝、语义钉扎

- [x] 3.1 manager `rejected_connections: AtomicU64` + `count_rejection`（首次与每 100 次节流告警）覆盖连接上限/队列满/TLS 许可三处；`connection_overflow_is_counted_as_a_bounded_resource_rejection` 测试断言
- [x] 3.2 帧读取路径 payload 改 `bytes::Bytes`（BytesMut 单次分配 + freeze）；`DataDecoder::decode(Bytes)`、`Piece` 持有 `Bytes` 切片、body 经 `Buffer::from(bytes.slice_ref)` 零拷贝——arrow-buffer 58 自带 `From<bytes::Bytes>`，无需额外特性
- [x] 3.3 `ipc_body_buffer_shares_the_frame_payload_memory`（body 指针落在 payload 分配区间内）；`max_frame_len_rejects_oversized_frames_on_the_send_side`（发送侧拒绝）
- [x] 3.4 pump `register` 预算超限改为交付级继续（abort 分支 + warn + continue，仅 abort 失败才升级 Err）；manager 级 `pending_receipt_limit_aborts_the_rejected_batch` 重写 + pump 级 `pending_receipts_budget_exceeded_fails_delivery_not_attempt` 钉扎

## 4. 验证与文档

- [x] 4.1 remote 87/87、server agent 120/120、two_node 4/4、workspace 全量、clippy 0 warning、fmt clean；两处前置竞态测试顺手加固（failure_reporting 去掉 spawn 竞态、disconnect 注册宽限 10s→200ms）
- [x] 4.2 `auth_frame_roundtrip_timing`（#[ignore]，25 MiB×2 方向 / 400 帧）：release 实测明文 5.1ms vs 密封 84.4ms（sealed/plain ≈ 16.6×，密封路径有效吞吐 ≈ 600 MB/s——RustCrypto 标量 SHA-256，无 ARM SHA-NI）。64KiB 帧 ≈ 0.11ms/帧、1 MiB 帧 ≈ 1.7ms，相对网络与 Arrow 编解码占比 <2%；如 profile 显示成为瓶颈，后续可换 aws-lc-rs 的 SHA-NI 哈希（已在树）。debug 下 HMAC 失真 ~100×，勿以 debug 数字评估。
- [x] 4.3 docs en/zh deploy 页新增「数据面认证与秘密」段（v2 协议、BREAKING 秘密配置、升级窗口、5 变量表）+ CHANGELOG（Added 数据面认证/调优/零拷贝 + Changed 三条 BREAKING/行为修订）；`pnpm docs:check` 通过（145 页）
- [x] 4.4 `openspec validate --changes` 与归档前核验（7 项 delta 逐一落库核实）

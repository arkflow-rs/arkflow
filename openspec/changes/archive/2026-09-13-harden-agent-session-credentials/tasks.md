## 1. Hub 侧会话 TTL

- [x] 1.1 `HubConfig` 新增 `session_ttl_ms`(serde default `3_600_000`),接通 `lib.rs` 配置与测试构造点
- [x] 1.2 注册路径在 `NodeRecord` 记录 `session_expires_at_ms`;`RegisterResponse` 增加 `#[serde(default)] session_ttl_ms`
- [x] 1.3 三处认证点(`hub.rs` 1181 / 3103 / 4532 一带)在常量时间比较后增加过期判断,过期走既有 `HubError::Unauthorized` 且不变更注册表

## 2. Agent 侧收口

- [x] 2.1 `agent_auth_query` 不再携带 `session_token`(保留 `node_id` 等非凭证参数),更新对应单测断言
- [x] 2.2 重连退避改 equal-jitter(`sleep = rand(backoff/2 .. backoff)`,保持指数上界与 10s 上限)

## 3. 测试

- [x] 3.1 Hub 测试:注入极小 TTL,空闲会话过期后请求返回 401,节点注册表无变更
- [x] 3.2 Hub 测试:re-register 后新凭证有效、旧凭证 401,boot_id 未变时 commands/streams/operations 保留
- [x] 3.3 Hub 测试:`RegisterResponse` 含 `session_ttl_ms`;旧格式响应(缺字段)可被新 agent 结构反序列化
- [x] 3.4 端到端测试(参照 `two_node_job_smoke` 风格):命令执行中 TTL 过期 → agent re-register → 结果经重放路径到达 Hub 且幂等去重为一次终态
- [x] 3.5 Agent 测试:`/agent/commands` 请求 URL 不含 `session_token`、仅含 Bearer 头
- [x] 3.6 Agent 测试:jitter 后退避时长落在 `[backoff/2, backoff]` 区间且不超过上限

## 4. 文档与收尾

- [x] 4.1 `docs/docs/control-plane/2-deploy.md`(及运维页如有涉猎):写明混布升级顺序约束(先 Hub 后 Agent)与 `session_ttl_ms` 配置及推荐下界
- [x] 4.2 `cargo test --workspace --all-targets` 与 `cargo clippy --workspace --all-targets` 通过
- [x] 4.3 运行 `/opsx:verify` 校验实现与 artifacts 一致后归档

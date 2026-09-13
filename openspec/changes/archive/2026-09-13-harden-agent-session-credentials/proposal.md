## Why

Hub 平台阶段 2(生产基础)的现存唯一开发项是 Agent session token 短期化:今天 token 由 CSPRNG 铸造、常量时间比较(`hub.rs:2088-2095`、`hub.rs:1181`),但**没有过期时间、没有轮换**——仅 re-register 才换新(`hub.rs:2121-2129`),一个泄漏的 token 在 agent 进程存活期内永久有效。同时 agent 仍把 session token 放进 URL query(`agent.rs:2130`、`agent.rs:1483-1484`),恰是泄漏主通道(访问日志、代理)。此外 agent 重连退避无 jitter(`agent.rs:1371`),Hub 重启后全体 agent 锁步 re-register,构成注册路径风暴。这三点使阶段 2 的「认证加固」无法关闭。

## What Changes

- **Session token 绝对 TTL**:Hub 在注册时为每个会话记录过期时间(可配置,默认 1 小时),过期后一切 agent 请求返回 `401`;agent 走既有 re-register 循环透明换新。不引入滑动续期、不引入刷新端点——re-register 路径(`compute-node-agent` spec「Reconnect and graceful shutdown」)已存在且保留 boot_id 关联的全部状态。
- `RegisterResponse` 增加信息性 `session_ttl_ms` 字段(serde default,旧 agent 解析不受影响)。
- **关闭 query 泄漏通道**:Agent 不再在 URL query 中携带 session token,仅走 `Authorization: Bearer` 头;Hub 保留 query 回退以支持 legacy agent(升级窗口不关闭)。
  - **BREAKING**(仅限混布升级顺序):本版 Agent 对本版之前的 Hub 轮询会失败。升级顺序约束:先升级 Hub,后升级 Agent(单二进制发行下即「先升级 Hub 所在节点」)。
- **重连退避加 jitter**:agent 断线退避叠加随机抖动,消除 Hub 重启后全体 agent 同步 re-register 的风暴。
- Hub 对 query 通道认证继续记录 deprecation 警告(现状保持)。

## Capabilities

### New Capabilities

(无)

### Modified Capabilities

- `control-plane-hub`:新增「Agent session credential lifetime」需求——注册时赋予过期时间、过期拒绝(401 + 稳定 problem code、不变更注册表)、re-register 轮换凭证、注册响应通告 TTL;修正「Invalid agent session」场景中 "expired" 一词今天无实际对应的落差。
- `compute-node-agent`:修改「Command polling and execution」需求——凭证**仅**经 Bearer 头呈现(删除 query 双通道强制及其「Agent upgraded before the Hub」场景,替换为升级顺序约束场景;保留「Hub upgraded before the Agent」场景);修改「Reconnect and graceful shutdown」需求——补 jitter 场景,使全体会话丢失不产生同步注册风暴。

## Impact

- `crates/arkflow-server/src/hub.rs`:HubConfig 增加 `session_ttl_ms`;注册路径记录 `session_expires_at_ms`;三处认证校验点(`hub.rs:1181`、`hub.rs:3103`、`hub.rs:4532`)增加过期判断;`RegisterResponse` 加字段。
- `crates/arkflow-server/src/agent.rs`:删除 `agent_auth_query` 中的 token 携带(`agent.rs:2121-2132`);重连退避加 jitter;透传通告的 TTL(仅日志/观测用途)。
- `crates/arkflow-server/src/lib.rs`:HTTP 层对 401 problem code 的既有约定不变;配置接线。
- 文档:`docs/docs/control-plane/` 下涉及 agent 认证/升级的页面同步升级顺序约束。
- 无新依赖(`rand` 已在 workspace)。
- 明确不在本 change:每节点 bootstrap 凭证(阶段 4 身份体系)、显式吊销 API 与节点下线生命周期(阶段 3)、refresh/access 分离、HMAC 签名、mTLS、token 持久化(Hub 重启 → 全量 re-register 仍被接受,jitter 负责去同步)、operator 侧认证。

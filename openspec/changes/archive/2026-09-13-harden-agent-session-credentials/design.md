## Context

Agent 会话凭证的现状(2026-09-13 代码核实):

- 注册时 Hub 用 CSPRNG 铸造 32 字节 session token(`hub.rs:2088-2095`),仅存内存 `NodeRecord`;`cp_nodes` 表无 token 列(`storage.rs:1850` 的 upsert 列清单可证)。Hub 重启即全量失效,agent 自动 re-register。
- 认证仅做常量时间比较(`hub.rs:1181`、`hub.rs:3103`、`hub.rs:4532`),**无任何过期判断**——`control-plane-hub` spec「Invalid agent session」场景中的 "expired" 一词今天没有实现对应。
- token 仅在 re-register 时轮换;agent 进程存活期内永久有效。
- agent 在 `/agent/commands` 仍把 token 放 URL query(`agent.rs:2121-2132` 构造 query,`agent.rs:1483-1484` 使用),与 Bearer 头双通道并存;Hub 优先 header、query 仅作旧 agent 升级窗口回退。
- agent 重连退避 `250ms → ×2 → 上限 10s`,无 jitter(`agent.rs:1371`)。

约束:本 change 属 Hub 阶段 2 收口;阶段 4 才有身份体系(RBAC/OIDC),阶段 3 才有节点下线生命周期;必须与 `compute-node-agent` spec 既有的「Agent liveness is independent of command execution」「Command failures produce terminal results」语义兼容。

## Goals / Non-Goals

**Goals:**

- 泄漏的 session token 的有效窗口有硬上界(可配置,默认 1 小时),与 agent 是否存活无关。
- 轮换与恢复零协议破坏:复用既有 re-register 循环,agent 代码不需要理解「过期」这一新概念。
- 关闭 URL query 泄漏通道(agent 侧),同时不破坏 legacy agent 对新 Hub 的兼容。
- Hub 重启 / 会话过期引发的全量 re-register 不再锁步(jitter)。

**Non-Goals:**

- 每节点 bootstrap 凭证(`node_token` 保持全局共享)——阶段 4 身份体系。
- 显式吊销 API、节点删除/下线生命周期——阶段 3 节点运营。
- refresh/access token 分离、HMAC 请求签名、mTLS——不同威胁模型,见 Decisions D1 的取舍。
- token 持久化到 SQLite——引入静态秘密落盘问题;Hub 重启 → 全量 re-register 被接受。
- 滑动续期——被 D1 明确否决。
- operator 侧认证、审计事件对 401 的记录(注册拒绝审计已有,过期 401 音量低且不承载新信息)。

## Decisions

### D1:绝对 TTL(硬过期),不做滑动续期

探索阶段曾倾向「滑动续期(心跳即续期)」。落设计时否决,理由:

1. **滑动续期对健康 agent 的安全增益≈0**。Hub 每节点只有一份 token;真实 agent 每 5s 心跳会持续续期,被动泄漏给攻击者的同一 token 同样被续期——有效期仍约等于 agent 进程存活期。滑动 TTL 只对「静默超过 TTL 的 agent」收敛,这基本等价于今天的 lease 过期语义,不新增安全性质。
2. **绝对 TTL 代码更少、性质更强**。注册时记 `expires_at = now + ttl`,认证点加一个比较,无续期路径;泄漏窗口与 agent 健康状态无关地被硬上界。
3. **周期性 re-register 的成本已被既有机制消化**:
   - re-register 相同 `boot_id` 时 commands/leased_commands/streams/operations 全保留(`hub.rs:2135-2160`);
   - `report_seq` 重置有配套处理——Hub 同步重置存储层 observed cursors,agent 每会话从 1 重计数(`hub.rs:2214-2221`),这是 agent 进程重启每天都在走的路径;
   - churn 量化:默认 lease_ttl 15s(`lib.rs:120-122`,心跳 5s,稳态 ≈2.5 req/s/agent),session TTL 默认 1h → 每 agent 每小时 +1 次 register,占比可忽略。

被否决的替代:refresh/access 分离(新增端点+存储,收益仅「Hub 重启免 re-register」,而 boot_id 保留机制已使 re-register 廉价);HMAC 签名 / mTLS(能对抗「持凭证的活跃劫持者」,但 TTL 本来就不声称防这个——见 Risks;且调试/运维负担不成比例)。

### D2:TTL 默认 1h,可配置,响应通告

- `HubConfig` 新增 `session_ttl_ms`(serde default `3_600_000`)。
- 认证点判断 `now > session_expires_at_ms` → 走既有 `HubError::Unauthorized`(401,稳定 problem code),不变更注册表。
- `RegisterResponse` 增加 `#[serde(default)] session_ttl_ms`:新 hub → 旧 agent 冬眠字段被忽略;旧 hub → 新 agent 缺字段时 default 兜底。agent 侧仅用于日志/观测,不据此本地预过期(agent 不做客户端时钟判断,避免时钟偏移导致的假阴性)。

### D3:query 通道收口只动 agent 侧,Hub 回退保留

- agent 删除 query 中的 `session_token`(`node_id` 等非凭证参数照旧);Hub 的「header 优先、query 回退 + deprecation 警告」原样保留,legacy(main 分支)agent 继续工作。
- **升级顺序约束**:本版 agent ↔ 旧 hub 会因旧 hub 只认 query 而轮询失败。混布升级必须先 Hub 后 Agent;单二进制发行下即先升级 Hub 所在节点。该约束写入 spec 场景与部署文档。
- 之所以不在 Hub 侧直接删 query 回退:那会同时杀死 legacy agent 兼容,而 v1 尚未合入 main,存量部署全是「旧 hub + 旧 agent」,升级第一步必然是新 hub 面对旧 agent。

### D4:退避 jitter 用 equal-jitter

`sleep = rand(backoff/2 .. backoff)`(保持指数上界语义,测试可断言区间)。目标不是美学,而是 Hub 重启时 256 个 agent(`MAX_NODES`,`hub.rs:26`)的重试去同步,把注册路径的瞬时并发从 N 压到约 N/平均退避窗口内的散布。`rand` 已在依赖,无新依赖。

### D5:过期与长命令的重叠依赖既有重放机制,不新增协议

绝对 TTL 可能在命令执行中过期,导致结果上报 401。既有机制已覆盖:`CompletedCommandCache`(1024)在 re-register 后对未确认命令重放结果;job observation 有 park/redeliver;Hub 侧命令幂等元数据(`expires_at_ms` + 有界重试,`add-hub-job-audit-and-command-metrics` 已归档)吸收重投递。本 change 不为此新增任何机制,只补一个「TTL 过期打断命令 → 重放闭环」的专项测试。默认 1h ≫ 心跳间隔 5s,正常命令不应触界;超长 checkpoint 场景由运维调大 TTL 承载。

## Risks / Trade-offs

- [TTL 过期打断长命令的结果上报] → `CompletedCommandCache` 重放 + 命令 lease 重试兜底;专项测试钉住闭环;文档说明超长命令场景应调大 `session_ttl_ms`。
- [混布升级顺序错误 → agent 轮询失败] → Hub query 回退保留使「先 Agent 后 Hub」之外的组合都安全;升级顺序写入部署文档与 spec 场景;Hub 对 query 的 deprecation 警告保留,为下一版彻底移除回退留观测信号。
- [Hub 重启仍触发全量 re-register] → jitter 去同步后瞬时并发大幅下降;register 路径已有 `MAX_NODES` 上限与审计;这是接受的权衡(token 不落盘换来的)。
- [`session_ttl_ms` 与 `lease_ttl_ms` 配置组合不当(如 TTL < lease)] → 功能仍正确(agent 被 401 后 re-register,等效于更频繁轮换),仅 churn 增加;文档给出推荐下界(TTL ≥ 数倍 lease TTL)。
- [agent 本地不做预过期,过期瞬间的第一个请求必然吃一次 401] → 每个 TTL 周期每 agent 一次,代价一次往返 + 立即 re-register;换来零时钟依赖与零协议改动。

## Migration Plan

1. 合入后新二进制同时含新 Hub 与新 Agent;存量部署升级 Hub 节点后,旧 agent 经 query 回退继续工作(deprecation 警告出现在日志,提示后续升级 agent)。
2. agent 升级后仅走 Bearer;若其 Hub 未升级,轮询失败并走既有退避重连——按文档顺序升级可避免。
3. 回滚:回退到旧二进制即回到现状;无数据迁移(`cp_nodes` 无 schema 变更,TTL 仅存内存)。

## Open Questions

- 401 过期响应是否需要区分 problem code(如 `session_expired` 与通用 `unauthorized`)以便 agent 侧观测?倾向不区分——agent 对两者行为一致(重注册),区分仅利于 Hub 日志检索,可留给实现时按现有 problem code 约定最小处理。

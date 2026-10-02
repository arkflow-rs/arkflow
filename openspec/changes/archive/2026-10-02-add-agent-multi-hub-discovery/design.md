## Context

Hub HA 阶段 1/2 已落地：双存储后端（SQLite/PostgreSQL，`StorageActor` FIFO）、DB 租约选主（`cp_hub_lease` 单例行 + fencing epoch 写围栏）、standby 门控（`crates/arkflow-server/src/lib.rs:556-569`：除 health/readiness/liveness/metrics 外全部 503 `hub_standby`）、晋升重恢复、周期续约让位。租约快照 `HubLeaseSnapshot`（`crates/arkflow-server/src/storage/mod.rs:897`）只有 `holder/epoch/expires_at_ms`，不含 leader 的可连接地址。

Agent 侧现状：`HealthCheckConfig.hub_url: Option<String>`（`crates/arkflow-core/src/config.rs:166`）→ `NodeAgentConfig.hub_url: String`（`crates/arkflow-server/src/agent.rs:41`）；`run()`（`agent.rs:2086`）在循环外构建一次 reqwest client（`build_agent_client` 按地址是否 loopback 调整连接语义，`agent.rs:2248`），注册成功后会话内所有请求都打到该地址；会话失败回到循环顶对**同一地址**指数退避。数据面（本地 Stream/Job、shuffle 监听）与 Hub 会话完全解耦，切换天然不影响。

架构文档（`openspec/specs/hub-ha/architecture.md:60`）把阶段 3 定义为「自动故障检测与选主（已有，阶段 2）+ Agent 自动发现新 leader」。

## Goals / Non-Goals

**Goals:**

- Agent 接受多个 Hub 候选地址，任一 Hub 不可达或处于 standby 时自动切换，最终收敛到当前 leader。
- 故障转移只发生在会话边界（注册尝试 / 会话结束），不引入请求级重路由。
- standby 的 503 响应携带 leader 地址提示（`leader_url`），把发现成本从 O(n) 扫描降到一次探测。
- 单地址部署（含 HA 关闭）行为逐位不变；存储 DDL 幂等，无需停机迁移。

**Non-Goals:**

- 外部服务发现（DNS SRV、Consul、etcd、K8s）。
- 数据面任何改动；「零数据丢失同步复制」的新保证。
- Console 的 HA 拓扑 UI；跨 Hub 的会话迁移（切换 = 重新注册，沿用 `boot_id` 恢复与 completed-command 重放）。

## Decisions

### D1：候选列表配置形态——`hub_url` 改名列表化为 `hub_urls`（已获准 breaking）

`HealthCheckConfig` 的字段改名为 `hub_urls: Vec<String>`（YAML 列表）：空列表/缺省维持 standalone 模式语义，一项即单地址部署。**响亮失败靠哨兵字段保证**：结构体保留一个 `hub_url` 字段，其类型是自定义的 `DeprecatedHubUrl`（`Deserialize` 实现恒返回错误），serde 派生反序列化遇到旧键名时必然命中它——错误信息内联迁移指引（「`hub_url` 已改名并列表化为 `hub_urls`，请改写为 `hub_urls: ["…"]`」）。没有哨兵的裸改名会被 serde 的未知字段宽容静默吞掉，节点无提示降级为 standalone——本设计明确拒绝该路径。`from_engine` 据此构造 `NodeAgentConfig`：`hub_urls` 为去尾部 `/`、保序去重后的候选列表，内部 `hub_url` 字段保留为「当前活跃候选」（进程内概念，不参与序列化）。

*备选一*：新增 `hub_urls` 与 `hub_url` 并存合并——被否决（用户已允许破坏性更新）：双字段并存带来永久性的配置歧义与合并规则负担。
*备选二*：保留字段名 `hub_url` 仅改类型为列表——被否决（用户明确要求字段名一并改）：列表语义配复数名，配置面更诚实；响亮失败由哨兵字段承担，不依赖字段名巧合。

### D2：切换结构——克隆 config + 重建 client，不动 `run_session` 签名

`run()` 持有候选列表与游标；每次注册尝试前用「当前候选地址」克隆出一份 `NodeAgentConfig`（`hub_url = candidate`）并调用 `build_agent_client`。`run_session`、`register` 及所有 `format!("{}{}…", config.hub_url, …)` 调用点（约 20 处）零改动——活跃地址天然来自 config。reqwest client 按地址重建的代价只在切换时发生（故障转移是罕见事件），且 `build_agent_client` 的 loopback 判定逐地址准确。

*备选*：单一共享 client + 把 base URL 参数化穿透所有调用点——被否决：侵入面大、违背外科手术式修改原则。

### D3：失败分类——standby 503 立即轮换，连接失败计数轮换

注册/会话失败时区分三类：

1. **Standby（HTTP 503 + `hub_standby` 错误码）**：Hub 可达但非 leader——立即轮换到下一候选，不消耗退避（每候选间仅固定短歇 ~200ms 防忙转）；若 503 body 带 `leader_url` 提示且非当前地址，提示目标插队到候选最前。
2. **不可达/超时/其他状态码**：轮换计数 +1，完成一整圈仍无成功才应用既有指数退避（equal-jitter 保留，满足 compute-node-agent 的防踩踏需求）。
3. **成功**：重置退避，该地址提升为首选（移到候选列表首位，稳定排序），下轮扫描从它开始。

`post_json` 目前把非 2xx 折叠成错误字符串；需要让 `register` 能拿到结构化失败（状态码 + problem body 中的 `code`/`leader_url`）。做法：注册路径改用一个返回 `Result<Session, RegisterFailure>` 的辅助（`RegisterFailure::{Standby{leader_url: Option<String>}, Transport, Http(status)}`），会话内的心跳等仍走现有 `post_json`（它们的失败已经以会话结束的形式回到循环顶）。

### D4：租约行携带 `advertise_url`，standby 503 输出 `leader_url`

`HubHaConfig` 新增 `advertise_url: Option<String>`（leader 对 Agent 广播的 API 基址，如 `http://hub-a:8080`）。`try_acquire_hub_lease`/`renew_hub_lease` 增加该参数并随每次获取/续期写入租约行（None 清空——行始终镜像当前持有者的广播值）；`HubLeaseSnapshot` 增加 `advertise_url`。`cp_hub_lease` 新列 `advertise_url TEXT NULL`：SQLite 用既有 PRAGMA 守卫式 `ALTER TABLE` 模式，PostgreSQL 用 `ADD COLUMN IF NOT EXISTS`，启动幂等 DDL 收敛；内存测试后端与双后端契约测试同步扩展。

standby 中间件在返回 503 前，通过存储 actor 读当前租约快照（读命令豁免围栏），snapshot 未过期且带 `advertise_url` 时在 problem body 增加 `"leader_url": <addr>` 扩展字段；无提示字段时 Agent 退化为候选扫描。提示目标允许不在初始配置列表内（信任锚是初始配置的 Hub 集合：提示由已认证的 Hub 签发面给出），插入候选列表参与轮换，失败仍回落配置列表。

*备选*：standby 放行一个 `/leader` 查询端点——被否决：注册路径上反正会吃到 503，把提示内嵌省一次往返，且不扩大 standby 的可路由面。

### D5：观测——报告与日志

Agent report 的 metrics 增加计数器 `hub_failovers`（进程生命周期累计的切换次数）与 `connected_hub`（当前活跃基址）；每次切换以 `info!` 记录原因（`standby_advance` / `transport_error` / `leader_hint`）与 from→to 地址。Hub 侧无新端点；`leadership_transitions` 等既有观测不变。

### D6：会话语义不变

切换后走既有重新注册：`boot_id` 稳定 → 新 leader（共享持久库）识别同节点、重建会话；`CompletedCommandCache` 与 `pending_observations` 生命周期挂在 `run()` 局部变量上，天然跨切换保留，重放路径不变。旧 leader 在租约过期前可能仍接受心跳（存储层 fencing 已拒其变更写），Agent 不会主动「抢跑」——只在会话失败或 503 后切换，收敛由租约 TTL 界定。

## Risks / Trade-offs

- [旧 leader 存活期内 Agent 可能连着旧 leader 空转] → 存储围栏已保证旧 leader 无持久副作用；Agent 心跳/轮询持续失败或收到 503 后自然轮换，窗口有界于租约 TTL + 一圈扫描。
- [`leader_url` 提示被恶意/错误配置的 Hub 指向任意地址] → 信任域仍是初始配置的 Hub 集合（node_token 只发给它们）；提示仅影响候选顺序，目标不可达立即回落配置列表；切换日志把目标地址显式记入审计。
- [破坏性配置变更（改名 + 列表化）使存量部署在升级后无法启动，或裸改名导致旧配置被静默忽略] → `hub_url` 哨兵字段使旧键名在解析处必报错（非静默降级），错误信息内联迁移指引；示例 YAML 与文档随变更同步迁移；`--validate` 可在升级前离线发现。
- [每次切换重建 reqwest client 的连接池浪费] → 切换是罕见事件且池随 client drop 释放；不引入跨地址共享池的复杂度。
- [PostgreSQL 已有部署的列新增] → 单例行的表，`ADD COLUMN IF NOT EXISTS` 幂等、瞬时完成；回滚后多余列无害。

## Migration Plan

1. 升级前用 `--validate` 扫描存量配置：仍使用旧键名 `hub_url` 的会命中哨兵字段报带迁移指引的错误，改写为 `hub_urls: ["http://…"]`；`examples/control_plane_node.yaml` 与 8 个文档页（en/zh-Hans 各 4）同步迁移。
2. 先滚动升级 Hub 集群（新 DDL 幂等自愈；`advertise_url` 未配置时行为与现状一致——503 无提示字段）。
3. 为每个 Hub 实例配置 `ha.advertise_url`（leader 广播基址）。
4. 滚动升级 Agent，`hub_urls` 列出全部 Hub 地址（单条目即旧行为，仅语法变化）。

回滚：Agent 配置回退为单条目列表即可（语法需与二进制版本匹配）；`cp_hub_lease` 多余列无需清理。

## Open Questions

（无——advertise_url 的写入口、503 扩展字段名 `leader_url`、合并顺序均已在 D1–D4 定案；实现中若发现 standby 中间件读租约快照的命令通道缺失，按 D4 补一个只读存储命令即可。）

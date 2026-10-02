## Why

Hub 高可用的阶段 1（PostgreSQL 存储后端）与阶段 2（DB 租约选主 + fencing epoch 写围栏）已落地，多实例共享存储、standby 门控（`crates/arkflow-server/src/lib.rs:556-569`）与晋升重恢复都已可用；但 Agent 侧仍只能配置**单个** Hub 地址（`crates/arkflow-core/src/config.rs:166` 的 `hub_url: Option<String>` → `crates/arkflow-server/src/agent.rs:41`），会话失败后在 `run()` 的重连循环里对**同一地址**无限退避重试（`crates/arkflow-server/src/agent.rs:2088`、`agent.rs:2205`）。 Leader 宕机到 standby 晋升的窗口内（租约 TTL + 一次探测），Agent 只能原地等待；而 standby 的 503 响应只说"retry against the elected leader"却不告知 leader 在哪，Agent 也无法自行发现。`openspec/specs/hub-ha/spec.md:5` 明确标注「阶段 3 的 Agent 多 Hub 发现未实现」。本变更补齐阶段 3：Agent 支持多 Hub 地址与故障转移，standby 响应携带 leader 地址提示，实现架构文档（`openspec/specs/hub-ha/architecture.md:60`）所述「Agent 自动发现新 leader」。

## What Changes

- **BREAKING** **Agent 多地址配置**：`HealthCheckConfig.hub_url: Option<String>` 改名为 `hub_urls: Vec<String>`（YAML 列表，空列表/缺省 = standalone 模式，语义不变）；单地址部署改写为 `hub_urls: ["http://..."]`。结构体保留一个 `hub_url` 哨兵字段（自定义反序列化恒报错），使旧字段名无论何种形态都在解析时响亮失败并附迁移指引，而非被 serde 静默忽略后降级为 standalone。`NodeAgentConfig` 携带候选列表与活跃地址。
- **Agent 故障转移**：注册失败、会话中断、或收到 standby 的 503 `hub_standby` 时，按候选列表轮换到下一个 Hub 地址重建会话；连接成功的地址提升为首选（pin），后续故障从首选重新扫描。standby 503 视为「可达但非 leader」，立即切换不消耗完整退避；数据面（本地 Stream/Job、shuffle）不受切换影响。
- **Standby 携带 leader 提示**：`HubHaConfig` 新增 `advertise_url`（leader 对 Agent 广播的 API 基址），随租约 acquisition/renewal 持久化到 `cp_hub_lease` 行（新增列，SQLite/PostgreSQL 双后端）；standby 中间件在 503 problem body 中输出 `leader_url` 提示字段，Agent 命中提示则直接跳转 leader，把发现成本从 O(n) 扫描降到一次探测。
- **可观测性**：Agent 报告新增 hub 故障转移计数与当前连接的 hub 地址；日志记录每次切换的原因（连接失败 / standby 切换 / leader 提示跳转）。
- **文档**：配置参考、控制面部署/运维页（含 zh-Hans）更新多 Hub 配置与故障转移语义。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `hub-ha`：新增阶段 3 需求——Agent 多 Hub 发现与故障转移、租约行携带 leader 广播地址、standby 503 响应的 leader 提示契约；现 spec 中「阶段 3 未实现」的表述随之收敛。
- `compute-node-agent`：Agent 注册配置从单地址扩展为候选列表；注册失败/会话中断的处理从「同址退避重试」扩展为「候选轮换 + 首选提升」。

## Impact

- **代码**：
  - `crates/arkflow-core/src/config.rs` — `HealthCheckConfig` 字段改名 `hub_url` → `hub_urls`（列表类型）、旧键哨兵字段与迁移指引错误、校验。
  - `crates/arkflow-server/src/agent.rs` — `NodeAgentConfig` 候选列表、`run()` 重连循环的地址轮换、standby 503 识别、leader 提示跳转、报告字段。
  - `crates/arkflow-server/src/hub.rs` / `hub/leadership.rs` — `HubHaConfig.advertise_url`，租约获取/续期时写入。
  - `crates/arkflow-server/src/storage/mod.rs`（及 postgres.rs / 迁移工具）— `cp_hub_lease` 新列、`HubLeaseSnapshot` 扩展、双后端契约。
  - `crates/arkflow-server/src/lib.rs` — standby 中间件 503 body 增加 `leader_url`。
- **配置面（BREAKING）**：`health_check.hub_url` 改名列表化为 `hub_urls`；`examples/control_plane_node.yaml` 与 8 个文档页需同步迁移——en 与 zh-Hans 各 4 页：`operate/control-plane/deploy.md`、`operate/control-plane/overview.md`、`build/jobs.md`、`reference/configuration.md`（zh-Hans 位于 `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/` 镜像路径）。
- **存储**：`cp_hub_lease` 表新增一列（幂等 DDL，SQLite `ALTER TABLE` 守卫 + PostgreSQL `ADD COLUMN IF NOT EXISTS`），无需停机迁移。
- **兼容性**：HA 关闭（默认）时所有新路径不生效；单条目列表的 Agent 运行行为与既有单地址部署一致（仅配置语法破坏）。

## Non-goals

- 不引入外部服务发现（DNS SRV、Consul、etcd、Kubernetes API）；候选列表只来自静态配置与运行期学到的 leader 提示。
- 不改动数据面：shuffle 对等连接、WAL、checkpoint 逻辑不变，Hub 切换期间数据面照旧运行。
- 不做「零数据丢失同步复制」的新保证（架构文档阶段 3 提及的该项属于存储层后续工作，本变更只覆盖发现与转移）。
- 不做 Console 的 HA 拓扑管理 UI。
- 不改变 Agent 会话语义：切换后仍通过重新注册（`boot_id` 恢复）获得新会话，completed-command 缓存与 pending observations 跨切换保留。

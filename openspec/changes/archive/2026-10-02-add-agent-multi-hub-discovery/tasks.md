## 1. 配置面：多 Hub 候选列表（BREAKING：改名 + 列表化）

- [x] 1.1 `crates/arkflow-core/src/config.rs`：`HealthCheckConfig.hub_url: Option<String>` 改名为 `hub_urls: Vec<String>`（空列表/缺省 = standalone 语义不变），并保留 `hub_url` 哨兵字段（`DeprecatedHubUrl` 类型，Deserialize 恒报错）使旧键名响亮失败且错误信息内联迁移指引（改写为 `hub_urls: ["…"]`）；`--validate` 路径校验每项可解析出 scheme+host、去尾部 `/`、保序去重；补齐改名/哨兵/校验的单测
- [x] 1.2 `crates/arkflow-server/src/agent.rs`：`NodeAgentConfig` 新增 `hub_urls` 字段，`from_engine` 由列表填充候选（内部 `hub_url` 为活跃地址，空列表返回 None）；更新 `test_node_config` 等测试构造；`cargo test -p arkflow-core config` 通过
- [x] 1.3 运行 `ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot` 再生成 `docs/static/config-schema.json`，确认 `hub_urls` 列表类型出现、旧 `hub_url` 消失
- [x] 1.4 迁移 `examples/control_plane_node.yaml` 为 `hub_urls: ["http://127.0.0.1:8080"]`，`cargo test -p arkflow --test examples_validate` 通过

## 2. 存储层：租约行携带广播地址

- [x] 2.1 `crates/arkflow-server/src/storage/mod.rs`：`HubLeaseSnapshot` 增加 `advertise_url: Option<String>`；`StorageBackend` 的 `try_acquire_hub_lease`/`renew_hub_lease` 增加 `advertise_url` 参数（None 清空），内存测试后端同步实现
- [x] 2.2 SQLite 后端：`cp_hub_lease` 幂等加列（沿用既有 PRAGMA tableinfo 守卫式 `ALTER TABLE` 模式）并在 acquire/renew SQL 中写入该列
- [x] 2.3 PostgreSQL 后端：DDL 增加 `ADD COLUMN IF NOT EXISTS advertise_url TEXT`，acquire/renew 语句同步扩展；核对 `storage/migrate_tool.rs` 无需变更（列可为空）
- [x] 2.4 扩展双后端契约测试：acquire 写入广播地址、renew 更新广播地址（epoch 不变）、None 清空、`HubLeaseSnapshot` 暴露该值

## 3. Hub 侧：advertise_url 与 standby 提示

- [x] 3.1 `crates/arkflow-server/src/hub/leadership.rs`（及装配处）：`HubHaConfig` 新增 `advertise_url: Option<String>`，启动时校验其可解析为绝对 URL（配置了才校验），leader 的 acquire/renew 调用携带该值
- [x] 3.2 `crates/arkflow-server/src/lib.rs` standby 中间件：503 前读取租约快照（读命令豁免围栏，缺失则补只读存储命令），快照未过期且带 `advertise_url` 时在 problem body 增加 `leader_url` 字段
- [x] 3.3 Hub 集成测试：standby 503 携带提示字段（快照存活）、快照无广播地址/HA 关闭时 503 body 与现状逐位一致

## 4. Agent 侧：候选轮换与故障转移

- [x] 4.1 `agent.rs` 注册路径结构化失败：新增 `RegisterFailure::{Standby{leader_url: Option<String>}, Transport, Http(status)}`，`register` 解析 503 problem body 的 `code`/`leader_url`，替换现有一律折叠为字符串的错误路径（仅注册路径，会话内请求保持现状）
- [x] 4.2 `run()` 重连循环改造：持有候选列表与游标，按设计 D2/D3 实现——克隆 config + 重建 `build_agent_client`；standby 立即轮换（固定短歇）、整圈失败才走既有 equal-jitter 指数退避、成功提升首选、leader 提示插队候选最前；每次切换 `info!` 记录原因与 from→to
- [x] 4.3 `report()` 增加 `connected_hub`（当前活跃基址）与 `hub_failovers` 累计计数器
- [x] 4.4 Agent 集成测试（stub 双 Hub）：候选 1 返回 503 `hub_standby` 时无退避切换到候选 2 并完成注册；503 带 `leader_url` 提示时下一次尝试直达提示目标；全部候选不可达时保留指数退避与抖动；单地址配置的重连行为与现状一致（回归）

## 5. 验证与文档

- [x] 5.1 `cargo test --workspace --all-targets` 与 `cargo clippy --workspace --all-targets` 全绿
- [x] 5.2 文档（en）：全量替换 `hub_url` → `hub_urls` 列表形态——涉及 `docs/docs/reference/configuration.md`（含 BREAKING 迁移说明与旧键报错示例）、`docs/docs/operate/control-plane/deploy.md`、`docs/docs/operate/control-plane/overview.md`、`docs/docs/build/jobs.md`；deploy.md/overview.md 增加多 Hub 部署、`ha.advertise_url` 配置与故障转移语义小节（yaml 代码块按 `docs/DOCUMENTATION.md` 标注 validate 分类）
- [x] 5.3 文档（zh-Hans）：`docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/` 下镜像 4 页（reference/configuration.md、operate/control-plane/deploy.md、operate/control-plane/overview.md、build/jobs.md）同步替换与增补；`pnpm docs:check` 通过
- [x] 5.4 归档时同步：`openspec/specs/hub-ha/spec.md` 序言「阶段 3 的 Agent 多 Hub 发现未实现」表述更新为已落地，`architecture.md` 迁移路径标注阶段 3 完成状态

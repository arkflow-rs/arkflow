# Stability Small Knives

## Why

全量代码审查(2026-11-15,CODE-REVIEW-2026-11-15.md)的批次 2~5 中有 9 个**改动量小、且多为半修状态**的稳定性 P1:控制面 mutation 响应仍返回未脱敏明文凭据(server/api/jobs.rs:250,256,264,288,769,901,1078,1178,1268,1345,1406,1419——读路径已在 #1365/#1366 接入 redact_record_fields,写路径裸奔);GET /jobs、rollouts、approvals 三个列表无分页(api/jobs.rs:106-116、rollouts.rs:73,违反 control-plane-service 既有"Collection responses SHALL provide stable envelopes and pagination metadata"约定);API 栈无每请求超时(api/mod.rs:495-506、866-868 只有 body 限制,Slowloris 面);PostgreSQL 续约的 epoch 读取与 guarded UPDATE 不原子(storage/postgres_methods.rs:3153-3162,毫秒级 split-brain 窗口,SQLite 版正确);WAL PerEntry store 追加失败仍不归还序列号(core/wal/mod.rs:537-539,序列化侧已修,store 侧漏网,后果是永久 seq 缺口卡死 ack 60s 循环);JobRuntime stop/stop_all 无界 join 且持锁(server/agent/kernel.rs:1094,1118——start 路径已有的有界 teardown 模式未接到 stop);Kafka output close() 的 30s 同步 flush 直接在 async 上下文执行(plugin/output/kafka.rs:393,慢 broker 挂死 tokio worker);hub nodes 注册在写锁内 await 存储往返(server/hub/nodes.rs:499-509,慢查询拖住全舰队心跳);Job 深校验仍打开运行中 Job 的 redb 独占锁(executor/job_runner_adapter.rs:31,357——流路径已在 configuration.rs:341-357 用 with_temporary 修掉,job 路径漏网,违反 configuration-management 自述的校验资源约束)。

这批的共同点:修复模式在仓库里都已有正确参照(读路径脱敏、/nodes 分页、SQLite lease CAS、start 有界 teardown、流路径 with_temporary),本质是把已有模式接到漏网的路径上,风险低、收益直接。

## What Changes

- **控制面 mutation 响应脱敏**:create/update/upgrade/rollback/restore/recovery/trigger/desired_state 等全部 mutation 端点的响应 JobRecord 过 `redact_record_fields`,与读路径一致。
- **列表分页**:GET /jobs、rollouts、config approvals 增加 `page`/`page_size` 参数与分页元数据。**BREAKING**:三个集合响应从裸数组改为 `Page<T>` 信封(与 /nodes、/streams 一致);console 客户端在本 change 内同步适配。
- **API 每请求超时**:控制面 HTTP 栈加 `TimeoutLayer`(tower-http),超时行为与限流层一致地返回标准问题响应。
- **PG lease 原子续约**:`renew_hub_lease` 改为 guarded `UPDATE ... RETURNING`,消灭 epoch 读取与条件更新之间的 split-brain 窗口;双后端契约测试对齐。
- **WAL 序列归还**:PerEntry 追加的 store 写入失败时归还已预留的 seq,消除永久序列缺口。
- **stop/stop_all 有界等待**:kernel 停止路径复用 start 路径已有的有界 teardown(`await_previous_teardown` 模式),不再无界 join 且持锁。
- **Kafka close flush 出池**:30s flush 移入 `spawn_blocking`,不再阻塞 tokio worker。
- **nodes 注册锁收窄**:`get_node_labels` 的存储 await 移出 nodes 写锁作用域。
- **Job 校验不再持有 live redb 独占锁**:job 深校验路径改用临时 state backend(与流路径 `with_temporary` 同型),对运行中 Job 的 `--validate`/apply/rollback 不再必然失败。

## Capabilities

### New Capabilities

(无——全部行为落入既有能力)

### Modified Capabilities

- `control-plane-credentials`: 修改"Credential material SHALL never be disclosed"——非披露范围显式覆盖 mutation 响应。
- `control-plane-service`: 修改"Resource-oriented system API"——jobs/rollouts/approvals 集合纳入分页约定。
- `control-plane-api`: 修改"Unified control HTTP server"——控制面路由 SHALL 有每请求超时上界。
- `hub-ha`: 修改"leader SHALL 周期续约并在丢失时让位"——续约的 epoch 读取与条件写入 SHALL 原子(两后端一致)。
- `input-durability`: 新增"WAL 序列预留 SHALL 在追加失败时归还"。
- `compute-node-agent`: 新增"生命周期停止 SHALL 有界落定"(镜像既有 start 有界等待需求)。
- `configuration-management`: 修改"Validation resources SHALL be released before real startup"——校验 SHALL NOT 打开运行中 Job 的 durable state backend(临时后端替代)。

## Impact

- **代码**:`crates/arkflow-server/src/api/jobs.rs`、`api/mod.rs`、`hub/nodes.rs`、`storage/postgres_methods.rs`、`agent/kernel.rs`;`crates/arkflow-core/src/wal/mod.rs`、`configuration.rs`、`executor/`(job 校验适配);`crates/arkflow-plugin/src/output/kafka.rs`。
- **API 面**:/jobs、rollouts、approvals 响应可能变化(分页信封)——console 客户端在本 change 内同步适配,策略见 design.md。
- **文档**:控制面 API 行为变化需同步 `docs/docs/` 与 zh-Hans 树。
- **测试**:mutation 脱敏断言、PG lease 并发续约、分页、WAL seq 归还、stop 有界落定均为审查报告测试债第 1 类点名项,本变更一并清偿。

## Non-goals

- 不做数据面强制认证(P0,唯一 P0,需要协议决策,单独 openspec change)。
- 不做 listener 级 HTTP/1 header-read timeout(慢客户端头读取阶段限速):该相位先于请求派发,每请求超时覆盖不到;实现需换 hyper-util listener 栈或升级 axum,属后续项,spec 如实声明为部署级(反向代理)依赖。
- 不做性能批次(EXPR_CACHE 分隔符混叠、JSON 双解析、窗口 journaled O(状态)、SQL SessionState 克隆、max_connections 连接模型等)。
- 不做 CI 卫生项(docs-check workflow 的 examples/** 过滤、--locked、workspace.lints、rand 统一)。
- 不做连接器批次剩余项(HTTP output Mutex 串行化、Pulsar ca_file)。
- 不改错误变体契约(connector-recovery-contract 已有契约不在此触碰)。
- 不处理 CODE-REVIEW 报告的 P2/P3(另批)。

# Proposal: fix-review-p2-remainder

## Why

`openspec/CODE_REVIEW_2026-09-29.md` 批次 D 的收尾：该清单中的 P1 全部闭环、批次 D 的大半已由 `fix-checkpoint-round-timeouts` / `fix-runtime-manager-races` / `harden-operator-auth-middleware` / `fix-control-plane-quick-wins` / `fix-console-p2-batch` 落地，但经逐项核实（2026-10-03），以下 P2 项**在当前 HEAD（043f0693）仍然存在**，是打 v1.0 前批次 D 的最后一块：

- **内核**：window keyed buffers 无条数上限（`crates/arkflow-core/src/executor/window.rs:490` `BTreeMap<(i64, String), AggregateBuffer>` 无界，watermark 停滞或高基数 key 下内存炸弹——event-time gate 已有 1M 行上限，window 侧没有）；join/stateful 链并行度被 `graph.rs:1248-1252` 静默覆写为 1（用户显式配置被无声丢弃，AB-BA 锁序安全靠跨文件隐式约束兜底）；源链 barrier 分支内 `current_positions`/`snapshot_state`/`send_downstream` await 无 cancellation 守卫（`task.rs:560-628`）；`Err(msg) if message == "input channel closed"` 字符串流控（`task.rs:1412`，字符串铸造于 `:2558`）。
- **网络 shuffle**：`RemoteAck::mark_held`/`release_held` 溢出路径对有界 failures 通道（1024）做**阻塞** `send`，卡死 tokio worker（`remote.rs:1224,1243`）；pending replay 限条数（4096）不限字节，每帧上限 256MB（`remote.rs:71,1286-1291`）；receipt 读循环 30s 读空闲即拆链——下游慢（回执延迟）被误判为死连接触发重连风暴（`remote.rs:3074-3118`）。
- **控制面**：SQLite actor 的 rusqlite 同步调用直接跑在 tokio worker 上（`busy_timeout=5s` 队头阻塞整个 actor，`storage/mod.rs:940-947`、`sqlite.rs:69-124`，全目录零 `spawn_blocking`）；`declared_node_allocations` 每 reconcile tick 对全 fleet O(N²) `JobPlan::compile` 无缓存（`placement.rs:203-264,658`）；storage 层零 tracing、维护循环 13 处 `let _ =` 吞错（`lib.rs:808-847`）、actor 无队列深度指标；`parse_operator_credential` 任一解析失败回退 Admin 且整串作密钥（`nodes.rs:126-156`——`alice|typo|secret` 静默变成 Admin 凭据）；OIDC known-kid 命中即永久有效、无周期刷新/吊销（`oidc.rs:138-155`）；控制面 apply 先落版本后执行，失败留下从未生效的"可回滚"坏版本（`control_plane.rs:406-410`），版本库路径写死相对路径 `.arkflow/config-history`（`:85`）。
- **console**：请求层无超时/AbortSignal（连接黑洞 UI 冻结，`console/src/api.ts:306-316`）；SSE 断线固定 1s 重连无退避（`api.ts:538`）；diff 基线取"第一个不是自己的版本"，3+ 版本时任意（`features/configuration.tsx:166-169`）；4 个状态选项 + 6 条 DAG 连线校验文案硬编码英文，zh 用户看到英文（`features/jobs.tsx:103-106`、`job-dag.ts:102-124`）。
- **杂项**：engine 无条件 `signal::unix`，core 在 Windows 编不过（`engine/mod.rs:10,151-152`）；health_check JSON schema 漏 9 个实现字段（hub_urls/node_id/node_token/agent_lease_ttl_ms/agent_session_ttl_ms/data_port/data_host/observability/hub_url）却 `additionalProperties:false`（`component/mod.rs:381-395` vs `config.rs:140-198`），`thread_num` schema 默认值 1 与实现 `num_cpus::get()`（`pipeline/mod.rs:24-32`）不一致——合法配置被 schema 判非法。

## What Changes

**内核（arkflow-core/executor）**
- window 聚合缓冲增加 `(window, key)` 条目上限（配置 `max_buffered_keys`，默认 65536）：超限按最老 window-start 优先驱逐 + 节流告警（对齐 join `max_per_key` 先例）；驱逐的窗口数据显式计为丢失（warn 字段含驱逐量）。
- join/stateful 链并行度：用户显式配置 >1 时**构建期显式拒绝**（指名单链约束），替代静默覆写；未配置保持默认 1。
- 源链 barrier 分支的三个 await 补 cancellation 守卫：取消时走既有 round 失败路径（保留上一有效 checkpoint），不再悬挂。
- 新增专用 `Error` variant 承载 "input channel closed"（替代字符串匹配），铸造点与匹配点同步替换。

**网络 shuffle（arkflow-core/executor/remote.rs）**
- `mark_held`/`release_held` 溢出路径改 `try_send`：failures 通道满时不再阻塞 worker，改为 tracing::error 告警（信号经既有 round 失败语义兜底）。
- pending replay 增加字节预算（默认 256MiB，配置可调）：超预算不再 register 新 pending，走既有失败路径。
- receipt 读空闲判定分级：有待回执的活跃连接读空闲超时从 30s 提升到独立的"回执等待预算"（默认 10 分钟），消除慢下游误拆链；无待回执时维持 30s。

**控制面（arkflow-server）**
- SQLite actor 命令执行包 `spawn_blocking`（`Handle::current().block_on` 驱动），rusqlite/`busy_timeout` 阻塞不再占用 tokio worker。
- placement 的 `JobPlan::compile` 结果按 spec 内容缓存（job_id + spec hash 键，容量上限 + 淘汰），消除每 tick O(N²) 编译。
- storage actor 增加 dispatch 级 tracing（操作名/时长）与队列深度 gauge；维护循环 13 处 `let _ =` 改为显式日志。
- `parse_operator_credential` fail-closed：字符串含 `|` 但解析失败（未知 role/空 id/空 secret）返回错误，启动期校验并列出畸形凭据；**不含 `|` 的纯静态 token 模式保持不变**。
- OIDC JWKS 周期刷新（可配置间隔，默认 1h）：known-kid 缓存随整表替换自然吊销；未知 kid 触发的即时刷新节流（5s）保持。
- 控制面 apply/rollback 改为"执行成功后才保留版本记录"（失败时补偿删除刚保存的版本）；版本历史目录支持环境变量 `ARKFLOW_CONFIG_HISTORY_DIR` 覆盖（默认仍 `.arkflow/config-history`）。

**console**
- `request()` 增加默认 30s 超时（AbortSignal），超时报友好错误；SSE 断线重连改指数退避（1s 起步、上限 30s、连接成功重置）。
- diff 基线改为按版本序列取"当前版本的直接前驱"，不再任取。
- 剩余报错路径 i18n：状态筛选 4 个选项 + DAG 连线校验 6 条文案（en/zh）。

**杂项（arkflow-core）**
- engine 信号处理 `cfg(unix)` 门控，Windows 走 `ctrl_c`（core 可在 Windows 编译）。
- health_check schema 补齐 9 个缺失字段（含 observability 子对象与 deprecated hub_url 标注）；`thread_num` schema 移除失真的 `default: 1`（描述注明默认为 CPU 数）；重新生成 config-schema.json。

## Capabilities

### New Capabilities

（无——全部为既有能力的收口修复）

### Modified Capabilities

- `unified-execution-kernel`: join/stateful 链显式并行度拒绝；源链 barrier 分支取消守卫；"input channel closed" 专用错误 variant。
- `stream-config-compilation`: 窗口 buffer 链上显式 `pipeline.thread_num > 1` 编译期拒绝（默认 CPU 数不视为显式选择，编译为单并行度，与旧钳制行为一致）。
- `columnar-window-operators`: 聚合缓冲 `(window, key)` 条目上限与驱逐告警。
- `network-shuffle-data-plane`: 回执溢出非阻塞告警；pending replay 字节预算；读空闲分级超时。
- `control-plane-service`: SQLite actor 阻塞隔离、placement 编译缓存、storage 可观测（tracing/队列深度/维护错误日志）。
- `control-plane-identity`: operator 凭据解析 fail-closed（保留纯静态 token 模式）。
- `hub-oidc-auth`: JWKS 周期刷新与 known-kid 吊销。
- `configuration-management`: apply 先执行后留版本（失败补偿删除）；版本历史目录可配置。
- `control-console`: 请求超时、SSE 退避重连、diff 基线选择。（报错路径 i18n 属既有 `console-i18n` 规格「界面文案字典化」的实施缺口，修复不改变该规格，故无 delta。）
- `component-registry-export`: health_check schema 与实现对齐、thread_num 默认值诚实化。

## Impact

- `crates/arkflow-core/src/executor/{window.rs,task.rs,remote.rs,graph.rs,join.rs}`、`crates/arkflow-core/src/{lib.rs,error_helpers.rs,engine/mod.rs,control_plane.rs,component/mod.rs}`
- `crates/arkflow-server/src/{storage/{mod.rs,sqlite.rs},hub/{placement.rs,nodes.rs},oidc.rs,lib.rs,metrics.rs}`
- `console/src/{api.ts,i18n/{en.ts,zh.ts},features/{configuration.tsx,jobs.tsx,job-dag.ts,job-editor.tsx}}`
- 生成的 `docs/static/config-schema.json` 需以 `ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot` 重新生成
- 无新增第三方依赖；window `max_buffered_keys` 与 remote 字节预算/回执预算为新增可选配置（带默认值，向后兼容）；join/stateful 链显式并行度 >1 从"静默忽略"变为构建错误（诚实化，属修正而非破坏——此前该配置从未生效）

## Non-goals

- **window 每 watermark tick 全量克隆/命名空间扫描与 `session_late_masks` O(rows×buffers) 的性能重构**——正确性敏感（快照/persist 语义交织），遵循既有先例（openspec 设计规则"先出基准再立项"），独立 change 另行评估。
- **Agent session token URL query 兼容窗口移除**——协议演进需与 Agent 升级顺序协同（既有 spec 已含 deprecation 警告与混布升级约束），待版本策略（批次 E）定夺。
- **Error 分类收紧 / dead variant 清理 / `Option<&String>` 收缩**——属批次 C（API 面收缩，v1.0 semver 前一次性），本 change 仅新增专用 variant 承载一处字符串流控。
- **console token 换发代理、DLQ、引擎级 output 重连、PG-SESSION-FENCE**——已立项评估过的独立事项，不在本批。
- 远程边**处理级**去重残留、join 去重键个数上限（总量 = 键数 × max_per_key）——投递级去重已覆盖；键数上限留待 join 算子演进时一并处理。

# Design: fix-review-p2-remainder

## Context

批次 D 收尾：16 个分散在内核/网络 shuffle/控制面/console/杂项的 P2 修复，全部有 CR 清单的 file:line 实证（见 proposal）。各项彼此独立，无跨项依赖；共同约束是**外科手术式修改**——每项只触碰清单点名的路径，不重构周边。三项前例决定设计基调：event-time gate 的 `DEFAULT_HELD_ROW_CAP`（条数上限 + 节流告警，`fix-kernel-watermark-stall-bounds`）、join 的 `max_per_key`（容量驱逐最老行 + 节流告警）、checkpoint round 超时（显式错误走既有失败语义）。

## Goals / Non-Goals

**Goals:**
- 消除三处静默无界（window keys、replay 字节、failures 阻塞）与两处静默降级（并行度覆写、credential 回退 Admin）。
- 取消安全与错误分类的最小收口（barrier 分支守卫、专用 variant）。
- 控制面执行模型不阻塞 runtime worker（SQLite actor）、热点路径去 O(N²)（placement 缓存）、可观测补底（tracing/gauge/日志）。
- console 韧性（超时/退避）与正确性（diff 基线）。
- 配置面诚实：schema 与实现对齐、Windows 可编译。

**Non-Goals:**（见 proposal Non-goals；window 性能重构、session query 窗口移除、批次 C API 收缩、token 换发代理等）

## Decisions

### D1 window 缓冲上限：条目计数驱逐，复用 join 先例（不按字节）

`buffers: BTreeMap<(i64, String), AggregateBuffer>` 以 `(window_start, key)` 为键，BTreeMap 天然按 window_start 有序。上限计**条目数**（`(window, key)` 对）而非字节：字节需侵入 `AggregateBuffer` 估算 Arrow 内存，耦合深；条目数与 join `max_per_key`、gate `HELD_ROW_CAP` 的"计数上限"先例一致，且高基数 key 场景（1 分钟窗口 × 百万 key/秒）条目数正是爆炸维度。溢出时按 BTreeMap 首键（最老 window_start）整条驱逐直至回到上限，节流 warn（10s 间隔，字段：驱逐条目数/当前深度/上限），被驱逐窗口的聚合数据**显式计为丢失**——这与"watermark 停滞时行已不可能及时产出"的现实一致，优于 OOM。配置挂 `WindowOperatorConfig.max_buffered_keys`（serde default 65536），所有窗口族（tumbling/sliding/session）共享同一守卫，实现在 buffer 插入的公共路径。**替代方案（否决）**：超限即失败整个任务——窗口聚合是最常见的内存压力点，fail-hard 会把"可降级"变成"不可用"，且与 gate/join 的驱逐语义不一致。

### D2 并行度：显式拒绝而非断言崩溃

`graph.rs` 读取 `__arkflow_processor_parallelism` 后在 join/stateful 链静默改写为 1。改为：**显式配置 >1 且链含 join/stateful 算子 → 构建期返回错误**（指名"该链含 join/有状态算子，必须单并行度"）。用构建错误而非 `assert!`：assert 在 release 也是 panic，构建错误走既有 `Err` 路径并能在 `--validate` 期给出可诊断信息（对齐 `component-config-honesty`）。默认值路径（未配置）行为不变（仍为 1）。

**实施修订（2026-10-03）**：stream 编译器此前把 `pipeline.thread_num`（默认 = CPU 数）**无条件**注入为 `__arkflow_processor_parallelism`，导致默认配置也会触发上述拒绝（全量测试抓出 4 个文档片段与 1 个 example）。修正：编译器在窗口 buffer 链上区分"显式配置"（`thread_num != default_thread_num()` 且 >1 → 编译错误，信息含流名与修正指引）与"默认值"（编译为 1，与旧钳制行为逐位一致）；非状态链照旧透传（默认 CPU 数并行度契约不变）。JobSpec 直接路径仍由 `graph.rs` 拒绝。

### D3 barrier 分支取消守卫：select 包裹，取消走 round 失败语义

barrier 分支内的 `current_positions()` / `snapshot_state()` / `send_downstream()` 逐个以 `tokio::select! { _ = cancelled.cancelled() => …, result = fut => … }` 包裹；取消时返回错误进入既有 round 失败路径（保留上一有效 checkpoint、数据面继续）——与 `fix-checkpoint-round-timeouts` 的超时处理同构，不新造状态机。`send_downstream` 被取消时已发出的部分帧由既有 at-least-once 重放兜底。

### D4 "input channel closed" 专用 variant

新增 `Error::InputChannelClosed`（unit-style variant，`#[error("input channel closed")]`），铸造点（`task.rs:2558` 附近）与匹配点（`task.rs:1412`）同步替换。全仓 grep 确认无第三个匹配点后落地。不做大改（Error 分类收紧属批次 C）。

### D5 remote 溢出路径非阻塞：try_send + tracing::error

`mark_held`/`release_held` 的 outbox `try_send` 失败后，对 failures 通道（有界 1024）的**阻塞** `send` 改为 `try_send`；再失败（failures 也满）时 `tracing::error!`（含 quad）。理由：failures 满意味着读循环已停摆，此时阻塞一个 worker 只会让 runtime 整体恶化，而该 edge 随后必然因读循环停摆被 supervisor 拆除——信号不会真正丢失。**替代方案（否决）**：failures 改无界——违背反压不变量（AGENTS.md）。

### D6 replay 字节预算：注册期记账，超预算走失败

`PendingReceipts::register` 现仅查条数（4096）。增加近似字节记账：`replay` 的 `MessageBatchRef` 以 `get_array_memory_size()` 之和估算（register 时累加、完成时扣减），超过 `max_pending_bytes`（NetworkManagerConfig 新字段，默认 256 MiB）时**拒绝注册**——返回错误进入既有发送失败路径（上游分支收 Failed 回执/错误上浮，作业走恢复）。字节是近似值（Arrow 估算），不做精确记账。条数上限保留（防海量小帧）。

### D7 读空闲分级：按"是否待回执"选预算

receipt 读循环统一用 `read_idle_timeout`（30s）。改为：`PendingReceipts` 非空（存在待回执的在途批次）时使用独立的 `receipt_wait_timeout`（新配置，默认 10 分钟）——慢下游处理中 withholding 回执不再 30s 误拆链；空 pending 时维持 30s 快速发现死连。实现：读循环每轮按 `pending.is_empty()` 选超时值传入 `read_frame_with_limits`。**替代方案（否决）**：引入心跳帧——wire format 变更，PLANNING 已记录跨节点 trace 传播时才动 wire format，不为超时调优引入协议变更。

### D8 SQLite actor：spawn_blocking 包命令执行

actor 循环保持 `tokio::spawn`（队列接收仍在 async），单条命令的 `dispatch(&store, command).await` 包为 `let handle = Handle::current(); spawn_blocking(move || handle.block_on(dispatch(&store, command))).await`。SQLite 方法内部全是同步 rusqlite（`Arc<Mutex<Connection>>`），`block_on` 在 blocking 线程合法且不占 worker；busy_timeout 5s 最坏情况只占一个 blocking 线程（默认池 512）。FIFO 顺序不变（逐条 dispatch）。PostgresBackend（sqlx 真 async）共用此路径无害——sqlx 的 await 在 block_on 里照常工作。**替代方案（否决）**：逐方法加 spawn_blocking——52 个方法全动，违背外科手术原则。

### D9 placement 缓存：spec-hash 键 + 容量淘汰

`declared_node_allocations` 每次对每 job `serde_json::from_str + JobPlan::compile`。缓存放 reconcile 循环持有的状态：`HashMap<(job_id, u64 /*spec string hash*/), Arc<JobPlan>>`，spec 字符串先 `fxhash`/默认 hasher 取 hash，命中直接用；容量上限 1024，超限 clear（简单防泄漏；placement 不需要 LRU 精度）。正确性：spec 变更 → hash 变 → 必然 miss 重编。

### D10 credential fail-closed：以 `|` 存在性区分两种模式

纯静态 token（无 `|`）是合法模式（`operator_token = "my-token"` → Admin 整串为密钥），必须保留。修正仅针对**含 `|` 但解析失败**（未知 role / 空 id / 空 secret / 段数不足 3）：`parse_operator_credential` 返回 `Option`，None 时（a）鉴权路径视该凭据为不可匹配（不崩溃请求路径），（b）**启动期校验**遍历全部 operator 凭据，发现畸形即拒绝启动并列出原文（脱敏：只报前 8 字符 + 长度）。多凭据配置下单个畸形不再静默获得 Admin。

### D11 OIDC JWKS 周期刷新：tokio interval 整表替换

在 OIDC 认证初始化处起 `tokio::time::interval(jwks_refresh_interval)`（配置，默认 1h，`hub.oidc.jwks_refresh_interval_ms` 或既有配置节——实施时按现有配置结构就近挂载）周期性整表拉取 JWKS 替换缓存；known-kid 因整表替换自然吊销（新表无该 kid → 下次命中 miss → 401）。保留：未知 kid 即时刷新（5s 节流）、provider 不可达时**保留旧表继续服务**（不因刷新失败打掉 Hub——延续现有"rotation must not take the Hub down"语义，但刷新成功即收敛吊销）。

### D12 apply 顺序：补偿删除而非两阶段

`save_with_parent` → `replace_config` 失败时，best-effort `delete_version`（ConfigVersionStore 新增最小方法）补偿删除刚保存的版本并返回原错误；rollback 同构。不做"pending/active 状态列"——版本存储结构变更影响面大，补偿删除在单进程本地存储上足够（失败残留窗口极小且下次 apply 前仍可人工清理）。历史目录：`ConfigVersionStore::new` 前读 `ARKFLOW_CONFIG_HISTORY_DIR` env，默认值不变。

### D13 console：AbortSignal.timeout + 指数退避 + 前驱基线

- `request()`：`AbortSignal.timeout(30_000)` 与调用方传入 signal 合并（`AbortSignal.any`，Safari ≥17.2；console 无旧 Safari 支持承诺，实施时如 tsconfig/lib 不满足则退化为 setTimeout+abort controller 手工实现，二者等价）。超时错误（`DOMException.name === 'TimeoutError'`）映射为可读文案。
- SSE 重连：delay = min(1000 × 2^attempt, 30_000)，成功建立（收到首事件）后 attempt 归零。
- diff 基线：版本按自身序号/创建时间降序排序后取当前版本之后的第一个（即直接前驱）；无前驱时退化为现状（任意异 id 版本）并禁用 diff 按钮（无前驱 = 首版本，无意义 diff）。
- i18n：`jobs.tsx` 四状态 option 与 `job-dag.ts` 六条文案改 key（`jobs.state.*`、`dag.edge.*`），en/zh 补词条；`job-editor.tsx` 渲染 issues 处走 `t()`。

### D14 engine Windows：cfg 门控

`#[cfg(unix)]` 走现 `signal(SignalKind::…)`，`#[cfg(windows)]` 走 `tokio::signal::ctrl_c()`；非 unix 非 windows 目标 `cfg` 编译期占位（`compile_error!` 不必要——用 `cfg(any(unix, windows))` 包整体 + else 空 future）。CI 无 Windows target，验证以 `cargo check --target x86_64-pc-windows-msvc` 不可行时，至少保证 `cfg` 结构正确且 unix 路径零变化（本机全量测试回归）。

### D15 schema 对齐：补字段、去失真默认值

`component/mod.rs` 的 health_check schema 块补 9 字段（类型/描述与 `config.rs` 实现一致；`hub_url` 标 `deprecated: true`；`observability` 子对象按其结构体递归展开）；`thread_num` 删除 `"default": 1` 改 description 注明"defaults to the number of CPUs"。改完以 `ARKFLOW_REGENERATE_DOCS=1` 重跑生成测试，`docs/static/config-schema.json` 随之更新；`--validate` 对含新字段的配置不再误报。

## Risks / Trade-offs

- [window 驱逐 = 该窗口数据丢失] → 节流 warn 必须含驱逐计数与上限，文档明示；默认上限足够大（65536 keys），正常负载不可达。
- [join/stateful 并行度 >1 由静默变报错，可能"破坏"既有配置] → 此类配置从未生效（一直被覆写为 1），报错是诚实化；错误信息给出修正指引（去掉该注解或移除 join/stateful 算子）。
- [SQLite actor block_on 于 spawn_blocking] → 需确认 dispatch future 内无 `block_in_place`/嵌套 runtime 假设；测试覆盖 actor 全命令面（既有 184 server 测试回归）。
- [OIDC 周期刷新失败保留旧表] → 吊销延迟 = 刷新失败持续期；tracing::warn 记录连续失败，运维可感知。
- [补偿删除失败（apply 失败 + 删除也失败）] → 残留坏版本仍可被后续 publish 覆盖父链，影响限于 diff 展示；warn 记录残留路径。
- [SSE 退避使事件延迟最长 30s] → 上限可接受（事件流非控制面关键路径）；手动刷新即重置。
- [字节预算为估算值] → 阈值语义是"数量级防护"而非精确配额；文档明示近似。

## Migration Plan

全部行为变化都有默认值/无配置即可运行，无迁移步骤；唯一行为变化点（并行度 >1 报错、畸形凭据拒绝启动、apply 失败不留版本）均属"修正静默错误行为"，发布说明中列出即可。回滚 = revert 提交。

## Open Questions

（无——各项决策已按先例与 CR 清单定夺；实施中如遇 D14 的 Windows 目标工具链不可用，以 cfg 结构正确性 + unix 全量回归为验收标准。）

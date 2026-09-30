# v1.0 就绪度代码审查记录（2026-09-29）

> 审查基线：main @ `3afcc14`（含 #1268 console-hub-alignment）。两轮深潜：①发布就绪度宏观评估（测试/clippy/docs/console 四门禁全绿 + 发布工程盘点）；②代码层深潜（执行内核、并发纪律、持久化正确性、控制面、插件层、core 非执行器、console 前端，全部论断带 file:line 证据）。
> 用途：修复项跟踪清单。各项落地时按 openspec 流程立项，归档后从本清单划掉。
> 总体画像：内核执行器 > 持久化本地后端 ≈ 并发纪律 > 控制面领域逻辑 > console > core API 外壳 > **插件层（质量断层）**。

---

## P1 — 数据正确性 / 可用性（v1.0 硬阻塞）

| # | 缺陷 | 证据 | 修复方向 | 建议 change |
|---|---|---|---|---|
| ~~P1-1~~ ✅ | **S3 WAL 后端异步路径必崩（2026-09-29 已修复：`fix-s3-wal-async-safety`——store 调用经 spawn_blocking 驱动、构造期 block_on 移至短命线程、runtime 安全 Drop、rewind 毒化 floor + cursor 内存镜像 + 纠正性 flush、异步路径集成测试即原实证复现。剩余：段回收/压缩与并行 PUT 死代码仍待独立立项）**：S3Store 全部 trait 方法内部 `self.runtime.block_on(...)`，引擎唯一调用方式是异步路径（`Wal::append/acknowledge → store 方法`）。临时 `#[tokio::test]` 实测 `store.cursor()` 第一调用即 panic："Cannot start a runtime from within a runtime"（tokio 1.53.1）。附带：`rewind_cursor` 走默认实现（manifest 未 flush 时静默 no-op）、`acked_hwm` 无回退路径（源提交失败后周期 flush 仍推进 cursor，违反 input-durability spec "Both WAL backends keep the replay guarantee"）、段回收未实现（s3.rs:897-905 注释自认）、压缩与并行 PUT worker 是死代码。全部 e2e 测试为同步 `#[test]`，异步路径零覆盖 | `wal/s3.rs:661,675,685,729,751,787`；调用方 `wal/mod.rs:411,502,519,704,760`、`stream_adapter.rs:283-287` | WalStore 方法异步化，或同步方法内改 `spawn_blocking` 驱动私有 runtime；补 rewind/acked_hwm 补偿；补段回收；清理或实装死代码；**补一条异步路径集成测试**；短期可先标注 experimental 从文档降级 | `fix-s3-wal-async-safety` |
| ~~P1-2~~ ✅* | **Hub 租约 fencing epoch 是纸面围栏（2026-09-29 已修复：`fix-lease-epoch-write-fencing`——【遗留跟进项 PG-SESSION-FENCE（CodeRabbit 二轮，P2）：PG 侧守卫会话（FOR SHARE 事务）若在变更执行中途死亡，锁被释放而独立会话上的变更事务仍可其后提交——彻底修复需把变更整体搬入守卫会话（sqlx Transaction 独占模型下的会话路由 + SAVEPOINT 嵌套，约 250 行改造）；窗口已远窄于原 P1-2（需守卫连接故障而非时序），SQLite 侧无此问题（单连接守卫即写事务）】——38 个变更类存储命令经 Fenced 信封携带声明 epoch，actor 执行期对照租约行校验，失配以 StaleLeader（503 stale_leader）拒绝且零副作用；无租约行（未启用 HA）逐位不变；promote 时 epoch 先于 recovery 写发布）**：lease CAS（takeover epoch+1）正确，但 StorageBackend 全部写方法无 epoch 参数，旧 leader 在感知 Lost 前（≤ttl/3 窗口）的 reconcile 写无任何存储层谓词拒绝。`leadership.rs:246` 注释宣称的 fencing 语义未实现 | `storage/mod.rs`（trait 无 epoch）；`leadership.rs:242-253,246` | 写路径加 epoch 谓词（`WHERE epoch = ?` 或等价），或续期失败即刻冻结 reconcile tick | `fix-lease-epoch-write-fencing` |
| ~~P1-3~~ ✅ | **网络 shuffle 投递去重竞态（2026-09-29 已修复：`fix-review-quick-wins`——per-key 投递锁 + 单调 max 更新 + 重叠回归测试）**：检查（`seq <= last`）→ `send_async().await` → `delivered.insert()` 无条件覆盖（非 max）。重连重叠期同一 seq 可双投递；旧连接完成后把 delivered 水位回写为低位，后续重放全部穿透去重 | `executor/remote.rs:2651-2687` | insert 改 max 更新 + send 后复查 | `fix-remote-dedup-race` |
| ~~P1-4~~ ✅ | **Kafka L3 offset 行级归因错误（2026-09-29 已修复：`fix-review-quick-wins`——`__meta_ext` topic 逐行校验 fail-closed + 3 单测）**：`transactional_offsets_for_batches` 对批次每行统一套用 group topic，不校验行级来源。扇入/多输入图混入第二 Kafka 源的行会把 offset 以 max 语义折进 group topic 分区 → 位点被事务性跳前，中间记录静默跳过 | `output/kafka.rs:654-687` | 逐行校验 `__meta_*` 来源 topic 与 group topic 一致，不一致则 fail-closed | `fix-l3-offset-topic-attribution` |
| ~~P1-5~~ ✅ | **6 个 input 违反取消安全契约（2026-09-29 已修复：`fix-input-cancellation-safety`——codec 解码与 ack 构造前移至后台任务、通道改载 Delivery、read 单 await 化；契约测试框架含门控 codec 与引擎形态取消探针，websocket/http 端到端 + 违例/合规双证）**：`recv`（弹出消息=副作用）之后还有 `await codec decode`——引擎 select 取消 read future 即静默丢消息；源链 idle_tick 每 100ms 触发，属常规路径。波及 MQTT（manual ack 真提交，丢即永久）、Pulsar（还有第二个 await）、NATS、Redis（List 模式弹出即提交）、WebSocket；根因是同一复制粘贴的 `codec_helper` 模式。契约原文：`arkflow-core/src/input/mod.rs:266-275` | `input/mqtt.rs:198-209`、`pulsar.rs:232-247`、`nats.rs:360-383`、`redis.rs:406-414`、`websocket.rs:149-174`；`input/codec_helper.rs:30-39` | codec decode 前置到后台任务（claim 前），或公共路径统一改造；配套契约合规测试框架（真实 select 取消下跑每个 input 的 read） | `fix-input-cancellation-safety` |
| ~~P1-6~~ ✅ | **SQL processor 错误路径泄漏池 context → 4 错后静默挂死（2026-09-29 已修复：`fix-review-quick-wins`——Drop guard 释放 + acquire 10s 有界 + 回归测试）**：只有成功路径 `release_context`，临时表路径 4 个 `?` 与缓存路径全部 `?` 早退泄漏；池大小 4、空池 `acquire()` 1ms 忙等死循环，第 5 次 schema drift 类错误起管线无任何报错永久挂起 | `processor/sql.rs:243,217-228,322-372`；`context_pool.rs:91-103` | 错误路径补 release（RAII guard 最稳）；acquire 改 condvar/notify 并加超时日志 | `fix-sql-context-pool-leak` |
| P1-7 | **Pulsar output 功能性损坏**：`client.producer().build()` 不带 topic，pulsar 6.8.0 下必然 "topic not set"，connect 不可能成功；即便修好，`send_non_blocking` 丢弃返回的 SendFuture，write Ok 不代表落 broker——fire-and-forget 冒充可靠投递。疑似从未在真实 Pulsar 端到端跑通 | `output/pulsar.rs`（build 无 topic）、`:176`（丢弃 SendFuture） | build 带 topic + 等待/处理 SendFuture（至少 collect 结果按错失败）；补真连测试或标 experimental | `fix-pulsar-output` |
| P1-8 | **multiple_inputs 三连缺陷**：① `connect()` 无在运行守卫，引擎重连后再 spawn 一套读任务（旧的没死，任务翻倍、TaskTracker 无限累积）；② 子 input 报错不 return 热循环；③ 错误通道是生产路径唯一 `flume::unbounded()` | `input/multiple_inputs.rs:51-97,73-84,132` | 重连前 cancel+wait 旧任务；错误路径退出；通道改 bounded | `fix-multiple-inputs-lifecycle` |
| P1-9 | **batch/memory buffer 失败与关闭路径丢数据**：batch processor `close()` 直接 `clear()` 丢弃已消费未结算 ack 的缓冲；flush concat 失败时已 pop 消息不回填；memory buffer "capacity" 不设限（文档宣称最大累积数，实现只用于提前 notify）；`process_messages` 先 pop 再 concat 失败丢整批；flush 与 close 共用 token（flush 即永死） | `processor/batch.rs:120-125,80-91,56-70`；`buffer/memory.rs:153-171,119-133,206-217` | 失败路径回填/结算 ack；close 与 flush 分离 token；capacity 真实生效；补 ArrayAck 补偿（对齐 core VecAck） | `fix-buffer-data-loss-paths` |
| ~~P1-10~~ ✅ | **HTTP input bind 失败被吞（2026-09-29 已修复：`fix-review-quick-wins`——bind 移入 connect + 断连改 Disconnection + 端口占用测试）**：`TcpListener::bind().expect()` 在 spawn 任务里 panic，`connect()` 无条件置 connected 返回 Ok——端口占用时 input "已连接"但永久无数据，零错误暴露 | `input/http.rs:167,178,196-204` | bind 结果经 oneshot/通道回传 connect；JoinHandle 检查 panic | `fix-http-input-bind` |
| P1-11 | **Pulsar input ack 活锁**：后台任务持 consumer 互斥锁等 `next().await`（锁横跨整个等待期），ack 需同一把锁——消息流停止时 ack 永久排队，管线卡死 | `input/pulsar.rs:186-189` vs `:340-349` | 锁只覆盖消费动作不覆盖等待（取消息出锁后再 await），或 ack 走独立通道 | `fix-pulsar-input-ack-livelock` |
| P1-12 | **Console 把 operator token 内联进公开 bundle**：`VITE_API_TOKEN` 构建期静态替换进 JS，任何访客可从 bundle 抠出写权限凭据；无 `.dockerignore`（`.env` 会打进公开镜像）；根 `.gitignore` 无 `.env` 规则 | `console/src/api.ts:305`、`console/Dockerfile:5`、`.env.example:2` | 补 `.dockerignore` + `.gitignore` 规则；README/.env.example 写明静态 token 仅限受信网络、生产走 OIDC；中期走 token 换发代理 | `fix-console-token-exposure` |

## P2 — 高优先健壮性 / 边界正确性

**内核（executor/）**
- 三处 watermark 驱动的无界缓冲：event-time gate `held` 完全无界（`event_time_gate.rs:84`）、window buffers 无键数上限（`window.rs:2442-2451`）、join `by_key` 只在全员上报 watermark 时清理——处理时间源混入即静默内存炸弹（`task.rs:1740-1744` 耦合）。→ 加字节/条数上限 + 告警。
- checkpoint 全链路无 round 超时（`kernel_handle.rs:205-322`）+ barrier 无通道优先级；sink 写与状态快照无超时（`task.rs:2640`、`barrier.rs:361-364`）——shutdown 可挂死。→ round 级 deadline + sink/快照超时。
- ~~join inner 容量驱逐静默丢最老行且无日志（`join.rs:716-718`，outer 会作为 unmatched 发出）——高倾斜 key 下产出错误 join 结果。~~ ✅ 2026-09-29 已修复（`fix-review-quick-wins`：按侧独立节流 warn——字段含侧别/key/深度/上限/窗口累计，决策函数以合成时钟确定性测试）。
- join AB-BA 锁序靠 `graph.rs:1248-1252` 强制并行度=1 的跨文件隐式约束兜底。→ 显式断言或单一锁序重构。
- window 每 watermark tick 3 次全量 buffer 克隆 + 全命名空间 scan（`window.rs:1836,1932,2535-2649`）；`session_late_masks` O(rows×buffers)（`window.rs:1106-1114`）。
- `RemoteAck::mark_held` 满队列时同步 `send` 阻塞 tokio worker（`remote.rs:1224,1243`）。
- 远程边 pending replay 限条数不限字节（4096 帧 × MAX_FRAME_LEN 256MB，`remote.rs:71,1285-1305`）；读空闲 30s 误判慢下游（`remote.rs:3075-3078`）。
- 源链 barrier 分支不检查 cancellation（`task.rs:567-598`）；`Err(msg) if message == "input channel closed"` 字符串流控（`task.rs:1374`）应改专用 variant。

**core 非执行器**
- RuntimeManager 注册索引用 `entries.len()`（`runtime.rs:430`），replace_config 部分变更后两条活跃流可共享 job id → 指标互相覆盖、状态命名空间/目录冲突（`stream_compiler.rs:29-30`、`job_runner_adapter.rs:158,410`）。→ 稳定唯一 id。
- stop/restart 竞态：stop 已返回成功而流复活（`runtime.rs:900-909,951-992`）。→ per-entry 串行化。
- `input_name` 不变量静默丢失：`filter_columns`/`new_binary_with_origin` 置 None（`lib.rs:357-381,333-355`），join buffer 对无名批次静默丢弃（plugin `buffer/join.rs:84-104`）——processor 中途过滤列，下游 join 无声变空。→ 构造器保留 origin 或 join 显式报错。
- `${file:}` 无沙箱任意读取（`secret.rs:315-331`）+ 配置端点脱敏只按 key 名（中性 key 原样返回）。→ 路径白名单/开关。
- `pipeline::Pipeline` 死 pub API 且语义与执行器相反（静默剥离 ack，`pipeline/mod.rs:69-85`）。→ 删除。
- Error 分类不足：`Process` 412 处垃圾桶、`InvalidConfig`/`LockTimeout` 从未构造（dead variant）、Arrow/DataFusion 错误全部折叠为 Process（`error_helpers.rs:98-120`）。→ 收紧。
- engine 无条件 `signal::unix`——core Windows 编不过（`engine/mod.rs:10,151-152`）。
- config schema 与实现漂移：health_check schema 漏全部 hub/agent/observability 字段却 `additionalProperties:false`（`component/mod.rs:381-395` vs `config.rs:155-191`）；`thread_num` 默认值不一致。`HealthCheckConfig` 命名大杂烩。
- 控制面 apply 先持久化版本后执行（失败留下从未生效的"可回滚"坏版本，`control_plane.rs:401-435`）；版本库写死相对路径 `.arkflow/config-history`（`:85`）。

**控制面（server）**
- operator 认证样板逐 handler 手写 37 次、无"逐路由 401"矩阵测试（`lib.rs` 全文）。→ 提升为路由层中间件 + 矩阵测试。
- SQLite actor 的 rusqlite 同步调用直接跑 tokio worker（`busy_timeout=5s` 队头阻塞，`storage/mod.rs:704-1085`、`sqlite.rs:2388-2405`）。→ spawn_blocking 驱动。
- `hub_problem` 把存储故障映射为 400（`lib.rs:3705-3720`），`hub_stream` 却正确映射 503——同一错误不同语义；`GenerationConflict` 应 409。
- 分页溢出修了 helper 漏 5 处复制粘贴副本：`lib.rs:896,2671,2757,3844,3909`（`(page-1)*page_size` debug 构建 usize::MAX 直接 panic）。
- `declared_node_allocations` 每 reconcile tick 全 fleet O(N²) plan 编译无缓存（`placement.rs:203-264`）。
- storage 层 tracing 为零、maintenance 循环 `let _ =` 吞错（`lib.rs:774-780`）、actor 无队列深度指标。
- `parse_operator_credential` 解析失败回退 Admin 且整串作密钥（`nodes.rs:126-156`）；OIDC 已知 kid 永久缓存不吊销（`oidc.rs:39-42`）；session token 走 URL query 兼容窗口仍在（`lib.rs:3286-3322`）。

**插件层（P2 批）**
- schema registry 多版本消息 `concat_batches` 报错（`schema_registry.rs:254-256`）；现有"多版本"测试两个 id 用同一 schema 文本，绕开真实演进场景——测试虚假信心（`:729-745`）。
- debezium tombstone（空 payload）整批失败（`debezium.rs:62-63`）；envelope 元数据覆盖业务同名字段 op/source/before（`:119-124`）。
- modbus 错误分类用 Process 而非 Disconnection（`modbus.rs:119-163`，引擎直接判死不走重连）；nats 首次订阅失败同病（`nats.rs:203-207`）。
- 文档/示例与实现脱节批次：batch 官方示例缺 `timeout_ms` 必 build 失败且文档字段 `size`/`interval` 不存在（`batch.rs:156-157`）；sql input 文档 `poll_interval` 不存在（`sql.rs:344-360`）；redis output 示例用不存在的 `stream` 变体（`redis.rs:208-224`）；五个 output 的 `value_field` 被解析从不读取；websocket `headers` 死配置；file/sql/modbus `codec` 字段 dead_code；json/stdout/python 文档字段不存在。
- vrl UInt64→i64 静默回绕（`vrl.rs:257`）；不支持类型静默转 Null（`:345-352`）。
- SQL output 仅支持 5 种 Arrow 类型，其余运行期才报错（`output/sql.rs:513-557`）；build 接受 codec 但 write 必然失败（`:401-405`）。
- SQL processor 临时表路径 `?` 早退跳过 deregister_table（`sql.rs:229-236`）——下一批可命中陈旧临时表。
- MQTT output 无重连机制、二次 connect 泄漏旧 eventloop（`output/mqtt.rs:128-139`）；SQL output close 不关连接（`sql.rs:452-455`）。
- json/protobuf 坏消息整批失败无单条隔离/DLQ（`json.rs:57-58`、`protobuf.rs:134-138`）。
- python processor `sys.path.insert().unwrap()` 可 panic、路径优先级反转（`python.rs:117-121`）、无超时熔断。
- 窗口族 Notify 丢唤醒竞态（有 timer 兜底，仅延迟一周期）；跨 input 异构 schema 合并必失败（`buffer/window.rs:150-161`）。
- schemaType 缺失默认 PROTOBUF——旧版 registry 对 Avro 常省略（`schema_registry.rs:323-327`）；gate 失败结果永久缓存（`:165-192`）。

**console（P2 批）**
- 全站零 Error Boundary——任一 render 异常即白屏（全 src/ 无 componentDidCatch）。→ 必补。
- `waitForOperation` 7.5s 硬上限把仍在执行的操作报成失败（`api.ts:542-566`）。
- job-editor 校验失败只显示泛化 "Validation failed"，服务端真实报错被吞（`job-editor.tsx:417` + `api.ts:322-329` 抛裸对象非 Error）。
- Hub 只读配置视图 Rollback 不 gate editable、`canMutate` 未传入（`configuration.tsx:282`、`app.tsx:282-285`）；`compare()` 的 diff 基线取"第一个不是自己的版本"（3+ 版本时任意）（`configuration.tsx:157-161`）。
- request 无 AbortSignal/超时（连接黑洞 UI 冻结且不亮 stale）；SSE 断线固定 1s 重连无退避（`api.ts:536`）。
- 硬编码英文集中在报错路径：状态筛选（`jobs.tsx:103-106`）、DAG 连线校验文案（`job-dag.ts:102-124`，zh 用户看到英文）。

## P3 — 结构债（不阻塞 v1.0，但 1.0 前最划算的几件）

- **core pub 面收缩**（v1.0 semver 前置）：~685 pub item + executor/wal 520 行 pub；8 个 builder trait 全线 `Option<&String>` 应为 `Option<&str>`（改即 breaking，趁消费者只有自家 crate 一次做完）；删 `Pipeline`、dead Error variants；`HealthCheckConfig` 改名拆分；`resolve_candidate_payload`（Hub 专用）移出 core；`Temporary::get` 泄漏 DataFusion `ColumnarValue` 进公共 trait。
- **server 结构**：lib.rs 6.7k 行 94 handler 双 API 巨石；hub/ 15 子模块 `use super::*` 伪拆分；storage 5 层 × 45 操作 ~2000 行委托样板（宏/泛型通道可砍 2/3）；SQL `?N→$N` 每次执行重写零缓存且双方言双份维护；agent.rs 4.5k 行 8 种关注点、字符串命令协议；无版本号 DB 迁移（无 PRAGMA user_version）。
- **executor 结构**：remote.rs 5 合 1（wire/TLS/认证/注册表/pump）；window.rs 4.8k 行；`CheckpointHook` god-struct（10 个 Option 字段 6 类关注点）；7 个 run_graph_* + 7 个 run_job_* 兼容壳；state_journal 指针身份 finalize 防护（`state_journal.rs:215-236`）。
- 杂项：`INITIALIZATION: OnceLock` 缓存首次失败（plugin `lib.rs:38-58`）；kernel watcher 50ms 轮询任务潜在泄漏（`kernel_handle.rs:465-482`）；StorageActor panic 无重启；`SharedCheckpointStore` 每操作一线程（`agent.rs:140-161`）；`wal/mod.rs:395` 注释与实现矛盾；console 无 ESLint、`JOB_DETAIL_INTERVAL_MS` 死常量自打脸、JobVersions 绕过数据层。

## 流程与发布工程（第一轮评估结论，v1.0 前置）

1. 版本严重滞后：Cargo.toml 仍 0.5.0，最后 tag v0.5.0（2025-10-19），此后全部演进未发版 → 先发 0.6 释放积压。
2. 无 release 自动化（二进制产物/GitHub Release/crate publish 全无；唯一通道 docker tag push）；CI 缺 clippy/fmt/覆盖率/多平台（rust.yml 装了组件但没跑）。
3. 无 CHANGELOG、无 SECURITY.md、无产品版本策略/升级指南（compatibility.md 只讲文档快照）。
4. `arkflow-server` CLI（含 migrate）未进 CLI 参考文档；MAX_NODES=256 与长稳结论只在内部 PLANNING 未进用户文档。
5. **新增：插件层契约合规测试框架**——在真实 select 取消下跑每个 input 的 read（把取消安全从注释契约变成 CI 门禁），否则 P1-5 类缺陷会回归。

## 建议修复批次（合计约 3-5 周）

- **批次 A（数据正确性）**：P1-1、P1-3、P1-4、P1-5、P1-6、P1-9 + join inner 驱逐日志（P2）。
- **批次 B（组件可用性）**：P1-2、P1-7、P1-8、P1-10、P1-11、P1-12。
- **批次 C（API 面收缩，v1.0 semver 前一次性）**：P3 第一条全部。
- **批次 D（P2 高优）**：内核无界缓冲/超时批、RuntimeManager 两缺陷、input_name、认证中间件化+矩阵测试、Error Boundary、文档脱节批。
- **批次 E（发布工程）**：上节 1-4。

# ArkFlow 代码库全量精读报告

> 评估日期:2026-11-14 ~ 2026-11-15
> 范围:全部 316 个代码文件(255 .rs / 40 ts+tsx / 5 mjs / 12 yml / 4 toml),含全部测试模块清单级覆盖
> 方法:6 轮审查;本人逐行精读 ~2.7 万行核心(executor 全部生产代码、Ack 体系、WAL 全文、checkpoint 协调器、job 校验器、WatermarkTracker、runtime/config/engine/secret、kafka.rs 生产段全文);14 个并行子审查覆盖其余全部文件
> 说明:工作树内有并行会话在改文件(udf/、diagnostics 等),本报告是即时快照;行号以当日工作树为准

---

## 0. 总裁定

**核心是生产级的,边缘是准生产级的,工程纪律是罕见级的。**

以"20 个月、1155 commits、单人主导的开源项目"为参照系:top 1% 水平。以"成熟商业系统"为参照系:核心达标,外围差一轮治理。综合 **B+/A-**,执行内核与恢复体系单独评 **A**。

质量分布呈"坚硬的核,柔软的壳":半径 ~2 万行的 A 级内核(执行器/事务日志/checkpoint/HA 栅栏),在对抗性审查下**始终零 P0/P1**;全部 P0/P1 落在边缘层,且约 80% 源于三个可治理的根因(见 §5),而非设计错误。

---

## 1. 分维度评分

| 维度 | 评分 | 依据摘要 |
| --- | --- | --- |
| 架构设计 | A | 统一执行内核(Stream/Job/分布式一份模型)、列式 Arrow 数据模型、chain fusion + barrier 对齐 + key-group 路由;legacy 执行器彻底删除;Ack trait 为不变量设计 |
| 正确性 | A-(核)/ B+(整体) | 内核/事务/恢复/HA 零 P0/P1;连续 ack frontier、整表替换恢复、三重代际栅栏、epoch 防裂脑;18 个 P1 无钉扎测试是唯一保留意见 |
| 安全性 | B | 常数时间比较、default-deny+审计、OIDC fail-closed、secret 明文永不落盘;失分:数据面明文默认(P0)、mutation 响应未脱敏、ca_file 静默丢弃 |
| 性能 | B+ | hop 间真零拷贝(逐跳验证);硬伤:窗口 O(状态)/批、SessionState 每批克隆、JSON 双解析;基准盲区:状态路径零覆盖 |
| 测试 | A-(核)/ B(边界) | 883+249 测试,中位数为具名行为契约;自验证探针、失效注入 store、start_paused;短板:插件边界结构性不可见、fixture 复制、4 处 1s-lease flake 复发 |
| 工程质量 | A- | clippy 全绿、零 unsafe、unwrap 0.4% 全惯用、内存处处有界;失分:rand 三版本、无 workspace.lints、CI 无 --locked、死文件 |
| 过程与文档 | A | 50+ WHEN/THEN spec、活战略文档、注释即不变量(抽样 30+ 处零失实)、AGENTS.md 声明 8/9 核实属实 |
| AI/宣传一致性 | C+ | 5 个检索处理器真实存在但无重试、有 panic 路径;README 两处与代码相悖 |

---

## 2. 问题登记簿(终版)

### P0(1)

| # | 问题 | 位置 |
| --- | --- | --- |
| 1 | **数据面默认明文,握手后帧无认证/完整性保护**。仅 HMAC 握手;Data/Signal/Receipt 帧裸奔;秘密回退到全舰队共享 node_token。同网段攻击者可注入伪造 Data/Barrier 帧污染数据流与 checkpoint。mTLS 存在但 opt-in | core/executor/remote/{codec,wire,auth}.rs;server/agent/session.rs:45 |

### P1(29)

**数据语义 / 静默数据问题(5)**

| # | 问题 | 位置 |
| --- | --- | --- |
| 2 | MQTT output 静默丢行:topic 向量短于 payload 时无 else 分支(Kafka 兄弟已修) | plugin/output/mqtt.rs:222-229 |
| 3 | NATS output 同型静默 skip | plugin/output/nats.rs:163-165 |
| 4 | file/sql 输入把中途错误(对象存储 5xx、DB 断连)映射为 EOF → 部分数据集被当完整交付,无重连 | plugin/input/file.rs:480-484, input/sql.rs:188-192 |
| 5 | NATS 常规订阅结束 → EOF → 无界源静默结束整个 Job(兄弟连接器均发 Disconnection) | plugin/input/nats.rs:194-199 |
| 6 | S3 WAL 恢复 fail-silent:段中 CRC 错误当 torn tail 静默截断;manifest 引用段消失被 continue 跳过;零日志零指标 | plugin/wal/segment.rs:104-159, s3.rs:916-919 |

**可用性 / 资源(10)**

| # | 问题 | 位置 |
| --- | --- | --- |
| 7 | NATS JetStream 空闲忙转:no_wait fetch 立即空回 → 满核烧 + pull 风暴 | plugin/input/nats.rs:298-353 |
| 8 | WAL PerEntry 追加失败造成永久序列缺口:seq 已占未写,后续所有 ack 卡 60s 循环直到人工删库(修法:失败归还 seq) | core/wal/mod.rs:514-517 |
| 9 | 配置校验打开运行中 Job 的 redb 独占锁 → 控制面 apply/rollback(甚至对活引擎 --validate)对 durable-state Job 必然失败;违反本文件自述的"validation must not hold the redb exclusive lock" | core/configuration.rs:322-333 + job_runner_adapter.rs:29-43,360 |
| 10 | hub nodes 写锁跨 storage await:慢 SQLite 往返拖住全舰队 agent 心跳 | server/hub/nodes.rs:493-503 |
| 11 | JobRuntime stop/stop_all 对 kernel JoinHandle 无界 await 且持锁:内核忽略取消则永久卡死启停与优雅关闭(start 路径已有界,stop 漏了) | server/agent/kernel.rs:1063-1104 |
| 12 | Kafka output close() 同步 flush(30s)不在 blocking pool:慢 broker 挂死 tokio worker 30s | plugin/output/kafka.rs:393-399 |
| 13 | HTTP output 全部写串行化:Mutex 横跨 send+重试循环 | plugin/output/http.rs:137-249 |
| 14 | KafkaOutput build() 里 tokio::spawn 后台 flusher:构造即泄漏任务 + 隐藏 runtime 上下文要求 | plugin/output/kafka.rs:216-230 |
| 15 | embedding 跨 chunk 维度不一致 → panic(dim 取首向量 + FixedSizeListArray::new 是 try_new().unwrap());远程服务可打崩 AI 路径,两行可修 | plugin/processor/embedding.rs:333,342 |
| 16 | Python clone_ref 在 tokio executor 线程拿 GIL,超时保护之外:持 GIL 的 UDF 卡住 worker 整个时长 | plugin/processor/python.rs:98 |

**安全 / 一致性(4)**

| # | 问题 | 位置 |
| --- | --- | --- |
| 17 | 变更类 API 响应返回未脱敏明文凭据(读路径脱敏、写路径裸奔;当前角色模型下无跨权泄露,但不变量只靠巧合维持;10 处) | server/api/jobs.rs:250,256,264,288,769,901,1078,1268,1419,1178 |
| 18 | PG renew_hub_lease 的 epoch 读取与 guarded UPDATE 不原子:毫秒级 split-brain 窗口(SQLite 版正确) | server/storage/postgres_methods.rs:3069-3093 |
| 19 | EXPR_CACHE 键可被字段名混叠(逗号/冒号拼接):数据驱动的字段名可令缓存的物理表达式按错误 schema 求值 → 路由错值 | plugin/expr/mod.rs:118-125 |
| 20 | Pulsar ca_file 被接受、进 schema 文档、然后静默忽略(驱动无此 API):用户以为配了私有 CA 信任锚,实际用系统根 | plugin/pulsar/common.rs:178 |

**规模 / 性能(7)**

| # | 问题 | 位置 |
| --- | --- | --- |
| 21 | 数据面默认 max_connections=256,而每对 subtask 一条 TCP 连接:16×16 exchange 打满;溢出连接报告进无消费者的通道 | core/executor/remote/manager.rs:948-968,1302-1320 |
| 22 | 单帧上限 256MiB 按连接计 + 解码全量二次拷贝:256 连接 × jumbo batch 瞬时 ~64GiB | core/executor/remote/wire.rs:11, transport.rs:51, codec.rs:238 |
| 23 | 多个 queue-full → 整个 Job 失败的升级点(收据队列/nonce 缓存满等):瞬态拥塞变 job 重启 | core/executor/remote/manager.rs:283-298,171-228,936-941 |
| 24 | 任一边失败取消整个 attempt;入站只容忍 10s grace + 5 次重拨:30s 网络分区即需 Hub 重调度(隐式 SLO 应文档化/参数化) | core/executor/task.rs:332-350; manager.rs:1807-1855 |
| 25 | 窗口 journaled 模式 O(存活状态)/批:每批深克隆整个 buffers map + 全量 backend scan(随窗口历史无界增长),与流量无关地变慢 | core/executor/window/operator.rs:2210-2251 |
| 26 | SQL 处理器 cached fast path 每批克隆整个 DataFusion SessionState(仅缓存 miss 分支用到)+ 每批新建 TaskContext | plugin/processor/sql.rs:632,725 |
| 27 | JSON 解码每批 2 遍 parse + 2 遍 copy,schema 推断零缓存:输入侧最大成本 | plugin/component/json.rs:40,91-121; codec/json.rs:71 |

**API / 工程(3)**

| # | 问题 | 位置 |
| --- | --- | --- |
| 28 | GET /jobs 无分页(最重 per-item 载荷;所有兄弟端点都有 Page);rollouts/approvals 同病 | server/api/jobs.rs:100-120 |
| 29 | API 无服务端每请求超时(有 body 限制无 TimeoutLayer):Slowloris 面 | server/api/mod.rs:495-516 |
| 30 | `examples/**` 不在 docs-check.yml 路径过滤:AGENTS.md 声明"未注册示例挂 CI"在 CI 层可绕过 | .github/workflows/docs-check.yml:5-19 |

### P2(~95,按层归类,列代表性条目)

**内核(core)**
- OperationStore 按字典序驱逐,可挤掉 Running 中的操作(op-1,op-10,op-100 陷阱) — runtime.rs:76-80
- join 侧缓冲总量无界(仅 per-key;"bounded" 注释误导) — join.rs:162-170
- state.max_bytes 使 redb 写路径 O(表)(百万键 = 每写 ~200 万行解码) — state.rs:822-888
- restore_entry 用计数器门控写,违反"计数器永不门控"自 家契约;keys 计数漏增 — state.rs:754-795
- 非有限浮点和 → serde null → V2 解码失败 → legacy 回退 → 静默重建成全零 Int64 聚合(违反自家"宁可硬失败"哲学) — window/aggregate.rs:199,366,502-510
- to_binary 跳过 null 行致载荷错位(当前被非空列约定保护) — lib.rs:432
- OperatorKind::{Filter,Udf} 可反序列化但零消费者,schema 枚举只有 6 种(双向不一致) — job.rs:53-62; component/mod.rs:897
- join pending-unmatched 锁中毒静默丢 outer 行 — join.rs:613-615
- barrier drain 2ms 忙轮询 — task.rs:578-581
- 恢复 latest_local_checkpoint 对损坏 manifest 静默跳过 — job_runner_adapter.rs:726
- CheckpointRepository::delete 遇损坏 manifest 永久失败 — checkpoint.rs:471-474
- ConfigVersionStore::save 无 fsync(同库 checkpoint store 有) — configuration.rs:87-88
- overlay_value O(待决事务×mutation)/读 — state_journal.rs:419-440
- CLI 校验错误打 stdout;--validate 成功消息在 subscriber 之前发出(不可见) — cli/mod.rs:163-192

**网络 shuffle**
- manager ~44 处 .expect("lock"):一次持锁 panic 级联毒化整个数据面直到重启 — manager.rs 多处
- barrier 帧可滞留发送缓冲 100ms(仅 flush tick) — manager.rs:2376-2445
- EOF 分类靠错误串嗅探("early eof") — manager.rs:2023-2026
- 明文模式握手 TTL 后重放可抢占收据写槽 — manager.rs:1703-1706
- kernel_handle watcher 把失败转 Error::Process 字符串丢结构 — kernel_handle.rs:556-559

**插件**
- LLM stream 模式对非 SSE/空响应静默吐空串 — processor/llm.rs:247-252
- pgvector_search 无查询超时(仅 acquire 超时):坏计划挂死批处理 — processor/pgvector_search.rs:329-332
- AI 处理器零重试退避(429/5xx 即批失败重启循环;库里 RetryUtils 被测过但零调用) — embedding/llm/vector_search/milvus_search
- MQTT input ack close 后静默成功 + 可跨客户端代际 PUBACK — input/mqtt.rs:270-279
- HTTP input 吞发送失败仍返 200;schema 宣传不存在的 client 模式 — input/http.rs:154-156,311-323
- SQL output 标识符不转义;连接失败误映射 Process(引擎重连只认 Disconnection) — output/sql.rs:226-273
- InfluxDB escape 函数重复且 field 值不转义换行 — output/influxdb.rs:510-528
- memory buffer 文档按消息、实现按行数 — buffer/memory.rs:40-46,183-190
- qdrant collection 名拼进 URL 路径不编码;429 不重试;退避 ≥64 次溢出 — output/qdrant.rs:165-169,198-204,227
- milvus 内联重复 build_http_client、默认 retry 0 — output/milvus.rs:439-454
- stdout 输出在 async 里直写(管道阻塞 worker) — output/stdout.rs:77-87
- lookup/schema_registry 响应体无界读取 — processor/lookup.rs:524, codec/schema_registry.rs:640
- debezium 全墓碑批空 schema 不稳定;无 on_error 策略 — codec/debezium.rs:98-105
- schema_registry 兼容门禁判定的缓存永不复查;空凭据 unwrap_or_default — codec/schema_registry.rs:216-225,687-692
- modbus 无超时:慢设备迟响应被当下一轮响应(帧错位) — input/modbus.rs:143-190
- redis 错误子串分类过宽;LIST 模式 NoopAck 至多一次未文档化 — input/redis.rs:99-115,485-495
- 6 处 flume::bounded(1000) 魔数 vs 内核 1024 — input/ 多文件
- "未连接"错误变体轮盘:8 文件 5 种变体(内核按变体分支,行为随作者口味变) — 多文件
- buffer/ 整层为注册但不可达死代码(4 个潜伏缺陷:滑窗 drain 重复发射、join 丢数据配 ack、LIFO 序、tumbling 不按窗口),建议删除 — plugin/buffer/*
- vector_util 骨架只有 3/5 使用:truncate_body ×3、client 构建 ×3(语义暂同,漂移潜伏) — plugin/{embedding,llm,vector_search,milvus_search}
- pulsar input Ok(None)→EOF;死 _consumer_builder — input/pulsar.rs:350,179-188
- Azure 设 endpoint 即静默允许 HTTP(S3 需显式) — input/file.rs:370-374

**服务端**
- SQLite fence 内 panic → 事务悬挂 + fence_depth 卡死,永久 wedge(现无 panic 点,纯隐患) — storage/sqlite.rs:195-274
- list_intents N+1(4096 ids 逐个查) — storage/sqlite.rs:869-884
- cp_intents/cp_rollouts 等 5 表无修剪;cp_rollouts.correlation_id 无索引且在审批热路径 — storage/sqlite.rs:2899+
- PG commit-after-body-error 模式可提交部分工作(SQLite 回滚) — storage/postgres_methods.rs 全局模式
- /metrics 无认证 + 无限流豁免(文档化接受;JSON 路径 metrics_by_node 调两次) — api/diagnostics.rs:25-137
- create_user 查重不加 identity_write_lock(并发覆写) — hub/identity.rs:228-260
- job_upgrades/rollouts 内存缓存 insert-only,step-down 不清 job_upgrades — hub/job_orchestration.rs:184-187; leadership.rs:228-241
- act_rollout 无 phase CAS(读改写);升级排他性 check-then-insert — hub/rollout.rs:182-393; job_orchestration.rs:120-129
- 用户更新无 CAS 丢更新 — api/users.rs:264-335
- webhook SSRF(文档化接受;建议 redirect::none + 环回拒绝) — hub/webhooks.rs:40-42
- 审批 note 入审计无截断 — hub/approval.rs:347-353
- 跨 re-placement 的 checkpoint 轮次静默永不完成(24h 后修剪) — hub/operations.rs:764-796
- software_version 自由字符串进 Prometheus 标签(唯一无界基数点) — api/diagnostics.rs:90-97
- SSE resync:服务端发、console 客户端静默丢弃(契约半实现) — console/src/api.ts:945-948
- 操作历史"保护行"谓词靠 JSON LIKE 匹配(脆弱) — storage/sqlite.rs:953
- 通配 node scope 在 livespace 上授权为零(未文档化不对称) — api_contract.rs:127-134
- bootstrap:SQLite 备份失败残留半成品阻塞重试;id-floor 播种失败 warn-and-continue(自述可能主键覆写) — bootstrap.rs:146-153,386-399
- heartbeat/report interval 默认 Burst 模式 — agent/session.rs:385-387

**console**
- 单一全局错误边界(一页崩全壳) — app.tsx:96
- VITE_API_TOKEN 进公共 bundle(已文档化,无构建期警告) — api.ts:479-484
- 无 CSP/X-Frame-Options — nginx 模板
- waitForOperation 不可取消 — api.ts:998-1022
- Tailwind v4 引入但全手写 CSS(死依赖) — styles.css:1

**CI/工具**
- 第三方 action 未 SHA 固定(docker/helm 已做,其余未);多 workflow 缺 permissions 块
- 全仓库无 [workspace.lints](clippy 纪律只活在 CI 命令行)
- CI 无一 --locked:Cargo 可在 CI 静默改锁文件,架空锁卫生测试
- rand 0.8/0.9/0.10 三大版本共存(plugin Cargo.toml:89 vs 根 :72;锁卫生测试只看 arrow/datafusion/zstd)
- rustls + native-tls 双 TLS 栈并存(sqlx/redis/tungstenite 走 native-tls)
- test-doc-deploy.yml 每个 PR 全量构建 Docusaurus(无路径过滤,陈旧 action)
- legacy-stub 豁免 8 个仍含 26 活页的目录;绝对路由链接不校验;versioned_docs 242 页逃逸全部结构门禁
- 依赖集中化约定违规:plugin 重声明 object_store/tower/base64/bytes/num_cpus/once_cell

**文档矛盾(8 处,证据核实)**
- checkpoint "checksum authenticates"(实为未键控 FNV 只防损坏) — checkpoint.rs:776
- LZ4 文档 1-9 vs 校验 1-16 — wal/config.rs:141 vs wal/mod.rs:187
- schema "validation" 能力未接线;变体无 additionalProperties:false — component/mod.rs:27,897
- join "bounded" 仅 per-key — join.rs:12
- state 计数器"永不门控写" vs restore_entry 门控 — state.rs:476 vs 754
- configuration 流路径锁不变量 vs job 路径违反 — configuration.rs:293 vs 322
- CLI "exit it" 注释与实际机制不符 — cli/mod.rs:190
- README:MILVUS "one batched request" vs 每行一请求;性能声明无数字 — README.md:163,16

### P3 / 清理(~25)

- 死文件:plugin/auth_middleware.rs(不在模块声明,永不编译,含 stdenv 笔误——测试编译失败是误报,已实测 cargo check 通过)
- 死变体 OperatorKind::{Filter,Udf};死抽象 RetryUtils;死字段 output/redis connection_manager;console Snapshot/operationId
- pgvector 拒绝合法负 BIGINT id;NaN/Inf 生成非法向量文本 — pgvector.rs:405,360
- join 时间戳减法溢出/abs(i64::MIN) panic(±29 万年才触发) — join.rs:763
- 错误信息多空格 ×2 — api/mod.rs:261, job_runner_adapter.rs:870
- wal/s3 与 mqtt_tls 泄漏 tempdir;冒号命名/CRLF 等 20 余条小项

### 测试债(4 类)

1. **18 个已知 P1 零钉扎测试**(精确解释了它们为何存活)。优先补:Kafka close 有界 flush、EXPR_CACHE 混叠、file/sql 错误≠EOF、MQTT/NATS 丢行回归、file/sql stream-error≠EOF、PG lease 并发续约、mutation 响应脱敏断言
2. **1s-lease flake 复发 ×4**(hub/tests.rs:195、session_report_tests:256、:4932、serve_hub 选举尾断言)——库里已有正确模式(60s TTL + 直接注入 expires_at_ms,即 atomic_upgrade_fixture)未推广;core 侧 remote/tests 把 registration_grace 缩到 100ms(~10 测试)同型
3. **Docker 套件固定宿主端口**(9092/1883/4222/27017/6650/16379)+ 启动时 docker rm -f 清扫:并行 CI 互删容器;仅 2 个测试用 start_paused
4. **fixture 巨量复制**:JobRecord 字面量 ×30、KafkaInputConfig ×15、ObjectStoreWalConfig ×10、StateBackend 替身 ×6、JobSpec 全量字面量 ×10

**不变量钉扎矩阵(合并)**:连续 ack frontier / barrier drain 排除 held / 快照只含 applied / fired 先提交后 ack / WAL 连续推进 / 对齐溢出失败轮次 / 陈旧 attempt 拒绝 / 命令 at-most-once / 代际栅栏 / pause CAS / 升级状态机 / secret 不落盘 —— 全部钉死。唯一未钉:恢复整表替换(仅实现无断言)、PG lease 并发续约、webhook 自动暂停。

---

## 3. CI 门禁真实性核验(AGENTS.md 声明 vs 工作流)

| 声明 | 结论 |
| --- | --- |
| clippy -D warnings 是 PR 门禁 | ✅ 属实(rust.yml:89) |
| CI 跑 cargo test --workspace --all-targets | ✅ 基本(nextest + plugin 单独;doc tests 仅 plugin 跑) |
| inventory 快照 CI 失败 | ✅ 属实 |
| docs:check 双向所有权校验 | ✅ 属实 |
| 未注册 examples/ 挂 CI | ⚠️ **脚本属实但 CI 层可绕过**(路径过滤漏 examples/**) |
| README 三语对账 | ✅ 属实(空 inventory 空转,Rust 快照是真守卫) |
| YAML 片段分类双 gate | ✅ 属实(词汇表同步测试闭合漂移孔) |
| coverage/benchmark 是门禁 | ✅ 无此声明,工作流如实(观测非门禁) |
| 安全:无 pull_request_target / 注入 / 密钥暴露 | ✅ 核实干净 |

---

## 4. 覆盖账

| 层 | 生产代码 | 测试代码 |
| --- | --- | --- |
| arkflow-core | 本人逐行 executor 全部 + Ack/WAL/checkpoint/job/state/等;agent 补余量 | 100% 清单级,~70% 正文级 |
| arkflow-plugin | 3 agent 全组件 + 本人 kafka.rs 全文/kafka_txn/udf/lib | 100% 清单级,~40% 正文级 |
| arkflow-server | 3 agent 全模块(含存储双层/身份/限流/指标) | 100% 清单级,~60% 正文级 |
| arkflow 二进制 | main.rs 全文 | 3 集成测试逐行 |
| console | 1 agent 全覆盖 | 同左 |
| docs/scripts + CI workflows + Cargo.tomls | 1 agent 逐行 | — |

---

## 5. 三个根因与五个治理动作

**根因**(80% 的 P1 源于此):
1. **拷贝熵**:兄弟组件复制后各自漂移(MQTT/NATS vs Kafka 行对齐;qdrant/milvus/redis;embedding/llm 未并入 vector_util;第三种 Ack 复合变体)
2. **Error 变体是未治理的契约**:内核按变体分支(EOF=优雅结束/Disconnection=重连/其余=致命),连接器按作者口味选变体
3. **测试盲区**:core 测试钩子结构上看不见插件边界

**动作(按投入产出)**:
1. 数据面强制认证(1 个协议决策,关闭唯一 P0)
2. Error 变体写进 spec + 连接器一致性测试(连带消 3 个 P1)
3. 连接器共享骨架:vector_util 收编 5 处、payload 对齐收进 output/payload.rs、重试复用 RetryUtils(连带 ~5 个 P1)
4. 推广 1s-lease 既有模式到 4 个复发点 + 按 §2 测试债清单补钉扎
5. 机械修复包:redb 锁(configuration.rs)、embedding 跨 chunk 校验、PG lease 用 UPDATE...RETURNING、mutation 响应过 redact_record_fields、/jobs 分页、[workspace.lints] + rand 统一 + CI --locked

---

## 6. 结语

每一行生产代码、每一个测试模块、每一个门禁脚本都在本报告的账上。这个代码库最难得的不是某一行代码写得好,而是处处能看到"作者知道自己在防范什么":spec 承载契约、回归测试记录 bug 史、注释可被逐行核对、错误信息教正确写法。它的 1 P0 + 29 P1 是存量清偿,不是复利负债——完成 §5 的五个动作后,可以不加限定词地称为生产系统。

---

## 7. P1 复核(2026-11-21,逐条核实)

§2 登记簿是 2026-11-14/15 快照;其后 #1365/#1366/#1367/#1372 落地,本节刷新 29 条 P1 中两条批次未覆盖的 14 条状态。行号已更新为 2026-11-21 main。

**已修复(1)**
- ~~#16~~ Python GIL:timeout 现在覆盖"GIL attach + 信号量获取 + 调用"全程,注释记录了修复动机(python.rs:103-107)。

**仍在(6)**

| # | 问题 | 当前位置 |
| --- | --- | --- |
| 13 | HTTP output 互斥锁横跨 send+重试循环,全部写串行化 | plugin/output/http.rs:134-137(`Arc<Mutex<Option<Client>>>` 于 send 入口锁定) |
| 20 | Pulsar ca_file 被解析、进测试,驱动侧静默丢弃 | plugin/pulsar/common.rs:178(全文件无消费点) |
| 21 | 数据面 max_connections 默认 256,16×16 exchange 打满;溢出报告进全局通道(生产 Job 不消费,仅 log) | core/executor/remote/manager.rs:73(默认值),:949-961(限制+全局通道) |
| 22 | 单帧上限 256MiB 按连接计 + IPC body 解码全量二次拷贝(`body.to_vec()`) | core/executor/remote/wire.rs:11,codec.rs:238 |
| 25 | 窗口 journaled 每批深克隆整个 buffers map + 全量 backend scan | core/executor/window/operator.rs:2211-2228(克隆),:2244 起(scan) |
| 30 | docs-check.yml 路径过滤不含 examples/** | .github/workflows/docs-check.yml |

**部分修复/缓解(7)**

| # | 已修部分 | 剩余部分 |
| --- | --- | --- |
| 14 | close() 取消 flusher 任务(kafka.rs:371) | 构造期即 spawn(runtime 上下文要求);build 后未 close 即 drop → 任务永久泄漏(无 Drop 实现) |
| 15 | API 响应内一致性已校验(embedding.rs:218) | `append_vector_column` 仍取首向量 dim,:344 `FixedSizeListArray::new` 跨响应不一致路径未防御 |
| 19 | 缓存键已含 schema(name:Type),修复重排/换型复用 | 字段名混叠仍可行:单字段名 `x:Int64,y` ≡ 两字段 `x: Int64` + `y: Utf8`(expr/mod.rs:122-124 逗号 join 无转义) |
| 23 | mark_held/receipt 溢出改显式 try_send 升级+日志;pending receipt 有条目数+字节预算 | 预算超限仍返回 Err 上抛;是否 job-fatal 需语义确认(原升级点已大幅重构) |
| 24 | 边失败 watcher 先发布原因再取消(task.rs:338-345) | 10s grace / 5 次重拨常量仍在(manager.rs:81-83),SLO 未参数化 |
| 26 | plan 缓存(analyzed+physical 按 schema 键控)已加 | `ctx.state()` 每批仍无条件克隆(sql.rs:636) |
| 27 | 流式 schema 推断消除 serde 双解析+Value 树;单遍解码器消除 concat 拷贝 | 每批仍推断一遍 schema,零缓存(codec/json.rs:75,component/json.rs:35) |

**分批(更新 §5 视角)**
- A 批小刀二批(机械,低风险):#13、#14 剩余、#15 剩余、#19 剩余、#20、#30 —— 6 条,纯插件/CI。
- B 批热路径性能:#25、#26 剩余、#27 剩余 —— 先立 benchmark 锚点。
- C 批 remote 数据面:#21、#22、#23 剩余、#24 剩余 —— 与 P0(数据面认证)同一子系统,并入同一轮协议设计,不单独做。

## Why

2026-11-15 全量代码审查（仓库根 `CODE-REVIEW-2026-11-15.md`）登记 1 P0 + 29 P1；`fix-silent-data-loss`（#1367）与 `stability-small-knives`（#1372）已消化其中 15 条。2026-11-21 逐条复核（登记簿 §7）确认剩余 14 条中 **6 条完全仍在、7 条部分修复**。本 change 收敛其中插件/CI 层的 6 条机械修复（A 批"小刀二批"）——全部为 fail-fast/缩界/生命周期级修法，低风险、可钉扎测试。

逐条证据（2026-11-21 main 行号）：

| # | 问题 | 证据 |
| --- | --- | --- |
| 13 | HTTP output 互斥锁横跨 send+重试循环，全部写串行化（慢端点拖住所有并发写） | `crates/arkflow-plugin/src/output/http.rs:134-137`，`Arc<Mutex<Option<Client>>>` 于 send 入口锁定直至函数返回 |
| 14 | Kafka output 构造期即 `tokio::spawn` 1s flusher 循环：构造要求 runtime 上下文；build 后未 close 即 drop → 任务永久泄漏（无 `impl Drop`） | `crates/arkflow-plugin/src/output/kafka.rs:218`（spawn）、:371（仅 close() 取消） |
| 15 | embedding `append_vector_column` 取首向量 dim，跨响应维度不一致 → `FixedSizeListArray::new` panic（远程服务可打崩 AI 路径） | `crates/arkflow-plugin/src/processor/embedding.rs:333`、:344；:218 的响应内校验不覆盖跨响应拼接 |
| 19 | EXPR_CACHE 缓存键字段名混叠：单字段名 `x:Int64,y`（类型 Utf8）与两字段 `x: Int64` + `y: Utf8` 渲染出相同键 `x:Int64,y:Utf8` → 缓存的物理表达式按错误 schema 求值，路由错值 | `crates/arkflow-plugin/src/expr/mod.rs:122-124`（逗号 join 无转义/长度前缀） |
| 20 | Pulsar `ca_file` 被解析、进 schema 与测试，驱动侧静默丢弃——用户以为配置了私有 CA 信任锚，实际用系统根 | `crates/arkflow-plugin/src/pulsar/common.rs:178`（字段定义），全文件无消费点 |
| 30 | docs-check.yml 路径过滤不含 `examples/**`：AGENTS.md 声明"未注册示例挂 CI"，但示例文件变更不触发 docs-check，CI 层可绕过 | `.github/workflows/docs-check.yml`（全文件无 examples 引用） |

## What Changes

- **#13 HTTP output**：send 路径去除跨 await 的互斥锁——`Client` 内部为 Arc，clone 后释放锁再发请求；锁仅保护 connect 时的 client 初始化。并发写不再互相串行化。
- **#14 Kafka output**：flusher 任务生命周期移出构造期——改为首次 `connect()` 成功后启动（异步上下文必然存在），并补 `Drop` 兜底取消，build-后-drop 不再泄漏任务。
- **#15 embedding**：`append_vector_column` 构建列前校验向量集合维度一致，不一致返回指名维度的显式错误，MUST NOT panic。
- **#19 EXPR_CACHE**：缓存键改用无歧义编码（长度前缀）根治字段名混叠，附恶意字段名钉扎测试。
- **#20 Pulsar ca_file**：**BREAKING**——从静默忽略改为显式拒绝：配置含 `ca_file` 时 build 期报错（指名驱动无此 API），同步移除 schema 宣告与文档（EN + zh-Hans）。该字段从未生效，依赖者本就未受保护。
- **#30 docs-check.yml**：`paths` 过滤加入 `examples/**`，未注册示例变更触发 CI 门禁。

## Capabilities

### New Capabilities

（无——#13/#14 为锁缩界与任务生命周期实现修复，无 requirement 级行为契约变化，不新增 http-output/kafka-output capability。）

### Modified Capabilities

- `expr-row-routing`：新增 requirement——表达式计划缓存的键 SHALL 对任意字段名（含含 `,`/`:` 等分隔符的名字）无歧义；键冲突 MUST NOT 导致按错误 schema 求值（#19）。
- `ai-vector-ingestion`：embedding 的维度一致性校验从"单响应内"扩展为"进入列构建前全量"，跨响应不一致 SHALL 显式报错（#15）。
- `component-config-honesty`：被解析宣传但驱动无法实装的配置项 SHALL 在 build 期显式拒绝（以 pulsar `ca_file` 为首个实例），MUST NOT 静默丢弃（#20）。
- `documentation-quality-gates`：docs-check CI 门禁的路径过滤 SHALL 覆盖 `examples/**`，未注册示例无法绕过门禁（#30）。

## Impact

- **代码**：`crates/arkflow-plugin/src/output/http.rs`、`output/kafka.rs`、`processor/embedding.rs`、`expr/mod.rs`、`pulsar/common.rs`
- **CI**：`.github/workflows/docs-check.yml`
- **文档**：pulsar 输入/输出文档页（EN + zh-Hans）移除 `ca_file`；schema 快照测试（inventory）随元数据同步
- **破坏性**：仅 #20——`ca_file` 从"静默无效"变为"显式报错"，无静默数据面影响
- **无依赖变更**；全部修复不引入新 crate

## Non-goals

- **remote 数据面 4 条 P1（#21/#22/#23/#24）**：与 P0（数据面强制认证）同属 `core/executor/remote/` 子系统，并入未来协议 change 一次设计，不单独动。
- **热路径性能 P1（#25/#26/#27）**：B 批，前置条件是先立 benchmark 锚点（调查进行中）。
- **P0 数据面强制认证**：协议级决策，单独 change。
- **HTTP output 连接池/重试语义重构**：仅缩界去锁，不改重试策略。

# Design — fix-plugin-p2-batch

## Context

`openspec/CODE_REVIEW_2026-09-29.md` 插件层 P2 批共 12 组缺陷，已在 main @ `b2d7bc2` 逐条核实仍存在（行号有漂移，见 proposal 引用）。涉及 `crates/arkflow-plugin` 的 codec/input/output/processor/buffer 五类组件。约束：**arkflow-core 零改动**（复用既有 `Error::Disconnection` 引擎重连契约与 `input-durability` ack 语义）；遵循 openspec 设计规则「surgical changes over rewrites」；release profile 下迭代用 debug 构建。

关键既有事实（实现时直接依赖）：
- 引擎只对 `Error::Disconnection` 重连（`arkflow-core/src/executor/task.rs:750-792`），其余错误（含 `Process`/`Connection`）使 source 任务失败（`:793-800`）；引擎**没有** output 级重连。
- pulsar output 已实装 `value_field`（`output/pulsar.rs:203` 起：显式列名 → 逐行取该列值（Binary/Utf8）作 payload，null/类型不符报错），是四个 output 补齐的现成先例。
- Kafka input 已在 codec 之前拦截 null-payload tombstone 并自行结算（`input/kafka.rs:410-464`）；codec 层的空 payload 处理是补充防线。
- `buffering-stage-drain` spec 已为 batch processor/memory buffer 建立「合并失败保留已持有消息与 ack」契约，window buffer 家族是同类漏网。

## Goals / Non-Goals

**Goals:**
- 消除插件层的静默数据丢失/腐坏/失效路径（见 proposal What Changes 六组）。
- 组件元数据（CLI `--schema`、server `/component` API 暴露的 schema/example）与真实配置面一致，且内置示例可通过真实构建。
- 所有行为变化有测试钉住；固化缺陷行为的既有测试同步修订。

**Non-Goals:**
- 见 proposal Non-goals（DLQ、引擎级 output 重连、SQL 复杂类型、内核/console P2、schema 语义演进映射、redis stream/sql 轮询实装）。

## Decisions

### D1. schemaType 缺省：按 schema 文本内容判定，不改缺省值也不加配置
备选：(a) 缺省改 AVRO（符合 Confluent 旧 registry 语义，但破坏「旧 registry + Protobuf schema + 省略 schemaType」的现有可用配置）；(b) 新增 `default_schema_type` 配置（把缺陷转嫁给用户）；(c) **内容判定**（选定）：schema 文本 trim 后以 `{` 开头且可解析为 JSON 对象 → Avro，否则按 Protobuf 解析。Avro schema 恒为 JSON 对象、proto 源码恒不是，判定无歧义；两个方向的真实演进配置都保持可用。判定失败（非 JSON 对象又非合法 proto）报错并说明尝试过的两种解析。spec 同步把「缺省 MUST 视为 PROTOBUF」改为内容判定。

### D2. 兼容门禁：通过结论永久缓存，失败结论按最小间隔重试
`gate_state: OnceCell<Result<(), String>>`（`schema_registry.rs:163`）改为 `RwLock<Option<Result<(), String>>>` + `last_attempt: Instant`。成功 → 永久缓存（spec 原语义保留）；失败 → 间隔不足 `GATE_RETRY_INTERVAL`（常量 30s）时直接返回上次错误（不发请求），到期后下次 decode 重试。不加配置项（避免配置面膨胀；常量可后续按需开放）。修订 `test_gate_result_is_cached`：通过后不重试保留，新增「失败 → registry 恢复 → 下次重试成功」用例（wiremock 序列响应）。

### D3. 批次归一合并：插件 crate 内共享 helper
新增 `crates/arkflow-plugin/src/component/batch_merge.rs`（暂名）：`normalize_and_concat(batches) -> Result<RecordBatch>`——计算字段并集（同名同类型保留；同名不同类型报错并指名两类型；缺列以全 null 列补齐；列序取首个批次顺序、新列追加），再 concat。两个消费方都在插件 crate：schema_registry 多版本解码、window buffer 跨 input 合并。**不放 core**（core 零改动约束；将来第三处出现再上移）。修订两个「同 schema 文本」多版本测试为真实演进（id1 少一列 / id2 加列）。

### D4. debezium 冲突消解：业务值保留原列，元数据改投 `__debezium_<name>`
备选：(a) envelope 胜出 + warn（现状可见化，但业务数据仍被覆盖——腐坏未修）；(b) 报错（把静默腐坏变成整批停摆，过激）；(c) 业务胜出、丢弃 envelope 值（丢操作语义）；(d) **双双保留**（选定）：业务字段列值不动，envelope 元数据（`op`/`ts_ms`/`source_db`/`source_table`/`before`/`source`）在同名冲突时写入 `__debezium_<name>` 列并每批 warn 一次。无冲突时元数据列名不变（既有管道零影响）。跨行冲突集不一致由既有 JSON→Arrow 推断的并集语义天然处理。tombstone：0 字节 payload 跳过该行 + warn（与 Kafka input 的 null-payload 拦截语义对齐）；JSON 字面量 `null` 维持现状（解析为全 null 行，非 tombstone）。

### D5. 错误分类：纯函数 helper + 单测
modbus 四个读路径与 nats 首次订阅/fetch 的错误映射收敛为 `fn classify(e) -> Error` 式小函数：连接类/IO/超时 → `Disconnection`（引擎重连），协议/反序列化 → `Process`（数据毒丸不应触发无限重连循环）。modbus 目前零测试——分类函数纯逻辑可直接单测（不必起真 modbus 服务）。nats 对齐 JetStream 循环已有的 Disconnection 用法（`nats.rs:337,351`）。

### D6. MQTT output：写路径触发的惰性重连，有界
备选：(a) 常驻 supervisor 任务自动重连（组件多、故障被隐藏）；(b) **写时惰性重连**（选定）：publish 失败或发现 eventloop 已死 → 重连序列【abort 旧 eventloop JoinHandle → 旧 client best-effort disconnect → 建 client + spawn 新 eventloop → 重试 publish】，退避 1s/2s/4s 共 3 次，仍失败返回 `Error::Connection`；`connected` 标志在 eventloop 任务退出时即置 false（现在是 close 才复位，`:141`）。修复泄漏点：二次 connect 覆盖 `client`/`eventloop_handle` 前先清理旧值（`:113-143`）。引擎无 output 重连框架，本变更不引入——这是组件本地恢复。

### D7. window buffer：先合并后出队 + select 唤醒
- **失败不丢**：现 `process_window` 在 `buffer/window.rs:113` 先 `queue.remove(&input_name)` 把队列整体取出，`:160` 合并失败即丢 batch 与 ack。改为取出的局部变量在**任何失败路径上原样放回队列**再返回 Err（ack 不 settle 不 abort，留在队列随重试）。类型真冲突（D3 归一报错）同样走此路径。
- **异构 schema**：用 D3 helper 归一后 concat；缺列 null 填充，类型冲突报错（fail-loud，不猜 cast）。
- **close 挂起**：读者等待从 check-then-wait 改为 `tokio::select! { _ = notify.notified() => .., _ = close_token.cancelled() => .. }`，token 触发后按 `buffering-stage-drain` 的 close 语义排空余量再结束；三个窗口 buffer（tumbling/sliding/session）共用 `process_window` 与同型读者循环，一并修复。现有测试用 `timeout(200ms)` 包裹测不出挂起——补「close 时空队列读者有限时间内返回」用例。

### D8. vrl fail-loud（BREAKING 行为变化）
- UInt64：`i64::try_from(v)` 失败 → `Error::Process`（错误含列名与原值），删除裸 `as i64`（`vrl.rs:256`）。
- 不支持类型（List/Struct/Decimal/Dictionary 等）：返回错误指名列与类型，删除「整列写 Null + error! 日志」路径（`vrl.rs:345-352`）；downcast 失败分支（如 `:171-177` 静默缺列）同样显式报错。
- 备选（否决）：保持 Null 但升级日志——审查定性即「静默丢失」，日志无人值守等于未修；字符串化（否决）：类型惊喜且丢数值语义。错误信息提示用 `filter_columns`/SQL 预投影规避。

### D9. json/protobuf 隔离：`on_error: fail | skip`，缺省 fail
- Fail（缺省）= 现状整批报错，路径不动（json 仍是 join + 整体推断，避免热路径每消息一次 schema 推断的性能回退）。
- Skip：逐消息解码——json 逐条 `try_to_arrow`、protobuf 逐条 `protobuf_to_arrow`（本就逐条，`:104-107`，只需失败时不 `?` 返回），坏消息 warn（含序号与错误）后跳过，好消息经 D3 归并产出。全坏 → 返回错误（不产出空批次假装成功）。
- 配置落在两个 codec 的 config struct（`on_error` serde 缺省 `fail`）。

### D10. SQL output：扩类型 + 构建期拒 codec + close 关连接
- `matching_data_type`（`output/sql.rs:505-559`）扩展：Int8/16/32 → i64 参数、UInt8/16/32 → u64 参数、Float32 → f64、Date32/Date64 → ISO 日期字符串、Timestamp（任意 unit）→ RFC3339 字符串；Unsupported 报错指名**列名与类型**（现在只报类型）。Pg 路径 `u as i64` 强转（`:228`）保持现状并在错误信息文档化（既有行为，非本批新增风险）。
- build 时配置了 codec → 构建错误「sql output writes typed columns; codec is not supported」（现在 build 静默接受、write 必然失败于 Binary 列，`:400-427,598-604`）。
- close（`:452-455`）：除 cancel token 外，从 `conn_lock` take 连接并显式关闭（现在 token 无使用者、连接随析构）。

### D11. python processor：无 panic 装配 + 超时
- `sys.path`：组装目标序列 `[python_path 按配置顺序..., "."]`，逐项 `PyList::insert` 收集 `PyResult`，失败 → `Error::Process`（删 `unwrap()`，`python.rs:117-121`）；插入前查 `contains` 去重（修跨实例重复追加污染进程级 sys.path）。顺序语义：配置列表靠前 = 优先级高（修现状反转）。
- 超时：`timeout_ms: Option<u64>`，缺省 60000；`process()` 的 `spawn_blocking` 外包 `tokio::time::timeout`，超时 → `Error::Process`（含已耗时）。已知限制：超时只放弃等待、阻塞线程要等 UDF 自然返回才能回收（见 Risks）。
- 元数据 schema 同步真实字段（`script` 非 required、补 `module`/`function`/`python_path`/`timeout_ms`、删幽灵 `extra_packages`）。

### D12. 元数据诚实批：逐项对齐 + 门禁测试
- 幽灵字段修正/死配置移除清单见 proposal；`value_field` 四个 output 复用 pulsar 的 `field_payloads` 逻辑——**抽取为共享模块**（`output/payload.rs`，pulsar 一并改用），kafka/mqtt/nats/redis 的 write 路径在配置了 `value_field` 时走列取值、否则维持 codec 编码。
- websocket `headers`：用 tokio-tungstenite 的 request builder 构建 handshake 请求（`connect_async(request)`，现在 `:98` 只传 URL 字符串），逐 header 写入。
- **门禁**：新增测试遍历注册表全部组件元数据——`config_example`（存在时）必须能通过声明的 schema 校验且能反序列化为组件真实 config struct（不做 IO；个别 builder 构建期联网的组件允许仅做反序列化校验并注明）。这把「示例缺 timeout_ms」类缺陷变成 CI 红灯，防止回潮。

### D13. 既有测试修订清单（固化缺陷行为 → 钉住新行为）
`schema_registry.rs`：`test_gate_result_is_cached`（失败重试部分）、`test_multi_version_each_resolves` / `test_multi_version_avro`（同 schema 文本 → 真实演进）、`test_rest_resolver_bearer_auth`（无 schemaType 断言 Protobuf → 内容判定）。`debezium.rs`：补 tombstone 与同名冲突用例。`codec/json.rs`：`test_json_codec_decode_invalid_json` 保持（fail 缺省）+ 补 skip 用例。vrl/sql output/window buffer/python/元数据新增用例见对应决策。

## Risks / Trade-offs

- [vrl fail-loud 使依赖静默 Null 的现有管道突然报错] → 错误信息给出规避路径（`filter_columns`/SQL 预投影）；发布说明标注 BREAKING。
- [python 超时放弃的阻塞线程持续占用 blocking 池槽位直至 UDF 返回] → 文档明示；缺省 60s 足够宽松，死循环 UDF 至少不再挂死流。
- [json skip 模式逐条推断有性能成本] → 仅 opt-in 路径；fail（缺省）路径字节不变。
- [schemaType 内容判定遇非常规 schema 文本] → 仅在字段缺省时启用；判定失败显式报错列出两种尝试，不静默猜。
- [window buffer 归一在极高列数并集下放大批次宽度] → 类型冲突本就报错；缺列 null 填充的宽度增长是异构输入的固有代价，warn 提示。
- [元数据修正使既有配置无法通过 `--validate`]（file/sql/modbus 的 `codec`、redis 幽灵变体等）→ 运行时 serde 仍容忍未知字段（未加 `deny_unknown_fields`），仅 schema 校验拒绝；发布说明列明迁移（删除无效字段）。
- [MQTT 惰性重连在长时间断连下每次 write 都走 3 次退避] → 有界重试后返回错误交给上层重试语义，不在组件内无限循环。

## Migration Plan

无运行时迁移。配置层：受 BREAKING 影响的字段在 `--validate` 即报错（快失败）；生成产物（component-inventory.json / config-schema.json）随实现一并再生。回滚即回退 commit，无状态/格式残留。

## Open Questions

无阻塞项。三个缺省值（gate 重试间隔 30s、python 超时 60s、MQTT 重连 1s/2s/4s×3）为实现缺省，评审可调。

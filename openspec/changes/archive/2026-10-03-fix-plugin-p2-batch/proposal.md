# fix-plugin-p2-batch

## Why

v1.0 就绪度代码审查（`openspec/CODE_REVIEW_2026-09-29.md` 第 58-70 行「插件层（P2 批）」）在插件 crate 发现 12 组健壮性/边界正确性缺陷，经逐条核实（main @ `b2d7bc2`）**全部仍然存在**，仅 pulsar 的 `value_field` 与 nats JetStream 循环的错误分类在此前变更中已顺带修复。这批缺陷的共性是「静默」：静默丢数据（window buffer 合并失败连 ack 一起丢，`buffer/window.rs:113,148-161`）、静默腐坏数据（vrl UInt64 回绕 `processor/vrl.rs:256`、debezium 元数据覆盖业务同名字段 `codec/debezium.rs:119-124`）、静默失效（schemaType 缺省按 Protobuf 误判 `codec/schema_registry.rs:322-327`、兼容门禁失败结果永久缓存 `:163-192`）、静默不生效（4 个 output 的 `value_field` 解析后从不读取、websocket `headers` 死配置、`codec` 字段在 file/sql/modbus input 上 `#[allow(dead_code)]`）、以及向用户撒谎的组件元数据（batch/redis/sql 等的 schema 与示例按现状配置必然构建失败，如 `processor/batch.rs:188` 示例缺必填 `timeout_ms`、`output/redis.rs:221-224` 示例用不存在的 `stream` 变体）。另有可用性缺陷：modbus/nats 瞬时错误归类 `Process` 致引擎判死不重连（`input/modbus.rs:119-164`、`input/nats.rs:206-215`，引擎契约见 `arkflow-core/src/executor/task.rs:750-800`）、MQTT output 无重连且二次 connect 泄漏旧 eventloop（`output/mqtt.rs:113-143`）、SQL output 仅支持 5 种 Arrow 类型且配 codec 必然运行期失败（`output/sql.rs:505-559,400-427`）、SQL processor 临时表错误路径跳过注销（`processor/sql.rs:219-241`）、python processor 可 panic 且 UDF 无超时（`processor/python.rs:117-121,47-80`）、json/protobuf 一条坏消息整批失败无隔离（`codec/json.rs:44-51`、`codec/protobuf.rs:100-121`）。P1 批次已全部落地，本批次是 v1.0 前插件层质量断层的收口。

## What Changes

**Codec 正确性**
- schema_registry：多版本（不同 schema 文本）消息合并前先做 schema 归一（并集 + 缺列 null 填充），不再整批 `concat_batches` 失败；`schemaType` 缺省时按 schema 文本内容判定（JSON 对象 → Avro，否则 Protobuf），替换硬编码 PROTOBUF 缺省；兼容门禁只永久缓存通过结论，失败（尤其瞬时故障）在后续 decode 按最小间隔重试。
- debezium：空 payload tombstone 跳过并告警（不再整批失败）；envelope 元数据与业务字段同名时业务值保留原列、元数据改投 `__debezium_<name>` 列并告警（不再静默覆盖）。

**错误分类与恢复**
- modbus/nats input：瞬时读/订阅错误归类 `Error::Disconnection`，让引擎按既有契约重连。
- MQTT output：publish 失败时本地有界重连（退避 + 次数上限），重连先 abort 旧 eventloop 任务再替换 client，`connected` 标志反映真实状态。

**数据腐坏与资源**
- vrl：UInt64 超出 i64::MAX 返回错误（不再静默回绕）；不支持的 Arrow 输入类型返回错误（不再静默转 Null）。
- SQL output：列类型覆盖扩至全部整型/浮点宽度 + 时间类型（映射为字符串参数），不支持列的报错指名列与类型；配置 codec 时构建期拒绝；`close()` 真正关闭连接池连接。
- SQL processor：临时表注销改为 RAII guard，`?` 早退不再跳过。
- python processor：`sys.path` 设置不再 `unwrap()` 可 panic，按配置顺序插入且跨实例去重；UDF 调用增加可配置超时（缺省有界）。
- json/protobuf codec：新增 `on_error: fail | skip`（缺省 `fail` 保持现状），`skip` 逐条隔离坏消息并告警。

**Window buffer**
- 跨 input 异构 schema：合并前归一到并集 schema（缺列 null 填充，类型真冲突才报错）；合并失败时队列与 ack 原样保留可重试（对齐 `buffering-stage-drain` 既有契约），不再丢数据丢 ack。
- 读者等待改为对 `notified()` 与关闭 token 的 `select`，close/flush 时空队列不再可能永久挂起。

**组件元数据诚实（BREAKING，见各条）**
- 修正全部幽灵字段/示例：batch（`count`/`timeout_ms`）、sql input（移除 `poll_interval` 并修正"轮询"描述）、redis output（真实 4 变体 + 可构建示例）、json processor（`value_field`/`fields_to_include`）、stdout（`append_newline`）、python（真实字段集合）。
- 实装文档已宣传的功能：kafka/mqtt/nats/redis output 的 `value_field`（复用 pulsar `field_payloads` 先例）、websocket input 的 `headers`（握手请求携带）。
- 移除无意义死配置：file/sql/modbus input 的 `codec` 字段（file input 由 DataFusion 按格式整文件解析，codec 无接入点）。**BREAKING**：配置了这些死字段的 YAML 此前被静默忽略，元数据 schema 修正后将无法通过 schema 校验；stream 级 `codec:` 键（InputConfig 顶层）同样在构建期显式拒绝（对齐 sql output 的 codec 拒绝先例），不再构建后被静默丢弃。
- **BREAKING**（行为变化）：vrl 不支持类型/溢出由静默变为报错；debezium 同名冲突列的取值方反转（业务值胜出）。

## Capabilities

### New Capabilities
- `codec-error-isolation`：json/protobuf codec 逐条坏消息隔离（`on_error` 配置，缺省 fail 保持现状，skip 隔离并告警）。
- `connector-recovery-contract`：input 连接器瞬时错误归类 Disconnection（modbus/nats）；output 本地重连契约（MQTT 有界重连、无任务/连接泄漏、状态标志真实）。
- `python-processor-contract`：python processor 无 panic 的 sys.path 装配（配置顺序、跨实例去重）、UDF 调用超时上界。
- `component-config-honesty`：组件元数据 schema/示例与真实配置面一致——幽灵字段移除、宣传功能必须实装（或从表面移除）、内置示例必须可通过真实构建。

### Modified Capabilities
- `schema-registry-integration`：多版本解码在 schema 真实演进（不同 schema 文本）时归一合并而非报错；`schemaType` 缺省从「MUST 视为 PROTOBUF」改为内容判定；兼容门禁失败结论可重试。
- `debezium-cdc-parsing`：tombstone（空 payload）跳过语义；元数据列与业务同名字段的冲突消解（业务值保留、元数据改投保留名）。
- `vrl-processor`：UInt64 溢出与不支持输入类型改为显式报错（替换静默回绕/静默 Null）。
- `sql-output`：列类型覆盖扩展与指名报错；配置 codec 构建期拒绝；close 关闭连接。
- `buffering-stage-drain`：契约主体从 batch processor/memory buffer 扩展到 window buffer 家族（合并失败保留队列与 ack、异构 schema 归一、关闭唤醒读者）。

## Impact

- **代码**：`crates/arkflow-plugin/src/` 下 codec（json/protobuf/schema_registry/debezium）、input（modbus/nats/websocket/file/sql）、output（kafka/mqtt/nats/redis/sql/stdout）、processor（vrl/python/sql/batch/json）、buffer（window 及 tumbling/sliding/session）。`arkflow-core` 预计零改动（错误分类复用既有 `Error::Disconnection`；window buffer 归一逻辑放插件侧）。
- **既有测试需同步修订**（它们固化了当前缺陷行为）：`schema_registry.rs` 的 `test_gate_result_is_cached`（失败缓存部分）、两个多版本测试（同 schema 文本绕开演进）、`debezium.rs` 无 tombstone 用例、`json.rs` 整批失败断言（缺省路径保持，补 skip 路径）。
- **生成产物**：`docs/reference/component-inventory.json`、`docs/static/config-schema.json` 需 `ARKFLOW_REGENERATE_DOCS=1` 重新生成（CI 快照门禁）。
- **文档**：kafka/nats/mqtt/redis output（value_field 由"宣传未实装"变为真实）、websocket input（headers）、json/protobuf codec（on_error）、schema_registry（schemaType 判定）、vrl/python、sql output（类型表）——en 与 zh-Hans 两棵树同步。
- **依赖**：无新增 crate（websocket headers 用 tokio-tungstenite 既有 request 构建能力；MQTT 重连为本地逻辑）。

## Non-goals

- 不建 DLQ（死信队列）基础设施——隔离仅到「跳过 + 告警」，路由到外部死信主题留待独立变更。
- 不做引擎级 output 重连框架——仅 MQTT output 本地恢复；其余 output 失败语义不变。
- 不做 SQL output 的复杂类型（Struct/List/Decimal）映射——扩到数值全宽度与时间类型即止。
- 不做内核/console 的 P2 项（无界缓冲、checkpoint 超时已有 `fix-checkpoint-round-timeouts`，运行时竞态已有 `fix-runtime-manager-races`，Error Boundary 等 console 批另行立项）。
- 不做 schema registry 的语义级 schema 演进映射（列改名/迁移）——仅结构并集归一。
- 不实现 redis stream 变体或 sql input 轮询——两者均为幽灵配置，本变更移除而非实装。

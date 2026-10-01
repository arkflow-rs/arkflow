# Tasks — fix-plugin-p2-batch

## 1. 共享基础设施

- [x] 1.1 新增 `crates/arkflow-plugin/src/component/batch_merge.rs`：`normalize_and_concat(batches)` —— 字段并集、缺列全 null 列补齐、列序取首批次顺序追加新列、同名不同类型报错（含列名与两类型）；单测覆盖：同 schema 直通、缺列归一、类型冲突报错、空输入
- [x] 1.2 在 `component/mod.rs`（或对应注册处）挂载新模块，`cargo test -p arkflow-plugin batch_merge` 通过

## 2. schema_registry codec

- [x] 2.1 `schemaType` 缺省改内容判定：trim 后 JSON 对象 → Avro，否则 Protobuf，双失败报错列出两种尝试（`codec/schema_registry.rs:322-327`）；修订 `test_rest_resolver_bearer_auth`（无 schemaType 断言），新增缺省+Avro 文本、缺省+proto 文本、双失败三个用例
- [x] 2.2 多版本归一合并：`decode` 路径用 `normalize_and_concat` 替换裸 `concat_batches`（`schema_registry.rs:254-257`）；把 `test_multi_version_each_resolves` / `test_multi_version_avro` 改为真实演进（两 id 不同 schema 文本：加列/缺列），新增同名不同类型冲突用例
- [x] 2.3 门禁失败重试：`gate_state` 改 `RwLock<Option<Result>>` + `last_attempt`，通过永久缓存、失败按 30s 最小间隔重试（间隔内返回上次错误不发请求）（`schema_registry.rs:163-192`）；修订 `test_gate_result_is_cached`（通过不重试保留），新增 wiremock 序列响应「失败→恢复→自愈」用例

## 3. debezium codec

- [x] 3.1 空零长度 payload 跳过 + warn（`codec/debezium.rs:62-63`），不使整批失败；新增混批用例（1 空 + 2 合法 → 2 行）与 JSON 字面量 `null` 维持现状用例
- [x] 3.2 同名冲突消解：业务值保留原列，envelope 元数据改投 `__debezium_<name>` 并每批 warn（`debezium.rs:119-124`）；新增 `after.op` 业务字段冲突用例与无冲突不受影响用例

## 4. json/protobuf 坏消息隔离

- [x] 4.1 两个 codec config 增加 `on_error: fail | skip`（serde 缺省 `fail`）；`fail` 路径字节不变
- [x] 4.2 `skip` 路径：json 逐条 `try_to_arrow`、protobuf 逐条 `protobuf_to_arrow`（失败 warn 含序号与原因后跳过），好消息经 `normalize_and_concat` 合并；全坏返回错误不产空批次（`codec/json.rs:44-51`、`codec/protobuf.rs:100-121`）；单测：混批隔离、全坏报错、异构好消息归并、缺省仍整批失败

## 5. 连接器错误分类与恢复

- [x] 5.1 modbus 四个读路径错误映射 `Error::Disconnection`（IO/连接/超时类），协议类保持 `Process`（`input/modbus.rs:119-164`）；分类逻辑提为可单测的纯函数并补单测
- [x] 5.2 nats 首次订阅与 connect 期 fetch 失败改 `Disconnection`（`input/nats.rs:206-215,278-298`，对齐 JetStream 循环 `:337,351` 既有用法）；补错误分类断言测试
- [x] 5.3 MQTT output：eventloop 退出即置 `connected=false`；publish 失败/连接失效时惰性重连（先 abort 旧任务 + best-effort disconnect 旧 client，再重建，1s/2s/4s 共 3 次），耗尽返回 `Connection` 错误（`output/mqtt.rs:113-143`）；基于 MockMqttClient 补：重连不泄漏旧任务、标志真实、耗尽报错三用例

## 6. processor 加固（vrl / python / sql）

- [x] 6.1 vrl：UInt64 用 `i64::try_from`，超界报错含列名与原值（`processor/vrl.rs:256`）；不支持类型与 downcast 失败分支改显式报错（含列名、类型、`filter_columns` 规避提示），删除静默 Null 路径（`vrl.rs:171-177,345-352`）；单测：u64::MAX 报错、范围内正常、List 列报错、支持类型回归
- [x] 6.2 python：`sys.path` 装配去 `unwrap()`（错误返回 `Error::Process`）、配置顺序生效（修反转）、跨实例查重去重（`processor/python.rs:117-121`）；单测覆盖顺序与去重
- [x] 6.3 python：`timeout_ms: Option<u64>`（缺省 60000），`process()` 的 `spawn_blocking` 包 `tokio::time::timeout`，超时报错含耗时（`python.rs:47-80`）；单测：短 UDF 正常、超时路径返回错误
- [x] 6.4 sql processor：临时表注销改 RAII guard（Drop 时 deregister 主表与临时表），`?` 早退不再跳过（`processor/sql.rs:219-241`）；单测：构造 register 后执行失败路径，验证后续批次同池 context 可正常注册

## 7. sql output

- [x] 7.1 扩展 `matching_data_type`：Int8/16/32→i64、UInt8/16/32→u64、Float32→f64、Date32/Date64→ISO 日期串、Timestamp（任意 unit）→RFC3339 串；Unsupported 报错含列名与类型（`output/sql.rs:505-559`）；单测覆盖新增类型映射与报错文案
- [x] 7.2 build 配置了 `codec` 即拒绝（配置错误指明原因）（`output/sql.rs:598-604`）；单测：带 codec 构建 Err、不带构建 Ok
- [x] 7.3 `close()` 显式 take 并关闭数据库连接（`output/sql.rs:452-455`）；单测或既有 mock 路径验证连接关闭调用

## 8. window buffer

- [x] 8.1 `process_window` 失败保留：任何失败路径（含归一冲突）把已取出队列原样放回再返回 Err，ack 不 settle 不 abort（`buffer/window.rs:113,148-161`）；单测：合并失败后再次 read 可重试同一批
- [x] 8.2 异构 schema 归一：合并改用 `normalize_and_concat`（缺列 null、类型冲突走 8.1 保留路径）；单测：缺列归并、类型冲突报错且队列保留
- [x] 8.3 读者唤醒：三个窗口 buffer（tumbling/sliding/session）读者循环改 `select!`（`notified()` vs 关闭 token），token 触发按 close 语义排空后结束；单测：close 时空队列读者在有限时间（非 200ms 短超时掩盖）返回

## 9. 组件元数据诚实批

- [x] 9.1 修正幽灵字段/示例元数据：batch（`count`/`timeout_ms`）、sql input（删 `poll_interval`、修"轮询"描述）、redis output（真实 4 变体 + 可构建示例）、json processor（`value_field`/`fields_to_include`）、stdout（`append_newline`）、python（真实字段集、`script` 非必填、补 `module`/`function`/`python_path`/`timeout_ms`、删 `extra_packages`）
- [x] 9.2 抽取 pulsar `field_payloads` 为 output 共享模块，pulsar 改用；kafka/mqtt/nats/redis write 路径实装 `value_field`（列缺失/类型不符报错，未配置走 codec 编码不变）；各补单测（列取值、缺失列报错、未配置回归）。CR 追加：LargeBinary/LargeUtf8 的 null 单元与 Binary/Utf8 一致显式报错（原 `.flatten()` 静默丢行会使 payload 与行级 topic/key 错位），payload 模块补四种类型的行对齐与 null 拒绝测试
- [x] 9.3 websocket input 实装 `headers`：握手请求经 request builder 携带配置头（`input/websocket.rs:98`）；单测或集成验证握手头存在，未配置回归
- [x] 9.4 移除 file/sql/modbus input 的死 `codec` 字段（struct 字段 + `#[allow(dead_code)]` + 元数据 schema 条目，`input/file.rs:157-158`、`input/sql.rs:132-133`、`input/modbus.rs:66-67`）；确认示例与文档无引用。CR 追加：stream 级 `codec:` 键构建期显式拒绝（三 builder + `dead_codec_config_is_rejected_by_builders` 端到端测试）
- [x] 9.5 新增注册表诚实门禁测试：遍历全部组件元数据，`config_example` 必须通过声明的 schema 校验且反序列化为真实 config struct（构建期联网的组件仅做反序列化并注明）；确保 batch/redis 旧示例若未修会被拦截

## 10. 文档与生成产物

- [x] 10.1 以 `ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot` 再生 `docs/reference/component-inventory.json` 与 `docs/static/config-schema.json`，快照测试通过
- [x] 10.2 en 文档更新：kafka/nats/mqtt/redis output（value_field 实装语义与限制）、websocket input（headers）、json/protobuf codec 页（`on_error`）、schema_registry 页（schemaType 内容判定、门禁重试）、debezium 页（tombstone 与 `__debezium_*` 冲突语义）、vrl 页（fail-loud 行为与规避）、python 页（路径顺序、`timeout_ms`）、sql output 页（类型表、codec 拒绝、close）、batch/sql/redis/stdout/json 元数据相关页面核对
- [x] 10.3 zh-Hans 树同步 10.2 全部页面；`pnpm docs:check` 通过
- [x] 10.4 README.md / README_zh.md 组件清单核对（无新增/删除组件，预计无改动，跑 `pnpm docs:check` 确认）

## 11. 收尾验证

- [x] 11.1 `cargo test --workspace --all-targets` 全绿（含既有测试修订后）
- [x] 11.2 `cargo clippy --workspace --all-targets` 无新告警
- [x] 11.3 `./target/debug/arkflow --config examples/<受影响示例> --validate` 通过；`arkflow components list --format json` 输出与新元数据一致
- [x] 11.4 更新 `openspec/CODE_REVIEW_2026-09-29.md`：划掉「插件层（P2 批）」全部已落地条目并注明本变更名

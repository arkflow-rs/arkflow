## Why

热路径审计第二批（`openspec/PLANNING.md` 第 10.2 节）的机械项已随 `optimize-hotpath-mechanical-batch-2`（2026-10-14 归档）与 `cache-sql-physical-plan` 闭环，剩余候选即 batch 2 归档 Non-goals 明确划为"独立 change"的三项，均经本次代码逐项核实成立：

1. **debezium 每消息三次 JSON 遍历 + before 深拷贝**（`crates/arkflow-plugin/src/codec/debezium.rs:73-98,128-175`）：每条 Envelope 先 `serde_json::from_slice` 解析为 `Value`（遍历 1），`flatten_envelope` 在 delete 场景 `before.clone()` 深拷贝整棵业务对象（debezium.rs:141），再 `serde_json::to_vec` 把扁平化行重新序列化回 NDJSON 文本（遍历 2），最后 `component::json::try_to_arrow` 对整段文本做流式 schema 推断 + tape 解码——即**同一批字节被完整 JSON 解析两遍**（遍历 3 = 解码侧的 tape 解析）。CDC 解码吞吐的天花板在这里。
2. **VRL 每行独立 MessageBatch + 每格 key String**（`crates/arkflow-plugin/src/processor/vrl.rs:61,88-104,590-594`）：`process` 对 N 行输入产出 N 个独立批（`ProcessResult::Multiple`），下游按批为粒度的有状态处理（如 SQL 聚合）退化为每批 1 行；`insert` 每格执行 `name.to_string()`（vrl.rs:592，行×列次堆分配）；`(*msg_batch).clone()` 整批克隆仅为了拿到所有权（vrl.rs:61）。
3. **SQL output 逐 cell async 转换 + 三重深拷贝**（`crates/arkflow-plugin/src/output/sql.rs:414-447,474-506,510-635`）：`write_batch` 先 `(**msg).clone()` 整批深拷贝（sql.rs:422），再对每 cell 调用 async `matching_data_type`（无 await 点的 async fn，且每 cell 重复 downcast 同一列，sql.rs:434-436），扁平 `Vec<SqlValue>` 经 `chunks().to_vec()` 逐行再克隆一次（sql.rs:440-443，`String` 值二次堆分配），事务路径 `rows.clone()` 每批再克隆整张行表（sql.rs:126,142）。

延续第一批/第二批的先例，本 change 打包第三批中**语义保持或仅收窄批粒度语义、无需新设计**的机械项：每项独立可回退，既有等价/差分/文本断言测试全绿为门槛，release 计时测试量化收益。

## What Changes

- **debezium 列式累积**：`DebeziumJsonCodec::decode` 重构为"单趟解析 + 列式构建"——每条 Envelope 仅 `serde_json::from_slice` 解析一次，扁平化为 owned `Map` 后直接进入**值域 schema 推断 + 值域 Arrow builder 构建**，消灭重新序列化与解码侧二次 JSON 解析；delete 场景改为"先序列化 `before` 为 JSON 文本列、再 move 进业务行"消灭 `before.clone()` 深拷贝。新增值域推断与 `try_to_arrow`（字节域）的**差分测试**：对含混合数值拓宽、嵌套 object/array、字段首现排序、null 列、冲突列名、delete/tombstone/null 字面量的语料，断言两条路径输出的 schema 与值逐字节等价。既有单测（含冲突/容错/tombstone 语义）全绿为门槛；新增 `#[ignore]` release 计时测试（对齐 `json_decode_timing` 模式，旧字节路径内联为 oracle）。
- **VRL 合并单批输出**：`process` 把每输入行的程序输出行**扁平化为单个输出批**（字段集为批内首现顺序的并集，行序保持输入序；程序返回数组的元素展开为连续行；全空批返回 `ProcessResult::None`）——`vrl_values_to_message_batch` 本就支持多行向量化（vrl.rs:432-588），现仅按输入行逐个调用（vrl.rs:88-91）；改为整批一次调用。**批粒度语义收窄**：`ProcessResult::Multiple`（每输入行一批）不再出现，下游每批为粒度的处理（SQL 聚合、json 编码）从 N×1 行批恢复为 1×N 行批。同时消除每格 `name.to_string()` 分配（键按列构建一次）与 `(*msg_batch).clone()` 整批克隆（改借用读取）。既有 `array_output_splits_into_rows_and_multiple_batches` 测试的 Multiple 断言更新为合并语义。
- **SQL output 列式转换**：`write_batch`/`insert_row` 改列式转换——每列 downcast 一次（非每 cell）、转换函数去 async 化（无 await 点）、按列构建 `Vec<SqlValue>` 后 transpose 成行（消灭扁平 vec + `chunks().to_vec()` 二次克隆）、`write_batch` 去掉 `(**msg).clone()`、事务路径 `build_*_insert` 改借用行数据消灭 `rows.clone()`。生成的 SQL 文本与绑定值序列保持逐字节不变（既有 `sql()` 文本断言测试全绿为门槛）。

**审计修正（只修正认知，不改代码）**：`openspec/PLANNING.md` 10.2 节中"窗口算子每批全量状态快照 / extract_keys / accumulate""event_time_gate classify_row / split_by_physical_partition""protobuf encode / Kafka output / stdout / generate input / memory buffer / partitioned 路由 / fanout_ack / TrackingAck"各条目均已在 batch 2 闭环，但 PLANNING 10.2 未勾划——本 change 归档时同步修正该清单（连同本批三项）。

## Capabilities

### New Capabilities

（无——本 change 为既有行为的性能保持性改造，唯一行为收窄（VRL 批粒度）落入既有 `vrl-processor` capability）

### Modified Capabilities

- `debezium-cdc-parsing`: 新增"单趟解析、值域列式构建"要求与差分等价场景（schema/值与扁平化行的 JSON 语义一致：列序为批内首现顺序、Int64+Float64 拓宽、缺失列 null、嵌套结构、tombstone/null 字面量语义不变）。
- `vrl-processor`: 新增"批粒度"requirement——一个输入批产出单个输出批（行序保持、数组展开、空批 None、异构行字段并集补 null），取代隐含的每输入行一批行为。

不设 delta 的项：SQL output 列式转换（`sql-output` spec 的批量插入/upsert/事务契约不变，SQL 文本与绑定值逐字节不变，既有 `sql()` 文本断言与类型转换单测钉住，纯实现级）。

## Impact

- **插件 crate**：`crates/arkflow-plugin/src/codec/debezium.rs`（值域推断 + 列式构建）、`component/json/infer.rs`（推断内核复用面，值域入口）、`processor/vrl.rs`（合并输出、键分配、去克隆）、`output/sql.rs`（列式转换）。
- **测试/基准面**：debezium 差分测试 + `debezium_decode_timing`（#[ignore] release）；vrl 合并语义测试改写 + `vrl_merge_timing`（#[ignore] release，旧逐行路径内联为 oracle）；sql output 既有 SQL 文本断言 + 转换等价测试 + `sql_output_conversion_timing`（#[ignore] release）。
- **文档面**：`docs/docs/components/processor/vrl.md` 与 zh-Hans 对应页需声明新批粒度语义（用户可见语义变化）；debezium 解码语义无变化，组件文档无需改动；若新增 benchmark 场景则同步 `docs/develop/benchmark.md` en/zh。
- **无依赖变更、无配置格式变更、无 YAML 示例语义变更**（examples 全部不受影响，vrl 示例输出粒度变化不影响校验）。

## Non-goals

- **event_time_gate `collect_outcomes` 混合结果组拷贝**：batch 2 已判定"全 Emit 快路径已被 arrow 零拷贝切片覆盖，混合组优化收益不明确"，继续观察 profile，不动。
- **Avro L2 reader 复用**：上游 apache-avro API 阻塞（owned-reader 能力），维持 backlog。
- **SQL output 直绑（跳过 `SqlValue` 中间层）**：列式转换先落到 `Vec<Vec<SqlValue>>` 的无冗余构建，直绑涉及 sqlx 类型推断面（Postgres 参数类型由绑定值类型决定），收益不确定、风险不成比例，留待 profile 证明转换仍是瓶颈再议。
- **VRL 输出批的列式累积重构**（vrl_values → builder 逐格 `remove` 已是单趟）：`get_arrow_data_type` 首值定型的异构行静默置 null 行为保持现状，不在本批扩大语义。
- **Kafka 事务路径**：batch 2 已核实为每批 1-2 次往返，不立项。

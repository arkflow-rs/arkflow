# Design

## Context

热路径第三批的三项均已在 proposal 中给出 file:line 证据。约束：每项独立可回退；既有测试全绿为门槛；性能语义类改动以差分/等价测试钉死；release 计时测试量化收益（第一批/第二批的既定模式）。

关键前置事实（本次核实）：

- **serde_json 启用了 `preserve_order`**（Cargo.lock 中 serde_json 1.0.151 依赖 indexmap，该 feature 是唯一引入路径；feature 统一后全图生效）。因此 `serde_json::Value::Object`（IndexMap，插入序）与 `infer.rs` 的 `CowValue::Object`（ObjMap = IndexMap + FxBuildHasher，首现序）键序语义一致——值域推断的列序与"扁平化行序列化 → 字节域推断"的列序天然一致，差分测试在运行时对拍两条路径，feature 漂移时两侧同步变化，不会假绿。
- `infer.rs` 的推断内核（`InferredType::merge`/`coerce_data_type`/`collect_field_types_from_object` 等）直接消费 `&CowValue`，且 `CowValue` 已实现 `Deserialize<'de>`（可从字节零拷贝构建）。缺的是 `Serialize`（`before`/`source` JSON 文本列需要）与值域→Arrow builder 的解码器（现有解码走 arrow-json tape，从字节二次解析）。

## Goals / Non-Goals

**Goals**：三项每批/每消息开销消除；debezium 输出与旧路径差分等价；VRL 输出批粒度语义以 spec + 测试钉死；SQL output SQL 文本与绑定值逐字节不变。

**Non-Goals**：见 proposal（event_time_gate 混合组、Avro L2、SQL 直绑、VRL 异构行定型语义、Kafka 事务路径）。

## Decisions

### D1 — debezium：Envelope 借用式单趟解析 + 值域推断复用 + 值域解码器（差分钉死）

**新解码管线**（每条 Envelope 只做一次 JSON 解析，全程无中间字节）：

1. `serde_json::from_slice::<CowValue>(bytes)` 一次解析为借用型 `CowValue`（字符串借用原文、零堆分配；空 payload（tombstone）的 skip 与"无效 JSON 报错"语义不变，错误文案不变）。
2. 扁平化在 `CowValue` 上进行：业务行 = `after`（或 delete 时 `before`）**move** 出 envelope 对象（`ObjMap`）；`before`/`source` JSON 文本列：**先 `serde_json::to_string(&before)`（需要给 `CowValue` 补 `Serialize` 实现，借用序列化）再 move**，消灭 `before.clone()`；meta 提取（op/ts_ms/db/table）走既有 `insert_meta` 冲突语义（冲突检测/改名/warn 日志不变）。
3. **值域 schema 推断**：对批内扁平化行的 `&CowValue` 直接调用现有推断内核（`collect_field_types_from_object` 及数组/嵌套助手）——与字节域 `infer_json_schema_streaming` 共享同一套 `InferredType`/合并/序规则，schema 等价由构造保证 + 差分测试复核。
4. **值域解码器**（新代码，唯一的新语义面）：按推断 schema 把每行 `CowValue` 写入 Arrow builders，递归支持 `Null/Boolean/Int64/Float64/Utf8/List/Struct`。标量→列的匹配规则以 arrow-json tape 解码的**可观测行为**为准（含已知角点：Int64 列遇 Float 静默截断、Utf8 列收非字符串值、标量落入 List 列、缺失字段 null），实现先按最合理解读，差分语料跑出的偏差逐一对齐 arrow-json 实际行为。
5. **差分测试**：语料覆盖——各标量类型、null 列、混合数值拓宽、字段跨行首现排序、嵌套 object（→Struct 递归）、标量数组/结构数组/混合数组、裸标量落入 List 列、Int64 列浮点角点、Utf8 列非字符串角点、冲突列名、delete（business 取 before）、tombstone 混批、`null` 字面量与非对象 envelope（array/number → 全 null meta 行，语义保持）、全 tombstone 空批。每条语料断言：新路径输出与"扁平化行序列化 → `try_to_arrow`"oracle 的 schema（含列序）与全部列值逐字节等价；失败集一致。

**备选否决**：(a) RawReader 按 record 切片解码——仍需二次解析字节，只省 concat buffer，达不到审计终态；(b) 自研完全独立的列式推断——与 `infer.rs` 差分过的字节域机器重复实现，漂移风险高；(c) 仅做 serialize-before-move 小修——不解决二次解析主成本（解析占三遍中的两遍）。

### D2 — VRL：输出行扁平化为单批 + 键按列构建一次 + 去整批克隆

- `process` 中 `output: Vec<Vec<VrlValue>>`（每输入行的输出行集合）先 `flatten` 为 `Vec<VrlValue>`，再调一次 `vrl_values_to_message_batch`（该函数本就以 `Vec<VrlValue>` 为行集向量化构建，字段集取首行、逐行 remove——合并是自然能力，不是新代码路径）。行序 = 输入行序 + 数组展开序；空集 → `ProcessResult::None`；`Multiple` 分支删除。
- 键分配：`message_batch_to_vrl_values` 每列循环外构建键一次，行循环内 clone 键（`vrl` 的 ObjectMap 键为廉价 clone 语义，实现时以实际 `vrl` crate 版本的键类型为准——若为 String 则此优化退化为"分配一次 + clone N 次"仍优于每格 `to_string()`，若为 Arc<str> 则近乎免费）。
- `(*msg_batch).clone()` 消除：`message_batch_to_vrl_values` 改收 `&MessageBatch`（只读列与 schema，无需所有权）。
- **行为收窄的边界**：`array_output_splits_into_rows_and_multiple_batches` 测试后半段（每行对象 → Multiple）改为"单批 N 行"；数组展开前半段不变。异构行字段并集补 null 的规则与现函数对数组的既有规则一致（首行定型、类型不匹配的槽位为 null），不扩大语义。

### D3 — SQL output：列式转换 + 去 async + 借用化，SQL 文本逐字节不变

- 转换函数 sync 化（`matching_data_type` 无 await 点，去 `async`/`.await`；错误文案逐字节保留，含列名）。
- 列式构建：每列 downcast 一次 → 每列 `Vec<SqlValue>`（窄宽度拓宽/时间格式化规则不变）→ 按 `rows[r].push(cols[c][r])` 单趟 transpose（预分配 `num_rows` 个行 vec），消灭扁平 vec + `chunks().to_vec()` 二次克隆。
- `write_batch` 去 `(**msg).clone()`（读借用即可）；`build_mysql_insert`/`build_postgres_insert` 与事务路径改收 `&[Vec<SqlValue>]`（`push_values(rows.iter())`），消灭 `rows.clone()`；`Utf8` 绑定改 `s.as_str()`（sqlx 对 MySQL/Postgres 的 `&str` Encode 与 `String` 线上格式一致；`Null` 仍 `None::<String>` 占位语义——实现时核对两方言 NULL 参数类型声明一致，不一致则该列保持 owned String）。`u64 → i64`（PG）、RFC3339 时间字符串化等绑定值序列保持不变。
- 等价门槛：既有 `sql()` 文本断言（含 upsert 双方言/全 key 列回退）全绿；新增转换等价测试（宽类型混合批，断言新旧转换产物 `Vec<Vec<SqlValue>>` 逐值相等——旧实现内联为测试 oracle）+ `#[ignore]` 计时测试。

## Risks / Trade-offs

- [值域解码器与 arrow-json tape 角点行为偏差（如裸标量入 List 列、非字符串入 Utf8 列的具体取值）] → 差分语料先行、oracle 即真实旧路径；偏差逐一以 arrow-json 实际输出对齐，钉死在测试里；不明确角点在语料中显式列出。
- [`CowValue` 补 `Serialize` 后 `before`/`source` 文本列与 serde_json::Value 序列化的字节差异] → preserve_order 下两侧同为插入序；差分测试逐字节断言文本列内容；Map 键序规则变化时双路径同变。
- [VRL 合并后下游收到"更宽"的批（字段并集补 null）] → 行为收窄是本 change 目的（spec 新 requirement 钉死）；vrl.md en/zh 同步声明；下游 SQL 对缺失列的行为变化由批粒度恢复（1×N）抵消。
- [SQL 绑定值类型推断面（&str vs String、NULL 参数声明）] → 既有 SQL 文本测试 + 转换等价测试双门槛；`#[ignore]` 真库测试（若 CI 存在）不本批新增，文本与值等价已由单测覆盖。
- [debezium 借用式解析对 `serde_json` 错误文案的漂移（列名/位置信息）] → 错误断言测试保留"Invalid Debezium envelope JSON"前缀断言；结构型解析与手工导航等价（同为 `from_slice`）。

## Migration Plan

纯实现级 + 一处语义收窄（VRL 批粒度），无配置/数据迁移。回退策略：三项相互独立，逐项 revert 即可；VRL 合并语义若需回退，revert 单个提交恢复 Multiple 行为（测试同步回退）。

## Open Questions

- vrl crate 当前版本 ObjectMap 键的具体类型（String vs Arc<str>）——实现时读 `Cargo.lock` 锁定版本源码确认，不阻塞设计。
- arrow-json tape 对"标量落入 List 列"的精确行为（包装单元素 vs null）——差分语料给出答案后按 oracle 对齐，不阻塞设计。

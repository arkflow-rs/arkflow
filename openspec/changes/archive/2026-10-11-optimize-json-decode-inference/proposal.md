# Proposal: optimize-json-decode-inference

## Why

热路径审计第二批第一项（`openspec/PLANNING.md` 10.2）。`component/json.rs` 的 `try_to_arrow` 是全部 JSON 入口的公共解码函数（`json` codec `codec/json.rs:73,89,116`、`debezium_json` codec `codec/debezium.rs:97`、`json_to_arrow` processor `processor/json.rs:71`、benchmark 数据面 `benchmark.rs:174,193`），每批固定付出三份与解码本体无关的成本：

- **推断趟全量物化 Value 树**：`component/json.rs:33-36` 先 `infer_json_schema` 把全部记录逐条解析成 `serde_json::Value`（每条一次 Map 分配 + 每字段 String 键分配），推完即丢——纯为得到 schema，却付出了与第二遍同量级甚至更高的解析成本（ValueIter 路径，非 tape）。
- **第二遍 tape 解码**：`component/json.rs:48-54` `ReaderBuilder` 重新全量解析。此遍本身已是快速路径，问题在于它依赖第一遍的存在。
- **分块 + concat 拷贝**：`Reader` 内部按 `batch_size`（默认 1024）分块产出，`component/json.rs:52-60` 把多个 `RecordBatch` `concat_batches` 成一个——批超过 1024 行时全部列再深拷贝一次。

Kafka input 批量化（`optimize-kafka-input-batching`）后，decode 每批一次调用（≤1024 payload 合并），双遍开销按批摊薄；但相对 tape 本体，推断趟仍是 2-4× 的浪费（Value 树分配主导），且 `on_error: skip` 模式（`codec/json.rs:85-118`）仍逐条调 `try_to_arrow` 判定坏消息——每条两趟全量解析。

## What Changes

1. **单次 flush 出批**：`try_to_arrow` 出批路径改用公开 API `ReaderBuilder::build_decoder()` + `decode()` 循环 + 结束时**单次** `flush()`（解码器容量按输入行数估计——NDJSON 每行一条记录时换行数即行数上界）——常规输入一次出单批，删除 `concat_batches` 拷贝与中间块；记录密度超出估计的极端输入回退块拼接，产出仍为一个 RecordBatch。EOF 语义与 `Reader::read()` 完全一致（无尾换行的末条记录正常结算）。
2. **流式低分配 schema 推断**：新增推断模块，逐行提取记录（镜像上游 `ValueIter` 语义）并以借用型 JSON 树（`CowValue`）合并类型状态机，不物化任何 `Value` 树；合并规则严格镜像 arrow-json `reader/schema.rs` 的 `InferredType` 状态机（文档序、`is_i64` 否则 Float64、Null→Any 惰性、数组按首元素分类、scalar⊕list coerce、不兼容合并报错），以差分测试钉死与 `infer_json_schema` 的逐字节等价（含失败集）。
3. `on_error: skip` 的逐条判定自动受益（推断趟单趟完成解析校验+类型分类），无代码改动。
4. benchmark 新增 `json-decode` 场景（decode-only，与 `codec-json` 编码往返区分），并补 release 计时（对齐 `kafka_batch_assembly_timing` 模式）；CHANGELOG 与 PLANNING 10.2 勾销同步。

## Capabilities

### New Capabilities
- `json-decode-pipeline`: JSON→Arrow 解码管线的形态契约——单批产出（不分块不拼接）、流式推断与全量记录 union 语义的严格等价（含字段序与失败集）。

### Modified Capabilities

（无——`codec-error-isolation` 的全量推断语义（取宽/新字段成列/nullable）原样保持，现有 Requirement 无需修改。）

## Non-goals

- **不做任何跨批 schema 缓存（含最初设想的投影路径缓存）**：无投影列集时，缓存会让后续批的新字段被静默丢弃（违反 `codec-error-isolation`「仅出现在后续记录的字段 SHALL 成为列」）；实施中进一步发现**投影列集固定也无法安全缓存**——arrow-json tape 解码对 Int64 列遇到浮点值是 `NumCast` 静默截断（`1.5 → 1`）而非报错（`reader/primitive_array.rs` F64→`NumCast::from` 分支），「解码失败即漂移信号」的错误回退设计覆盖不了这条漂移路径，任何精确的漂移检测都需要一次与推断本身等价的扫描，缓存净收益趋零。两趟（廉价推断趟 + tape 趟）即为精确语义下的最优形态。
- 不引入 simd-json（需可变缓冲与自有 Value 类型，无法接入 arrow-json tape 解码；收益不确定，依赖面大）。
- 不 fork arrow-json tape 内部实现做真正的单遍解码（需复制私有 `TapeDecoder`/`ArrayDecoder`，维护成本高）。
- 不改 JSON encode 路径与 `arrow_to_json` processor。
- 不改 `codec-error-isolation`、`debezium-cdc-parsing` 等既有 spec 的任何 Requirement。
- 不改 debezium 自身的每消息 3 遍 JSON 与 before 深拷贝（PLANNING 10.2 另列独立项；本变更只加速其 `try_to_arrow` 段）。

## Impact

- **代码**：`crates/arkflow-plugin/src/component/json.rs`（出批重构）、`crates/arkflow-plugin/src/component/json/infer.rs`（新增流式推断器）、`crates/arkflow-plugin/src/benchmark.rs`（新场景）。
- **调用方零改动**：`codec/json.rs`、`codec/debezium.rs`、`processor/json.rs`、`benchmark.rs` 的调用签名不变。
- **依赖**：无新增依赖（serde/serde_json 已在树内）。
- **配置面**：无新配置字段；docs 组件页无需变更（行为语义不变）。
- **风险集中点**：推断等价性——以「差分测试 vs `infer_json_schema`」+ 复刻 arrow-json 自带边界用例（mixed arrays、nested structs、struct-in-list、null 矩阵、>i64::MAX 数值）兜底。

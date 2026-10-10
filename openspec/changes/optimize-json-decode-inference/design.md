# Design: optimize-json-decode-inference

## Context

`try_to_arrow`（`crates/arkflow-plugin/src/component/json.rs:22-63`）的现状三段式：`infer_json_schema`（ValueIter → 每条记录物化 `serde_json::Value` 树）→ `ReaderBuilder` tape 解码 → `collect::<Vec<_>>()` + `concat_batches`。arrow-json 58.4 的事实：

- `Reader` 内部即 `Decoder` 循环（`reader/mod.rs` `Reader::read()`：`fill_buf` → `decoder.decode(buf)` 直到 `decoded != read`，然后 `decoder.flush()`）；分块仅因 `batch_size`（默认 1024）让 `decode` 提前返回、`next()` 中途 flush。
- `Decoder::flush()` 明确「忽略 batch_size，一次返回全部缓冲行」；`TapeDecoder::finish()` 仅在括号未闭合（`has_partial_row`）时报错——**末条记录无尾换行可正常结算**（codec 路径 `b.join(b"\n")` 恰好无尾换行）。
- 推断规则集中在 `reader/schema.rs`：`InferredType`（Scalar(IndexSet<DataType>) / Array(Box) / Object(IndexMap) / Any）+ `coerce_data_type` + `generate_schema`（所有列 `nullable=true`）。

`fields_to_include` 投影（`processor/json.rs:37-44`）在推断结果上 `Schema::project`，列集 = include 集与出现列的交集，**与批内容无关**（`filter_map` 天然丢弃未 include 的键）。

## Goals / Non-Goals

- **Goals**：解码稳态成本从「Value 树推断趟 + tape 趟 + concat」降到「借用树推断趟 + tape 趟」（无投影路径与投影路径一致）；全部现有语义逐字节保持。
- **Non-Goals**：见 proposal Non-goals（不做任何跨批 schema 缓存、不引入 simd-json、不 fork tape、不动 encode）。

## Decisions

### D1 出批形态：`build_decoder()` + decode 循环 + 单次 flush

```rust
let estimated_rows = content.iter().filter(|&&b| b == b'\n').count() + 1;  // NDJSON 行数上界
let mut decoder = ReaderBuilder::new(schema).with_batch_size(estimated_rows).build_decoder()?;
let mut rest = content;
while !rest.is_empty() {
    match decoder.decode(rest)? {
        0 => { /* 阈值命中：flush 出块，兜底拼接（仅多记录挤一行时可达） */ }
        n => rest = &rest[n..],
    }
}
let batch = decoder.flush()?;   // Some(整批) 或 None（0 行）
```

- 与 `Reader::read()` 的 EOF 处理逐语句一致（无尾换行末条正常结算），仅去掉中途 flush——NDJSON 一次出**单批**，`concat_batches` 与中间 `Vec<RecordBatch>` 删除。
- **实现期发现（修正 D1 初稿）**：`batch_size` 不只控制提前返回阈值，还按 `batch_size × num_fields` 控制 tape 预分配——直接取 `usize::MAX` 会按输入字节数放大预分配（不可行）。改为按换行数（NDJSON 行数上界）估计容量并封顶（`MAX_ESTIMATED_ROWS = 65_536`；自 CR 复审：预分配随行数×schema 列数线性增长，HTTP 大 body 等不受 batch_max_bytes 约束的入口需要有界的 upfront 分配）：行数在封顶内一次 flush 出单批；行数超出封顶（或直接喂 `decode_with_schema` 的非逐行打包输入）时 decode 返回 0，回退「flush 已缓冲块 + concat」兜底，产出仍为一个 RecordBatch。
- 空输入/全空行保持现状：flush 返回 `None` ⇒ 返回 `RecordBatch::new_empty(schema)`（schema 仍来自推断，空 schema 列形状不变）。
- **备选已否决**：`with_batch_size(usize::MAX)` 一把梭（预分配按字节放大）；维持默认 1024 + 无条件 concat（≤1024 行也无谓过一遍拼接）。

### D2 流式推断器：借用型 JSON 树 + 镜像 `InferredType` 合并状态机

新模块 `component/json/infer.rs`：

- 入口 `infer_json_schema_streaming(content: &[u8]) -> Result<Schema, Error>`，记录提取**逐行镜像**上游 `ValueIter`（`str::trim` → 空行跳过 → 每行恰一个 JSON 值，行内尾随内容报 "Not valid JSON"——`{"a":1}{"b":2}` 之类两侧同为 Err）。
- 每条记录经 serde 反序列化为**借用型树** `CowValue<'de>`（字符串/键 `Cow::Borrowed` 引用输入，不物化 `serde_json::Value`；标量载荷仅保留供错误 Debug 输出）；对象用 `Vec<(Cow<str>, CowValue)>` 手工复刻本工作区 `preserve_order`（IndexMap）语义：首现位置 + 值替换（差分测试证实上游输出为文档序而非排序序）。
- 合并状态机逐条镜像 `reader/schema.rs`（等价性的规范来源，不是近似）：
  - 字段序 = 跨记录首现序（对象内按文档序插入）；
  - 数值：`Int` / 小 `UInt`（≤i64::MAX）为 Int64，其余（`UInt` 超界、`Float`）为 Float64（serde_json `is_i64` 语义，含 >i64::MAX 大数用例）；
  - `Null`：字段不存在则插 `Any` 占位，已存在不动；**数组分支的初值替换同样覆盖 Any 占位**（上游 `is_none_or_any`，差分测试抓到的缺口之一）；终态 `Any` → `DataType::Null`；
  - 数组按**首元素**分类（nested-array / struct-array / scalar-array），空数组 → `Any`（终态 `List(Null)`）；scalar-array 遇嵌套元素报同款错误；scalar⊕array 合并 coerce 为 List；struct 数组递归合并；
  - 不兼容合并（Object vs Scalar 等）→ 同语义 `JsonError`；非对象顶层记录 → 同款错误。
- **等价性兜底（验收主体）**：差分测试——固定语料（复刻 arrow-json 自带用例：mixed_arrays、nested_structs、struct_in_list、nested_list、null 矩阵、>i64::MAX、非法 JSON、非对象记录、转义/重复键、深嵌套）+ 确定性随机生成器（嵌套深度/类型杂度参数化，300 例）断言与 `infer_json_schema` **完全相等**（字段序、nullable、失败集三重对齐）。
- **备选已否决**：纯 `IgnoredAny` 流式 visitor（零分配但状态机复杂度失控、serde seed 生命周期难穿 `StreamDeserializer`）、继续用 `infer_json_schema`（Value 树成本不动）、simd-json（Value 类型不兼容）、fork tape 推断（维护成本）。

### D3 已否决：跨批 schema 缓存（原「投影路径缓存 + 错误回退」）

原设想：`fields_to_include` 列集固定 ⇒ 新字段本就不进 schema，processor 跨批复用投影 schema，类型冲突报错即回退全量重推断。**实施中被差分/单测推翻**：

1. **tape 解码对 Int64 列遇浮点值不报错**：`reader/primitive_array.rs` 的 `TapeElement::F64` 分支走 `NumCast::from(v)`，`1.5 → Some(1)` **静默截断**（仅超范围/字符串/布尔才报错）。缓存命中批把 `{"v":1.5}` 解成 Int64 的 `1`，而逐批推断必然拓宽为 Float64——「报错即漂移信号」的前提不成立，回退设计检测不到这条最常见的漂移。
2. 任何精确的漂移检测（新字段、float-into-int）都需要一次与推断本身等价的字节扫描；而 D2 已把推断趟降到「借用树合并」的廉价水平，缓存节省的正是这一趟——净收益趋零，复杂度（锁、回退、错误窗口）为正。
3. 无投影路径缓存则直接违反 `codec-error-isolation`「后续记录的新字段 SHALL 成列」（静默丢列）。

结论：**撤销缓存**，`processor/json.rs` 保持无状态；两趟（推断 + tape）为精确语义下的最优形态。`decode_with_schema` 保留为 `component/json` 内部公共助手（单批出批即由它实现）。

### D4 benchmark 与计时

- 新场景 `json-decode`：1000 行 NDJSON（复用 `sample_batch` 的内容形态）仅计 `try_to_arrow`（无 encode 段），进 `benchmark-suite` 报告两种格式（spec 为「至少覆盖」，新增场景不改 normative）。
- `#[ignore]` release 计时测试 `json_decode_timing`：新旧路径对照（对照实现保留在测试内：`infer_json_schema` + Reader + concat 的旧行联），对齐 `kafka_batch_assembly_timing` / `protobuf_batch_decode_timing` 模式，产出 PLANNING 勾销数字。

## Risks / Trade-offs

- **推断等价性是最大风险**（上游状态机细节多）：以 D2 差分测试为主要防线——实施中已当场抓到三处缺口（`is_none_or_any` 占位替换、按行提取、`preserve_order` 文档序）并按上游修正；任何后续差分失败同样按上游行为修推断器，禁止「两边都改」。上线前新增/既有全部 json 相关测试必须全绿。
- **推断器与 ValueIter 的解析器边界差异**（如巨型数字精度、非法 UTF-8、深嵌套栈）：记录提取已按行镜像 `ValueIter`（trim/空行/尾随内容报错），UTF-8 校验粒度同为整行；深嵌套递归与 Value 树递归同栈深，无新风险。
- **concat 兜底的极端输入**：多记录挤一行超出按换行数估计的容量时回退「flush 块 + concat」，产出仍为一个 RecordBatch（行序/值不变），仅性能退化到与旧路径相当。

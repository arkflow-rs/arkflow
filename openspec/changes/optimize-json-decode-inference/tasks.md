## 1. component/json：单次 flush 出批

- [x] 1.1 `component/json.rs` 出批路径改 `build_decoder()` + `decode()` 循环 + 单次 `flush()`，删除 `collect<Vec<_>>` + `concat_batches`；空批/无尾换行/0 字节推进防御按 design D1；现有 json codec/processor/debezium 全部测试不修改通过（`cargo test -p arkflow-plugin json`）。实现修正：`Decoder` 的 `batch_size` 同时控制预分配与提前返回阈值，直接取 `usize::MAX` 会按字节放大预分配——改为按换行数估计行数设置容量，多记录挤一行触发阈值时回退「flush 块 + concat」兜底（产出仍单批），spec 措辞已同步
- [x] 1.2 新增边界单测：5000 行单批产出（断言单批与行值）、无尾换行末条结算、空输入空批形状——补齐 spec「单批产出」三场景
- [x] 1.3 `arkflow-plugin` 触及模块 clippy/fmt 零新告警

## 2. 流式推断器 + 差分等价

- [x] 2.1 新增 `component/json/infer.rs`（模块布局按 design D2）：`infer_json_schema_streaming(content) -> Result<Schema, Error>`，serde 流式 visitor 镜像 `InferredType` 状态机（IndexMap 首现序、i64/else-Float64、Null→Any、数组首元素分类、scalar⊕list coerce、同语义错误）。实现要点：记录提取镜像上游 `ValueIter` 的**按行**语义（trim/空行跳过/行内尾随内容报错），对象用 `Vec<(Cow<str>, CowValue)>` 手工复刻 `preserve_order` IndexMap 的首现位置+值替换语义，字符串借用零分配
- [x] 2.2 差分测试（固定语料）：复刻 arrow-json 自带边界用例（mixed_arrays、nested_structs、struct_in_list、nested_list、null 矩阵、>i64::MAX、非法 JSON、非对象顶层记录），断言与 `infer_json_schema` 输出 schema 完全相等 / 双方同 Err
- [x] 2.3 差分测试（随机语料）：确定性种子随机 JSON 生成器（嵌套深度、类型杂度参数化，300 例），逐例断言两实现结果一致
- [x] 2.4 `try_to_arrow` 推断趟切换为 streaming 版本；全部既有 json 相关测试（codec json、processor json、debezium、docs 片段）不修改通过。差分测试当场抓到三处等价性缺口并按上游修正：① `is_none_or_any`（Any 占位列遇数组需替换为初值）② 记录提取必须按行（`{"a":1}{"b":2}` 上游报 trailing characters）③ 字段序为文档序（serde_json preserve_order 在本工作区生效）
- [x] 2.5 clippy/fmt 零新告警（标量载荷仅用于错误 Debug 输出，加注释 + allow(dead_code)）

## 3. 投影路径跨批 schema 缓存（实施中撤销）

- [x] 3.1 ~~`JsonToArrowProcessor` 增 `schema_cache` + 错误回退~~ **撤销**：差分/单测实施中发现 arrow-json tape 解码对 Int64 列遇浮点值走 `NumCast` **静默截断**（`1.5→1`）而非报错（`reader/primitive_array.rs` F64 分支），错误回退检测不了该漂移；精确漂移检测需一次与推断等价的扫描，缓存净收益趋零。已回退 `processor/json.rs` 改动（保留有效的「未投影后见字段」测试），proposal/design/spec 同步记录否决依据
- [x] 3.2 缓存行为测试随撤销移除；`processor::json` 8 例全绿
- [x] 3.3 `cargo test -p arkflow-plugin` 全量绿（redis_cluster 两例环境残留容器致假失败，按 AGENTS.md 清理后 2/2 通过）；触及 crate clippy 零告警

## 4. 基准、收尾与门禁

- [x] 4.1 benchmark 新增 `json-decode` 场景（1000 行 NDJSON 仅计 `try_to_arrow`，markdown/JSON 两格式一致），场景名清单测试同步；`run_suite` smoke（`scenarios_produce_finite_positive_throughput`）通过
- [x] 4.2 `#[ignore]` release 计时测试 `json_decode_timing`：新管线 vs 内联旧实现（infer_json_schema + Reader + concat oracle），200k 行：4 列 **1.52M vs 0.98M rows/s ≈1.55×**、200 列宽记录 **31.6K vs 23.9K rows/s ≈1.33×**（初版纯 Vec 合并在 4 列测得 1.84×，coderabbit 复审指出宽记录 O(f²) 后改 IndexMap/IndexSet + Fx 哈希器并加宽记录用例）；数字已回填 PLANNING 10.2 勾销该第二批项（含 schema 缓存否决结论）
- [x] 4.3 CHANGELOG `[Unreleased]` 已补条目（~1.8×、差分等价、单批出批、新场景）；确认无配置面/行为面变化 ⇒ 无 docs 页任务
- [x] 4.4 门禁：`cargo test -p arkflow-plugin` 全量绿（redis_cluster 环境残留按 AGENTS.md 清理后 2/2）、触及 crate clippy/fmt 零告警、`openspec validate` 通过；全 workspace 测试与 clippy 交 PR CI 复核（与 kafka 变更同惯例）

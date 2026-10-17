# Results — optimize-hotpath-mechanical-batch-3

Release 计时（本机 macOS arm64，`cargo test --release -p arkflow-plugin --lib -- --ignored <name> --nocapture --test-threads=1`，每项 3 轮取最优；旧路径内联为 oracle 同机同轮对拍）。

| 项 | 工作负载 | 新路径 | 旧路径（oracle） | 倍数 |
| --- | --- | --- | --- | --- |
| debezium 解码（`debezium_decode_timing`） | 200k Envelope（5 业务字段含 tags 数组 + source 嵌套 lsn），batch 1000 | 606.5 ms / **329.8K rows/s** | 1651.2 ms / 121.1K rows/s | **≈2.72×** |
| VRL 合并单批（`vrl_merge_timing`） | 200k 行 × 3 列（Int64/Utf8/Float64），batch 1000，程序 `.` | 78.6 ms / **2.54M rows/s** | 413.5 ms / 483.7K rows/s | **≈5.26×** |
| SQL output 转换（`sql_output_conversion_timing`） | 1000 行 × 18 列全类型（含 1/3 null）× 200 批 = 3.6M cells | 98.1 ms / **36.7M cells/s** | 161.1 ms / 22.4M cells/s | **≈1.64×** |

## 说明

- **debezium**：旧路径 = 解析 Envelope → 扁平化（delete 深拷贝 before）→ 重新序列化 NDJSON → `try_to_arrow`（流式推断 + tape 二次解析）；新路径 = 单趟借用解析（`CowValue`）→ 原地扁平化（before 先序列化后 move）→ 值域推断（复用同一 `InferredType` 内核）→ 值域 builder 解码。14 条差分语料（标量/拓宽/嵌套 struct+list/首现排序/null 列/全六冲突列/delete/tombstone/`null` 字面量与非对象 envelope/文本列转义与 unicode/数值 source.db/u64 拓宽/int+float 拓宽值/三类失败场景）断言 schema（含列序）与渲染值逐字节一致、失败集一致，首轮全部通过。
- **VRL**：倍数主要来自消灭每输入行一个 `MessageBatch`（每行一次 schema/RecordBatch 构建）；行到 VRL 值的转换两侧相同。**审计修正**：vrl 0.36 `ObjectMap = BTreeMap<KeyString, Value>`、`KeyString(String)`，每格键分配为值模型固有（预构建后 clone 同样分配），不可消除，该子项不立项。合并带来的语义收窄（单输出批、字段并集、首个非 null 值定型、逐行非对象显式报错）以 `vrl-processor` spec delta + 5 条新测试钉死，vrl.md en/zh 已声明。
- **SQL output**：转换本身的倍数（真实写入受网络/数据库摊薄）。消灭：整批 `clone()`、每 cell async 调用与重复 downcast、扁平 vec + `chunks().to_vec()` 二次克隆、事务路径 `rows.clone()`（改借用绑定 `&str`）。`columnar_rows_match_per_cell_oracle_exactly` 在 0/1/3/7/64 行 × 18 类型（含 null）上与旧逐 cell 实现逐值相等；既有 `sql()` 文本断言（双方言 upsert / 全 key 回退）全绿。

## 门槛

- `cargo test --workspace --all-targets`：2033 通过、17 ignored（`redis_cluster` 两条首跑失败为残留 testcontainers 容器占用 :16379，清理后通过——AGENTS.md 已记录的环境问题）。
- `cargo clippy --workspace --all-targets`：无告警；`cargo fmt --check`：通过。
- `pnpm docs:check`：通过（142 页 / 49 组件）。

## 归档后 CR 修复（2026-10-17）

- **P1 数据丢失（自审发现并修复）**：合并批后，同一字段在不同行的非 null 值类型不一致时，旧实现按"首个非 null 值定型"把另一侧静默置 null——而合并前每行独立成批，两侧都能保留。现改为显式报错（指明字段名、两处类型与行号，提示 `to_float()`/`to_string()`），符合 D1/D4「不静默丢数据」；新增 `conflicting_field_types_across_rows_fail_loudly`（Int/Str、Int/Float、Bool/Int 三例）与 `null_on_one_row_does_not_conflict_with_a_typed_field`。spec scenario「跨行类型冲突显式报错」+ vrl.md en/zh 同步。
- **P2 错误文案**：`component/json/values.rs` 原先把类型标签当列名（"column `Int64`"）。改为线程化真实列路径（嵌套为 `o.m` / `l[]`），错误形如 `JSON decode failed for column `o.m`: expected string, found ... at row 2`。
- **P3 分配**：debezium 六个元信息列的键由每行 `to_string()` 改为 `Cow::Borrowed(&'static str)`（`insert_meta` 只接受六个字面量），业务列的键本就从输入借用——正常路径每行键分配归零，仅冲突列 `__debezium_<name>` 分配。
- **已记录的行为收窄**：`[]`（每行都扇出为空）由"每行一个空批"变为 `ProcessResult::None`；spec scenario 与文档已覆盖。
- 复核：`cargo test -p arkflow-plugin --lib` 全绿（89 项相关测试）、clippy/fmt 无告警、`pnpm docs:check` 通过。

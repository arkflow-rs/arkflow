## 1. debezium 列式累积

- [x] 1.1 `infer.rs`：为 `CowValue` 补 `Serialize` 实现（借用序列化，Map 走插入序）；确认 ObjMap/CowValue 可从 `codec/debezium.rs` 访问（必要时提升 `pub(super)`/`pub(crate)` 可见性）
- [x] 1.2 `codec/debezium.rs`：重构 `decode` 为单趟管线——`from_slice::<CowValue>` 借用解析（tombstone/`null` 字面量/非对象 envelope/无效 JSON 语义与文案不变），扁平化在 `CowValue` 上进行，`before` 先序列化后 move（消灭 `before.clone()`），meta 提取复用 `insert_meta` 冲突语义
- [x] 1.3 `component/json/`：新增值域 schema 推断入口（复用 `collect_field_types_from_object` 等内核，输入 `&[ObjMap<Cow<str>, CowValue>]`）与值域→Arrow builders 解码器（Null/Boolean/Int64/Float64/Utf8/List/Struct 递归，角点行为对齐 arrow-json tape：Int64 列浮点截断、Utf8 列非字符串、标量入 List 列、缺失字段 null）
- [x] 1.4 差分测试：语料覆盖标量/拓宽/嵌套/排序/冲突/delete/tombstone/null 字面量/非对象 envelope/全 tombstone，断言新路径与 `try_to_arrow` oracle 的 schema（含列序）+ 值逐字节等价、失败集一致；既有 debezium 单测全绿
- [x] 1.5 新增 `#[ignore]` release 计时测试 `debezium_decode_timing`（旧字节路径内联为 oracle，对齐 `json_decode_timing` 模式）

## 2. VRL 合并单批输出

- [x] 2.1 `processor/vrl.rs`：`process` 输出行 flatten 为单个 `Vec<VrlValue>` 后一次调用 `vrl_values_to_message_batch`，删除 `Multiple` 分支；空集 → `ProcessResult::None`；逐行 `require_row_object` 守住 D4 显式报错（合并后非首行标量不得退化为全 null 行）；字段集改为全批并集（首现序）、列类型取首个非 null 值
- [x] 2.2 键分配削减：**审计修正**——vrl 0.36 `ObjectMap = BTreeMap<KeyString, Value>` 且 `KeyString(String)` 为 String 支撑，每格键分配是值模型固有（预构建后 clone 同样分配），不可消除、不立项；`message_batch_to_vrl_values` 改收 `&MessageBatch` 消除 `(*msg_batch).clone()` 已落地
- [x] 2.3 `array_output_splits_into_rows_and_multiple_batches` 改写为合并语义（`array_output_and_multi_row_input_merge_into_one_batch`）；新增异构行并集补 null、数组行与对象行交错并入同批、null 首现不冻结类型、中途非对象显式报错四条测试；既有 vrl 单测全绿（27 通过）
- [x] 2.4 新增 `#[ignore]` release 计时测试 `vrl_merge_timing`（旧逐行路径内联为 oracle）

## 3. SQL output 列式转换

- [x] 3.1 `output/sql.rs`：转换 sync 化（去 `matching_data_type` 的 async/await，错误文案逐字节保留）；每列 downcast 一次，按列构建 `Vec<SqlValue>` 后单趟 transpose 成行
- [x] 3.2 去 `write_batch` 的 `(**msg).clone()`（改借用）；`build_*_insert` 与事务路径改收 `&[Vec<SqlValue>]` 消灭 `rows.clone()`；Utf8 绑定改 `&str`（核对两方言 NULL 参数类型声明一致）
- [x] 3.3 转换等价测试（旧实现内联为 oracle，宽类型混合批逐值断言相等）；既有 `sql()` 文本断言（upsert 双方言/全 key 回退/占位符）全绿
- [x] 3.4 新增 `#[ignore]` release 计时测试 `sql_output_conversion_timing`

## 4. 验证

- [x] 4.1 `cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新增告警；`cargo fmt --check` 通过
- [x] 4.2 release 计时测试跑数并留档 results（`cargo test --release -p arkflow-plugin --lib -- --ignored debezium_decode_timing vrl_merge_timing sql_output_conversion_timing --nocapture`）
- [x] 4.3 文档：`docs/docs/components/processor/vrl.md` 与 zh-Hans 对应页声明新批粒度语义；`pnpm docs:check` 通过
- [x] 4.4 复核 `openspec/PLANNING.md` 10.2 节勾划状态（batch 2 已闭环但未勾划的条目 + 本批三项），归档时同步修正

## 1. 列式累积器（avro_arrow.rs）

- [x] 1.1 新增 `AvroArrowAccumulator`：构建期从 writer schema 生成 `Vec<Field>` + 对应列 builder（leaf 类型集与 `leaf_to_arrow`/`null_column` 一一对应，含 `[null, T]` union 的 nullable 追加、Decimal128 precision/scale、时间戳 tz 常量 UTC）；不支持类型沿用现有错误文案构建期拒绝。旧单行路径（`field_to_arrow`/`leaf_to_arrow`/`null_column`）删除，映射单源化
- [x] 1.2 `push(&AvroValue)` 逐字段追加（record 字段数/顺序/union 语义校验沿用旧错误文案，含"union schema 遇非 union 值"分支）；`finish()` 产出多行 `RecordBatch`
- [x] 1.3 `avro_to_arrow` 改为累积器薄封装（单消息 push + finish），公开签名不变；新增 `avro_read_value` 供 codec 复用逐消息 reader 步骤
- [x] 1.4 既有 avro_arrow 测试全绿（直调 `avro_value_to_arrow` 的 5 个测试改经 `push_value` helper 走累积器路径）——映射等价的第一重证据

## 2. codec 分组（schema_registry.rs）

- [x] 2.1 decode 循环按 schema id 首次出现顺序分组（`Vec<(u32, Group)>`，Avro 累积器 / Protobuf 每消息批次同构分组），同组共用 `Arc<CachedSchema>`，每组一个多行批次
- [x] 2.2 组间仍走 `normalize_and_concat`；空批/失败语义与错误文案不变；实施中发现并如实声明一处既有不一致的修正：多消息批此前经并集合并丢失非空标记（单消息批保留），现统一按 writer schema——delta spec 已声明
- [x] 2.3 新增测试：`test_avro_single_id_batch_accumulates_columnar`（3 消息单 id、nullability 保持）、`test_avro_mixed_id_batch_groups_by_first_appearance`（交错 id 的行分组/列序断言）；既有 91 codec 测试（含混版本 wiremock）零修改全绿

## 3. 验证与收尾

- [x] 3.1 门禁：`cargo test -p arkflow-plugin` 823 passed（redis_cluster 2 例失败为已知环境性端口占用——清理残留容器后 2/2 通过）；clippy 0 告警（extend_with_drain 与未用 import 两处已修）；fmt clean；`pnpm docs:check` 通过
- [x] 3.2 release 基准前后对比留档（同机同参数默认 example，best-of-3）：
  - **vs Arc 版（PR #1309 后）**：w5 208,193 → **974,613 rows/s（4.68×）**；w25 45,415 → **308,649 rows/s（6.80×）**；w100 9,830 → **85,145 rows/s（8.66×）**——提升随宽度增长，符合 L3 为 O(字段数) 每消息成本的定性
  - **vs 最初基线（深拷贝版）累计**：w5 5.6×、w25 8.3×、w100 10.5×
  - 副产：三场景单轮耗时合计 ~1.9s → ~0.28s，套件整体更快
- [x] 3.3 CHANGELOG `[Unreleased]` 补条目（#1310）；PLANNING 成本栈记录更新——L3 闭环，仅剩 L2 留 backlog（需上游 apache-avro PR）
- [x] 3.4 契约复核：delta spec Scenario 证据——「单版本批列式累积等价」→ `test_avro_single_id_batch_accumulates_columnar` + avro_arrow `multi_row_accumulation_matches_per_message_batches`；「同 batch 多版本/演进合并/类型冲突」→ 既有 `test_multi_version_avro` / `test_multi_version_each_resolves` / `test_multi_version_conflicting_column_types_error`；分组行序/列序 → `test_avro_mixed_id_batch_groups_by_first_appearance`

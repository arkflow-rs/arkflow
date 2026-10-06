## 1. Avro 批级转换（avro_arrow.rs）

- [x] 1.1 新增列计划 + builder 枚举：plans 元素含 name/nullable/叶类型闭式枚举（含 union unwrap、decimal (p,s)、timestamp 时区；多分支 union/嵌套/不受支持类型的拒绝与文案沿用现状），按 plans 构造 typed builder 集合；decimal 的 `with_precision_and_scale` 与 timestamp 的 `with_timezone` 在 finish 后作用于数组（同现状单元素路径）
- [x] 1.2 新增批转换器 `AvroBatchConverter`：`new(schema)`（不做校验）→ `push(payload)` 逐条处理（reader 构建 + read_value 不动；首条消息逐字段 [schema 检查 → tee 进 plans → 值检查 → append]，后续消息仅 [值检查 → append]，值级校验——字段数/字段名不匹配、null 于非 nullable、值-schema 不匹配、decimal 范围——文案与现状逐字一致、触发位置同现状）→ `finish(force_nullable)`（N≥2 全列 nullable=true 复刻归一提升，N=1 保持 plans nullability）
- [x] 1.3 单值 `avro_to_arrow` 与批级路径对齐（N=1 特例或保留原实现，以测试对齐为准）；既有 22 个 avro 测试全绿
- [x] 1.4 批级全类型覆盖测试：多行 + null 混排（nullable union 全叶类型、decimal、uuid、时区类型断言列类型（含 `Decimal128(p,s)`、`Timestamp(_, Some("UTC"))`）、值序与 nullability 分支）

## 2. Protobuf 批级转换（component/protobuf.rs）

- [x] 2.1 新增批转换器 `ProtobufBatchConverter`（同构形态）：descriptor 全字段集（全 nullable）plans 于首条 `DynamicMessage::decode` 成功后的字段遍历中 tee，后续消息仅值提取 append（缺字段→null 列；每消息每字段的 `get_field_by_name` 查找随 plans 一并消除），unsupported kind 走现状 catch-all 拒绝文案，单值 API 对齐
- [x] 2.2 protobuf 既有测试全绿 + 批级多行测试（缺字段→null 列、行序断言）

## 3. decode 动态接线（schema_registry.rs）

- [x] 3.1 decode 循环保持现状操作顺序（逐消息:parse_wire_format → resolve_cached → 解码 → 字段遍历），首个 id 建批转换器、同 id `push`；**出现第二个 id 时余下消息切回现状单行路径**，最终 `normalize_and_concat([前缀多行 batch, 后续单行 batches])`（前缀在头部，行序 = 消息序，无双解码）；全程单一 id → `finish` 后直接返回，跳过 normalize_and_concat；空批保持现状空 RecordBatch，ensure_gate 位置不变
- [x] 3.2 等价性与不变量测试：同输入（N=1、N≥2、null 混排）下批级输出 vs 逐消息+normalize_and_concat 输出逐列断言相等，**显式断言 nullability 分支（N=1 非 union 列 nullable=false / N≥2 全列 nullable=true）**；混合 id 行序测试（输出行序 = 消息序，并集合并语义、前缀多行+后缀单行共存）；错误顺序测试——①fetch 失败消息先于坏 wire 头消息 → 报 fetch 错误（无预扫描抢跑）；②首条消息更早字段值级错误 + 更晚字段 schema 错误 → 报更早字段的错；③多处坏消息首错归因于全局最早者

## 4. 验证与留档

- [x] 4.1 门禁：`cargo test -p arkflow-plugin` 全绿、`cargo clippy --workspace --all-targets` 0 新告警、`cargo fmt --all` clean
  - 实测：`cargo test -p arkflow-plugin` 全绿（redis_cluster 首跑 2 例失败为已知环境性端口占用——残留 testcontainers 容器占 16379，`docker rm -f` 后 `cluster_*` 7/7 通过，与本次改动无关）；clippy 0 告警（新测试代码 4 处 lint 已修）；fmt clean。模块明细：avro_arrow 19 / schema_registry 37（32 既有 + 5 新增）/ protobuf 17（15 既有 + 2 新增）
- [x] 4.2 release 基准 A/B：既有 avro-decode-w5/w25/w100，count=400k 单轮 ×3 取最优，与父提交对照（方法沿用上轮留档），数据记入本 tasks 备注
  - **留档（2026-10-06，本机 macOS arm64，release example 二进制，count=400k/runs=3/warmup=1，best-of，两侧各跑 2 轮校验、轮间波动 <2%）**：`avro-decode-w5` 267,647 → 1,177,413 rows/s（**×4.40**）｜`avro-decode-w25` 57,701 → 413,021 rows/s（**×7.16**）｜`avro-decode-w100` 12,386 → 112,833 rows/s（**×9.11**）。宽度越大提升越大（每字段分配随宽度线性放大，与 L3 成本模型一致）。既有场景对照（无回归）：linear-sql 690k、groupby-sql 825k、filter-project-sql ≈80 万级、codec-json 1,692-1,713 batches/s（与上轮留档同量级）。绝对值较上轮 Arc 版留档（207k/45.5k/9.8k）为高，含当日机器状态差异；本次 A/B 为同会话两侧对照，以比率为准
- [x] 4.3 契约复核：delta spec 五个 Scenario 均有测试证据；openspec 校验通过
  - 「同 id 多消息累积为单一批次」→ `schema_registry::tests::same_id_messages_accumulate_into_one_batch_with_legacy_semantics`（codec 级 N=3 行序/全 nullable）+ `avro_arrow::tests::batch_converter_output_matches_per_message_concat`（与逐消息+归一合并逐列相等）+ `batch_converter_multi_row_all_types`（14 列全类型）
  - 「单条消息保持映射 nullability」→ 同上两测的 N=1 分支（`field(0).is_nullable() == false`，与单批直通一致）
  - 「混合 id 行序与合并语义不变」→ `mixed_schema_ids_keep_message_row_order`（1/2/1/2 消息序、并集列、v1 行新列 null）+ 既有 `test_multi_version_avro`/`test_multi_version_each_resolves`/`test_conflicting_types`（32 既有测试全绿）
  - 「错误优先级与归因不变」→ `fetch_error_precedes_later_bad_wire_header`（无预扫描抢跑）+ `payload_decode_error_precedes_schema_shape_error`（plans 不前置于 reader 错误）+ `avro_arrow::tests::batch_converter_keeps_error_texts_and_order`（消息内字段级交错、后续消息值级校验与文案）+ `batch_converter_decimal_behaves_like_the_single_value_path`
  - 「Protobuf 同 id 累积」→ `protobuf_same_id_messages_accumulate_in_order`（codec 级）+ `protobuf::tests::batch_converter_multi_row_matches_per_message_concat`（与逐消息+归一合并逐列相等）+ `batch_converter_rejects_unsupported_kind_like_single_path`（kind 拒绝文案）

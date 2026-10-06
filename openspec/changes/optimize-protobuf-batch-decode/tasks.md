## 1. Protobuf 批级转换（component/protobuf.rs）

- [x] 1.1 新增 `ProtobufBatchConverter`：持有 `MessageDescriptor`（值语义，廉价 Arc 克隆），列计划（name/字段号/叶类型闭式枚举）于首条消息字段遍历中 tee（unsupported kind 走现状 catch-all 拒绝文案），后续消息 `get_field_by_number` 取值 append 进类型化 builders，`finish()` 以 `RecordBatchOptions::row_count` 直出多行 batch（覆盖零字段消息）
- [x] 1.2 单值 `protobuf_to_arrow` 原样保留（processor 路径与既有测试不動）

## 2. decode 接线（codec/schema_registry.rs）

- [x] 2.1 `Group::Protobuf(Vec<RecordBatch>)` 改为 `Group::Protobuf(ProtobufBatchConverter)`：首条消息建组并 push，后续 push，组间 `normalize_and_concat` 不变；空批、门禁、wire format 失败语义不变

## 3. 测试

- [x] 3.1 单元等价性：批级输出 vs 逐消息+归一合并逐列相等（全部 kind、未设置字段默认值、行序、全列 nullable）
- [x] 3.2 codec 级：`protobuf_same_id_messages_accumulate_in_order`（N=3 行序 + 全 nullable）；`fetch_error_precedes_later_bad_wire_header`（顺序语义守卫）；`payload_decode_error_precedes_schema_shape_error`（累积器构建不前置于读取错误，Avro 侧同守卫）
- [x] 3.3 kind 拒绝文案：批级与单值路径同文案（嵌套 message 用例）

## 4. 验证与留档

- [x] 4.1 门禁：`cargo test -p arkflow-plugin` 全绿、`cargo clippy --workspace --all-targets` 0 新告警、`cargo fmt --all` clean
  - 实测：模块明细 protobuf 19 / schema_registry 37（34 既有含 #1310 新增 + 3 新增）；redis_cluster 环境性失败处置同前（残留容器清后 7/7）
- [x] 4.2 ad-hoc release 计时留档：`#[ignore]` 计时测试（逐消息+归一 vs 批级，同参数多次取最优），数据记入本 tasks 备注
  - **留档（2026-10-07，本机 macOS arm64，release 测试二进制，26 字段宽消息（int64×10/string×5/double×2/bool×2/int32×2/bytes×2/uint64×2/uint32），1000 消息/批 × 200 批 = 200k 行，best-of-3）**：逐消息+归一合并 67,470 rows/s → 批级 converter 1,249,704 rows/s（**×18.5**）。复现：`cargo test --release -p arkflow-plugin --lib -- --ignored protobuf_batch_decode_timing --nocapture`
- [x] 4.3 契约复核：delta spec 四个 Scenario 均有测试证据；openspec 校验通过
  - 「同 id 多消息累积为单一批次」→ `protobuf::tests::batch_converter_multi_row_matches_per_message_concat`（与逐消息+归一合并逐列相等）+ `schema_registry::tests::protobuf_same_id_messages_accumulate_in_order`（codec 级 N=3 行序 + 全 nullable）
  - 「未设置字段保持 proto3 默认值语义」→ `batch_converter_multi_row_matches_per_message_concat`（name 未设置行 = 空字符串非 null，与逐消息路径一致）
  - 「kind 拒绝文案与归因不变」→ `batch_converter_rejects_unsupported_kind_like_single_path`（嵌套 message 用例，批级与单值同文案）
  - 「混合组次序稳定」→ 既有 `test_multi_version_each_resolves`/`test_multi_version_*`（#1310 分组语义，protobuf 组接入后全绿）+ codec 级行序断言
  - 顺序守卫（跨变更有效）：`fetch_error_precedes_later_bad_wire_header`、`payload_decode_error_precedes_schema_shape_error`

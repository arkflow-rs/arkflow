## 1. 推断全量化

- [x] 1.1 `crates/arkflow-plugin/src/component/json.rs:28`：`Some(1)` → `None`；确认 `fields_to_include` 投影与 `concat_batches` 在全量推断下行为不变
- [x] 1.2 新增合并解码回归：`{"v":1}`/`{"v":1.5}`/`{"v":2.9,"tag":"x"}` → Float64 `[1.0,1.5,2.9]` + `tag` 列保留（nullable）；全整数列仍 Int64；`codec/json.rs` 与 `processor/json.rs`（json_to_arrow 整批路径）各一组
- [x] 1.3 修正 `test_json_codec_skip_widens_types_like_fail_mode`（`codec/json.rs:361`）：按真实分化行为重写断言（skip 拓宽不截断 / fail 语义保持），修正名不副实的文档串
- [x] 1.4 单消息路径回归：`on_error: skip` 逐条解码与 Kafka 逐消息解码输出与既有断言一致

## 2. 文档与门禁

- [x] 2.1 `docs/docs/`（en）与 zh-Hans 对应页：json codec 与 `json_to_arrow` 处理器的全量推断语义、混型列放宽 Float64 的下游适配说明
- [x] 2.2 门禁：`cargo test -p arkflow-plugin` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`cargo fmt --all`；`pnpm docs:check`；`ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot`（若元数据示例受类型放宽影响则同步）
  - 注：lib 单测 764 全绿（含全部 json 回归）；`cargo clippy -p arkflow-plugin --all-targets` 仅剩 `tests/kafka_eos.rs`（并发 kafka EOS 变更所有）2 条既有告警；`pnpm docs:check` 通过；注册/ schema 未变，无需重新生成 inventory。`--test kafka_eos` 在并发 agent 的 flux 中间歇失败（该测试全程 `codec: None`，与本次 JSON 推断改动无关）。
- [x] 2.3 契约复核：delta spec 四个 Scenario 均有测试证据；tasks 勾选
  - 混见→Float64：`test_json_codec_merged_decode_widens_int_float_and_keeps_late_fields`、`test_json_to_arrow_whole_batch_widens_types_and_keeps_late_fields`、`test_json_codec_skip_widens_types_like_fail_mode`；新字段保留：前两者的 `tag` 列断言 + `test_json_codec_skip_merges_heterogeneous_good_messages`；单消息不变：既有 skip/kafka 逐消息测试原样通过；全整数 Int64：`test_json_codec_merged_decode_all_int_stays_int64`、`test_json_to_arrow_whole_batch_all_int_stays_int64`。

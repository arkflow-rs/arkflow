## 1. 核心修复

- [x] 1.1 `lib.rs` `filter_columns`：构造后 `set_input_name(self.get_input_name())`；单测：带名批次过滤列后 input_name 保留
- [x] 1.2 `lib.rs` `new_binary_with_origin`：同上保留；单测同上
- [x] 1.3 `buffer/join.rs`：无名批次从 `continue`（trace）升级为 warn + `discarded_unnamed_batches` 计数器；单测：无名批次被丢弃且计数器递增

## 2. 验证

- [x] 2.1 `cargo test -p arkflow-core --lib` 全绿（含新测试）
- [x] 2.2 `cargo test -p arkflow-plugin --lib buffer` 全绿（含新测试）
- [x] 2.3 `cargo test --workspace --all-targets` 全绿
- [x] 2.4 `cargo clippy --workspace --all-targets` 零新增告警
- [x] 2.5 `openspec validate fix-input-name-invariant` 通过

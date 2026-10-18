## 1. Avro L2：批内 reader 复用

- [x] 1.1 `avro_arrow.rs` 新增 `AvroValueReader<'s>`（`GenericDatumReader` 构造一次、`read_value(&self)` 跨消息复用）+ `#[ignore]` A/B 计时测试 `avro_reader_reuse_timing`（每消息构建 vs 批内复用，w5/w25/w100；release 实测 +41.7%/+12.8%/+6.7%）
- [x] 1.2 `schema_registry.rs` decode 改两段式：pass 1 逐消息 parse wire + resolve（错误中止顺序与历史管线一致），pass 2 逐消息解码、`AvroValueReader` 按 id 惰性构建一次复用；两个错误顺序钉死测试（`fetch_error_precedes_later_bad_wire_header`、`payload_decode_error_precedes_schema_shape_error`）保持绿
- [x] 1.3 语义等价门槛：`cargo test -p arkflow-plugin --lib` 全绿（852/852，含交织 id 首现分组、单 id 列式累积、多版本并集/冲突、缓存命中、兼容性门禁）
- [x] 1.4 官方 benchmark 前后对比：`cargo run -p arkflow --release --example benchmark` 记录 `avro-decode-w5/w25/w100` 数据入 results.md（1.05M→1.71M / 396K→442K / 109K→116K rows/s，对照 PLANNING 10.1 同机基线 +63%/+11.6%/+6.0%）

## 2. event_time_gate 混合组拷贝：定夺

- [x] 2.1 `event_time_gate.rs` 新增 `#[ignore]` 计时测试 `event_time_gate_group_copy_timing`（uniform Emit 零拷贝 vs Hold+Drop 混合两组 + 裸 filter 微基准；release 实测 2 组分拆拷贝 ≈2.6ns/行，端到端 <0.2%）
- [x] 2.2 结案不做代码改动：PLANNING 10.2 该条目勾划为"已测得不显著"，证据引用本 change

## 3. 收口验证

- [x] 3.1 `cargo test --workspace --all-targets` 全绿 + `cargo clippy --workspace --all-targets` 无新增告警（`redis_cluster` 首轮失败为 AGENTS.md 已知环境问题——18 小时前残留 testcontainers 容器占用 :16379，`docker rm -f` 后 2/2 通过；clippy 修复 1 处 timing 测试内的 `is_multiple_of` 后 0 告警）
- [x] 3.2 PLANNING 10.2/10.3 更新：Avro L2 闭环（批内复用，无需上游）、gate 混合组结案、第二批候选清单勾划
- [x] 3.3 `openspec validate` 通过后归档本 change

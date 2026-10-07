## 1. Protobuf 列式解码接入（codec + processor）

- [x] 1.1 `codec/protobuf.rs` decode 改为 `ProtobufBatchConverter::push/finish` 直出单批：fail 模式首错即返、skip 模式坏消息不 push 并计 skipped，错误文案与既有逐字一致；删除逐消息 `protobuf_to_arrow` + `normalize_and_concat` 组合
- [x] 1.2 `processor/protobuf.rs` decode 接入同一转换器，行为语义与错误文案不变（顺带消除空载荷 `batches[0]` panic 隐患）
- [x] 1.3 等价性测试：同批消息"转换器单批"vs"逐消息解码再 concat"输出 RecordBatch 相等（`protobuf_to_arrow` 降为 `#[cfg(test)]` oracle）；既有 codec/processor 测试零修改通过（42 passed）
- [x] 1.4 `protobuf_batch_decode_timing` 基准随 release 验证跑通（见 4.2）

## 2. 窗口算子 O(n²) cast 修复

- [x] 2.1 `executor/window/operator.rs` accumulate：窄整型 value 列（Int8/16/32、UInt*）cast 提到行循环 × 窗口成员循环外，批前一次 `BatchValueColumn` 规整
- [x] 2.2 新增测试 `narrow_int_value_columns_match_int64_and_stay_linear`：Int32 vs Int64 聚合等值对照（null 跳过）+ 20k 行批 <10s 线性度粗粒度守卫

## 3. 指标缓存（原 3.1 SQL 单分区已实施后实测回归剔除，见 design.md D3）

- [x] 3.1 ~~`component/sql.rs` SessionContext 改 `with_target_partitions(1)`~~ → 实施后四轮实测 groupby -11%/filter -12%（多分区聚合的查询内跨核并行收益 > spawn/concat 开销），回退剔除，结论留档 PLANNING 第十节
- [x] 3.2 `ChainHooks` 增加 `chain_metrics: Option<Arc<ChainMetrics>>`，两个真实构造点（task.rs / kernel_handle.rs）构建期解析注册；`dispatch_data`/`process_chain`/worker pool 改用预解析引用；快照导出路径零改动
- [x] 3.3 内核既有指标/快照测试全绿（arkflow-core 1121 passed，含 chain metrics 计数断言测试迁移到新签名）

## 4. 验证与收尾

- [x] 4.1 门禁：`cargo test --workspace --all-targets` 全绿（redis_cluster 失败为已知 testcontainers 泄漏环境问题——清理残留容器后 2/2 通过；http unreachable 单例失败为系统代理 502 瞬时干扰，孤立复跑通过）；clippy 零新告警；fmt clean；`pnpm docs:check` 通过（142 页/49 组件）
- [x] 4.2 release 基准前后对比留档（同机多轮取稳）：剔除 3.1 后 linear 672K/groupby 821K/filter 827K rows/s，全部回到基线（676K/832K/849K）±噪声内；avro w5/w25/w100 = 1.01M/406K/110K 持平；`protobuf_batch_decode_timing` release 实测 1,207,659 rows/s（200k 行 165.6ms，转换器路径）
- [x] 4.3 CHANGELOG `[Unreleased]` 补条目（含 3.1 剔除的诚实记录）；PLANNING 新增第十节（审计梯队清单 + 3.1 实测结论），本批项勾销

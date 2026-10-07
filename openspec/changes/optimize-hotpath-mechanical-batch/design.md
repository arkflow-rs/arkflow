## Context

三项互相独立的机械优化，分布在 codec 入口（protobuf 独立 codec/processor）、执行内核（窗口算子 value 列 cast、每批指标查找）。全部要求：行为语义零变化（错误文案、输出 schema、skip/fail 模式逐字保留）、公开 API 零破坏、既有测试零修改通过（新增测试除外）。`ProtobufBatchConverter`（component/protobuf.rs:522-615）已存在且被 schema_registry 路径与 `protobuf_batch_decode_timing` 基准（×18.5）验证。

## Goals / Non-Goals

**Goals:**
- 独立 protobuf codec/processor decode 接入 `ProtobufBatchConverter`，消除每消息 schema/batch 重建与批尾全量拷贝。
- 窗口算子窄整型 value 列 cast 从 O(rows²) 降到每批一次。
- `ChainHooks` 构建期缓存 `Arc<ChainMetrics>`，每批指标路径零锁零分配。

**Non-Goals:**
- SQL `target_partitions` 调整（见 D3 实测记录，已剔除）。
- SQL 物理计划缓存、protobuf encode 优化、JSON schema 缓存、Kafka input 批聚合、event_time_gate 行级分配清零、窗口状态快照改造（PLANNING 第十节梯队，各自独立 change）。
- 不改 `KernelMetrics::chain()` 公开签名（构建期注册与非热路径调用方保留）。
- 不引入新依赖、不改任何 YAML 配置面。

## Decisions

### D1：protobuf codec/processor decode 直用转换器，不经 normalize_and_concat

`codec/protobuf.rs` 的 descriptor 对所有消息唯一（构建期解析一次），批内不存在 schema 演进——与 schema_registry 路径不同，这里**不需要**按 id 分组与并集合并。decode 循环改为：每批新建一个 `ProtobufBatchConverter::new(descriptor.clone())`（descriptor 是 `Arc` 内包装，clone 一次/批可忽略），逐消息 `push`，`finish()` 直出一个多行批次。skip 模式：坏消息不 push、计入 skipped（语义 = 现状"跳过该消息"）；fail 模式：首个错误即返回，错误文案沿用转换器/解码层的既有文案。`processor/protobuf.rs` 的 decode 路径同构替换（顺带消除旧代码空载荷时 `batches[0]` 的 panic 隐患）。`protobuf_to_arrow` 保留为 `#[cfg(test)]` 的逐消息 oracle，供等价性对拍测试引用。

边界推演（skip 模式）：转换器的 kind 拒绝是 descriptor 静态属性——若有任何 push 成功过，descriptor 必然全支持，不存在"半启动状态被 finish"的路径；全失败批走既有 all-bad 错误，converter 状态不会被消费。

### D2：窗口 value 列 cast 提升到行循环外，按"首个 value 列"每批一次

`accumulate` 只消费 `value_columns.first()`，match 原位于 行循环 × 窗口成员 两层循环内（比"每行一次"更差）。提升为批前一次构造 `BatchValueColumn<'a>` 枚举（Count/Int/Float/Float32），窄整型在此一次 `cast(column, Int64)`（`Int64Array::from(casted.to_data())` 零拷贝取得所有权），行循环仅 downcast 取值。cast 是保持 validity bitmap 的纯函数，null 语义与逐行 cast 逐位一致；不支持的类型错误从"首个成员后"提前到批前（该错误由列类型决定，属静态确定性，提前报错不改变可观测行为——旧路径首行首个窗口也会在创建空 buffer entry 后立即失败）。

### D3：~~SQL 会话固定单分区~~ —— 实施后实测回归，剔除

审计初判 `target_partitions = 1` 可消除千行小批的 RepartitionExec+concat 固定开销。实施后同机 benchmark 四轮实测：groupby-sql 832K→733-749K rows/s（-11%）、filter-project 849K→736-761K（-12%），linear +0.7%（噪声内）。归因：多分区执行给 GROUP BY 提供**查询内跨核并行**（partial aggregate 在 N 个 tokio 分区并行），并行收益超过 spawn/channel/concat 开销；单分区反而串行化聚合本体。结论固化进 PLANNING 第十节（后续若重估须以 EXPLAIN 物理计划对比立项，不得直接改配置）。已从本 change 剔除并回退 `component/sql.rs`。

### D4：ChainMetrics 构建期解析进 ChainHooks，计数器对象保持同一

`ChainHooks` 增加 `chain_metrics: Option<Arc<ChainMetrics>>`，两个真实构造点（`task.rs` 的 `run_graph_with_metrics_startup`、`kernel_handle.rs` 的 hook 组装）构建期经 `KernelMetrics::chain()` 解析一次并注册进既有 BTreeMap——快照导出（data-plane-observability）看到的仍是同一组计数器对象，零改动。`dispatch_data`/`process_chain` 签名增加预解析引用参数（同时保留 `RuntimeMetrics` 引用——process_chain 还需 bump processing_errors/output_* 等遗留计数器）；worker pool（`ProcessorWorkerPool::start`）随 hook 克隆传递。`KernelMetrics::chain()` 公开方法保留。

## Risks / Trade-offs

- [转换器与逐消息路径的边界差异] → 对拍测试：`test_codec_columnar_batch_equals_per_message_merge`（同批消息两条路径输出 RecordBatch 相等）；全量既有 codec/processor 测试零修改通过为门禁。
- [Chain 生命周期与 metrics 注册错位（job 重启重建 hook）] → `chain()` 的 entry+or_default 语义幂等，重启重解析返回同一 Arc；内核指标/快照测试（core 1121 项）验证计数连续。
- [窗口 cast 提升改变 null/错误语义？] → 不变：null 位图随 cast 保持；类型错误静态确定，见 D2。新增 `narrow_int_value_columns_match_int64_and_stay_linear`（Int32 vs Int64 等值对照 + 20k 行批 <10s 线性度守卫）。

## Migration Plan

纯内部实现变更，无部署/回滚面；单 PR 交付，出问题直接 revert。

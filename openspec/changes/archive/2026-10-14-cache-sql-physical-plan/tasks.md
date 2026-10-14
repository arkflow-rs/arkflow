## 1. 懒扫描源（DataSource）

- [x] 1.1 `SwapBatchTable` 重构为共享内槽 `Arc<RwLock<RecordBatch>>`（`scan(&self)` 无法取自身 Arc，槽位与 source 共享），公开行为（swap/schema）不变
- [x] 1.2 实现 `SwapBatchSource: DataSource`——`open` 执行期读当前批、schema 一致性 fail-closed 校验、`RecordBatch::project` 应用规划期投影、`MemoryStream` 返回；`partition_statistics` 报规划期当前批精确行数；`with_fetch`/`fetch`/`try_swapping_with_projection`/`repartitioned` 维持「不支持」默认（与现状忽略 limit/filters 一致）；`scan()` 改返回 `DataSourceExec::from_data_source(...)`

## 2. 物理计划缓存

- [x] 2.1 `ContextPlanCache` 增 `physical: Option<(SchemaRef, Arc<dyn ExecutionPlan>)>` 槽位；`execute_with_cached_plan` 在 `!time_dependent && !recursive` 且 schema 匹配时经 `reset_plan_states` 刷新后复用（执行算子内嵌一次性态：repartition 通道/join future/TopK 阈值），否则 `create_physical_plan` 并按 schema 缓存；`LogicalPlan::RecursiveQuery` 检测入 OnceLock 守卫；时间折叠/漂移/temporary 路径行为不变

## 3. 测试

- [x] 3.1 缓存不变量测试：同 schema 连续批各得各的数据（既有 `plan_cache_reflects_each_batch` 通过）；schema 漂移重规划成功（既有测试通过）；`now()` 跨批推进（既有测试通过）；新增「物理规划每 schema 至多一次」断言测试与「执行期 schema 不匹配 fail-closed」单元测试（直接构造 SwapBatchSource 校验错误路径）
- [x] 3.2 等价性测试：同一查询同批序列，缓存路径输出与强制每批物理重规划基线逐批 schema/数值一致（含投影列序）
- [x] 3.3 `#[ignore]` release 计时测试 `sql_physical_plan_timing`：缓存路径 vs 每批 `create_physical_plan` 基线（沿用 protobuf_batch_encode_timing 模式），千行批 × 数百批，双形态（全表投影聚合 / 列投影过滤）

## 4. 验证与收尾

- [x] 4.1 `cargo test -p arkflow-plugin` + `cargo clippy --workspace --all-targets` + `cargo fmt` 全绿；workspace 测试无回归
- [x] 4.2 release benchmark SQL 三场景（linear/groupby/filter-project）前后对比，结果记入 results.md；PLANNING.md 10.2 对应条目划掉

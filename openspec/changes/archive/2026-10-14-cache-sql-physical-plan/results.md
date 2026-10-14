# cache-sql-physical-plan — 实施结果

## 交付内容

`SqlProcessor` 快路径（无 temporary 表）新增**物理计划缓存**：

1. `SwapBatchTable` 重构为共享内槽 `Arc<RwLock<RecordBatch>>`；`scan()` 不再在规划期捕获批数据，改返回懒源 `SwapBatchSource`（实现 DataFusion 54 `DataSource` trait，经 `DataSourceExec::from_data_source` 包装）——`open()` 执行期才读当前批、校验 schema 一致性（fail-closed）、应用规划期投影。
2. `ContextPlanCache` 增 `physical: Option<(SchemaRef, Arc<dyn ExecutionPlan>)>`；`!time_dependent && !recursive` 且 schema 匹配时复用，否则每批 `create_physical_plan`（现状行为）。
3. **定向重执行刷新 `refresh_for_reexecution`**：自底向上仅重建携带一次性执行态的节点（`RepartitionExec`/`SortExec`/五类 join），重链祖先，无状态节点 ptr_eq 共享——刷新 0.4μs/批 vs 官方 `reset_plan_states` 全树重建（慢 ~7%）vs `create_physical_plan` 74.5μs/批。
4. 守卫：`now()` 等时间折叠查询与 `WITH RECURSIVE`（工作表一次性态）排除在物理缓存外（OnceLock 一次判定）。

## 关键实测过程（记录以防重蹈）

- **二次执行 panic**：缓存计划直接 `collect` 会在 `RepartitionExec` panic（"partition not used yet"，节点内嵌分发通道一次性态）——重执行必须刷新。
- **动态过滤排查**（`reset_plan_states` 文档限制）：探针实证 join+filter 不产生动态过滤（懒源拒绝 pushdown）；TopK（ORDER BY+LIMIT）产生 `DynamicFilterPhysicalExpr`，由 `SortExec::reset_state` 正确重建；GROUP BY HAVING 无实态。
- **计时测试臂标签假阴性**：`drop_cache` 布尔参数传反，一度得出「缓存慢 0.67x」的错误结论；阶段分解探针（lookup/build/collect 三相计时）定位：`create_physical_plan` 实为 74.5μs/批（大头），刷新仅 0.4μs——修正后缓存路径真实胜出。

## 性能（release，本机，M 系列最佳值）

隔离计时（`sql_physical_plan_timing`，1000 行批 × 200 批，min-of-5）：

| 形态 | 缓存路径 | 每批重规划 | 加速 |
| --- | --- | --- | --- |
| aggregate（sum） | 9,289,166 rows/s | 5,857,752 rows/s | **1.59x** |
| filter-project | 5,275,693 rows/s | 3,934,117 rows/s | **1.34x** |

阶段分解（aggregate 形态，200 批累计）：create_physical_plan **14.91ms**（74.5μs/批）→ 定向刷新 **0.079ms**（0.4μs/批）；collect 两臂一致（5.4 vs 6.1ms，噪声内）。

端到端 benchmark（`cargo run -p arkflow --release --example benchmark`，200K 行，min-of-3，对比 batch-2 后基线）：

| 场景 | 基线 | 本次 | 变化 |
| --- | --- | --- | --- |
| linear-sql | 702,317 rows/s | 714,001 | **+1.7%** |
| groupby-sql | 768,726 | 782,354 | **+1.8%** |
| filter-project-sql | 799,166 | 853,432 | **+6.8%** |
| （未触碰场景 codec-json/state-backend/avro-*） | — | — | ±2~12% 噪声 |

端到端增幅小于隔离增幅的原因：benchmark 管线中 SQL 处理只占批时间的一部分（generate+json 解码+内核分发摊薄）；规划节省是每批固定 ~70μs，批越小/查询越复杂（join/窗口/多聚合）收益占比越高。

## 测试

- 既有 17 个 SQL processor 测试全部保持通过（缓存各不变量、schema 漂移、并发、now() 推进、EXPLAIN、池不泄漏、temporary 路径）。
- 新增：`physical_plan_built_at_most_once_per_schema`（构建计数器钉住每 schema 至多一次 + 漂移后重建）、`swap_source_fails_closed_on_schema_mismatch`（含投影列序验证）、`cached_physical_plan_matches_full_replan`（八查询形态 × 4 轮，含 TopK 跨批交替值域抓阈值泄漏、join+filter、HAVING、DISTINCT、窗口，逐批与全独立重规划比对 schema+数值）、`recursive_query_bypasses_physical_cache`（守卫 + 等价 + 构建计数）、`sql_physical_plan_timing`（#[ignore] release 计时）。
- `cargo test --workspace --all-targets`、`cargo clippy --workspace --all-targets`、`cargo fmt` 全绿。

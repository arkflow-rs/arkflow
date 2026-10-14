## Context

`SqlProcessor.execute_with_cached_plan` 已缓存 analyzed 与 optimized 逻辑计划（`ContextPlanCache`，按池化 `SessionContext` 地址 + batch schema 键控；`now()` 等时间折叠查询跳过 optimized 缓存），但每批仍执行 `create_physical_plan`。根因：`SwapBatchTable::scan`（TableProvider::scan）在规划期把当前批 clone 进 `MemorySourceConfig::try_new_exec`，物理计划因此绑定规划时刻的数据，跨批复用必然读到旧批。

DataFusion 54.1。`datafusion-datasource` 提供 `DataSource` trait（`open`/`fmt_as`/`output_partitioning`/`eq_properties`/`partition_statistics`/`with_fetch`/`fetch`/`try_swapping_with_projection`），经 `DataSourceExec::from_data_source` 包装为 `Arc<dyn ExecutionPlan>`——这是官方自定义扫描源的扩展点（`MemorySourceConfig` 自身即此形态）。

## Goals / Non-Goals

**Goals:**
- 查询与 schema 不变时，物理计划每池化上下文最多构建一次；后续批次零重规划直达 `collect`。
- 输出与逐批重规划逐字节等价（含投影列序、类型、错误路径）。
- 时间折叠查询、schema 漂移、temporary 路径行为与现状完全一致。

**Non-Goals:**
- temporary 路径（表内容逐批变化，按批重注册/重规划是既有设计，不缓存）。
- 逻辑计划缓存策略变更（analyzed/optimized 现状保留）。
- 跨 processor / 跨 schema 的全局计划缓存；查询间共享。
- 物理计划自适应（按后续批次统计信息重选 join 构建侧等）。

## Decisions

1. **自定义 `DataSource`（懒读取）而非自定义 `ExecutionPlan`。**
   `DataSourceExec::from_data_source(SwapBatchSource)` 免费获得 schema/分区/explain 展示/指标的标准语义；只需实现数据面。备选（手写 `ExecutionPlan` + `PlanProperties`）要复刻 `MemoryExec` 的属性/统计行为，面积更大。`SwapBatchTable` 重构为持有 `Arc<RwLock<RecordBatch>>` 内槽（`scan(&self)` 拿不到 self 的 Arc，共享槽位让 source 与 table 解耦共享）。

2. **`open()`（执行期）读当前批 + 防御性 schema 校验，投影用 `RecordBatch::project`。**
   stream 生成时 `read()` 当前批、校验 `batch.schema() == plan_schema`（fail-closed，spec 要求），`project(&indices)` 后以 `MemoryStream` 单批返回。投影语义与 `MemorySourceConfig` 内部一致（列选取）；不用 `try_new_exec` 包装以免再建一层 exec。

3. **`partition_statistics` 报告规划期当前批的精确行数。**
   物理规划（join 构建侧、repartition 决策）会消费统计信息；从当前批计算（`num_rows` exact、byte 尺寸 unknown）使缓存首建时的计划形态与现状一致。统计只在规划期被调用，不产生每批开销。

4. **复用经定向重执行刷新，而非直接 collect 缓存计划。**
   实现期发现（原设计未预见）：部分执行算子把一次性执行态存在节点内部——`RepartitionExec` 的分发通道（二次执行 panic "partition not used yet"，实测复现）、join 构建侧 future/materialized build、TopK 动态过滤阈值。DataFusion 54 的官方助手 `reset_plan_states`（transform_up + 每节点 `reset_state`，文档定位即「使计划可重执行」）可覆盖，但实测其全树逐节点重建成本过高（每节点走 `check_if_same_properties` 属性比对 + 节点重建）。最终落地为**定向刷新 `refresh_for_reexecution`**：自底向上仅重建携带执行态的节点类型（`RepartitionExec`、`SortExec`、五个 join 类型；与覆写 `reset_state` 的 DataFusion 节点集合对齐，经 trait upcast 逐类型 downcast 判定），重链其祖先，无状态节点（Projection/Filter/Aggregate/Coalesce/DataSource 叶）按 `Arc::ptr_eq` 判定未变即直接共享。实测刷新成本 **0.4μs/批**（聚合形态，1K 行批），对比 `create_physical_plan` 的 **74.5μs/批**（阶段计时探针实测）。
   - **递归 CTE 排除**：重执行机制文档明确 recursive query 不可重执行（`WorkTableExec` 工作表一次性，二次执行返回空）。分析期检测 `LogicalPlan::RecursiveQuery`（与 time_dependent 同侧的 OnceLock 守卫），命中则每批重规划（现状行为）。等价测试以「漏缓存 → 第二批返回空」的方式钉住守卫。
   - **动态过滤实证**：探针实测三查询形态——join+filter 不产生动态过滤（懒源拒绝 filter pushdown，机制不接线）；`ORDER BY+LIMIT`（TopK）产生真实 `DynamicFilterPhysicalExpr`，由 `SortExec::reset_state` 重建（等价测试用跨批交替数值范围钉住阈值泄漏）；`GROUP BY HAVING` 仅有 `dynamic_filter: None` 字段（无实态）。

5. **`filters`/`limit` 推送维持忽略（现状行为）；`partition_statistics` 报规划期当前批精确行数**，缓存首建时的计划形态（join 侧选择等）与现状一致。

6. **计时测试沿用 batch-2 模式：`#[ignore]` + release 手跑。**
   A/B 对比「缓存路径」vs「清空 physical 槽强制每批物理重规划」（测试侧直接操作私有缓存，同一池化上下文、analyzed/optimized 均命中缓存，精确隔离物理规划差异）。开发中曾因臂参数（`drop_cache` 布尔）传反得出「缓存更慢」的假阴性，阶段分解计时探针（lookup/build/collect 三相分别计时）定位并纠正——最终数字：aggregate **1.59x**（9.29M vs 5.86M rows/s）、filter-project **1.34x**（5.28M vs 3.93M）。benchmark 三 SQL 场景做端到端佐证。

## Risks / Trade-offs

- [缓存冻结规划期决策] 后续批次行数变化不再影响 join 侧/repartition 选择 → 结果不受影响（纯计划形态），接受；统计来自首建批，与"若该批独立规划"的形态一致。
- [DataFusion 升级破坏 `DataSource` 私有形态] trait 为公开 API，但方法集合随版本演进 → 升级 Arrow/DataFusion 时此处需适配（与 `MemorySourceConfig` 使用同级风险）。
- [定向刷新硬编码「带态节点」类型集合] DataFusion 引入新的带执行态算子时，若未加入 `carries_execution_state` 集合 → 该类查询复用可能 panic 或输出错误。缓解：集合与覆写 `reset_state` 的官方算子对齐（join/排序/repartition 全家族），等价性测试覆盖八种查询形态，且 SQL processor 计划算子域封闭（单表扫描 + 标准 SQL 物理算子）；升级 DataFusion 时此函数是审查点。
- [未知算子携带不可重置态] 同上；EXPLAIN/ANALYZE 覆盖于既有测试。递归查询显式排除。
- [volatile 函数（`random()` 等）被缓存冻结] 物理表达式在每次执行时求值，不缓存求值结果 → 语义不变；仅常量折叠产物（时间字面量）有冻结风险，已由 `time_dependent` 排除覆盖。
- [schema 校验开销] 每批一次 `Schema` 比较（字段元数据逐项）→ 相对每批物理规划（μs~ms 级）可忽略。

## Migration Plan

纯内部优化，无配置/协议变化，直接合入；回滚即恢复 `scan()` 返回 `MemorySourceConfig` 执行计划并移除 `physical` 缓存槽。

## Open Questions

无。

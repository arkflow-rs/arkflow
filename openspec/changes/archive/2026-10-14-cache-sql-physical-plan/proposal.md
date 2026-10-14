## Why

SQL processor（`crates/arkflow-plugin/src/processor/sql.rs`）的热路径上，analyzed/optimized 逻辑计划已按 schema 缓存，但**物理计划每批重建**：`execute_with_cached_plan` 每批调用 `create_physical_plan`（sql.rs:434），其代价（表达式物理化、分区/排序决策、`EnforceDistribution` 规划）在查询固定、schema 不变时是纯重复开销。无法直接缓存现有物理计划的原因：`SwapBatchTable::scan` 在规划期把当前批数据捕获进 `MemorySourceConfig`，返回的计划绑定的是规划那一刻的数据。这是热路径审计（PLANNING.md 10.2）第二批候选中剩余的最重一项，benchmark 三条 SQL 场景（linear/groupby/filter-project）均经过此路径。

## What Changes

- 为 `SwapBatchTable` 实现懒数据扫描源：`scan()` 不再捕获当前批数据，改为返回自定义 `DataSource`（持有共享批槽位 + 规划期 schema + projection），执行期（`open`）才读取换入的当前批并应用投影。
- `ContextPlanCache` 新增 `physical` 槽位：非时间折叠查询（不含 `now`/`current_date`/`current_time`/`current_timestamp`）在 schema 不变时缓存物理计划，后续批次直接 `collect` 缓存计划。
- 时间折叠查询、schema 漂移、temporary 路径行为不变（仍每批重规划）。
- 执行期 schema 一致性防御：扫描源在 `open` 时校验当前批 schema 与规划期 schema 一致，不一致则显式报错（fail-closed，理论不可达——缓存按 schema 键控）。
- 新增 `#[ignore]` release 计时测试（缓存路径 vs 每批物理重规划基线，沿用 protobuf/json timing 测试模式）。

## Capabilities

### New Capabilities
- `sql-physical-plan-cache`: SQL processor 物理计划复用的行为契约——懒扫描源执行期取数、schema 键控缓存、时间折叠查询排除、输出与逐批重规划等价。

### Modified Capabilities
<!-- 无：analyzed/optimized 计划缓存行为不变，无 spec 级修改 -->

## Impact

- 代码：`crates/arkflow-plugin/src/processor/sql.rs`（SwapBatchTable/scan、ContextPlanCache、execute_with_cached_plan、新测试）。无新依赖（`datafusion::datasource` 已有导出）。
- 行为：SQL 查询输出逐字节等价（缓存计划与逐批重规划）；错误路径、取消安全、上下文池语义不变。
- 性能：预期消除每批 `create_physical_plan` 开销（千行小批上占比显著），release 计时测试与 benchmark SQL 三场景验证。
- 文档：无用户可见配置面变化；benchmark 文档不变（不新增场景）。

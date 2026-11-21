## Why

2026-11 代码审查确认三个热路径性能 P1（`CODE-REVIEW-2026-11-15.md` §7）：#25 窗口 journaled 每批深克隆整个 buffers map + 全量 backend scan（`crates/arkflow-core/src/executor/window/operator.rs:2211-2250`，且 :2252 即使无 stale key 也全量 scan）、#26 SQL 处理器每批无条件克隆 SessionState（`crates/arkflow-plugin/src/processor/sql.rs:632`，plan 缓存命中时克隆纯浪费）、#27 JSON 解码每批 schema 推断无缓存（`crates/arkflow-plugin/src/codec/json.rs:75`）。修复它们之前必须先立 benchmark 锚点——审查报告原话"基准盲区：状态路径零覆盖"，没有锚点的优化没有验收标准。

现状盘点（2026-11-21 调查）：仓库自建场景套件（`crates/arkflow-plugin/src/benchmark.rs`，9 场景 + CI 观测工作流）中：

- **#27 已有现成锚点**：`json-decode` 场景（benchmark.rs:241-261）计的就是无缓存推断路径，零成本可用；
- **#26 只有端到端间接覆盖**：linear-sql 场景里 generate/JSON 解码摊薄了 `ctx.state()` 克隆的占比，看不出热路径浪费；
- **#25 完全零覆盖**：窗口 journaled 持久化是三个热路径中唯一没有任何基准观测的，也是代价随键基数无界增长的那个。

## What Changes

- **新增场景 `window-journal-n1` / `window-journal-n100` / `window-journal-n1k`**（#25 锚点）：走生产流路径（compile_stream + run_job，统一内核公开入口），tumbling_window buffer + ephemeral state → 生产路径必然构建 journaled 窗口 operator；键基数 N=1/100/1_000 三档（沿 avro-decode-w5/w25/w100 的宽度矩阵先例），直接可视化每批成本随 N 的增长曲线。
- **新增场景 `sql-hot-cache`**（#26 锚点）：经公共注册表 `ProcessorConfig::build` 构造 sql processor，untimed 预热使 plan 缓存全命中，随后计时同 schema 1000 行批 × K 的稳态——缓存命中路径每批耗时即 `ctx.state()` 克隆浪费的直接度量。
- **新增场景 `json-decode-w200`**（#27 增强）：把既有 `#[ignore]` 计时测试（`component/json.rs:221`）中的宽 200 列负载挪进套件作变体——宽 schema 下推断占比更高，对每批推断成本更灵敏。
- **文档**：`docs/docs/develop/benchmark.md` 场景表补上**漏列的既有 `json-decode` 行**（文档缺陷，本次盘点发现）+ 三个新场景行；zh-Hans 对应页同步。

## Capabilities

### New Capabilities

（无——不新增 capability，全部场景挂进既有 benchmark-suite 套件。）

### Modified Capabilities

- `benchmark-suite`：场景覆盖 requirement 扩展——窗口 journaled 持久化（键基数矩阵）、SQL 缓存命中稳态、宽 JSON 解码变体纳入"至少覆盖"清单。

## Impact

- **代码**：`crates/arkflow-plugin/src/benchmark.rs`（新场景 + run_each 注册）+ 新场景的 smoke 测试
- **文档**：`docs/docs/develop/benchmark.md` 与 zh-Hans 对应页（场景表 4 行：补 1 漏行 + 加 3 新行）
- **CI**：`benchmark.yml` 不改——新场景自动被 main-push 观测覆盖；PR 打 `benchmark` label 可获得修复前后的 artifact 对比（90 天保留）
- **无新依赖、无运行时行为变化**：锚点 change 只读不动三个热路径本体

## Non-goals

- **不修 #25/#26/#27 本体**：修复是后续 change，本 change 只立验收基准。
- **不引入 criterion、不建 benches/ 目录**：沿既有套件模式（决策见 design D1）。
- **不给吞吐数值加通过/失败断言**：既有"报告格式稳定 / CI 非门禁"契约不变。
- **不把 #25 的 in-core operator 级微基准列为本 change 主体**（`StateJournal` 为 `pub(crate)`，插件侧无法直接构造；生产流路径已足够暴露 O(全键) 曲线——core 内 `#[ignore]` 计时测试留作可选增强）。

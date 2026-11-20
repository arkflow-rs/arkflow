## Why

2026-11 代码审查（`CODE-REVIEW-2026-11-15.md` §7）确认三个热路径性能 P1，验收锚点已由 `add-hot-path-benchmark-anchors` 立好（`window-journal-n1/n100/n1k`、`sql-hot-cache`、`json-decode`/`json-decode-w200`），本 change 按锚点逐项消峰。三处共同点：**每批为整批支付与"本批触及数据"无关的状态成本**——

- **#25 窗口 journaled 持久化**（`crates/arkflow-core/src/executor/window/operator.rs:2211-2250`）：每批深克隆**整个** buffers map（仅为本批 dirty 键的 put_compact 与 stale 探测），且**每批**全量 `backend.scan(namespace)` 做 stale 清理——即使本批零删除、零脏键。成本随存活窗口数与后端条目数无界增长，与流量无关。同 spec 既有先例条款（"per-batch snapshot cost SHALL NOT grow with the number of open windows"）已修复补偿快照路径，本 change 收口同一哲学的持久化路径。
- **#26 SQL 处理器**（`crates/arkflow-plugin/src/processor/sql.rs:636`）：`ctx.state()` 每批无条件执行——DataFusion 54.1.0 中 `SessionContext::state()` 返回**整个 SessionState 的深克隆**（`self.state.read().clone()` + `mark_start_execution`，源码核实于 registry），但缓存命中路径（time-dependent 查询本就被 spec 排除在 plan 复用外）只用 task_ctx 执行，克隆纯浪费。
- **#27 JSON 解码**（`crates/arkflow-plugin/src/component/json.rs:35`、`codec/json.rs:75`）：每批全量重跑流式 schema 推断，零缓存——serde 双解析已消除（#1367 前置修复），剩余的推断一遍在宽 schema 下占比最高（`json-decode-w200` 锚点：3.7s vs 窄档 349ms）。

## What Changes

- **#25 持久化增量化**：`persist_buffers_for_fired` 改为 dirty 键**定向查找**（去除整表克隆）；stale 后端扫描**条件化**——仅在"有缓冲被移除（fire 过期删除 / `max_buffered_keys` 驱逐）或 `load_from_backend` 恢复后首次持久化"时执行，其余批次跳过。stale 清理语义保持：被移除键的后端条目仍在下一次持久化时删除，重启不复活已过期窗口。
- **#26 state 克隆移出缓存命中路径**：`execute_with_cached_plan` 的 `ctx.state()` 深克隆仅在 plan 缓存 miss 与时间依赖（不缓存、逐批重优化）路径发生；缓存命中路径直接以既有物理计划 + `task_ctx` 执行。临时表路径（`execute_query_with_statement`）同样处理。行为不变——输出逐字节一致，由既有 spec（时间依赖不缓存、schema 失效即失效）保证。
- **#27 schema 内容哈希缓存**：`try_to_arrow` 增加进程级（per-instance）schema 缓存，键 = 内容哈希 + `fields_to_include` 投影集；命中跳过推断一遍直接解码。**健壮性边界（诚实声明）**：精确字节相同才命中——固定模板/重复 payload 负载收益最大（~消除一遍解析），逐条变化的负载零收益也零回归。差分测试保证命中路径产物与推断路径字节一致。

## Capabilities

### New Capabilities

（无。#27 为纯内部优化，输出字节等价，由既有差分测试体系钉扎，不立新 capability。）

### Modified Capabilities

（无 MODIFIED——两处均为既有契约的**加强**而非变更，用 ADDED 表达：）

- `columnar-window-operators`：ADDED——窗口 journal 持久化的每批成本 SHALL NOT 随存活窗口数或后端条目数增长；stale 扫描 SHALL 仅在缓冲移除/恢复后执行。
- `sql-physical-plan-cache`：ADDED——plan 缓存命中快路径 SHALL NOT 每批深克隆 SessionState。

## Impact

- **代码**：`crates/arkflow-core/src/executor/window/operator.rs`（persist 路径）、`crates/arkflow-plugin/src/processor/sql.rs`（state 获取点）、`crates/arkflow-plugin/src/component/json.rs` + `codec/json.rs`（schema 缓存）
- **验收协议**：每个修复项跑锚点对比——同机同命令 `cargo run -p arkflow --release --example benchmark -- --json`，锚点 commit vs 本 change commit 的 JSON 报告 diff（PR 打 `benchmark` label 由 CI 出对）
- **无新依赖**、无配置面变化、无运行时语义变化（输出/持久化语义逐字节等价）

## Non-goals

- **不动窗口 fire 路径的逐键事务提交成本**（n1k 锚点中 EOF fire 的 O(N) commit 是另一笔账，属 commit 合并/批量优化范畴，另行立项）。
- **不做"单调累积推断器"/"键扫描守卫"等有健壮性风险的 #27 方案**（见 design 拒绝理由）。
- **不改 remote 数据面与 P0 认证**（C 批并入协议 change）。
- **不做 SQL 处理器连接池/表注册语义重构**——仅移动 state 获取点。

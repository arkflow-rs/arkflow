## Why

issue #87「Add a benchmark」+ PLANNING 7.3 方向④：现有 `kernel_perf_baseline.rs` 只有一个 #[ignore] 测试场景，无法作为对外展示的基准。需要一个任何人一条命令即可复现的公开基准，产出稳定格式的报告（人读 markdown + 机器可读 JSON），覆盖内核主路径（线性 SQL、有状态分组、过滤投影）、JSON 编解码与状态后端。

## What Changes

- `arkflow-plugin/src/benchmark.rs`：场景库——线性 SQL 聚合、GROUP BY 有状态聚合、过滤投影、JSON 编解码往返、redb 状态后端读写；统一 warmup + 多次测量（报告 best 与 median），markdown/JSON 两种报告格式。
- `crates/arkflow/examples/benchmark.rs`：公开入口 `cargo run -p arkflow --release --example benchmark [-- --count N --runs N --warmup N --json]`。
- `kernel_perf_baseline.rs` 保留为回归哨兵，头注释指向公开基准。
- 文档：`docs/docs/develop/benchmark.md`（en/zh）：运行方法、场景表、方法学（release 构建、warmup、不设性能断言）。

## Capabilities

### New Capabilities

- `benchmark-suite`: 公开基准的场景集、运行接口与报告格式需求。

## Impact

- `crates/arkflow-plugin/src/benchmark.rs`（新增）、`lib.rs`、`crates/arkflow/examples/benchmark.rs`（新增）
- `docs/docs/develop/benchmark.md` en/zh；PLANNING 方向④更新。

## Non-goals

- 不做跨引擎对比（不做与他家引擎的对比声明，报告只呈现本引擎数据）。
- 不在 CI 中运行基准（编译检查 + tiny smoke 测试即可，避免 CI 抖动）。
- 不做分布式/多节点基准（控制面场景另立项）。

## Why

公开基准套件（`cargo run -p arkflow --release --example benchmark`）目前只能本地手跑，性能回归要靠开发者主动对比两台机器/两个 commit 的报告——实际上没人定期做（本仓库最近的两次 Avro 解码优化共 5-10 倍提升，全靠一次性人工基准发现成本结构）。把基准挂进 CI 并把结果发布为**可观测产物**，每次合并/PR 都自动留一个数据点，回归在评审时即可被看到。`benchmark-suite` spec 明确禁止对吞吐做通过/失败断言（CI 不因机器速度失败），因此本变更是**观测而非门禁**：数字进 job summary 与 artifact，判断留给人。

## What Changes

- 新增 `.github/workflows/benchmark.yml`：push 到 main（每次合并一个数据点）+ `workflow_dispatch`（手动）+ PR 打 `benchmark` 标签（性能相关 PR 按需触发，避免每个 PR 都付 ~15-30 分钟 release 构建成本）。release 构建跑全场景，markdown 表写入 GitHub Actions job summary（`$GITHUB_STEP_SUMMARY`），JSON 报告上传为 workflow artifact（保留 90 天供跨 run 对比）。
- 不加入 required checks、不断言任何吞吐阈值——`benchmark-suite` 的"SHALL NOT 断言"契约保持不变。
- `docs/docs/develop/benchmark.md` 与 zh-Hans 对应页补 CI 触发方式说明。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `benchmark-suite`: 新增一条 requirement——CI SHALL 在 main push/手动/PR 标签触发时运行基准并把结果作为 job summary 与 artifact 发布，且 SHALL NOT 作为合并门禁或对数值断言。

## Impact

- `.github/workflows/benchmark.yml`（新文件）：沿用 rust.yml 的 toolchain/protoc/rust-cache 约定（release profile 下 rust-cache 仍缓存依赖编译，workspace 重编 + LTO 约 15-30 分钟）。
- `docs/docs/develop/benchmark.md` + zh-Hans 对应页：触发方式一节。
- 零代码改动、零运行时影响。

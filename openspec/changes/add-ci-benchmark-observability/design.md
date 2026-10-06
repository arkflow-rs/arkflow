## Context

基准套件现状：本地命令入口（example benchmark），spec 明文禁止 CI 对吞吐断言（机器差异）。CI 现状：rust.yml（debug 快速门禁）、release.yml（tag 出包）。GitHub hosted runner 2 核、跨代际硬件，绝对数值不可跨 run 严格比较，但同一 run 的场景间比例与数量级（如最近 4.7-8.7 倍的列式提升）足够稳定，可观测。

## Goals / Non-Goals

**Goals:**

- 每次合并到 main 自动产出一个基准数据点，PR 可按需触发；结果直接可读（job summary 表格）且可跨 run 取回（JSON artifact）。
- 不增加每个 PR 的固定 CI 时长（release 构建贵，PR 侧按标签 opt-in）。

**Non-Goals:**

- 任何吞吐阈值断言或合并门禁（spec 契约）。
- 基准趋势数据库/外部服务（bencher/critcmp 类，待真实需求再立项）。
- 专用固定硬件 runner（自建 runner 属运维决策）。

## Decisions

1. **观测三通道：push(main) + workflow_dispatch + PR 标签 `benchmark`。** 备选：每个 PR 自动跑（被否——release + LTO 构建约 15-30 分钟，全员常态付出太贵）；仅 nightly（被否——PR 评审期看不到数据点，"每次观测"落空）。PR 触发用 `pull_request.types: [labeled]` + `if: github.event.label.name == 'benchmark'`，与 `synchronize` 重跑解耦：标签存在时 push 新 commit 也会跑（`contains(github.event.pull_request.labels.*.name, 'benchmark')`）。
2. **结果发布：markdown 表 append 到 `$GITHUB_STEP_SUMMARY`**（run 页面直接看，零额外权限）+ JSON 上传 artifact（`actions/upload-artifact`，retention 90 天，文件名含 short SHA 便于跨 run 对比）。
3. **构建沿用 rust.yml 约定**：dtolnay toolchain、apt 装 protoc、`Swatinem/rust-cache@v2`（release profile 下仍有效缓存依赖编译，key 含 profile 变化）。
4. **job 不进 required checks、`timeout-minutes: 60` 兜底**；基准二进制失败即 job 失败（构建/崩溃是真信号），但吞吐数值永不影响结论。

## Risks / Trade-offs

- [runner 噪声让绝对值波动] → 文档明示跨 run 对比仅供参考、同 run 场景比例更稳；不做任何自动断言。
- [release 构建时长] → rust-cache + 仅触发面受控；worst case 60 分钟超时兜底，不阻塞合并。
- [artifact 90 天过期] → 趋势回看窗口有限；足够发现近期回归，长期趋势库留 Non-goal。

## Migration Plan

纯新增 workflow，合入即生效；无迁移、无回滚成本（删除文件即回滚）。

## Open Questions

（无。）

## ADDED Requirements

### Requirement: CI 基准可观测且不做门禁

CI SHALL 提供基准工作流：push 到 main、手动触发（workflow_dispatch）、以及 PR 携带 `benchmark` 标签时，以 release 构建运行全套基准场景，并把 markdown 报告写入 workflow 的 job summary、把 JSON 报告上传为 artifact（文件名含提交短 SHA，保留期不少于 30 天）。该工作流 SHALL NOT 出现在必需检查（required checks）中，SHALL NOT 对任何吞吐数值做通过/失败断言（既有"报告格式稳定"契约不变）；构建失败或基准崩溃 SHALL 如实反映为 job 失败。

#### Scenario: main 合并自动产出数据点

- **WHEN** 一个提交合并到 main
- **THEN** 基准工作流以 release 模式运行全部场景，run 页面的 job summary 展示 markdown 表格，且可下载含该提交短 SHA 的 JSON 报告 artifact

#### Scenario: PR 标签按需触发

- **WHEN** 一个 PR 被打上 `benchmark` 标签（或带该标签的 PR 收到新推送）
- **THEN** 基准工作流在该 PR 上运行并发布同格式的 summary 与 artifact；未带标签的 PR SHALL NOT 触发

#### Scenario: 数值波动不影响结论

- **WHEN** 某次运行的吞吐数值显著低于历史 run
- **THEN** 工作流结论不受影响（不因机器速度失败）；是否构成回归由人对比报告判断

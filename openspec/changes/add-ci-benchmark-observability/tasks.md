## 1. CI 工作流

- [x] 1.1 新增 `.github/workflows/benchmark.yml`：触发 push(main)/workflow_dispatch/PR `benchmark` 标签（labeled + synchronize 带标签重跑）；单 job ubuntu-latest，toolchain/protoc/rust-cache 沿用 rust.yml 约定，`--release` 构建并运行 `cargo run -p arkflow --release --example benchmark -- --json`（双报告：stdout markdown + JSON 文件），`timeout-minutes: 60`
- [x] 1.2 结果发布：markdown 表 append 到 `$GITHUB_STEP_SUMMARY`（含触发提交短 SHA 与免责说明——跨 run 对比仅供参考）；JSON 上传 artifact（名含 short SHA，retention-days: 90）
- [x] 1.3 本地 YAML 校验（语法/动作版本对齐 rust.yml），确认 job 命名不会混入 required checks——js-yaml 校验通过 + summary 渲染脚本按 Actions 的缩进剥离规则端到端模拟验证（markdown 表格输出正确）

## 2. 文档与收尾

- [x] 2.1 `docs/docs/develop/benchmark.md` + zh-Hans 对应页：补"CI 自动运行"一节（触发条件、在哪看结果、对比建议）
- [x] 2.2 门禁：`pnpm docs:check`；openspec validate
- [x] 2.3 提交并开 PR，观察首次 workflow 运行（workflow_dispatch 或标签路径视仓库权限而定）

# Proposal: update-deps-security-node-1

## Why

dependabot 在 Node 侧报出 35 条告警（docs/pnpm-lock.yaml 32 条，含 shell-quote / websocket-driver 两个 **critical**；console/package-lock.json 3 条），全部为文档站与控制台的构建期/开发期传递依赖。分诊结论（对照各 GHSA 的 vulnerable range 与锁内实际版本）：29 个包可经范围内更新收敛；`lodash-es` 4.17.23 与 `qs` 6.14.2 为同 major 滞留（修复版在同 major 内，但依赖方钉住了旧 minor），可用 pnpm overrides 安全强推；其余 4 个为跨 major 或 RC-only（见 Non-goals）。两个 critical（shell-quote #161/#163、websocket-driver #167）经 `docs/pnpm-lock.yaml` 的范围内更新直接收敛。

## What Changes

- `docs/pnpm-lock.yaml`：对 31 个告警包执行 `pnpm update`（范围内），并新增 `docs/package.json` 的 `pnpm.overrides`（`lodash-es: ^4.18.0`、`qs: ^6.16.0`，同 major 强推）。关键落点：shell-quote 1.12.0、websocket-driver 0.7.5、js-yaml 3.15.2/4.3.2、fast-uri 3.1.8 等。
- `console/package.json`：devDependency `vitest` 4.1.10 → `^4.1.11`（medium 告警 #170/#166 的唯一修复版）；`console/package-lock.json` 以 npm 11 重新生成（npm 10.9.2 在该依赖图上有 `edgesOut` 崩溃 bug）。
- 不改任何应用代码；docs 双语言构建、docs:check 门禁、console tsc+vite 构建与 61 个 vitest 用例全部保持通过。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `documentation-quality-gates`: 新增一条 requirement——文档站依赖刷新（含 pnpm overrides）SHALL 保持构建产物与质量门禁契约不变（双语言构建成功、docs:check 通过、示例/清单校验不受影响）。
- `control-console`: 新增一条 requirement——console 开发依赖升级 SHALL 保持构建与测试契约不变（tsc -b && vite build 成功、vitest 全过），且 lockfile 再生成 SHALL 不改变运行时依赖面（仅 devDependencies 变动）。

## Impact

- 依赖清单：`docs/package.json`（+pnpm.overrides）、`docs/pnpm-lock.yaml`、`console/package.json`（vitest devDep）、`console/package-lock.json`。
- 验证面：Docusaurus 双语言构建、`pnpm docs:check`、console `npm run build` + `npm test`；CI 的 Build Docusaurus / docs-check / console 相关 job。
- 零 Rust 侧改动；零配置 schema 改动；无用户可见行为变化（均为构建期依赖）。

## Non-goals

- 不处理 4 个跨 major/RC 滞留包（留残记录，等上游依赖链自然收敛）：`serialize-javascript` 6.0.2（修复在 7.0.3+，webpack 工具链钉 6.x）、`minimatch` 3.1.2（修复在 10.2.x，legacy glob 链）、`uuid` 8.3.2（修复在 12.0.1+，8→9 跨 ESM 断裂）、`@babel/core`（唯一修复版 8.0.0-rc.6 非 GA）。
- 不为 console 迁移包管理器（保持 npm + package-lock.json；npm 11 仅本地生成工具，CI 不受影响）。
- 不升级 Docusaurus（3.9.2）或 Vite（8.2.0）等直接依赖的大版本。
- 不顺手做与本批安全收敛无关的 lockfile 全量刷新。

# Design: update-deps-security-node-1

## Context

dependabot Node 侧 35 条告警的收敟能力由两个事实决定：(1) 修复版是否在依赖方声明的 semver 范围内——在则 `pnpm update` 直接收敛；(2) 不在范围内时，若修复版与被钉版本**同 major**，pnpm overrides 可零风险强推；跨 major 则强行覆盖会破坏依赖方对旧 API 的预期（minimatch 3→10、uuid 8→12 ESM、serialize-javascript 6→7），只能等上游链条升级。console 侧的额外障碍：npm 10.9.2 在 console 依赖图上稳定复现 `Cannot read properties of null (reading 'edgesOut')`（连删除 node_modules 与 lock 后全新解析也崩溃），npm 11 一次通过——判定为 npm 自身 bug，与本仓库依赖无关。

## Goals / Non-Goals

**Goals:**
- 关闭两个 critical（shell-quote、websocket-driver）与所有同 major 可达的 high/medium 告警（预计 dependabot 重扫描后 35 条中关闭约 31 条）。
- 保持 docs 双语言构建、docs:check、console 构建/测试全绿。
- 改动仅限 lockfile 与两处 manifest 微调（pnpm.overrides、vitest devDep）。

**Non-Goals:**
- 跨 major 滞留包的强行覆盖（见 proposal Non-goals 清单）。
- 直接依赖大版本升级（Docusaurus/Vite/vitest 主版本）。
- Rust 侧任何改动。

## Decisions

1. **docs 用定向 `pnpm update <包名清单>` 而非全量 lock 重建**：仅推动 31 个告警包及其邻接解析，diff 集中（670+/638-），不引入无关漂移；全量重建会把 1300+ 包全部重解析，回归面不可控。
2. **overrides 只用于同 major 滞留（lodash-es、qs）**：同 major 语义兼容是 semver 契约保证的，覆盖风险为零；这是 overrides 的安全使用边界，跨 major 一律不覆。
3. **console lock 重建而非增量 patch**：npm 10 的 edgesOut 崩溃使增量路径不可用；npm 11 全量再生成后的锁与原锁仅 devDependencies 邻域差异（261 包，nanoid 单拷贝 3.3.19）。`package.json` 显式钉 `vitest: ^4.1.11` 保证再生成锚定修复版。
4. **告警闭环以 dependabot 重扫描为准，本地以版本-范围对照表预核**：镜像源无 audit 端点，本地用告警 JSON 的 vulnerable_range × 锁内版本逐条比对（已做，剩 4 包跨 major 滞留 + console nanoid 告警为陈旧——锁内仅 3.3.19，不在 `>=4.0.0` 范围内）。
5. **不启用 pnpm 的 auditConfig/更多治理开关**：保持变更最小，治理机制另行立项。

## Risks / Trade-offs

- **overrides 是长期占用**：lodash-es/qs 的覆盖会持续生效，若未来某依赖方需要旧 minor 语义（ semver 允许的破坏极罕见）需人工复核。在 package.json 内联注释不可行（JSON），由 proposal/design 记录动机。
- **console 锁全量再生成的 diff 较大**：以构建 + 61 用例 + 人工抽查关键包版本兜底；CI 的 console job 再验证一轮。
- **npm 11 与 CI 内 npm 版本不一致**：lockfile v3 格式两者兼容；CI 仅消费锁不重新解析，不受影响。
- **4 个滞留包的告警不会清零**：PR 描述预先列出清单与原因，避免误判为遗漏；revisit 条件为上游依赖链（webpack 工具链 / legacy glob / uuid 消费方 / babel 8 GA）升级。

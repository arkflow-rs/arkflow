## 1. Lockfile convergence

- [x] 1.1 在分支 `deps/security-node-1` 上：docs 侧对 31 个告警包执行定向 `pnpm update`；`docs/package.json` 增加 `pnpm.overrides`（`lodash-es: ^4.18.0`、`qs: ^6.16.0`）并 `pnpm install` 落锁
- [x] 1.2 console 侧：`package.json` 的 vitest devDep 钉 `^4.1.11`，以 npm 11 重新生成 `package-lock.json`（npm 10.9.2 edgesOut 崩溃的绕行）
- [x] 1.3 版本-范围对照核验：以 dependabot 告警 JSON 的 vulnerable range × 新锁内版本逐条比对，确认仅剩 4 个跨 major/RC 滞留包（serialize-javascript、minimatch、uuid 8.3.2、@babel/core）与 1 条陈旧告警（console nanoid：锁内 3.3.19 不在 `>=4.0.0` 范围）

## 2. Verification against the spec contracts

- [x] 2.1 docs：双语言生产构建成功（en + zh-Hans）；`pnpm docs:check` 全过（140 页、49 清单项、README 对照）
- [x] 2.2 console：`npm run build`（tsc -b && vite build）成功；`npm test` 61/61 通过
- [x] 2.3 运行时依赖面核验：console 前后锁的 `dependencies`（非 dev）解析不变；Rust 侧零改动确认（本变更不触碰 Cargo 文件）

## 3. Docs and ship

- [ ] 3.1 PR 描述携带：告警收敛表（35 条 → 预计关闭 ~31 条）、两个 critical 的落点版本、4 个滞留包的原因与 revisit 条件、npm 11 绕行说明
- [ ] 3.2 提交、开 PR、CI 绿后落地；归档本变更（delta 同步进 `documentation-quality-gates` 与 `control-console`）

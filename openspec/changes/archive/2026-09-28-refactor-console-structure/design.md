## Context

`console/src/features.tsx`（883 行）是早期单文件页面的遗留物，后续功能（Jobs、Rollouts、Audit、JobEditor、ComponentBrowser）已按 `features/` 目录约定拆出，形成新旧两代并存。当前外部调用方仅两处：`console/src/app.tsx:13` 与 `console/src/features.test.tsx:3`。行为级测试覆盖充分（features.test.tsx 约 702 行 + app.test.tsx 约 366 行），为纯移动重构提供了安全网。`app.tsx:33` 的 `pageFromLocation` 存在解构 bug，深链 `?page=` 恒回落 Overview。

本变更是控制台重构路线图（记忆 `console-refactor-roadmap`）的阶段①，数据层与路由层重构明确留待阶段②。

## Goals / Non-Goals

**Goals:**

- `features.tsx` 内的 6 个页面 + 共享组件按既有 `features/` 约定拆分，删除原文件。
- 修复 `?page=` 深链/刷新回落 Overview 的 bug。
- 全程零行为变化（除 bug 修复），现有测试仅改 import 路径。

**Non-Goals:**

- 不统一数据获取模式、不引入路由库、不变更 CSS（阶段②）。
- 不重构 `features/job-editor.tsx`。
- 不引入 barrel/兼容导出。

## Decisions

### D1: 拆分映射——逐字节移动，不顺手改进

| 源（features.tsx 行号） | 目标 |
|---|---|
| Snapshot/Command 类型（:24-37） | `features/types.ts` |
| Overview（:59-212） | `features/overview.tsx` |
| Runtime + RuntimeDetail（:213-460） | `features/runtime.tsx` |
| Configuration + convertConfiguration（:46-57, :461-662） | `features/configuration.tsx` |
| Components（:663-738） | `features/components.tsx` |
| Events + EventRow（:739-775, :837-852） | `features/events.tsx` |
| Settings（:776-802） | `features/settings.tsx` |
| OperationRow（:803-836） | `features/shared.tsx` |
| Pagination、Card、`number`、`active`（:41-44, :853-883） | `features/shared.tsx` |

理由：Alternative "新建 `ui/` 目录放共享件" 被否——共享件目前只有 4 个小组件，单文件足够，目录级抽象等阶段②组件层方案定了再说。移动时保持代码逐字节一致（含注释），以便 git rename 检测保留 blame 历史。

### D2: 删除 features.tsx，不留 barrel

调用方仅 2 处且全在自己手里，barrel 只会延续新旧并存的困惑。`app.tsx` 与 `features.test.tsx` 直接改为按文件导入。Alternative "保留 features.tsx 作 re-export 门面" 被否——违反最小 API 原则，且本仓库惯例（参照 `add-console-i18n`）不留兼容垫片。

### D3: bug 修复取最小 diff

`app.tsx:33` 的 `PAGES.some(([key]) => key === value)` 改为 `PAGES.some((p) => p === value)`——仅替换解构为直接参数，类型完全安全，语义即"成员判断"。Alternative `(PAGES as readonly string[]).includes(value)` 需要类型断言；阶段②引入真路由时该函数可能整体消亡，不值得现在做更大改动。

### D4: 每个新文件自含 import，不跨页面互相依赖

页面之间唯一的横向引用是共享件（shared.tsx）与类型（types.ts），页面文件彼此零依赖。这保证阶段②按页改造（如逐页迁移到 server-state）时互不牵连。

## Risks / Trade-offs

- [移动时无意改动代码，引入回归] → 纪律：纯移动 + 逐字节一致；全量 `npm test` + `typecheck` + `build` 通过为准；diff 审查以"目标文件内容 = 原文件切片 + import 头"为标准。
- [git blame 断裂] → 保持逐字节移动使 rename 检测生效；必要时拆"纯移动"与"bug 修复"两个 commit。
- [测试断言隐式依赖模块结构（如 vi.mock 路径）] → 迁移前先核对 features.test.tsx / app.test.tsx 的 mock 目标路径，随 import 一并更新。

## Migration Plan

单 PR 完成，无部署顺序问题（前端资产随控制台整体构建）。回滚 = revert PR。

1. 创建 `features/types.ts`、`features/shared.tsx` 及 6 个页面文件（纯移动 + import 头）。
2. 更新 `app.tsx`、`features.test.tsx` 导入路径，同步修复 `pageFromLocation`。
3. 删除 `features.tsx`。
4. `npm test && npm run typecheck && npm run build` 全绿。

## Open Questions

（无——数据层/路由层归属阶段②，已在 proposal Non-goals 锁定。）

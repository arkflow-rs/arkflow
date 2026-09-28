## Why

控制面前端的结构拆分只做了一半：`console/src/features.tsx`（883 行）仍是遗留巨石，同时容纳 6 个页面（Overview `:59`、Runtime `:213` + RuntimeDetail `:373`、Configuration `:461`、Components `:663`、Events `:739`、Settings `:776`）和共享组件（OperationRow `:803`、EventRow `:837`、Pagination `:853`、Card `:875`）以及格式化助手；而 Jobs、Rollouts、Audit、ComponentBrowser、JobEditor 已迁入 `features/` 目录，新旧两代组织方式并存。同时 `console/src/app.tsx:33` 存在真 bug：`PAGES.some(([key]) => key === value)` 对字符串数组做 `([key])` 解构取出的是首字符，断言恒为 false——**携带 `?page=jobs` 等深链打开或刷新页面时永远回落到 Overview**，URL 页面状态恢复完全失效，违背 `control-plane-console` spec "Operations application shell" 中 persistent navigation 的意图。当前调用方仅 `app.tsx:13` 与 `features.test.tsx:3` 两处，拆分窗口成本最低。

## What Changes

- 拆分 `console/src/features.tsx` 为 `features/` 目录下按页面组织的模块：`overview.tsx`、`runtime.tsx`、`configuration.tsx`、`components.tsx`、`events.tsx`、`settings.tsx`。
- 共享 UI 件与助手（Card、Pagination、OperationRow、EventRow、`number`、`active`）归拢到 `features/shared.tsx`；`Snapshot`/`Command` 类型移出为独立类型模块。
- `convertConfiguration` 随 Configuration 页面迁移。
- 删除 `features.tsx`，所有调用方（`app.tsx`、`features.test.tsx`）直接从新路径导入，**不留 barrel 兼容层**。
- 修复 `app.tsx` `pageFromLocation` 的解构 bug，使 `?page=` 深链与刷新正确恢复页面。
- 除该 bug 修复外零用户可见行为变化；现有行为级测试（features.test.tsx 702 行 + app.test.tsx 366 行）作为回归安全网。

## Capabilities

### New Capabilities

（无——本变更是纯内部结构重构，不引入新能力。）

### Modified Capabilities

- `control-plane-console`: "Operations application shell" 需求补充 URL 页面状态恢复的场景——深链或刷新时 SHALL 恢复 `?page=` 指向的页面（现状实现违反该意图，delta spec 将其显式化为可验证要求）。

## Impact

- `console/src/features.tsx` — 删除（883 行代码按上述地图迁移）
- `console/src/features/` — 新增 overview/runtime/configuration/components/events/settings/shared 及类型模块
- `console/src/app.tsx` — import 路径更新 + `pageFromLocation` 一行修复
- `console/src/features.test.tsx`、`console/src/app.test.tsx` — 仅 import 路径更新
- 无 npm 依赖变化，无后端/API 影响，无文档行为描述变化

## Non-goals

- 不统一数据获取模式（App 全局 snapshot 与页面自取双轨并存——归路线图阶段② server-state 层处理）。
- 不引入路由库或 hash 路由（归阶段②；本变更仅修 `?page=` 解析 bug）。
- 不变更 CSS 组织与视觉样式（归阶段②/③）。
- 不重构 `features/job-editor.tsx`（955 行，行为敏感，另行评估）。
- 不引入 barrel/兼容导出层。

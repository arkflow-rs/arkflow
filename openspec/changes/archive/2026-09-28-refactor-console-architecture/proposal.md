## Why

控制台的 URL 状态与数据层仍停留在阶段①之前（提案 `refactor-console-structure`，PR #1262）的形态：页面切换依赖 `?page=` 查询参数模拟路由（`app.tsx` 手写 `pageFromLocation`/`syncLocation`，浏览器返回键不可用）；数据获取双轨并行——App 顶层一次性 `Promise.all` 拉 8 个接口做 30s 全量轮询 + props 层层下发（`app.tsx:76-110`），Configuration/Rollouts/Audit/Components 又各自 fetch，两条路径的行为（轮询节奏、错误处理、SSE 响应）互不一致。每加一个页面都要在 snapshot 流水线里再穿一根 props。官方部署的 `console/nginx.conf:5` 已含 SPA fallback，路由升级的服务端条件已就绪。

本变更按用户选型（2026-09-28）引入 react-router 与 TanStack Query 承接路由与 server-state：路由边界、竞态/轮询/失效交给成熟方案，控制台获得真路径深链、返回键、按页面粒度的数据获取与失效。

## What Changes

- 引入 `react-router@^7.18.4`（Declarative Mode：BrowserRouter/Routes/NavLink；v8 要求 React ≥ 19.2.7，项目固定 React 18.3.1，故 pin v7），路由表：`/`、`/runtime`、`/jobs`、`/configuration`、`/rollouts`、`/components`、`/events`、`/audit`、`/settings`；`node_id` 保持为查询参数（横切筛选器）。
- 旧 `?page=jobs` 链接重定向到 `/jobs`（保留 node_id），书签兼容；删除 `pageFromLocation`/`syncLocation`/`PAGES` 手写路由。
- 引入 `@tanstack/react-query@^5`：App 顶层挂 `QueryClientProvider`；Overview/Runtime/Jobs/Events/Settings 的数据从全局 snapshot 迁为各自 `useQuery`（轮询节奏对齐现状：snapshot 资源 30s、Job 详情 5s、Rollout 详情 5s）；Configuration/Rollouts/Audit/Components 的自取 fetch 同样迁为 `useQuery`（保持无轮询现状）。
- 全局 snapshot（`Promise.all` 8 接口 + props 下发）删除；SSE 事件改为防抖 `invalidateQueries`（对齐 `REFRESH_DEBOUNCE_MS`），仅作用于 live 资源，避免打断 Configuration 草稿编辑状态。
- 节点筛选（`node_id`）进入 queryKey，切换节点自动重拉；"陈旧数据保留最后快照 + 重试"的语义由 `keepPreviousData` + `isError` 承接。
- 测试更新：App 以 provider 组合（沿用 LocaleProvider 先例），深链测试改为路径形式并新增 `?page=` 重定向用例。

## Capabilities

### New Capabilities

（无——不引入新能力。）

### Modified Capabilities

- `control-plane-console`: "Operations application shell" 需求的 URL 状态部分升级为路径式路由——页面 SHALL 以路径（如 `/jobs`）寻址并支持返回键/前进；`?page=` 旧链接 SHALL 重定向到对应路径。轮询与 SSE 失效的页面级粒度一并显式化。

## Impact

- `console/package.json` — 新增 `react-router`、`@tanstack/react-query` 两个运行时依赖
- `console/src/app.tsx` — 重写为纯 shell（路由出口 + SSE + 节点/语言选择器 + 错误横幅）
- `console/src/features/*.tsx` — 各页面数据获取改造（接口不变，仅消费方式变化）
- `console/src/app.test.tsx`、`console/src/features.test.tsx` — provider 组合与 URL 用例更新
- 依赖前置：PR #1262（阶段①）先合入——本变更基于 `features/` 拆分后的文件布局
- 服务端/部署无需变更（nginx fallback 已存在）

## Non-goals

- 不改 CSS 组织与视觉样式（归阶段③）。
- 不重构 `features/job-editor.tsx` 内部（其数据获取可接入 query，但编辑器逻辑不动）。
- 不新增页面、不改任何 API 端点契约。
- 不做 `?page=` 之外的历史 URL 兼容（该方案从未真正工作过，无存量深链）。

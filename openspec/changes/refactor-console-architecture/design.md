## Context

阶段①（PR #1262）完成后，`features/` 目录已是按页面组织的模块，但两根架构支柱仍是过渡态：路由靠 `?page=` 查询参数 + 手写同步函数，数据靠 App 顶层 snapshot（8 接口 Promise.all、30s 轮询、props 下发）与页面自取并存。用户已选定引入 react-router（Declarative Mode）与 TanStack Query（v5）——见记忆 `console-infra-library-preference`。官方 nginx 配置含 SPA fallback（`console/nginx.conf:5`），`?page=` 深链因阶段①修复虽可用但无路径语义、无返回键支持。

行为基线（必须保持）：轮询节奏（snapshot 30s / Job·Rollout 详情 5s / 其余不轮询）、SSE 驱动的防抖刷新（1.5s）、陈旧态横幅 + 最后快照保留、错误横幅、节点筛选贯通所有请求、`canMutate` 门控。

## Goals / Non-Goals

**Goals:**

- 真路径路由：9 个平铺页面、`?page=` 兼容重定向、返回键/前进可用、NavLink 激活态。
- 页面粒度的数据获取：每页 `useQuery` 自取，key 携带 `node_id`；删除全局 snapshot 与 props 数据管道。
- SSE 失效精确化：只失效 live 资源，不打断 Configuration 草稿等本地编辑态。
- 现有行为测试语义不变（URL 用例改为路径形式）。

**Non-Goals:**

- CSS/视觉（阶段③）、job-editor 内部逻辑、API 契约变更、新页面。

## Decisions

### D1: 路由 = react-router v7.18.4 Declarative Mode

（实现期修订：原定 v8.4.0，但 v8 peer 依赖要求 React ≥ 19.2.7，项目在 React 18.3.1；按设计中预设的兜底 pin 到 v7.18.4——同 API 面、`react >= 18`。React 19 升级不在本变更范围。）

`<BrowserRouter>` + `<Routes>`/`<Route>` + `<NavLink>`；不用 Framework/Data Mode（无 SSR、无 loader 需求，SPA 经反代静态托管）。路由表成为 PAGES 的唯一来源（替代 `PAGES` 常量 + `pageFromLocation` + `syncLocation`，三处手写代码删除）。

| 路径 | 页面组件 |
|---|---|
| `/` | Overview |
| `/runtime` `?node_id=` | Runtime |
| `/jobs` `?node_id=` | Jobs |
| `/configuration` `?node_id=` | Configuration |
| `/rollouts` `?node_id=` | Rollouts |
| `/components` | Components |
| `/events` `?node_id=` | Events |
| `/audit` | Audit |
| `/settings` | Settings |

- `node_id` 是横切筛选器不是路由 → 保持查询参数，`useSearchParams` 读写；切换节点不改变路径。
- 旧链兼容：根路由组件检测 `?page=` 参数 → `<Navigate to={`/${page}`} replace>` 保留其余查询参数；无参数时渲染 Overview。
- Alternative「hash 路由」被否：nginx fallback 已就位，无兼容收益且 URL 带 `#`。

### D2: server-state = TanStack Query v5，live 资源与静态资源两类 key

`QueryClientProvider` 挂在 App 根（与 LocaleProvider 并列，测试可整体包裹）。

**key 约定（前缀区分失效范围）：**

```
live 资源（SSE/轮询驱动）                  静态资源（挂载时取，不轮询）
['live','system']                          ['config', nodeId]
['live','nodes']                           ['config-versions', nodeId]
['live','streams', nodeId]                 ['components']
['live','jobs']                            ['audit', filters]
['live','operations', nodeId]
['live','events', nodeId]
['live','metrics', nodeId]
['live','job-detail', jobId]
['live','rollout-detail', rolloutId]
```

- SSE `streamEvents` 留在 App shell：事件回调防抖 1.5s 后 `queryClient.invalidateQueries({ queryKey: ['live'] })`（前缀匹配），一次失效覆盖所有挂载中的 live 查询；`config-*`/`audit`/`components` 不受影响，**编辑器草稿状态不会被重取打断**。
- 轮询：`refetchInterval` 对齐现状——live snapshot 类 30_000、`job-detail`/`rollout-detail` 5_000、静态资源不设。
- 陈旧语义（spec 场景"marks data stale, preserves the last safe snapshot"）：live 查询统一 `placeholderData: keepPreviousData`，`isError` 时页面顶部横幅 + 重试按钮（`refetch`），旧数据继续展示——与现状 `stale` 标志行为一致。
- 错误横幅通道：保留 session 级 React Context（`onError` 上抛），mutation 失败与 query `isError` 都走它。
- `waitForOperation` 保留在 api 层；操作完成后按资源类型 `invalidateQueries`（替代 `refresh()`/`onOperationChanged` 回调链）。
- `canMutate` 门控：由 `['live','nodes']` 派生，留在 shell 计算后传 props（纯派生值，不进 query）。

### D3: 迁移顺序 = 先立 Provider 骨架，再逐页搬家

1. 安装依赖、挂 Router + QueryClientProvider（App 组合 provider，测试沿用 `LocaleProvider` 先例整体包裹）。
2. 路由切换：nav 改 `<NavLink>`、页面挂到 `<Route>`、`?page=` 重定向；删除手写路由代码。
3. 逐页迁移数据：Overview → Runtime → Events → Settings → Jobs（含 job-detail 轮询）→ Rollouts（含 detail 轮询）→ Configuration → Components → Audit。每页一个小步提交，页面行为测试随迁。
4. 删除 snapshot 类型/管道与 `onRefresh`/`onNodesChanged` 类回调。

每步 `npm test` 全绿后才进下一步；中途不追求半成品可用。

### D4: 测试策略

- App 测试继续用 jsdom 真实 `window.history`（现测试已操作 `window.location`，与 BrowserRouter 兼容）；`?page=` 深链用例改为 `/?page=jobs` → 断言落在 Jobs 内容（测重定向），新增 `/jobs` 直达用例。
- 现有 mock fetch 方式不变；TanStack Query 对 mocked fetch 透明。轮询用例用 `vi.useFakeTimers` 或断言"首次拉取 + invalidate 后重拉"，不真等 30s。

## Risks / Trade-offs

- [Configuration 页 query 化后 SSE/重取打断草稿编辑] → D2 的 key 前缀隔离：`config` 不在 `['live']` 前缀下、不设 refetchInterval、`staleTime: Infinity`；仅 `nodeId` 变化才重取（与现状一致）。迁移该页时专项验证"验证中/未保存草稿在 SSE 事件后不丢失"。
- [已按 D1 修订 pin v7.18.4（v8 peer 依赖 React ≥ 19.2.7）] → 只用 Declarative Mode 稳定 API 面（v6 起未变）；待项目升级 React 19 后可零成本升 v8。
- [bundle 增大（router+query ≈ 20KB gz）] → 当前 509KB（minified），占比小；阶段③做代码分割时 JobEditor（xyflow）才是大头，本次不做分割。
- [逐页迁移期间新旧两套并存] → D3 顺序保证每步全绿；snapshot 管道在最后一个页面迁完后才删除，不出现真空期。
- [SSE 断连重连期间 invalidate 空转] → 保持现状语义：连接状态徽标独立展示，invalidate 不做连接感知。

## Migration Plan

单 PR（基于 #1262 合并后的 main），服务端零变更。回滚 = revert PR（无数据迁移、无 API 变更）。合并后观察点：Configuration 草稿在 live 事件后保持、节点切换重拉、返回键导航。

## Open Questions

（无——选型与 URL 形态已由用户确认，其余为本设计内决策。）

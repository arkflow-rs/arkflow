## 1. 依赖与 Provider 骨架

- [x] 1.1 `npm i react-router@^8.4.0 @tanstack/react-query@^5.104.0`；验证 v8 Declarative Mode 导入路径（`BrowserRouter`/`Routes`/`NavLink`/`useSearchParams` 来自 `react-router`）与 TS 类型
- [x] 1.2 App 根组合 `QueryClientProvider` + Router（与 LocaleProvider 并列），`QueryClient` 默认配置（`retry: 1`、`refetchOnWindowFocus: false`）；测试 helper 可整体包裹
- [x] 1.3 `npm test` 全绿（此步尚无行为变化）

## 2. 路由切换

- [x] 2.1 建路由表（9 页路径 + 根路由 `?page=` 重定向组件，保留其余查询参数）；nav 改 `<NavLink>` 激活态
- [x] 2.2 删除 `PAGES`/`pageFromLocation`/`syncLocation`/`goTo`，节点选择改 `useSearchParams`
- [x] 2.3 更新 app.test.tsx：URL 用例改路径形式；新增 `/jobs` 直达与 `/?page=jobs` 重定向用例

## 3. live 资源迁移（每页一步，迁移后 npm test 全绿再进下一页）

- [x] 3.1 Overview：`['live','system'|'nodes'|'streams'|'operations'|'events'|'metrics']` 组合查询替代 snapshot props
- [x] 3.2 Runtime：streams/operations/events 查询 + 30s `refetchInterval` + `keepPreviousData` 陈旧横幅
- [x] 3.3 Events：events 查询 + 分页总数（total）语义保持
- [x] 3.4 Settings：status 查询
- [x] 3.5 Jobs：列表查询迁移；`job-detail` 改 `refetchInterval: 5_000`（删除页面自管 setInterval）
- [x] 3.6 Rollouts：`rollout-detail` 同上（删除页面自管轮询）
- [x] 3.7 Operations 行组件（OperationRow cancel）完成后失效对应 live key，替代 `onOperationChanged` 回调

## 4. 静态资源迁移

- [x] 4.1 Configuration：`['config', nodeId]`/`['config-versions', nodeId]`，`staleTime: Infinity` 不轮询；专项验证：SSE 事件与 30s 周期后未保存草稿内容不丢失
- [x] 4.2 Components 与 Audit：`['components']`/`['audit', filters]` 查询，行为对齐现状（无轮询）

## 5. 拆除 snapshot 与 SSE 失效

- [x] 5.1 App shell：SSE 回调防抖 1.5s 后 `invalidateQueries({ queryKey: ['live'] })`；删除 snapshot 组合/类型与 props 数据管道、`onRefresh`/`onNodesChanged`/`onError` 数据回调
- [x] 5.2 确认 Overview 等页对 nodes 的 mutation（drain/maintain/resume）失效 `['live','nodes']` 后刷新正常

## 6. 验证

- [x] 6.1 `npm test`、`npm run typecheck`、`npm run build` 全绿
- [x] 6.2 手工/测试验证：返回键导航、节点切换重拉（queryKey 变化）、陈旧横幅在 API 失败时保留旧数据并提供重试
- [x] 6.3 验证 SSE 事件触发 live 查询重取且 Configuration 草稿不受影响（对照 spec 场景）

## 7. 文档

- [x] 7.1 更新 `docs/docs/operate/control-plane/console.md`：URL 路径式导航与 `?page=` 重定向说明
- [x] 7.2 同步 zh-Hans 对应文档

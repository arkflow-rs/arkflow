## 1. Error Boundary

- [x] 1.1 `console/src/error-boundary.tsx`：class 组件（componentDidCatch + getDerivedStateFromError），显示错误摘要 + Reload 按钮
- [x] 1.2 `app.tsx`：路由出口包裹 `<AppErrorBoundary>`

## 2. waitForOperation + api 错误

- [x] 2.1 `api.ts` waitForOperation：30×250ms → 120×500ms；超时文案改为"仍在执行，请刷新查看"
- [x] 2.2 `api.ts` request 函数：`throw { ... }` 改为 `throw Object.assign(new Error(message), { status, code, detail })`
- [x] 2.3 api.ts throw 改为 Error 实例后，job-editor 的 cause instanceof Error 分支自然命中（服务端 400 的 message 浮出）；既有 61 个 console 测试全绿含 api 错误路径覆盖

## 3. Configuration rollback gate

- [x] 3.1 `app.tsx`：`<Configuration canMutate={canMutate} ...>`
- [x] 3.2 `configuration.tsx`：Rollback 按钮 `disabled={!editable || !canMutate}`

## 4. 验证

- [x] 4.1 `cd console && pnpm typecheck && pnpm test && pnpm build` 全绿
- [x] 4.2 `openspec validate fix-console-p2-batch` 通过

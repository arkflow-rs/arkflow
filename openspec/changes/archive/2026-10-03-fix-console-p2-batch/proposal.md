## Why

P2 Console 批（`openspec/CODE_REVIEW_2026-09-29.md` Console P2 节）。四个缺陷：

1. **零 Error Boundary**（全 src/ 无 componentDidCatch/getDerivedStateFromError）：任何 render 异常（最现实触发：API 形状漂移，如 `jobs.tsx:425` 对 `detail.tasks` 直接 `.map`）卸载整棵树 → **白屏**。
2. **waitForOperation 7.5 秒假失败**（`api.ts:542-565`）：250ms × 30 次硬上限——超过 7.5s 的操作（节点滚动 apply 很正常）在 UI 报 "Operation timed out" 而服务端仍在执行且可能成功。
3. **job-editor 吞错**（`job-editor.tsx:417`）：`cause instanceof Error ? cause.message : t('editor.validationFailed')`——但 `api.ts:322-329` 的 `throw { ... } satisfies ApiError` 抛**裸对象**非 Error 实例，导致 validate 失败时用户永远只看到泛化 "Validation failed"。
4. **Hub 只读 rollback 漏勺**（`configuration.tsx:142-153`）：Rollback 按钮不 gate `editable`（Hub 模式下 draft 永不加载 → editable 恒 false → Save/Validate/Publish 全禁，但 Rollback 可点）；`canMutate`（节点租约 stale 禁变更）也没传给 Configuration（`app.tsx:284`）。

## What Changes

- **Error Boundary**：`AppErrorBoundary` class 组件包裹 app 路由出口，catch render 错误显示"Something went wrong" + 错误摘要 + 重试按钮（不白屏）。
- **waitForOperation**：上限从 30×250ms=7.5s 提升到 120×500ms=60s；超时错误文案改为"仍在服务端执行中，请稍后刷新查看结果"（不再是"timed out"暗示失败）。
- **api.ts 错误对象改抛 Error 实例**：`throw new Error(...)` 带 ApiError 属性（`cause instanceof Error` 分支自然命中，服务端真实报错浮出）。
- **Configuration 回传 canMutate**：app.tsx 传 `canMutate`；Configuration 的 Rollback 按钮 gate `editable && canMutate`。

## Capabilities

### New Capabilities

（无——console 行为修正，`control-plane-console` 为主 spec。）

### Modified Capabilities

- `control-plane-console`：console SHALL 有 Error Boundary、操作等待语义诚实、验证错误透传、只读视图无变更通道。

## Impact

- `console/src/api.ts`（waitForOperation + 错误对象）
- `console/src/app.tsx`（Error Boundary + canMutate 传递）
- `console/src/features/configuration.tsx`（Rollback gate）
- `console/src/features/job-editor.tsx`（错误透传受益，无需改动——api.ts 修复即解）
- 配套测试

## Non-goals

- 不实现每个子页面的局部 Error Boundary（全局一个足够防白屏）。
- 不改 waitForOperation 的轮询机制（500ms 间隔够温和；SSE/长轮询另行）。
- 不动 Hub 模式配置管理的"只读 + rollout 驱动"产品边界本身。

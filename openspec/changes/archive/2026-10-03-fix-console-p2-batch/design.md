## Context

四个 console 缺陷均来自 2026-09-29 审查，影响用户体验与可诊断性。

## Decisions

**D1 — 全局 Error Boundary 而非逐页**：一个 class 组件包路由出口，React 官方 componentDidCatch + getDerivedStateFromError；显示错误消息（截断 200 字符）+ "Reload" 按钮。局部 boundary 留后续。

**D2 — waitForOperation 上限 60s + 诚实文案**：120 次 × 500ms；超时错误从 "Operation timed out" 改为提示操作可能仍在执行、建议刷新查看——**不再暗示失败**（服务端可能已成功）。

**D3 — api.ts 抛 Error 实例**：`throw new Error(message)` 并赋值 ApiError 属性（status/code/detail）——`instanceof Error` 在 job-editor 与所有既有 `errorMessage()` 消费点正确命中；非 breaking（ApiError 接口不变）。

**D4 — Configuration gate**：props 增加 `canMutate`；Rollback 按钮 `disabled={!editable || !canMutate}`——与 Save/Validate/Publish 的 gate 对齐。

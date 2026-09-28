## 1. 前置核对

- [x] 1.1 核对 `features.test.tsx` / `app.test.tsx` 中 `vi.mock` 的目标模块路径与 import 清单，记录需随迁移更新的条目

## 2. 纯移动拆分（逐字节移动，仅加 import 头）

- [x] 2.1 创建 `features/types.ts`：迁移 `Snapshot`、`Command` 类型定义
- [x] 2.2 创建 `features/shared.tsx`：迁移 `Card`、`Pagination`、`OperationRow`、`EventRow`、`number`、`active`
- [x] 2.3 创建 `features/overview.tsx`：迁移 `Overview`
- [x] 2.4 创建 `features/runtime.tsx`：迁移 `Runtime` 与 `RuntimeDetail`
- [x] 2.5 创建 `features/configuration.tsx`：迁移 `Configuration` 与 `convertConfiguration`
- [x] 2.6 创建 `features/components.tsx`：迁移 `Components`
- [x] 2.7 创建 `features/events.tsx`：迁移 `Events` 与其行组件
- [x] 2.8 创建 `features/settings.tsx`：迁移 `Settings`

## 3. 调用方切换与 bug 修复

- [x] 3.1 `app.tsx`：import 改为按文件导入；`pageFromLocation` 中 `PAGES.some(([key]) => key === value)` 改为 `PAGES.some((p) => p === value)`
- [x] 3.2 `features.test.tsx`：import 更新为新路径（含 mock 目标路径）
- [x] 3.3 删除 `console/src/features.tsx`

## 4. 验证

- [x] 4.1 `npm test` 全绿（features.test.tsx 与 app.test.tsx 仅 import 变更，断言不变）
- [x] 4.2 `npm run typecheck` 与 `npm run build` 通过
- [x] 4.3 验证 `?page=jobs` 深链打开与在 jobs 页刷新均停留在 jobs 页（新增或调整 app 测试覆盖该场景）

## 5. 文档核对

- [x] 5.1 核对 `docs/docs/operate/control-plane/console.md` 无与深链行为冲突的表述，如需补充则说明 URL 页面状态恢复
- [x] 5.2 同步核对 zh-Hans 对应文档 `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/operate/control-plane/console.md`

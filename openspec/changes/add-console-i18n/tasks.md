## 1. i18n 骨架与测试基座

- [x] 1.1 创建 `console/src/i18n/`：`Locale` 类型（`zh`/`en`）、解析优先级（localStorage `arkflow.console.locale` → `navigator.language` 的 `zh` 前缀判断 → `en`，非法存储值视为未设置）、`LocaleContext` + Provider、`useT()` hook 与 `{name}` 占位符插值（缺项回退 `en`，永不渲染裸键）
- [x] 1.2 创建 `console/src/i18n/en.ts`（最小键集起步）与 `zh.ts`（`satisfies Record<TKey, string>` 穷举约束），在 `main.tsx` 挂载 Provider
- [x] 1.3 更新 `console/src/test-setup.ts`：测试环境 locale 固定 `en` 并清空 localStorage locale 键，验证既有测试全部通过
- [x] 1.4 新增 i18n 单元测试：解析优先级四场景（中文浏览器/英文浏览器/显式选择优先/非法存储值）、插值、fallback 回退
- [x] 1.5 验证：`npm run typecheck && npm test` 全绿

## 2. 文案抽取（en 基准字典，按文件分批）

- [x] 2.1 抽取 `app.tsx`：导航标签、顶栏、页头、全局状态提示 → 验证 typecheck + test
- [x] 2.2 抽取 `features.tsx`：页面标题、表格列名、按钮、空态/加载/错误提示 → 验证
- [x] 2.3 抽取 `features/jobs.tsx` → 验证
- [x] 2.4 抽取 `features/rollouts.tsx` → 验证
- [x] 2.5 抽取 `features/audit.tsx` 与 `features/component-browser.tsx` → 验证
- [x] 2.6 抽取 `api.ts` 前端自造错误串（`'Operation timed out'`、`'Control API unavailable'`、SSE/操作状态提示等）；后端 `ApiError.message` 透传路径不动 → 验证
- [x] 2.7 抽取 `features/job-editor.tsx`（949 行最大长杆，label/hint/校验提示，可分多次提交）→ 验证

## 3. zh 中文翻译包

- [x] 3.1 完成 `zh.ts` 全量翻译（与 en.ts 键集合穷举一致，翻译时按中文语序调整 `{name}` 占位符位置）
- [x] 3.2 启动 dev server 抽查各页面中文渲染（导航、表格、表单、错误提示、审计页），确认无裸键名、无英文残留

## 4. 语言切换器与 Intl 跟随

- [x] 4.1 顶栏新增语言切换器（列出 中文/English）：切换即时更新全树文案，写入 `localStorage`，刷新后保持
- [x] 4.2 `formatTime`（`api.ts`）及 `rollouts.tsx`、`jobs.tsx` 的日期/数字 `toLocaleString` 调用点改为跟随当前 locale（`zh` → `zh-Hans`，`en` → `en-US`）
- [x] 4.3 新增切换器与持久化测试（切换 → 断言文案与 localStorage → 重挂载断言保持）

## 5. 文档

- [x] 5.1 更新 `docs/docs/operate/control-plane/console.md`：新增界面语言小节（locale 解析顺序、切换器位置、支持语言清单）
- [x] 5.2 同步 zh-Hans 对应文档 `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/operate/control-plane/console.md`

## 6. 收尾验证

- [x] 6.1 遗留硬编码扫描：`grep` 检查 JSX 文本节点、`placeholder=`、`aria-label=` 中的英文残留，逐条清零或确认豁免
- [x] 6.2 `npm run typecheck && npm test && npm run build` 全绿
- [x] 6.3 dev server 手动走查 golden path：中文浏览器首访得中文 → 切英文 → 刷新保持 → 再切回中文

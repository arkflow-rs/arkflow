## Why

控制面 console（`console/`，React 18 + Vite）目前完全没有 i18n 基础设施，全部用户可见文案以英文硬编码在 JSX 中：导航与页面标题（`console/src/app.tsx:151-164`）、各功能页面（如 `console/src/features/audit.tsx:29-43` 的 `Audit history`、`Filter by action, actor, or resource`）、以及 `console/src/features.tsx`、`console/src/features/job-editor.tsx`（949 行，label/hint/校验提示密度最高）等。项目文档已有完整的 zh-Hans 双语覆盖（`docs/i18n/`，capability `documentation-i18n`），但 console 用户界面与文档语言不一致——中文用户面对纯英文控制面。三个测试文件（约 1,100 行）通过英文文本断言 DOM（`getByText('Audit history')` 等），文案变更必然牵动测试。另外日期/数字格式化直接调用 `toLocaleString()`（`console/src/api.ts:267`、`console/src/features/rollouts.tsx:248`、`console/src/features/jobs.tsx:288`），跟随浏览器 locale 而非界面语言，将来会出现"界面中文、日期英文格式"的混搭。

本变更一次性把文案抽取工作做掉，建立轻量 i18n 架构（零新增依赖），并以 zh 语言包作为首个（当前唯一）翻译。

## What Changes

- 新增 `console/src/i18n/`：locale 状态（React Context）、`useT()` hook、`{name}` 占位符插值、locale 检测与持久化，零新增 npm 依赖。
- 新增语言字典：`en.ts`（现有英文文案收拢，作为 fallback）与 `zh.ts`（新增中文翻译，当前唯一新增语言包）；TypeScript 以 `keyof typeof en` 对 zh 做穷举检查，漏译编译报错。
- locale 解析：`localStorage` 已存值优先 → 否则 `navigator.language` 以 `zh` 前缀判 `zh`、其余 `en`；顶栏新增手动切换器，选择持久化到 `localStorage`。
- Intl 日期/数字格式化（`formatTime` 等）改为跟随选定的 locale，而非浏览器默认。
- `console/src/api.ts` 中前端自造的错误串（如 `'Operation timed out'`、`'Control API unavailable'`）进字典。
- 测试环境将 locale 固定为 `en`，现有英文断言保持不变；仅新增 locale 检测/切换/持久化的测试。

**Non-goals**

- Rust 后端返回的 `ApiError.message` 保持英文，不做翻译——这需要后端错误码体系或前端错误码→翻译映射表，量级完全不同，留待后续独立变更。
- 不引入 i18next / react-intl 等第三方 i18n 库。
- 不新增除 zh/en 之外的语言包（架构就绪，翻译按需追加）。
- 不做服务端渲染或构建期 locale 分包。

## Capabilities

### New Capabilities

- `console-i18n`: 控制面 console 的界面语言行为——locale 解析与持久化、中英字典与 fallback、语言切换器、跟随 locale 的 Intl 格式化、测试 locale 固定。与 `documentation-i18n` 先例平行，覆盖跨页面的横切语言需求。

### Modified Capabilities

（无——`control-plane-console` 既有 REQUIREMENT 未约定界面语言，本变更只新增语言行为层，不改变任何既有需求。）

## Impact

- **代码**：`console/src/` 全部 7 个源文件（`app.tsx`、`features.tsx`、`features/*.tsx`、`api.ts`）的文案抽取；新增 `console/src/i18n/`；`main.tsx` 挂载 Provider。
- **测试**：`app.test.tsx`、`features.test.tsx` 增加 locale 固定 setup；新增 i18n 单元测试（检测/切换/持久化/插值/穷举）。
- **依赖**：无新增 npm 依赖。
- **后端**：无改动（`ApiError.message` 明确划出范围）。
- **体量**：约 300–450 条字符串，`job-editor.tsx` 为最大长杆；纯机械抽取，无技术难点。

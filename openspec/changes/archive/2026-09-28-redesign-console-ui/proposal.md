## Why

控制台的视觉层仍是项目早期的功能样式（`styles.css` 906 行手写、仅浅色主题、241 处 px 硬编码、卡片+投影的通用 SaaS 观感），与阶段②完成后已达行业标准的架构层不匹配。作为面向 SRE/平台工程师的控制面，视觉与交互需要对齐一线大厂控制台的水准（Vercel 的克制与发丝线边框、Linear 的紧凑字号与暗色优先、Stripe 的表格工艺、Grafana/Datadog 的状态色规范）。同时存在一处交互硬伤：6 处 `window.confirm`/`window.prompt` 原生阻塞弹窗承担危险操作确认（`overview.tsx:35`、`runtime.tsx:116`、`jobs.tsx:56`、`configuration.tsx:110`、`rollouts.tsx:40`、`job-editor.tsx:425`）。测试对 className 零依赖（已验证），样式层重写对测试完全不可见。

## What Changes

- 引入 **Tailwind CSS v4**（`@tailwindcss/vite` 插件，`@theme` 令牌）：`styles.css` 重组为设计令牌驱动的主题层，各组件样式迁移为工具类/语义类。
- **双主题（暗色默认）**：暗/浅/跟随系统三态切换器进入顶栏；选择持久化 `localStorage`（复用 i18n 的 LOCALE 先例）；首次加载跟随系统。
- **设计语言**（对齐一线控制台）：深蓝黑 `#0B0E14` 底 + `#12161F` 面板 + 1px 发丝线边框（去卡片投影）；UI 字号 13px 起步的紧凑阶梯；新增 JetBrains Mono 等宽字体承载数据（作业 ID、指标数字、YAML、时间戳，表格 `tabular-nums` 右对齐）；状态色 ok/danger/warn 一等公民化；交互 accent `#5B9DFF` 仅用于链接/主按钮/激活态；侧栏导航分组（观察/交付/管理）。
- **应用内确认对话框 + Toast** 替换全部 6 处原生 `window.confirm/prompt`：引入 `@radix-ui/react-alert-dialog`（无头对话框）与 `sonner`（Toast），回车=确认、Esc=取消，焦点圈闭。
- **加载骨架屏 + 统一空态**：列表/表格加载态用骨架占位，空态统一"说明 + 下一步动作"句式。

## Capabilities

### New Capabilities

- `console-theme`: 控制台视觉主题系统——暗/浅/跟随系统三态、持久化、语义令牌（状态色/强调色/等宽数据字体）作为可验证要求。

### Modified Capabilities

- `control-plane-console`: "Operations application shell" 及相关需求的呈现细节升级——危险操作确认 SHALL 使用应用内对话框（不再使用原生阻塞弹窗），操作结果 SHALL 以 Toast 反馈，加载态 SHALL 使用骨架屏。

## Impact

- `console/package.json` — 新增 `tailwindcss`、`@tailwindcss/vite`、`@radix-ui/react-alert-dialog`、`sonner`、`jetbrains-mono`（本地字体包）
- `console/src/styles.css` — 重写为 Tailwind 入口 + 令牌层
- 全部 `features/*.tsx` 与 `app.tsx` — className 迁移 + 确认对话框接入（测试中 6 处 `window.confirm` spy 需改为对话框交互）
- 新增 `features/confirm.tsx`（对话框封装）与 Toast 挂载
- 服务端零变更；i18n 字典新增少量键（en/zh 同步）

## Non-goals

- 不做 ⌘K 命令面板、表格排序、虚拟滚动。
- 不重绘 Job DAG 编辑器画布（xyflow 样式仅做主题色适配）。
- 不改任何 API 契约与数据流（阶段②成果原样保留）。
- 不新增除 en/zh 外的语言包。

## Context

阶段①② 完成后，`features/` 按页面组织、react-router 路由 + TanStack Query 数据层已就位，行为测试 50/50 且对 className 零依赖。视觉层是最后的欠账：906 行手写 CSS、浅色单主题、原生弹窗确认。用户方向已定：**对齐国际一线大厂控制台 UI**（参照 Vercel/Linear/Stripe/Grafana 各取一味），技术选型延续"成熟标准库"偏好（见记忆 `console-infra-library-preference`）。

## Goals / Non-Goals

**Goals:**

- Tailwind v4 接管样式层，设计令牌（色彩/字体/间距/圆角）集中到 `@theme`。
- 暗色默认的双主题，三态切换，持久化 + 跟随系统。
- 确认对话框 + Toast 替换全部原生弹窗。
- 骨架屏加载态 + 统一空态。
- 行为测试语义不变（确认交互从 `window.confirm` spy 改为对话框按钮交互）。

**Non-Goals:**

- ⌘K 命令面板、表格排序、DAG 画布重绘、i18n 结构变更、API 契约变更。

## Decisions

### D1: Tailwind CSS v4 + `@tailwindcss/vite`

`@import "tailwindcss"` + `@theme` 定义全部令牌；暗色用 **class 策略**（`@custom-variant dark`）而非 `prefers-color-scheme`——三态切换器（dark/light/system）需要可编程覆盖。`styles.css` 缩减为 Tailwind 入口 + 少量组件级残留（如 xyflow 覆写）。

- 语义令牌命名走"角色"而非"色值"：`bg`/`surface`/`surface-hover`/`border`/`text`/`text-muted`/`accent`/`status-ok`/`status-danger`/`status-warn`（各含暗/浅两套映射），组件只用角色令牌。
- Alternative「CSS Modules / vanilla-extract」被否：用户偏好行业标准生态，Tailwind v4 无配置化程度最高。

### D2: 主题 = 暗色默认 + 三态切换

解析顺序：`localStorage.arkflow.console.theme`（'dark'|'light'|'system'）→ 首次为 `system`（`matchMedia('(prefers-color-scheme: dark)')`）。生效主题写在 `<html data-theme>` 上（`data-theme="dark"` ↔ class 策略联动）。切换器入顶栏，与语言选择器并列；持久化先例复用 `i18n` 的 LOCALE 模式（抽 `readStored/store` 同构工具）。i18n 的 `resolveLocale` 先例测试模式照搬。

### D3: 设计语言令牌（对齐一线控制台的具体化）

```
暗色（默认）                    浅色
bg      #0B0E14                #F7F8FA
surface #12161F                #FFFFFF
hover   #1A2029                #F0F2F6
border  #232A36                #E1E5EC
text    #E8ECF3                #1A2333
muted   #8B95A7                #66727F
accent  #5B9DFF（交互件专用）    #2E6BE6
status  ok #34C98E · danger #F0615A · warn #E9A23B
        + 8% 透明 tint 底色，双主题各自校准对比度（≥4.5:1 文本）
```

- 字体：UI = Inter 400/500/600；数据 = JetBrains Mono（npm 包 `jetbrains-mono` 自托管，避免 CDN）；阶梯 12/13/14/16/20，UI 默认 13px。
- 表格：行高 44px、指标列 `tabular-nums` 右对齐、行高亮用 `hover` 令牌。
- 圆角 8px 统一（对话框 12px）；**去面板投影**，层次全靠 1px 边框 + 底色差。
- 侧栏分组：观察（概览/运行时/事件）· 交付（作业/发布）· 管理（配置/组件/审计）；组题用 12px muted，**不用 ALL-CAPS**。
- 动效仅 150ms ease 的 hover/主题过渡；`prefers-reduced-motion: reduce` 时全部禁用。
- 演化掉现有"生成感"元素：行摘要的 `A · B · C` 中点连接改为间隙布局，卡片投影全部移除。

### D4: 确认对话框 = Radix AlertDialog + Toast = sonner

- `@radix-ui/react-alert-dialog@^1`：无头、可访问性（焦点圈闭/Esc/aria）开箱即用；封装 `features/confirm.tsx` 导出两个 Promise 化 API：`confirm({title, body?, confirmLabel, cancelLabel?}) → boolean` 与 `prompt({title, body?, label, confirmLabel, initialValue?}) → string | null`（带输入框，供发布回滚采集 config version 并传入 `api.rolloutAction`），替换 6 处 `window.confirm/prompt` 调用点，调用方逻辑不变。
- `sonner@^2`：`<Toaster richColors position="bottom-right" theme={...} />` 挂 shell；mutation 成功/失败补 Toast（发布、drain、cancel、rollout 动作）。
- Alternative「原生 `<dialog>` + 手写」被否：可访问性细节（焦点陷阱/aria）自制成本高，与标准库偏好相悖。
- **window.confirm 的 6 处测试 spy 迁移**：改为断言对话框出现 + 点击确认/取消按钮（如 `screen.getByRole('dialog')` + `getByRole('button', {name: 'Drain'})`）。

### D5: 迁移顺序 = 先立主题骨架，再按"壳→页"推进

1. 安装依赖、`@theme` 令牌 + 主题切换器 + Toaster 挂载（此步视觉未变，全部类名并存）。
2. shell（侧栏分组/顶栏/横幅）→ 各页 JSX className 迁移，每页一步、测试全绿再进下一页。
3. ConfirmDialog 替换 6 处原生弹窗（每处伴随测试改写）+ sonner 接入 mutation 反馈。
4. 骨架屏/空态统一。
5. 删除 `styles.css` 遗留规则。

## Risks / Trade-offs

- [Tailwind 类名迁移量大（14 组件）且枯燥易漏] → D5 逐页推进 + 每步全量测试；迁移完成的页面从 styles.css 删除对应规则，残留规则在收尾步清零（`grep` 校验）。
- [暗色下状态色对比度不足] → tint 底 + 文本色双主题各自定值，验收标准写进 tasks（文本对比 ≥4.5:1，用现有 `state` 徽标逐色核对）。
- [主题切换闪屏（FOUC）] → `index.html` 内联 3 行读取 localStorage 的脚本，在首帧前设置 `data-theme`。
- [xyflow 画布与主题脱节] → DAG 画布仅做令牌级适配（背景/边框/选中色），不重排布局（non-goal）。
- [Radix + sonner 新依赖 bundle 增量] ≈ 8KB gz；阶段② 已引入 router/query，增量为可接受范围。

## Migration Plan

单 PR（基于 #1262 合并后的 main 或同分支续推）。回滚 = revert PR。风险最高点在 6 处确认对话框迁移，每处独立提交可单独 revert。

## Open Questions

- accent 色最终值（`#5B9DFF` 蓝 vs 青绿系）——默认按蓝实施，实现首个 commit 出双选项截图给用户定夺。

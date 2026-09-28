## 1. 依赖与主题骨架

- [ ] 1.1 安装 `tailwindcss@^4`、`@tailwindcss/vite@^4`、`@radix-ui/react-alert-dialog@^1`、`sonner@^2`、`jetbrains-mono`；vite 插件接入 + `styles.css` 改为 Tailwind 入口 + `@theme` 令牌（暗/浅两套映射，class 策略）
- [ ] 1.2 主题三态解析与持久化（`arkflow.console.theme`，localStorage 先例复用）；`index.html` 首帧前内联设置 `data-theme`；顶栏切换器（与语言选择器并列）+ en/zh 文案键
- [ ] 1.3 `npm test` 全绿（此步视觉未变）

## 2. 壳与逐页 className 迁移（每页一步，测试全绿再进下一页）

- [ ] 2.1 shell：侧栏分组导航（观察/交付/管理，12px muted 组题）、顶栏、连接徽标、警告/错误横幅迁移；去面板投影，层次改边框
- [ ] 2.2 Overview：cards → 密排指标行（等宽数字右对齐）、节点卡、近期活动
- [ ] 2.3 Runtime：表格行高 44px、tabular-nums、状态徽标 tint 底、分页器
- [ ] 2.4 Events + Settings
- [ ] 2.5 Jobs：列表、详情页签、版本/恢复面板
- [ ] 2.6 Rollouts + Configuration（textarea 等宽字体）
- [ ] 2.7 Components + Audit；styles.css 遗留规则清零（grep 校验）

## 3. 确认对话框与 Toast

- [ ] 3.1 封装 `features/confirm.tsx`：Radix AlertDialog 的 Promise 化 `confirmDanger()`；`<Toaster>` 挂 shell 并联动主题
- [ ] 3.2 替换 overview/runtime/jobs/configuration/rollouts/job-editor 六处 `window.confirm/prompt`（每处独立提交）；对应测试从 `window.confirm` spy 改为对话框交互断言
- [ ] 3.3 mutation 结果接入 Toast（发布/drain/cancel/rollout 动作），en/zh 文案键同步

## 4. 骨架屏与空态

- [ ] 4.1 骨架组件 + 各列表/指标面板加载态接入（`isPending` 驱动）
- [ ] 4.2 空态统一为"说明 + 下一步动作"句式（如过滤无匹配附"清除筛选"）

## 5. 验证

- [ ] 5.1 `npm test`、`npm run typecheck`、`npm run build`、prettier 全绿
- [ ] 5.2 双主题走查：9 页 × 暗/浅截图核对；状态徽标文本对比度 ≥ 4.5:1；`prefers-reduced-motion` 下无动画
- [ ] 5.3 验证主题持久化/跟随系统/首帧无闪屏（FOUC）

## 6. 文档

- [ ] 6.1 更新 `docs/docs/operate/control-plane/console.md`：主题切换、确认对话框、骨架屏说明
- [ ] 6.2 同步 zh-Hans 对应文档

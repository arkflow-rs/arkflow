## Context

console（`console/`，React 18 + Vite，runtime 依赖仅 react/@xyflow/yaml）约 3,600 行业务代码 + 1,100 行测试，全部英文文案硬编码在 JSX 与 `api.ts` 中；无路由、无全局状态库。测试通过英文文本断言 DOM。日期/数字用 `toLocaleString()` 跟随浏览器 locale。项目文档侧已有 Docusaurus `i18n/` 双语先例，但技术栈无关，不可复用。

动机与范围见 [proposal.md](proposal.md)；行为需求见 [specs/console-i18n/spec.md](specs/console-i18n/spec.md)。

## Goals / Non-Goals

**Goals:**

- 零新增依赖的 i18n 骨架：locale 状态、字典、插值、切换、持久化。
- zh 语言包作为首个翻译；英文用户界面行为与今日完全一致。
- 文案一次性抽取收拢，未来加语言 = 新增一个字典文件。
- TS 类型层面的漏译检查（编译期穷举）。
- 现有测试断言零改动。

**Non-Goals:**

- 后端 `ApiError.message` 翻译（保持英文，独立后续变更）。
- 第三方 i18n 库（i18next / react-intl / formatjs）。
- zh/en 以外的语言包；构建期 locale 分包、懒加载字典。
- ICU 复数/性别等完整消息格式——中英两种语言的插值场景用 `{name}` 占位符足够。

## Decisions

### D1：自制 `t()` 字典 + React Context，而非引入 i18n 库

console 体量小（~450 条字符串）、runtime 依赖刻意极简（现仅 3 个）。i18next + react-i18next 全家桶（包体、配置面、学习面）对这个体量是负资产。自制实现约 50 行：`LocaleContext` + `useT()` + 插值替换。备选"直接硬编码中文"被否——文案抽取这一最贵的工作两案都要做，多花的只是字典文件的壳，却保住了未来加语言的成本为一。

### D2：`en.ts` 为基准字典与 fallback，`zh.ts` 为覆盖翻译

```ts
// i18n/index.ts（示意）
const dictionaries = { en, zh } as const
export type TKey = keyof typeof en        // en 是 key 的唯一权威来源
function translate(locale: Locale, key: TKey, params?: Params): string {
  const dict = dictionaries[locale]
  let text = dict[key] ?? en[key]         // zh 缺项时回退 en，永不显示裸 key
  // {name} 占位符替换
}
```

- `keyof typeof en` 使 zh 字典可用 `satisfies Record<TKey, string>` 做编译期穷举检查，漏译即编译错误。
- en 字典内容 = 现有 JSX 文案的搬家，不是翻译工作；英文用户 UI 与今日逐字节一致。
- 备选"JSX 保留英文内联、t() 缺项回退内联默认值"被否：key 无权威清单，穷举检查失效，抽取一致性无法保证。

### D3：locale 解析优先级：localStorage → navigator.language → 'en'

初始化顺序：`localStorage['arkflow.console.locale']`（合法值 `zh`/`en`）→ `navigator.language` 以 `zh` 前缀判 `zh`（覆盖 zh-CN/zh-TW/zh-HK）→ 兜底 `en`。存储键非法值视为未设置。手动切换器写入 localStorage 并触发 Context 更新，全树即时重渲染，无需刷新页面。

### D4：Intl 格式化跟随选定 locale

`formatTime`（`api.ts:267`）等调用点改为接收当前 locale（`new Date(value).toLocaleString(locale === 'zh' ? 'zh-Hans' : 'en-US')`），消除"界面中文、日期英文格式"的混搭。`rollouts.tsx:248`、`jobs.tsx:288` 同步处理。数字 `toLocaleString` 同理。

### D5：测试固定 `en` locale

测试 setup（`test-setup.ts`）将 locale 固定为 `en` 且清空 localStorage 存值，现有 1,100 行英文断言（`getByText('Audit history')` 等）零改动通过。新增测试仅覆盖：解析优先级、切换持久化、插值、zh 字典穷举与 fallback。备选"测试跟随字典重构改写全部断言"被否——无行为收益，纯噪音 diff。

### D6：文案抽取按文件分批提交，单文件单 commit

`job-editor.tsx`（949 行）是最大长杆。按文件分批抽取（i18n 骨架 → app → features → jobs → rollouts → audit/component-browser → api → job-editor），每批 `npm run typecheck && npm test` 绿后提交，避免一次 ~450 条字符串的巨型 diff 难以审查与回滚。

## Risks / Trade-offs

- [抽取遗漏：某处 JSX 文案未进字典，双语下仍是英文] → 抽取完成后以 `grep -rn '>[A-Z]'`/`placeholder=` 类模式做遗留扫描；zh 字典 `satisfies Record<TKey, string>` 保证 key 层面无遗漏，但源码层遗漏靠扫描兜底。
- [插值边界：现有模板字符串含数字/英文单词，中文语序不同] → 字典值用 `{name}` 占位符，翻译时可自由调整语序；review 时逐条核对插值参数。
- [长字符串 key 不可读] → key 采用分层短名（如 `audit.filterPlaceholder`），字典文件内按页面分组注释；不接受以英文全文为 key（存储与 diff 噪音大）。
- [自制方案缺 ICU 复数] → 中英两种语言现有文案无复杂复数场景（`N matching` 类用占位符即可）；未来需要时再演进，不预设计。
- [localStorage 键名与未来其他偏好设置冲突] → 统一 `arkflow.console.` 前缀命名空间。

## Migration Plan

纯前端变更，无数据迁移。部署即生效；回滚 = 回退镜像。localStorage 中已写入的 locale 键在回滚版本中只是被忽略，无害。

## Open Questions

（无——方向问题已在探索阶段与用户对齐：架构先行、只上中文包、跟随浏览器 + 手动切换器、后端错误消息划出范围。）

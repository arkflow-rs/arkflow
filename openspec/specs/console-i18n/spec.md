## Purpose

Define the console UI localization system: locale resolution and persistence, the translation dictionary contract, and Intl formatting that follows the selected locale.

## Requirements

### Requirement: Locale 解析与持久化

Console SHALL 在启动时按以下优先级解析界面 locale：`localStorage` 中 `arkflow.console.locale` 的已存合法值（`zh` 或 `en`）优先；未设置时按 `navigator.language` 解析——以 `zh` 前缀开头的语言标签解析为 `zh`，其余解析为 `en`；解析失败兜底 `en`。存储中的非法值 MUST 视为未设置。用户手动选择 SHALL 持久化到 `localStorage` 并在后续会话中生效。

#### Scenario: 中文浏览器首次访问
- **WHEN** `localStorage` 无 locale 记录且 `navigator.language` 为 `zh-CN`
- **THEN** console 以中文渲染界面

#### Scenario: 英文浏览器首次访问
- **WHEN** `localStorage` 无 locale 记录且 `navigator.language` 为 `en-US`
- **THEN** console 以英文渲染界面

#### Scenario: 已有显式选择优先于浏览器语言
- **WHEN** `localStorage` 记录为 `en` 且 `navigator.language` 为 `zh-CN`
- **THEN** console 以英文渲染界面

#### Scenario: 非法存储值被忽略
- **WHEN** `localStorage` 记录为 `fr`（不支持的 locale）
- **THEN** 该值被视为未设置，按 `navigator.language` 重新解析

### Requirement: 语言切换器

Console SHALL 在顶栏提供语言切换器，列出 `zh` 与 `en` 两种语言。切换 SHALL 即时更新全树文案与 Intl 格式化（无需刷新页面），并持久化选择。

#### Scenario: 手动切换语言
- **WHEN** 操作者通过切换器将语言从英文切换为中文
- **THEN** 当前页面全部可见文案即时变为中文，选择被写入 `localStorage`，刷新后仍为中文

### Requirement: 界面文案字典化与 fallback

Console 的全部用户可见文案（页面标题、导航、表格列名、按钮、placeholder、aria-label、空态/加载/错误提示、前端自造的错误消息）SHALL 通过字典键渲染，不得在 JSX 中硬编码。字典以 `en` 为基准与 fallback：请求 locale 的字典缺少某键时 MUST 回退渲染 `en` 文案，MUST NOT 渲染裸键名。`zh` 字典 MUST 对 `en` 的键集合穷举（编译期可验证）。

#### Scenario: 中文界面渲染页面
- **WHEN** locale 为 `zh` 且操作者打开审计页
- **THEN** 页面标题、过滤输入框 placeholder、空态提示均以中文渲染

#### Scenario: 后端错误消息保持原文
- **WHEN** 控制面 API 返回包含英文 `message` 的错误载荷
- **THEN** console 透传显示该后端消息（本能力不翻译后端错误载荷）

#### Scenario: 前端自造错误消息跟随 locale
- **WHEN** locale 为 `zh` 且某轮询操作超时
- **THEN** 前端生成的超时错误提示以中文渲染

### Requirement: Intl 格式化跟随界面 locale

Console 的日期与数字格式化 SHALL 使用当前界面 locale 对应的 BCP 47 标签（`zh` → `zh-Hans`，`en` → `en-US`），而非浏览器默认 locale，界面语言与日期/数字格式 SHALL 一致。

#### Scenario: 中文界面下的时间戳
- **WHEN** locale 为 `zh` 且页面渲染某事件时间戳
- **THEN** 时间以 `zh-Hans` 格式渲染，与界面语言一致

### Requirement: 测试 locale 固定

Console 的组件测试 SHALL 在测试环境将 locale 固定为 `en` 并清空 locale 存储值，使既有英文文本断言不受 locale 解析影响。locale 解析、切换、持久化、插值与字典穷举 SHALL 有独立测试覆盖。

#### Scenario: 测试套件不随默认语言漂移
- **WHEN** 在英文浏览器环境的 CI 中运行 console 测试套件
- **THEN** 既有按英文文本断言的测试全部通过，且 locale 逻辑有独立测试

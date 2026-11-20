## Context

登记簿 §7 确认的 6 条插件/CI 层 P1。共同约束：全部为存量行为的"收口"而非新能力——修法空间小，关键是选对失败模式（fail-fast 优于静默）与生命周期归属（锁/任务不跨 await、不越构造期）。插件注册体系与 builder 签名（`build(name, config: &Option<serde_json::Value>, codec, resource)`）不变。

## Goals / Non-Goals

**Goals:**
- 并发写不被互斥锁串行化（#13）；任务泄漏与构造期 runtime 要求消除（#14）
- 恶意/意外输入只产生指名道姓的错误，MUST NOT panic（#15）、MUST NOT 路由错值（#19）
- 静默无效配置改为显式拒绝（#20）；CI 门禁不可绕过（#30）
- 每条修复附钉扎测试，固化"此 bug 不再回来"

**Non-Goals:**
- remote 数据面与热路径性能（见 proposal Non-goals）
- HTTP output 重试策略、Kafka flush 语义（仅生命周期归属变化，flush 行为不变）

## Decisions

**D1 #13：clone-out 而非换 RwLock/OnceCell。** send() 入口在短临界区内 `let client = guard.as_ref().unwrap().clone()` 后立即释放锁，整个请求构建 + 发送 + 重试循环持有的只是 `reqwest::Client`（内部 Arc，clone 廉价）。备选：`RwLock<Option<Client>>`（读锁仍跨 await，只是允许并发）、`OnceCell`（改动 connect/disconnect 语义，超需求）。clone-out diff 最小且锁完全不跨 await。
*风险：connect() 重连替换 client 与进行中的写并发 → trait 契约 connect 先于 write，最坏一批用旧 client 报连接错误走既有重试；可接受。*

**D2 #14：spawn 移入 `connect()` + `impl Drop` 兜底。** flusher 循环改为首次 connect 成功后启动（异步上下文必然存在，消除构造期 runtime 要求）；`impl Drop for KafkaOutput` 调 `cancellation_token.cancel()`，覆盖"connect 后未经 close 即 drop"的泄漏路径。备选：仅加 Drop（构造期 spawn 的 runtime 要求仍在）、仅移 spawn（connect 后 drop 仍泄漏）。两个都做，各堵一个口。
*风险：无——trait 契约 connect 先于 write，flusher 必然先于首批数据存在。*

**D3 #15：append 前全量校验，错误不 panic。** `append_vector_column` 对全部向量做一次长度一致性检查（与首向量比），不一致返回 `Error::Process` 同时指名两个维度；保留 :218 响应内校验（就近快失败）。备选 `FixedSizeListArray::try_new`（错误信息不含维度语义，且不覆盖 flat 长度错位）。空向量集（dim=0）维持现状。
*成本：每批 O(向量数) 一次长度比较，相对网络调用可忽略。*

**D4 #19：键改长度前缀编码。** 每个字段渲染为 `{name_len}:{name}:{type_len}:{type_render}`，全 schema 拼接后接 `\u{0}{expr_str}`。任意字节级字段名（含 `,`、`:`、`:Int64,` 形态）编码唯一。备选：转义分隔符（转义逻辑本身易错）、对键取哈希（丢失调试可读性，出问题无法人工比对）。钉扎测试：单字段名 `x:Int64,y`(Utf8) 与两字段 `x`(Int64)+`y`(Utf8) 必须产生不同键且各自按正确 schema 求值。
*风险：键格式变化使旧缓存失效——缓存为进程内存态，冷启动无影响。*

**D5 #20：移除字段 + builder 显式拒绝。** `ca_file` 从 config struct、schema 元数据、文档（EN+zh-Hans）中移除；builder 在 raw `serde_json::Value` 上检测该键存在即报错（serde 对未知键默认静默丢弃，仅删字段不足以暴露问题），错误信息指名"驱动无自定义 CA API，请移除该字段"。备选：保留字段仅 build 期报错（schema 仍宣告一个"只会报错"的字段，违反 component-config-honesty 的"仅声明真实接受的面"）；接入系统根信任之外的实现（pulsar 驱动无 API，不可行）。
**BREAKING**：字段从未生效，依赖者本就未获保护——从"静默无效"变"显式报错"是修复而非回归。同步更新 schema 快照（inventory 测试）。

**D6 #30：workflows 路径过滤加 `examples/**`。** 与既有 docs 路径并列加入 `paths`。AGENTS.md 声明的"未注册示例挂 CI"由此在 CI 层闭合。本地验证：yaml 语法 + `git diff` 审阅；CI 行为随首次示例变更 PR 验证。
*风险：示例编辑触发完整 docs 门禁，CI 时长略增——可接受，正确性优先。*

## Risks / Trade-offs

- [D1 一批数据在重连瞬间用旧 client 失败] → 走既有 Error::Connection 重试路径，无数据丢失（输出侧由 ack 序号保证有序）。
- [D2 Drop 中 cancel 为同步调用] → CancellationToken::cancel 无阻塞，Drop 安全。
- [D5 依赖 ca_file 的存量配置升级后启动失败] → 错误信息教学式指明移除；CHANGELOG/发布说明标注 BREAKING。
- [四条行为修复的钉扎测试若依赖网络/驱动] → #15/#19 纯内存可测；#13 断言锁不跨 await 以结构 + 现有 mock server 测试覆盖；#14 泄漏用任务计数或 token 状态断言；#20 断言 build 报错文本。

## Migration Plan

无状态迁移。唯一配置面变化（#20）随发布说明公告；其余为运行时行为收口，回滚即 revert 单个 PR。

## Open Questions

无。

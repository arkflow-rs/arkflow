## Context

2026-09-27 文档匹配轮后 main 合入约 20 个 PR。审计(三路并行,逐提交核对 en+zh)确认 7 项用户可见行为未同步到文档,其中 deploy.md 的围栏断言已与实现相反。zh-Hans 新鲜度报告当前 140/140 全覆盖、0 过时页——缺口全部是「内容未写」或「断言过时」,不是翻译债。本变更是纯文档变更,不改代码。

## Goals / Non-Goals

**Goals:**
- 消除 deploy.md(en+zh)与 #1272 实现矛盾的围栏断言,记录 stale-leader 拒写行为
- api.md(en+zh)Hub 小节补齐 4 个端点
- console.md(en+zh)补节点操作与 Audit 视图
- 取消安全契约进入用户文档(en+zh)
- sql.md、memory.md、deploy.md 补齐语义/边界说明(en+zh)
- 全程保持 `pnpm docs:check`、snippet/examples 测试与 zh 新鲜度报告全绿

**Non-Goals:**
- 不改 docs 工具链/门禁脚本
- 不动 openspec/specs 主规范与任何 Rust 代码
- 不新增独立页面(取消安全放现有 delivery-semantics 页,避免 sidebar 变更)

## Decisions

1. **单一 modified capability `documentation-accuracy`**:所有 7 项缺口都归结为「已发布行为必须在用户文档可见且不失实」。新增页面级 requirements 而非为每个 PR 建新 capability——上轮已验证该 capability 适合承载此类页面级不变量。备选(为 input-cancellation-safety 等 capability 各建 delta)被否:那些 capability 的 spec 已存在且不变,缺的只是文档落地。

2. **取消安全文档落点:concepts/4-delivery-semantics.md 加一节 + 六个 channel 型 input 页各加一句/链接**。语义全文只写一处,组件页引用,避免六份拷贝各自漂移。备选(每页完整展开)被否:重复内容会在下次实现变化时漏改。不动 sidebar(不加新页)。

3. **API 参考以实现路由为唯一事实来源**:写 api.md 前先读 crates/arkflow-server 的 hub 路由代码确认方法/路径/响应形态(含错误码),不从 commit message 或审计转述照抄。围栏段落同理:先确认 stale-leader 拒绝的实际错误形态(错误码/事件名)再落笔。

4. **en+zh 成对编辑**:每个任务同时改 en 页与 zh 镜像,任务完成即双语同步;中文页 yaml 块与英文逐字节一致(含注释,上轮教训)。这保证归档时 i18n 新鲜度报告仍为 0 过时。

5. **console 文档跟随 UI 实测描述**:Drain/Maintain/Resume 的触发后果(如 Drain 是否等价 API 的 drain action)以 console 页面与 hub API 实际行为为准,写文档前在本地跑一次 console 或读 console 源码对应 action 调用,避免想当然。

## Risks / Trade-offs

- [api.md 写入的响应形态与实现有出入(审计只给了端点清单,没给完整 schema)] → 任务里强制「先读路由代码再写」,任务验收含与实现比对
- [stale-leader 错误的具体错误码/事件名写不准] → 同上,以 crates/arkflow-server 实现为准;实现里找不到稳定名称就写行为描述而不写具体字符串
- [中文 yaml 块与英文不一致导致 docs:check 失败] → 保持约定:yaml 块逐字节复制 en,中文说明只在 yaml 外
- [delivery-semantics 页加节后 snippet 测试新增校验负担] → 新增 yaml 若有,必须走 example-manifest 或直接内联可解析;跑 docs_snippets_validate 验证

## Migration Plan

纯文档,无部署/回滚问题;PR 合入即生效。验证门:`pnpm docs:check` + `cargo test -p arkflow --test docs_snippets_validate --test examples_validate` + `node scripts/i18n-freshness-report.mjs`(应保持 0 过时)。

## Open Questions

(无——落点与事实来源均已在 Decisions 中确定,细节以实现代码为准。)

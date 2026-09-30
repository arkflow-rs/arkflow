## Why

2026-09-27 的文档匹配轮之后,main 又合入了约 20 个 PR,其中 7 项用户可见行为未同步到文档,另有一处文档断言已与实现直接矛盾:

- `docs/docs/operate/control-plane/deploy.md:122`(zh-Hans 镜像 :69)仍写 "full storage-level write fencing is a later HA stage",而 commit 4d5ea0e(#1272)已在存储写路径强制 lease fencing epoch、过期 leader 写入会被显式拒绝——文档与实现相反。
- `docs/docs/reference/api.md:230` 的 Hub 节只列 `GET /metrics`(Prometheus text),缺 #1268(3afcc14)新增的 `GET /api/v1/status`(机群聚合)、`/api/v1/metrics` JSON 协商(`format=json` → `{items, aggregate}`)、`POST /nodes/{id}/configuration/validate` 与 `GET .../configuration/diff`。
- `docs/docs/operate/control-plane/console.md:14` 的 Views 表无 Audit 行,全文未提 Drain/Maintain/Resume 节点操作与维护徽标(#1260,a8ad2ee)——用户可从 UI 发起破坏性节点操作但文档零描述。
- 输入取消安全契约(read() 取消不丢已解码消息、producer 侧解码,#1270)只在 `openspec/specs/input-cancellation-safety/spec.md` 有规范,docs/ 全库 grep 零命中;mqtt/nats/redis/websocket/pulsar/http 等取消敏感 input 页均未说明。
- `docs/docs/components/2-processors/sql.md` 无计划缓存行为、`now()`/`current_date` 每批新鲜求值保证(#1258)、连接池/context_pool 并发语义(#1269)。
- `deploy.md` 未描述 `--build-arg VITE_API_TOKEN` 构建注入与 `.env*` 不进构建上下文的保证(#1273,84313f2)。
- `docs/docs/components/1-buffers/memory.md` 未提零容量构建拒绝与 merge 失败保留队列重试(bb5c76c)。

`docs:check`/snippet 门禁只验结构与可解析性,验不出这些内容缺口(上轮已记录的盲区),所以需要一个专项匹配轮。

## What Changes

- **修正矛盾断言**:deploy.md(en+zh)删除「存储级写围栏属于后续 HA 阶段」的说法,改为记录 lease fencing epoch 强制与 stale-leader 拒写的可观察行为。
- **补 API 参考**:api.md(en+zh)补 Hub `/api/v1/status`、`/api/v1/metrics` JSON 协商、`/nodes/{id}/configuration/validate|diff` 端点(路由以 crates/arkflow-server hub API 实现为准)。
- **补 console 文档**:console.md(en+zh)Views 表加 Audit 行,补概览页节点 Drain/Maintain/Resume 操作、维护徽标说明。
- **新增取消安全契约用户文档**:concepts 页新增取消安全小节,并在取消敏感 input 页(mqtt/nats/redis/websocket/pulsar/http)说明 read() 取消语义。
- **补 SQL processor 语义**:sql.md(en+zh)补计划缓存、时间函数每批求值、连接池并发语义。
- **补部署侧 token 构建**:deploy.md(en+zh)补 `--build-arg VITE_API_TOKEN` 流程与 `.env*` 排除保证、OIDC 建议。
- **补 memory buffer 边界行为**(轻微):零容量构建拒绝、merge 失败重试。
- 全部改动 en+zh 双语同步,遵守 zh 页 yaml 块与英文逐字节一致的约定。

## Capabilities

### New Capabilities

(无——本变更是既有 capability 的文档落地与准确性修正,不引入新行为。)

### Modified Capabilities

- `documentation-accuracy`:新增「已发布行为必须在用户文档可见」的页面级 requirements——具体到 deploy.md 围栏断言、api.md Hub 端点、console.md 节点操作/Audit、input 取消安全、sql.md 计划缓存与连接池、memory buffer 边界行为,每条配可验证的 scenario。

## Impact

- 仅文档:docs/docs/** 与 docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/** 同路径镜像。
- 不改任何 Rust 代码、不改 openspec/specs/** 主规范、不改 docs:check 脚本。
- 验证门:`pnpm docs:check`、`cargo test -p arkflow --test docs_snippets_validate --test examples_validate`、zh 页 yaml 与 en 逐字节一致。

## Non-goals

- 不改文档工具链/门禁脚本(工具链强化是另一个方向,另行立项)。
- 不处理非 2026-09-27 之后合入内容的文档优化;zh-Hans 新鲜度报告当前 140/140、0 过时,无全量翻译债。
- 不翻译 console UI 或新增组件页面;不动 versioned_docs。

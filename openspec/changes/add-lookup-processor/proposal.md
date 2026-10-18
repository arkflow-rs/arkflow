# Add Lookup Processor (维表关联)

## Why

PLANNING 的能力完备性评估把「processor 工具箱偏薄——缺数据处理常用的 filter/mask/encrypt/http-lookup/schema-registry 等」列为真实缺口（`openspec/PLANNING.md:34`），且 8.3 节 join 路线将「temporal join（维表 lookup）」明确划为 Non-goals 留 backlog（`openspec/PLANNING.md:476`），8.5 节确认「temporal join」仍开放（`openspec/PLANNING.md:526`）。现状下用户做 enrichment 只有两个死胡同般的替代：SQL processor 的批内临时表 join（维表必须整体物化为静态批），或内核 keyed interval join（要求维表以第二条流输入且按事件时间窗口关联，`crates/arkflow-core/src/executor/join.rs:1-15`）——都覆盖不了最高频的形态：**流每行按 key 查询外部维表服务的当前值，追加列后下发**。

先行交付 lookup processor（维表查询形态）：插件层工作、复用已验证的模式（llm processor 的有界保序并发 `crates/arkflow-plugin/src/processor/llm.rs:17-21`、embedding processor 的列追加 `crates/arkflow-plugin/src/processor/embedding.rs:123`）、同时点掉 1.2 节点名的 http-lookup 缺口。内核版本化 temporal join（CDC 维表流按事件时间匹配）是另一个量级的内核工作，不在本 change。

## What Changes

- 新增 processor `lookup`：按配置的 key 列提取每行查找键 → 先查进程内缓存（TTL + 最大条目，未命中键含负缓存）→ 缓存未命中的去重后交给后端批量解析（有界、保序并发）→ 结果列按行对齐追加到 `MessageBatch`。
- 新增后端抽象 `LookupBackend`（async 批量查询契约），首个实现为 HTTP 后端：URL 模板 + method/headers（支持 secret 引用）+ 响应 JSON 到输出列的映射；经 wiremock 离线测试。
- 失败语义显式化：后端整体失败默认 fail-closed（批处理报错）；单键未命中按配置产出 null 列或丢弃行。
- 组件注册（builder + metadata schema）、组件文档页（en + zh-Hans）、example YAML + example-manifest 登记、双语 README 组件清单——全部走 CI 已检查的义务清单。

## Capabilities

### New Capabilities

- `lookup-processor`: lookup processor 的行为契约——key 提取、缓存语义（命中/负缓存/TTL/容量上界）、批量解析的并发与保序、结果列追加与行对齐、未命中与失败路径、配置校验。

### Modified Capabilities

（无——纯新增组件；join 报错指引已在 `fix-join-error-guidance` 中修正为诚实表述，本 change 不改变既有 capability 的需求。）

## Impact

- `crates/arkflow-plugin/src/processor/lookup.rs`（新增，主要工作量）；`processor/mod.rs` 的 `init()` 接线。
- 文档义务：`docs/docs/components/2-processors/lookup.md` + zh-Hans 对应页、`examples/` 新 YAML + `docs/reference/example-manifest.json`、`README.md`/`README_zh.md` 的 `README_COMPONENTS:processor` 清单。
- 无 arkflow-core 改动（列追加助手在 plugin 层已有先例）；无新增 workspace 依赖（reqwest/futures_util/serde_json 均已在用）。

## Why

PLANNING.md 7.3-3 的检索侧已有 qdrant 后端（processor `vector_search`），但 pgvector 后端只有摄取（output `pgvector`）没有查询——「向量库 output 扩展」时选定的两大后端中，pgvector 用户尚不能在流内做相似度检索，RAG 闭环在 Postgres 部署形态下缺最后一环。全部基建可复用：output `pgvector` 的 sqlx PgPool 模式与 SQL 文本断言测试（`crates/arkflow-plugin/src/output/pgvector.rs`），processor `vector_search` 的有界保序并发与结果 JSON 列语义。**选型对比**：OTel trace 内核插桩专项的依赖选型与 span 切面评估已记录于 PLANNING.md 7.3，仍需精读统一内核代码后单独立项，本轮继续延后。

## What Changes

- 新增 processor `pgvector_search`（`crates/arkflow-plugin/src/processor/pgvector_search.rs`）：
  - 读取输入 batch 的 `vector_field` 列（Fixed/List Float32），逐行对 Postgres+pgvector 执行 `SELECT ... FROM <table> ORDER BY <vector_column> <op> $1::vector LIMIT <top_k>`（`op` 由 `metric` 决定：cosine `<=>` / l2 `<->` / inner_product `<#>`）；
  - 每行命中以 JSON 数组文本追加为 `target_field`（默认 `matches`），元素含 `id`（表 id 列，文本形式）、`distance`（距离算子原始值）与可选 `payload`（jsonb 列解析为对象）；
  - 有界保序并发（`concurrency`，默认 4）；`url` 支持 secret 引用；loopback 代理绕过对 sqlx 不适用（直连 TCP），鉴权走连接串；
  - 失败（列缺失/类型错/null/空向量、SQL 执行错误）返回 `Error::Process` 走 error_output 语义。
- 组件以 `pgvector_search` 注册 builder + metadata schema，接入文档体系。

## Capabilities

### New Capabilities

- `pgvector-search`: pgvector_search processor 的配置形状、检索 SQL 生成（度量算子映射）、结果 JSON 列、错误语义。

### Modified Capabilities

<!-- 无既有能力需求级变更：pgvector output 不动。 -->

## Impact

- `crates/arkflow-plugin/src/processor/`：新增 `pgvector_search.rs`，`mod.rs` init 接线；无新增依赖（sqlx/futures-util 已在）。
- 文档：组件页、双 README 组件清单、示例 YAML + manifest 注册、inventory 重新生成。
- 测试：SQL 生成/行→JSON 映射的离线单测（文本断言，同 output pgvector 模式）；`#[ignore]` 真库集成测试。

## Non-goals

- 不做混合检索（BM25+向量）、重排序、过滤器（WHERE 下推）——按需后补。
- 不做 HNSW/IVFFlat 索引管理（DDL 与索引用户自建）。
- 不做 OTel trace 本体（专项已评估，见 PLANNING.md 7.3）。
- 不做相似度分数的语义换算（返回距离算子原始值 `distance`，度量语义由文档说明）。

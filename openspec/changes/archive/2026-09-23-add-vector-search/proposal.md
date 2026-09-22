## Why

PLANNING.md 7.3-3 的 AI 切片已交付摄取侧（embedding processor + qdrant/pgvector output）与生成侧（llm processor），但**检索侧仍为零**：用户无法在流内用向量做相似度查询——RAG 闭环缺最后一环。典型流式语义检索/增强生成管道「查询文本 → 向量化 → 检索 Top-K 邻居 → LLM 生成」中，中间的检索步骤目前只能绕出引擎。`reqwest`/`futures-util` 已在依赖中，embedding/llm processor 已确立有界保序并发、鉴权、loopback 代理绕过与进程内 mock 测试的完整模式。**选型对比**：OTel trace 需引入全新 opentelemetry 依赖栈（workspace 零 otel 依赖）且 span 切面设计面大，独立立项；后端取 qdrant（REST 可离线 mock，与既有 qdrant output 同构），pgvector 查询侧后续按需立项。

## What Changes

- 新增 processor `vector_search`（`crates/arkflow-plugin/src/processor/vector_search.rs`）：
  - 读取输入 batch 的向量列（`vector_field`，Fixed/List Float32），逐行调 qdrant `POST /collections/{collection}/points/search`（`vector`/`limit=top_k`/`with_payload`/可选 `score_threshold`）；
  - 有界保序并发（`concurrency`，默认 4）；每行检索结果序列化为 JSON 数组文本，追加为 `target_field`（Utf8 列，默认 `matches`），数组按 qdrant 返回的得分降序；
  - `api_key`（Bearer）/`headers`/`timeout_ms`/loopback 代理绕过与既有组件同语义，`api_key` 支持 secret 引用；
  - 失败（非 2xx、向量列缺失/类型错/null/空向量）返回 `Error::Process` 走 error_output 语义。
- 组件以 `vector_search` 注册 builder + metadata schema，接入文档体系；新增「完整 RAG 管道」示例打通查询文本 → embedding → 检索 → LLM 全链路。

## Capabilities

### New Capabilities

- `vector-search`（组件类型 `vector_search`）: vector_search processor 的配置形状、逐行检索与并发语义、结果 JSON 列、错误语义与鉴权（secret 引用兼容）。

### Modified Capabilities

<!-- 无既有能力需求级变更。 -->

## Impact

- `crates/arkflow-plugin/src/processor/`：新增 `vector_search.rs`，`mod.rs` init 接线；无新增依赖。
- 文档：组件页、双 README 组件清单、完整 RAG 管道示例 + manifest 注册、inventory 重新生成。

## Non-goals

- 不做 pgvector/qdrant 之外的检索后端（pgvector 查询侧后续立项）；不做过滤条件（filter）下推、混合检索（BM25+向量）、重排序（rerank）。
- 不做流式/增量近邻（ANN 索引由后端管理）；不做结果展开为多行（每行结果保持单 JSON 列，避免批形变）。
- 不做 MCP/UDF 形式的通用查询接口。
- 不引入向量相似度本地计算（纯后端检索）。

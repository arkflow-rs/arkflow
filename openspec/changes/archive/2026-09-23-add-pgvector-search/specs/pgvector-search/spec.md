# pgvector-search 变更（Delta）

## ADDED Requirements

### Requirement: pgvector_search processor 的配置与检索 SQL 语义

`pgvector_search` processor SHALL 接受配置 `url`（必填，Postgres 连接串，支持 secret 引用）、`table`（必填）、`vector_field`（可选，默认 `embedding`，输入向量列）、`target_field`（可选，默认 `matches`）、`id_column`（可选，默认 `id`）、`vector_column`（可选，默认 `embedding`）、`payload_column`（可选，默认 `payload`，显式置空禁用）、`metric`（可选，`cosine | l2 | inner_product`，默认 `cosine`）、`top_k`（可选，默认 5）、`concurrency`（可选，默认 4）、`max_connections`（可选，默认 4）与 `timeout_ms`（可选，默认 30000）。对每行 SHALL 执行一次 `SELECT "<id_column>"::text AS "id"[, "<payload_column>"::text AS "payload"] FROM "<table>" ORDER BY "<vector_column>" <op> $1::vector LIMIT <top_k>`（`<op>` 由 `metric` 映射为 `<=>`/`<->`/`<#>`），向量以 pgvector 文本格式 `[v1,v2,...]` 绑定，并以有界并发执行、按行序回填结果。

#### Scenario: cosine 检索 SQL 形状

- **WHEN** 配置 `table: documents`、`metric: cosine`、`top_k: 5`（payload 启用）
- **THEN** 生成的 SQL 为 `SELECT "id"::text AS "id", "payload"::text AS "payload" FROM "documents" ORDER BY "embedding" <=> $1::vector LIMIT 5`

#### Scenario: 三种度量的算子映射

- **WHEN** `metric` 分别为 `l2` 与 `inner_product`
- **THEN** 距离算子分别为 `<->` 与 `<#>`

#### Scenario: 禁用 payload 列

- **WHEN** 配置 `payload_column: ""`
- **THEN** 生成的 SQL 不含 payload 列，结果元素不含 `payload` 键

#### Scenario: 命中按行序回填为 JSON 列

- **WHEN** 输入 2 行向量且每行返回 2 个命中
- **THEN** 输出 batch 含 `matches` Utf8 列，第 i 行为其第 i 行命中的 JSON 数组文本，元素含 `id`/`distance`（及 `payload`）

#### Scenario: 有界并发且保序

- **WHEN** 输入 6 行、`concurrency` 为 2
- **THEN** 并发池不超过 2，输出仍按行序回填且全部成功

#### Scenario: 空 batch 直通

- **WHEN** 输入 batch 为 0 行
- **THEN** process 返回 `ProcessResult::None`，不执行任何查询

### Requirement: pgvector_search processor 的错误语义

以下情形 SHALL 返回指明列名与行号或原因的 `Error::Process`：`vector_field` 缺失、类型不是 Float32 列表、null 向量、空向量；行查询执行失败（sqlx 错误透传）；payload 文本解析失败。构建期 SHALL 拒绝 `url`/`table`/`vector_field` 为空与 `top_k`/`concurrency` 为 0 的配置。

#### Scenario: 查询执行失败透传

- **WHEN** 表不存在导致 SELECT 失败
- **THEN** process 返回错误，信息包含 sqlx/Postgres 原因

#### Scenario: null 向量行

- **WHEN** `vector_field` 列某行为 null
- **THEN** process 返回错误并指明行号

#### Scenario: 构建期校验

- **WHEN** 配置 `top_k: 0`、`concurrency: 0` 或必填字段为空
- **THEN** builder 返回 `Error::Config`

### Requirement: 组件注册与文档体系一致性

组件 SHALL 以 `pgvector_search` 注册 builder 与 metadata schema（`components list/show/schema` 可见），SHALL 有带 `components:` front matter 的文档页（含表形状与度量口径说明）、双 README 组件清单条目、注册进 example-manifest 的示例 YAML；生成的 inventory SHALL 与注册一致；真库集成测试 SHALL 标注 `#[ignore]` 并在文档说明运行方式。

#### Scenario: registry 一致性

- **WHEN** 运行 workspace 测试（registry_consistency、docs_inventory_snapshot）与 `pnpm docs:check`
- **THEN** 全部通过，生成文件无需手工编辑

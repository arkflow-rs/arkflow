# Capability: pgvector Output

## Purpose

Define the configuration, SQL generation and upsert semantics, vector/payload column mapping, and error semantics of the `pgvector` output: writing streaming batches as vectors and JSON payloads into PostgreSQL tables with the pgvector extension. (Purpose derived from the `add-pgvector-output` change; refine as the capability evolves.)

## Requirements

### Requirement: pgvector output 的配置与连接语义

`pgvector` output SHALL 接受配置 `url`（必填，Postgres 连接串，支持 secret 引用）、`table`（必填）、`vector_field`（可选，默认 `embedding`）、`id_field`（可选）、`payload_field`（可选，默认 `payload`，显式置空禁用 payload 列）、`max_connections`（可选，默认 4）与 `timeout_ms`（可选，默认 30000）。`connect` SHALL 建立 sqlx PgPool（上限 `max_connections`，获取超时 `timeout_ms`）；连接或获取失败 SHALL 返回 `Error::Connection`。

#### Scenario: 连接池建立与获取超时

- **WHEN** 配置 `url` 指向不可达地址且 `timeout_ms` 超时
- **THEN** connect（或首次 write 的池获取）返回 `Error::Connection`，错误含 sqlx 原因

#### Scenario: url 支持 secret 引用

- **WHEN** 配置 `url: "${env:PG_URL}"`
- **THEN** 物化后的配置为环境变量值，配置文件不含明文凭据

### Requirement: SQL 生成与 upsert 语义

output SHALL 以 `sqlx::QueryBuilder<Postgres>` 生成参数化 INSERT：列布局为 `[id_field?, vector_field, payload_field?]`，向量占位符后跟 `::vector` cast、payload 占位符后跟 `::jsonb` cast，标识符双引号包裹。配置 `id_field` 时 SHALL 追加 `ON CONFLICT ("<id>") DO UPDATE SET` 更新全部非键列（`EXCLUDED` 形式）；未配置则只 INSERT。向量 SHALL 接受 FixedSizeList(Float32)/List(Float32) 并序列化为 pgvector 文本格式 `[v1,v2,...]`；payload SHALL 为除向量/ID 列外所有列按行打包的 JSON 对象（字段名=列名），禁用时省略该列。

#### Scenario: 全列 INSERT 语句形状

- **WHEN** 配置 `table: documents`、`id_field: doc_id`，输入含向量列（2 维）与 `text` 列
- **THEN** 生成 SQL 为 `INSERT INTO "documents" ("doc_id", "embedding", "payload") VALUES ($1, $2::vector, $3::jsonb)` 形状，绑定值依次为 id、`[1.0,2.0]` 形式的向量文本、含 `"text"` 键的 JSON 对象

#### Scenario: 未配置 id_field 时仅 INSERT

- **WHEN** 未配置 `id_field`
- **THEN** 生成 SQL 不含 `ON CONFLICT`，列布局为 `[embedding, payload]`

#### Scenario: 禁用 payload 列

- **WHEN** 配置 `payload_field: ""`
- **THEN** 生成 SQL 只含 id 与向量两列，无 `::jsonb` cast

#### Scenario: upsert 冲突子句更新全部非键列

- **WHEN** 配置 `id_field: doc_id`
- **THEN** `ON CONFLICT ("doc_id") DO UPDATE SET "embedding" = EXCLUDED."embedding", "payload" = EXCLUDED."payload"`

#### Scenario: 批量多行一次执行

- **WHEN** 输入 batch 含 3 行
- **THEN** 生成一条含 3 组 VALUES 元组的 INSERT 并单次执行

### Requirement: 输入校验与错误语义

以下情形 SHALL 返回指明列名与行号的 `Error::Process`：`vector_field` 缺失、类型不是 Float32 列表、null 向量、空向量；`payload` 打包失败。空 batch SHALL 直接成功且不执行 SQL。组件构建期 SHALL 拒绝 `url`/`table` 为空的配置。

#### Scenario: null 向量行报错

- **WHEN** 向量列某行为 null
- **THEN** write 返回 `Error::Process`，信息含列名与行号，不执行 SQL

#### Scenario: 空 batch 直通

- **WHEN** 输入 batch 为 0 行
- **THEN** write 成功返回，不产生 SQL 执行

#### Scenario: 构建期校验

- **WHEN** 配置 `url` 或 `table` 为空
- **THEN** builder 返回 `Error::Config`

### Requirement: 组件注册与文档体系一致性

组件 SHALL 以 `pgvector` 注册 builder 与 metadata schema（`components list/show/schema` 可见），SHALL 有带 `components:` front matter 的文档页、双 README 组件清单条目、注册进 example-manifest 的示例 YAML；生成的 inventory SHALL 与注册一致；真库集成测试 SHALL 标注 `#[ignore]` 并在文档说明运行方式。

#### Scenario: registry 一致性

- **WHEN** 运行 workspace 测试（registry_consistency、docs_inventory_snapshot）与 `pnpm docs:check`
- **THEN** 全部通过，生成文件无需手工编辑

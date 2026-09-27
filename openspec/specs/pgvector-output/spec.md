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

INSERT 语句 SHALL 按参数化绑定生成（id/vector/payload 各占一个绑定），并 SHALL 按行分块使单条语句的绑定参数总数不超过 Postgres 的 65,535 上限（按当前列组合计算每行绑定数，块内行数取不超过上限的最大值）；各块顺序执行，任一块失败 SHALL 停止并返回错误。SQL 标识符（表名与列名）SHALL 以双引号引用且将标识符内的 `"` 转义为 `""`，含引号的标识符 SHALL NOT 破坏语句结构或改变目标对象。

#### Scenario: 大批量按绑定上限分块

- **WHEN** 配置 id_field 与 payload_field（每行 3 个绑定）且输入 batch 含 30,000 行
- **THEN** 生成的每条 INSERT 语句绑定数不超过 65,535，语句按行序顺序执行，全部成功时输出成功返回

#### Scenario: 单小批仍为单语句

- **WHEN** 输入 batch 含 2 行且配置 id_field 与 payload_field
- **THEN** 单条 INSERT 语句含 6 个绑定参数

#### Scenario: 标识符内嵌引号被转义

- **WHEN** 配置 `table: odd"table`、`vector_field: em"bedding`
- **THEN** 生成 SQL 中标识符以 `"odd""table"`、`"em""bedding"` 形式引用，语句可被 Postgres 解析为对应对象

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

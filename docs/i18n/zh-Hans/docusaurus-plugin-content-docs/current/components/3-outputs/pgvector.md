---
components: [pgvector]
description: ArkFlow pgvector 输出组件。
---

# pgvector

`pgvector` 输出(Output)将每个批次(Batch)的行 UPSERT 到启用了 [pgvector](https://github.com/pgvector/pgvector) 扩展的 PostgreSQL 表中。它与 [embedding 处理器](/zh-Hans/docs/components/processors/embedding)配合使用,让现有的 Postgres 直接充当向量存储——无需单独部署向量数据库。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `pgvector` |
| url | string | yes | — | Postgres 连接字符串。支持 [secret 引用](/zh-Hans/docs/reference/configuration#secret-references)。 |
| table | string | yes | — | 目标表(必须已存在)。 |
| vector_field | string | no | `embedding` | 存放向量的列(Float32 组成的 `FixedSizeList`/`List`)。 |
| id_field | string | no | — | 用作 UPSERT 冲突键的列(整数或字符串)。省略时写入普通 INSERT。 |
| payload_field | string | no | `payload` | 接收其余所有列(按行打包为 JSON 对象)的 jsonb 列。设为 `""` 可禁用。 |
| max_connections | integer | no | `4` | 连接池(connection pool)大小。 |
| timeout_ms | integer | no | `30000` | 连接池获取/连接超时。 |

## 表与 SQL 形态

表结构由你自行管理。对于下面的示例:

```sql
CREATE TABLE documents (
  doc_id BIGINT PRIMARY KEY,
  embedding vector(768),
  payload jsonb
);
```

每个批次产生一条参数化语句;向量以文本形式绑定并显式 `::vector` 转换,payload 则以 `::jsonb` 转换:

```sql
INSERT INTO "documents" ("doc_id", "embedding", "payload")
VALUES ($1, $2::vector, $3::jsonb)
ON CONFLICT ("doc_id") DO UPDATE
SET "embedding" = EXCLUDED."embedding", "payload" = EXCLUDED."payload"
```

:::note
设置了 `id_field` 时,目标列必须带有唯一/PK 约束——重新投递会覆盖同一行,对至少一次(at-least-once)语义友好。未设置时,每次尝试都会追加新行。流与表的 `vector(n)` 列之间维度不匹配会失败,并暴露 Postgres 的错误信息。
:::

## 示例

```yaml validate=fragment wrap=output
output:
  type: "pgvector"
  url: "postgres://postgres:${env:PG_PASSWORD:-}@localhost:5432/vectors"
  table: "documents"
  vector_field: "embedding"
  id_field: "doc_id"
```

### 完整流水线示例

```yaml validate=full
logging:
  level: info

streams:
  - id: docs-to-postgres
    input:
      type: memory
    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: embedding
          api_base: "http://localhost:9997/v1"
          model: "text-embedding-3-small"
          api_key: "${env:OPENAI_API_KEY:-}"
          field: "text"
    output:
      type: pgvector
      url: "postgres://postgres:${env:PG_PASSWORD:-}@localhost:5432/vectors"
      table: "documents"
      vector_field: "embedding"
      id_field: "doc_id"
```

## 针对真实数据库测试

工作区测试套件中包含一个默认忽略(ignored)的集成测试,需要带有 pgvector 扩展的真实 Postgres:

```bash
docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres pgvector/pgvector:pg16
cargo test -p arkflow-plugin --lib output::pgvector -- --ignored
```

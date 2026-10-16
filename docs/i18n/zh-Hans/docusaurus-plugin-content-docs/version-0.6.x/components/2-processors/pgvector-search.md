---
components: [pgvector_search]
description: ArkFlow pgvector Search 处理器组件。
---

# pgvector Search

`pgvector_search` 处理器(processor)对启用了 [pgvector](https://github.com/pgvector/pgvector) 扩展的 PostgreSQL 表逐行执行一次最近邻(nearest-neighbor)SELECT,并把匹配结果作为 JSON 数组文本列追加。表结构与 [pgvector 输出](/zh-Hans/docs/components/outputs/pgvector)写入的形状完全一致——`(id, embedding vector(n), payload jsonb)`——因此摄取与检索开箱即用地工作在同一张表上。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `pgvector_search` |
| url | string | yes | — | Postgres 连接串。支持 [Secret 引用](/zh-Hans/docs/reference/configuration#secret-references)。 |
| table | string | yes | — | 要搜索的表(必须已存在)。 |
| vector_field | string | no | `embedding` | 持有查询向量的批次列(Float32 的 `FixedSizeList`/`List`)。 |
| target_field | string | no | `matches` | 追加的匹配结果列名称。 |
| id_column | string | no | `id` | 作为匹配 id(文本形式)返回的表列。 |
| vector_column | string | no | `embedding` | 持有已存储向量的表列。 |
| payload_column | string | no | `payload` | 作为 payload 对象包含在每个匹配中的 jsonb 列。设为 `""` 可禁用。 |
| metric | string | no | `cosine` | 距离操作符:`cosine`(`<=>`)、`l2`(`<->`)、`inner_product`(`<#>`)。 |
| top_k | integer | no | `5` | 每行的邻居数量。 |
| concurrency | integer | no | `4` | 最大并发查询数;结果始终按行序回填。 |
| max_connections | integer | no | `4` | 连接池大小。 |
| timeout_ms | integer | no | `30000` | 从连接池获取连接的超时。 |

## 距离语义

`distance` 是 pgvector 操作符的原始结果,通过
`ORDER BY ... <op> $1` 按最近优先排序:

| metric | operator | 距离含义 |
|--------|----------|------------------|
| `cosine` | `<=>` | 余弦(cosine)距离,`0` = 方向完全一致,取值范围 `[0, 2]` |
| `l2` | `<->` | 欧氏距离,`0` = 完全一致 |
| `inner_product` | `<#>` | 负内积(值越小越相似) |

每个匹配对象携带 `id`(文本形式)、`distance` 和 `payload`(解析后的
jsonb 对象)——该 JSON 文本可直接喂给 [LLM 处理器](/zh-Hans/docs/components/processors/llm)
的提示词,或交给下游的 JSON 工具链。

:::note
失败情形(查询向量为 null/空、SQL 错误)会使整个批次(Batch)失败并进入
`error_output`。该处理器不会重试。连接池为惰性创建,因此构建组件时完全不会触碰网络。
:::

## 示例

```yaml validate=fragment wrap=processors
- type: "pgvector_search"
  url: "postgres://postgres:${env:PG_PASSWORD:-}@localhost:5432/vectors"
  table: "documents"
  metric: "cosine"
  top_k: 5
```

### 完整 RAG 查询管道

```yaml validate=full
logging:
  level: info

streams:
  - id: pgvector-rag-query
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
        - type: pgvector_search
          url: "postgres://postgres:${env:PG_PASSWORD:-}@localhost:5432/vectors"
          table: "documents"
          metric: "cosine"
          top_k: 5
    output:
      type: stdout
```

## 在真实数据库上测试

工作区测试套件中包含一个默认被忽略(ignored)的集成测试,它需要一个启用了
pgvector 扩展的真实 Postgres:

```bash
docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres pgvector/pgvector:pg16
cargo test -p arkflow-plugin --lib processor::pgvector_search -- --ignored
```

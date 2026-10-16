---
components: [qdrant]
description: ArkFlow Qdrant 输出组件。
---

# Qdrant

`qdrant` 输出(Output)通过 REST API 把每个批次(Batch)的行以 [Qdrant](https://qdrant.tech/) 点(Point)的形式 upsert。每行成为一个点:向量来自 `FixedSizeList(Float32)`/`List(Float32)` 列(通常由 [embedding 处理器](/zh-Hans/docs/components/processors/embedding)生成),可选的 id 列作为点的键,其余所有列都进入点的 payload。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `qdrant` |
| url | string | yes | — | Qdrant 基础 URL(例如 `http://localhost:6333`)。 |
| collection | string | yes | — | 目标集合名称。 |
| vector_field | string | no | `embedding` | 持有向量的列。 |
| id_field | string | no | — | 用作点 id 的列(无符号整数或字符串)。省略时,组件会为每行生成一个随机 UUID v4——重投递会插入重复的点,因此为满足至少一次(at-least-once)幂等性,请优先使用 id 列。 |
| payload_fields | array | no | 其余全部列 | 计入点 payload 的列。 |
| api_key | string | no | — | 以 `Authorization: Bearer` 形式发送。支持 [Secret 引用](/zh-Hans/docs/reference/configuration#secret-references)。 |
| timeout_ms | integer | no | `30000` | HTTP 请求超时。 |
| retry_count | integer | no | `3` | 连接错误与 5xx 响应的重试次数;4xx 响应立即失败。 |
| headers | map | no | — | 额外的 HTTP 头。 |

## 语义

- 写入使用 `PUT /collections/{collection}/points?wait=true`——每至多 1,000 个点为一个有界请求;更大的批次按序切分发送。
- upsert 按点 id 幂等:配置了 `id_field` 时,重投递会覆盖同一个点(对至少一次投递友好);未配置时每行都会生成新的 UUID,因此每次尝试都会插入一个新点。
- 向量维度必须与集合一致;不匹配时 Qdrant 返回 4xx,该批次失败(配置了 `error_output` 时路由到该输出)。
- 发往回环(loopback)端点(本地 Qdrant 实例)的请求始终绕过系统代理。

## 示例

```yaml validate=fragment wrap=output
output:
  type: "qdrant"
  url: "http://localhost:6333"
  collection: "documents"
  vector_field: "embedding"
  id_field: "doc_id"
```

### 完整管道示例

```yaml validate=full
logging:
  level: info

streams:
  - id: docs-upserts
    input:
      type: memory
    pipeline:
      processors:
        - type: json_to_arrow
    output:
      type: qdrant
      url: "http://localhost:6333"
      collection: "documents"
      vector_field: "embedding"
      id_field: "doc_id"
```

---
components: [lookup]
description: ArkFlow Lookup 处理器组件。
---

# Lookup

`lookup` 处理器(processor)用外部维表服务解析出的列 enrich 每一行。每行的查找键(一个或多个 key 列)经配置的后端解析,结果列按行对齐追加到批次上。

解析经过有界的进程内缓存:TTL 内已解析的键——包括未命中(负缓存)——不会再次请求后端。同一批次内,去重后的键以有界且保序的并发解析,重复键只会发出一次请求。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `lookup` |
| key_columns | string[] | yes | — | 构成查找键的输入列(Utf8、Int64、UInt64 或 Boolean)。多列按配置顺序连接为复合键。null 键单元遵循缺失语义,不发后端请求。 |
| columns | object[] | yes | — | 要追加的结果列,见下。 |
| columns[].name | string | yes | — | 追加的输出列名。不得与已有输入列重名。 |
| columns[].path | string | yes | — | 后端 JSON 记录内的点分路径(如 `profile.age`;数字段可用于数组索引)。 |
| columns[].type | string | no | `utf8` | 输出列类型:`utf8`、`int64`、`float64` 或 `bool`。值缺失或不可转型时该格为 null。 |
| backend | object | yes | — | 当前仅支持 `http`(见下)。 |
| backend.http.url | string | yes | — | 含 `{{key}}` 占位符的 URL 模板,按查找键替换。 |
| backend.http.method | string | no | `GET` | HTTP 方法(`GET`、`POST`、`PUT`、`DELETE`)。 |
| backend.http.headers | map | no | — | 额外的 HTTP 头。值支持 [Secret 引用](/zh-Hans/docs/reference/configuration#secret-references)。 |
| backend.http.encode_key | boolean | no | `true` | 对替换的键做百分号编码(RFC 3986 非保留字符保持字面),防止键值改写 URL 结构。 |
| ttl_ms | integer | no | `60000` | 已解析记录的缓存 TTL。 |
| negative_ttl_ms | integer | no | `ttl_ms` | 未命中(负缓存)的缓存 TTL。 |
| max_entries | integer | no | `100000` | 缓存容量上界;先清扫过期项,再逐出最久未使用项。 |
| concurrency | integer | no | `8` | 在途后端请求上限。 |
| timeout_ms | integer | no | `30000` | HTTP 请求超时。 |
| on_missing | string | no | `null` | 未解析键的处理:`null` 保留该行并填 null 列;`discard` 丢弃该行。 |
| on_backend_error | string | no | `fail` | 后端失败的处理:`fail` 使批次报错;`null` 将受影响键降级为不缓存的未命中。 |

## 解析语义

- **缓存命中** — `ttl_ms` 内已解析的记录(或 `negative_ttl_ms` 内的未命中)直接从缓存返回,不请求后端。
- **HTTP 404** — 解析为该键无记录:一次参与负缓存的未命中。
- **其他非 2xx、传输错误、超时** — 后端错误:默认使批次失败(配置了 `error_output` 时流入该边)。配置 `on_backend_error: null` 时,受影响键在本批降级为未命中且错误不缓存,后端恢复后下一次出现即可正常解析。
- **键未命中** — 默认保留该行并填 null 查找列;`on_missing: discard` 丢弃该行(这是处理决策而非失败——ack 不受影响)。

缓存的是按 `columns` 提取后的片段而非原始响应,宽维表记录只为所需字段付出成本。

发往回环(loopback)端点的请求(本地维表服务或 mock)始终绕过系统代理。

## 示例

```yaml validate=fragment wrap=processors
- type: "lookup"
  key_columns:
    - user_id
  columns:
    - name: user_name
      path: profile.name
      type: utf8
    - name: age
      path: profile.age
      type: int64
  backend:
    http:
      url: "http://dimension-service:8090/users/{{key}}"
      headers:
        X-Tenant: "acme"
  ttl_ms: 60000
```

### 完整管道示例

```yaml validate=full
logging:
  level: info

streams:
  - id: enrich-and-print
    input:
      type: kafka
      brokers:
        - broker1.example.com:9092
      topics:
        - orders
      consumer_group: arkflow-enricher
      start_from_latest: false
    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: lookup
          key_columns:
            - user_id
          columns:
            - name: user_name
              path: profile.name
              type: utf8
          backend:
            http:
              url: "http://dimension-service:8090/users/{{key}}"
        - type: arrow_to_json
    output:
      type: stdout
```

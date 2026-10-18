---
components: [lookup]
description: ArkFlow documentation page.
---

# Lookup

The `lookup` processor enriches each row with dimension-table columns resolved from an external service. Every row's lookup key (one or more key columns) is resolved against the configured backend, and the configured result columns are appended to the batch, aligned by row.

Resolution goes through a bounded in-process cache: a key resolved within its TTL — including a miss (negative caching) — does not hit the backend again. Within one batch, distinct keys are resolved with bounded, order-preserving concurrency, so duplicate keys collapse into a single request.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `lookup` |
| key_columns | string[] | yes | — | Input column(s) forming the lookup key (Utf8, Int64, UInt64, or Boolean). Multiple columns join into a composite key in configured order. A null key cell follows the missing-key semantics without a backend request. |
| columns | object[] | yes | — | Result columns to append; see below. |
| columns[].name | string | yes | — | Appended output column name. Must not collide with an existing input column. |
| columns[].path | string | yes | — | Dot path of the value inside the backend's JSON record (e.g. `profile.age`; numeric segments index into arrays). |
| columns[].type | string | no | `utf8` | Output column type: `utf8`, `int64`, `float64`, or `bool`. A value that is absent or not coercible yields a null cell. |
| backend | object | yes | — | Currently only `http` (see below). |
| backend.http.url | string | yes | — | URL template containing `{{key}}`, substituted per lookup key. |
| backend.http.method | string | no | `GET` | HTTP method (`GET`, `POST`, `PUT`, `DELETE`). |
| backend.http.headers | map | no | — | Extra HTTP headers. Values support [secret references](/docs/reference/configuration#secret-references). |
| backend.http.encode_key | boolean | no | `true` | Percent-encode the substituted key (RFC 3986 unreserved characters stay literal) so a key value cannot alter the URL structure. |
| ttl_ms | integer | no | `60000` | Cache TTL for resolved records. |
| negative_ttl_ms | integer | no | `ttl_ms` | Cache TTL for misses (negative caching). |
| max_entries | integer | no | `100000` | Cache capacity bound; expired entries are swept first, then the least recently used are evicted. |
| concurrency | integer | no | `8` | Maximum in-flight backend requests. |
| timeout_ms | integer | no | `30000` | HTTP request timeout. |
| on_missing | string | no | `null` | Unresolved key handling: `null` keeps the row with null lookup columns; `discard` drops the row. |
| on_backend_error | string | no | `fail` | Backend failure handling: `fail` errors the batch; `null` degrades the affected keys to uncached misses. |

## Resolution semantics

- **Cache hit** — a record resolved within `ttl_ms` (or a miss within `negative_ttl_ms`) is served from the cache without a backend request.
- **HTTP 404** — resolves the key to no record: a miss that participates in negative caching.
- **Other non-2xx, transport error, timeout** — a backend error: the batch fails by default (flowing to `error_output` when configured). With `on_backend_error: null`, the affected keys degrade to misses for that batch and the error is not cached, so a recovered backend serves the next occurrence.
- **Missing key** — by default the row is kept with null lookup columns; `on_missing: discard` drops it (a processing decision, not a failure — acknowledgement is unaffected).

The cached value is the extracted column fragment, not the raw response, so a wide dimension record only costs the fields you map.

Requests to loopback endpoints (a local dimension service or mock) always bypass the system proxy.

## Example

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

### Complete Pipeline Example

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

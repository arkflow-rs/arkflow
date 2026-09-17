---
components: [schema_registry]
sidebar_label: Schema Registry
---

# Schema Registry

The `schema_registry` codec decodes Confluent wire-format messages by resolving the embedded schema id from a Confluent Schema Registry at runtime. Each schema version (id) is fetched at most once and cached per codec instance, so multi-version schema evolution is supported within the same stream. Both **Protobuf** and **Avro** subjects are supported, dispatched on the registry's `schemaType` response. An optional subject compatibility gate fails the stream fast when a subject's registered compatibility level drops below the configured minimum.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Fixed value `"schema_registry"` |
| registry_url | string | yes | — | Confluent Schema Registry root URL, e.g. `http://localhost:8081` |
| message_type | string | conditional | — | Fully qualified Protobuf message type. Required for Protobuf schemas; omit for Avro. |
| subject | string | no | — | Registry subject for the compatibility gate. |
| min_compatibility | string | no | — | `none`, `backward`, `forward` or `full` (lowercase only; other values are rejected at config load). Minimum subject compatibility level enforced on the first decoded message. Requires `subject`. |
| auth | object | no | — | Registry authentication configuration |
| auth.type | string | yes (if `auth`) | — | Authentication method: `basic` or `bearer` |
| auth.username | string | no | — | Username for `basic` mode |
| auth.password | string | no | — | Password for `basic` mode |
| auth.token | string | no | — | Token for `bearer` mode |

## Examples

Protobuf topic:

```yaml
codec:
  type: schema_registry
  registry_url: http://localhost:8081
  message_type: com.example.User
```

Avro topic with a compatibility gate:

```yaml
codec:
  type: schema_registry
  registry_url: http://registry:8081
  subject: orders-value
  min_compatibility: backward
```

Bearer form:

```yaml
codec:
  type: schema_registry
  registry_url: http://registry:8081
  message_type: com.example.User
  auth:
    type: bearer
    token: ${SR_TOKEN}
```

See `examples/schema_registry.yaml` and `examples/schema_registry_avro.yaml`.

## Semantics

### Wire format

```
[0x00 magic][4-byte big-endian schema id][payload]
```

The codec validates the magic byte, splits out the id and payload, then resolves the schema from the registry using the id.

### Workflow

1. Parse the Confluent wire format (magic + id + payload).
2. Resolve the schema by id (`GET {registry}/schemas/ids/{id}`), caching it per id. The dispatch target comes from the response's `schemaType`:
   - `PROTOBUF` (also the default when the field is absent) → build a `MessageDescriptor` and decode the payload via the flat Protobuf→Arrow mapping.
   - `AVRO` → parse the Avro writer schema and decode the payload via the flat Avro→Arrow mapping.
3. Merge the single-row batches of the request into one columnar batch.

Schema resolution is abstracted behind a pluggable `SchemaResolver` trait (`RestSchemaResolver` for production, an in-memory implementation for tests), so the wire format / caching / multi-version logic can be unit-tested without a real registry.

### Avro → Arrow mapping

The flat mapping mirrors the Protobuf one: top-level record fields become columns. Supported types:

| Avro type | Arrow type |
|-----------|------------|
| `null` | Null |
| `boolean` | Boolean |
| `int` | Int32 |
| `long` | Int64 |
| `float` | Float32 |
| `double` | Float64 |
| `bytes`, `fixed` | Binary |
| `string`, `enum` | Utf8 |
| `uuid` | Utf8 |
| `date` | Date32 |
| `time-millis` / `time-micros` | Time32(ms) / Time64(µs) |
| `timestamp-millis` / `timestamp-micros` | Timestamp(ms/µs, UTC) |
| `local-timestamp-*` | Timestamp (no timezone) |
| `decimal` | Decimal128 (precision ≤ 38) |
| `["null", T]` union | nullable T |

Nested records, arrays, maps and unions with more than two branches are rejected with an explicit error rather than silently flattened.

### Subject compatibility gate

With `subject` and `min_compatibility` configured, the first decoded message triggers a single `GET {registry}/config/{subject}?defaultToGlobal=true` request. The subject's registered level is ranked (`NONE` < `BACKWARD`/`FORWARD` incl. their `_TRANSITIVE` variants < `FULL` incl. `FULL_TRANSITIVE`); if it is below the configured minimum the stream fails with an error naming the subject, the actual level and the requirement. The verdict (pass or fail) is cached for the codec lifetime, so the config endpoint is hit at most once. This catches compatibility-policy degradation (e.g. a subject switched to `NONE`) at the pipeline instead of silently decoding incompatible future versions.

## Notes / Non-goals

- Decode-side only: it resolves schemas by id; it never registers new schemas, and encoding emits line-delimited JSON (registry-agnostic).
- `schemaType` absent in the registry response is treated as `PROTOBUF` (ArkFlow convention kept for backward compatibility; note this differs from the Confluent API default of AVRO).
- Debezium envelope flattening is not performed — payloads are mapped as their registered schema declares; nested envelope structs (`source`, `before`) are rejected by the flat mapping.
- Protobuf schema references (imports) are not supported — only single-file schemas.
- Avro decoding uses the writer schema only (no reader-schema resolution), matching the Protobuf path.
- Local schema compatibility derivation (reader/writer schema resolution checks, `POST /compatibility`) is out of scope; the gate reads the registry's subject configuration, which is the compatibility authority.

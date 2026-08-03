---
sidebar_label: Schema Registry
---

# Schema Registry

The `schema_registry` codec decodes Confluent wire-format Protobuf messages by resolving the embedded schema id from a Confluent Schema Registry at runtime. Each schema version (id) is fetched at most once and cached per codec instance, so multi-version schema evolution is supported within the same stream.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Fixed value `"schema_registry"` |
| registry_url | string | yes | — | Confluent Schema Registry root URL, e.g. `http://localhost:8081` |
| message_type | string | yes | — | Fully qualified Protobuf message type name |
| auth | object | no | — | Registry authentication configuration |
| auth.type | string | yes (if `auth`) | — | Authentication method: `basic` or `bearer` |
| auth.username | string | no | — | Username for `basic` mode |
| auth.password | string | no | — | Password for `basic` mode |
| auth.token | string | no | — | Token for `bearer` mode |

## Examples

```yaml
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - test-topic
  consumer_group: arkflow-sr
  codec:
    type: schema_registry
    registry_url: http://localhost:8081
    message_type: com.example.User
```

```yaml
codec:
  type: schema_registry
  registry_url: http://registry:8081
  message_type: com.example.User
  auth:
    type: basic
    username: ${SR_USER}
    password: ${SR_PASS}
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

See `examples/schema_registry.yaml`.

## Semantics

### Wire format

```
[0x00 magic][4-byte big-endian schema id][Protobuf payload]
```

The codec validates the magic byte, splits out the id and payload, then resolves the schema from the registry using the id.

### Workflow

1. Parse the Confluent wire format (magic + id + payload).
2. Resolve the Protobuf schema by id (`GET {registry}/schemas/ids/{id}`), caching the descriptor per id.
3. Build a `MessageDescriptor` and decode the payload into a columnar Arrow batch.

Schema resolution is abstracted behind a pluggable `SchemaResolver` trait (`RestSchemaResolver` for production, an in-memory implementation for tests), so the wire format / caching / multi-version logic can be unit-tested without a real registry.

## Notes / Non-goals

- Only Protobuf schemas are supported; Avro / JSON Schema are not supported.
- It only resolves schemas on the consumer side; it never writes or registers new schemas.
- It does not explicitly validate BACKWARD/FORWARD compatibility (enforced by the registry).
- Protobuf schema references (imports) are not supported — only single-file schemas.

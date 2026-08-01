# schema_registry

`schema_registry` is a **codec** that decodes Confluent wire-format Protobuf messages by resolving the schema id from a Schema Registry at runtime. It supports schema evolution: each schema version (id) is fetched once and cached, so multiple versions can coexist in the same stream.

## When to use

- Consuming Protobuf messages produced by Confluent serializers (Kafka ecosystem standard wire format).
- CDC / pipeline scenarios where the source schema evolves and producers register new versions to a Schema Registry.

## Wire format

```
[0x00 magic][4-byte big-endian schema id][Protobuf payload]
```

The codec validates the magic byte and strips the id + payload; the id is resolved to a Protobuf schema via the registry.

## Configuration

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
    auth:                       # optional
      type: basic               # or bearer
      username: ${SR_USER}
      password: ${SR_PASS}
```

| Field | Description |
| --- | --- |
| `registry_url` | Confluent Schema Registry base URL |
| `message_type` | Fully-qualified Protobuf message type |
| `auth` | Optional `basic` (username/password) or `bearer` (token) |

## How it works

1. Parse the Confluent wire format (magic + id + payload).
2. Resolve `id` to a Protobuf schema (`GET {registry}/schemas/ids/{id}`), cached per id.
3. Build the `MessageDescriptor` and decode the payload into a columnar Arrow batch.

The schema fetch is pluggable via a `SchemaResolver` trait (`RestSchemaResolver` for production, in-memory for tests), so the wire-format / caching / multi-version logic is unit-testable without a live registry.

## Non-goals

- Avro / JSON Schema types (Protobuf only for now).
- Writing/registering schemas (read-only consumer).
- Explicit BACKWARD/FORWARD compatibility checking (enforced by the registry).
- Protobuf schema references (imports) — single-file schemas only.

## Example

See `examples/schema_registry.yaml`.

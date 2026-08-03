---
sidebar_label: Schema Registry
---

# Schema Registry

The `schema_registry` codec decodes Confluent wire-format Protobuf messages by resolving the embedded schema id from a Confluent Schema Registry at runtime. Each schema version (id) is fetched at most once and cached per codec instance, so multi-version schema evolution is supported within the same stream.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: codec-schema_registry-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| auth | object | no | — | no | Optional registry authentication. |
| message_type | string | yes | — | no | Fully-qualified Protobuf message type. |
| registry_url | string | yes | — | no | Confluent Schema Registry base URL. |
<!-- END AUTO -->

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

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.

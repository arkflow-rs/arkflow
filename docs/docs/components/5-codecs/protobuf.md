---
sidebar_label: Protobuf
---

# Protobuf

The Protobuf codec converts between binary Protobuf messages and columnar Arrow `RecordBatch`es using a descriptor compiled from `.proto` files at startup. Decoding parses each byte payload against the configured `MessageDescriptor`; encoding reverses the process. Use it when an input emits raw Protobuf (no Confluent schema-id prefix).

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: codec-protobuf-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| message_type | string | yes | — | no | Fully-qualified Protobuf message type name. |
| proto_includes | array | no | — | no | Include paths for proto resolution. |
| proto_inputs | array | yes | — | no | Paths to .proto files. |
<!-- END AUTO -->

## Examples

```yaml
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - users
  consumer_group: arkflow
  codec:
    type: protobuf
    message_type: com.example.User
    proto_inputs:
      - /etc/arkflow/proto/user.proto
```

```yaml
codec:
  type: protobuf
  message_type: test.TestMessage
  proto_inputs:
    - /etc/arkflow/proto/test_message.proto
  proto_includes:
    - /usr/include/protos
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.

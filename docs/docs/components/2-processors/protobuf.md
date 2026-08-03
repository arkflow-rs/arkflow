

# Protobuf

The Protobuf processor converts between Apache Arrow batches and Protocol Buffers messages. It registers two processor types: `arrow_to_protobuf` serializes Arrow columns into Protobuf binary data, and `protobuf_to_arrow` decodes Protobuf binary data into an Arrow batch. Message descriptors are loaded from `.proto` source files (or prebuilt descriptor sets).

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: processor-protobuf_to_arrow-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| message_type | string | yes | — | no | Fully-qualified Protobuf message type name. |
| proto_includes | array | no | — | no | Include paths for proto resolution. |
| proto_inputs | array | yes | — | no | Paths to .proto files. |
| value_field | string | no | — | no | Name of the binary column holding the Protobuf wire-format bytes (defaults to '__value'). |
<!-- END AUTO -->

## Examples

### Arrow to Protobuf

```yaml
- processor:
    type: "arrow_to_protobuf"
    proto_inputs: ["./protos/example.proto"]
    message_type: "example.MyMessage"
    fields_to_include:
      - "field1"
      - "field2"
```

### Protobuf to Arrow

```yaml
- processor:
    type: "protobuf_to_arrow"
    proto_inputs: ["./protos/example.proto"]
    proto_includes: ["./protos/"]
    message_type: "example.MyMessage"
    value_field: "data"
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.

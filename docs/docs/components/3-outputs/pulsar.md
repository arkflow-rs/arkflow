

# Pulsar

The Pulsar output publishes messages to an Apache Pulsar topic. It supports token and OAuth2 authentication and uses a single shared producer per output.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: output-pulsar-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| auth | object | no | — | no | Pulsar authentication configuration. |
| service_url | string | yes | — | no | Pulsar service URL. |
| topic | string | yes | — | no | Destination topic (supports \{field\} placeholders). |
| value_field | string | no | — | no | Record field used as the payload. |
<!-- END AUTO -->

## Examples

### Basic Pulsar Producer

```yaml
output:
  type: "pulsar"
  service_url: "pulsar://localhost:6650"
  topic:
    type: "value"
    value: "persistent://public/default/my-topic"
```

### With Token Authentication

```yaml
output:
  type: "pulsar"
  service_url: "pulsar+ssl://secure-pulsar:6651"
  topic:
    type: "value"
    value: "persistent://public/default/secure-topic"
  auth:
    type: "token"
    token: "${PULSAR_TOKEN}"
```

### With OAuth2 Authentication

```yaml
output:
  type: "pulsar"
  service_url: "pulsar+ssl://secure-pulsar:6651"
  topic:
    type: "value"
    value: "persistent://public/default/events"
  auth:
    type: "oauth2"
    issuer_url: "https://auth.example.com/oauth2"
    credentials_url: "file:///etc/pulsar/credentials.json"
    audience: "urn:pulsar:cluster"
```

## Input schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.

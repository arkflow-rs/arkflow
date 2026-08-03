---
sidebar_label: Pulsar
---

# Pulsar

The Pulsar input subscribes to an Apache Pulsar topic and supports four subscription types — exclusive / shared / failover / key_shared — with optional Token or OAuth2 authentication.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-pulsar-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| auth | object | no | — | no | Pulsar authentication configuration. |
| retry_config | object | no | — | no | Retry behaviour for failed messages. |
| service_url | string | yes | — | no | Pulsar service URL (e.g. pulsar://localhost:6650). |
| subscription_name | string | yes | — | no | Subscription name. |
| subscription_type | string | no | — | no | Subscription type. |
| topic | string | yes | — | no | Topic to subscribe to. |
<!-- END AUTO -->

## Examples

```yaml
input:
  type: "pulsar"
  service_url: "pulsar://localhost:6650"
  topic: "my-namespace/my-topic"
  subscription_name: "my-subscription"
```

```yaml
input:
  type: "pulsar"
  service_url: "pulsar://pulsar-cluster:6650"
  topic: "persistent://my-tenant/my-ns/events"
  subscription_name: "consumer-group-1"
  subscription_type: "shared"
```

```yaml
input:
  type: "pulsar"
  service_url: "pulsar+ssl://secure-pulsar:6651"
  topic: "secure-topic"
  subscription_name: "secure-subscription"
  auth:
    type: "token"
    token: "${PULSAR_TOKEN}"
```

```yaml
input:
  type: "pulsar"
  service_url: "pulsar+ssl://pulsar.cloud:6651"
  topic: "cloud-topic"
  subscription_name: "oauth-subscription"
  auth:
    type: "oauth2"
    issuer_url: "https://auth.example.com"
    credentials_url: "file:///path/to/credentials.json"
    audience: "pulsar-cluster"
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.

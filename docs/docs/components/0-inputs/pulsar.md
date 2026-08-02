---
sidebar_label: Pulsar
---

# Pulsar

The Pulsar input subscribes to an Apache Pulsar topic and supports four subscription types — exclusive / shared / failover / key_shared — with optional Token or OAuth2 authentication.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"pulsar"` |
| service_url | string | yes | — | Pulsar service URL, e.g. `pulsar://host:6650` or `pulsar+ssl://host:6651`; cluster URLs separated by commas |
| topic | string | yes | — | Topic, e.g. `persistent://tenant/namespace/topic` or a short name |
| subscription_name | string | yes | — | Subscription name |
| subscription_type | string | no | `"exclusive"` | Subscription type: `exclusive` / `shared` / `failover` / `key_shared` |
| auth | object | no | — | Authentication configuration, see table below (tagged enum) |
| retry_config | object | no | — | Retry configuration, see table below |

### auth

`auth` is a tagged enum (distinguished by the `type` field) with two mutually exclusive forms:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"token"` or `"oauth2"` |
| token | string | yes (token) | Token string |
| issuer_url | string | yes (oauth2) | OAuth2 issuer URL |
| credentials_url | string | yes (oauth2) | OAuth2 credentials URL |
| audience | string | yes (oauth2) | OAuth2 audience |

### retry_config

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| max_attempts | integer | yes | — | Maximum number of retry attempts |
| initial_delay_ms | integer | yes | — | Initial backoff delay (ms) |
| max_delay_ms | integer | yes | — | Maximum backoff delay (ms) |
| backoff_multiplier | number | yes | — | Exponential backoff multiplier |

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

## Notes

- Metadata: `__meta_topic`, `__meta_message_id`, `__meta_publish_time`, `__meta_ingest_time`.
- Subscription types: `exclusive` (single consumer, ordered), `shared` (round-robin, unordered), `failover` (primary/standby, ordered), `key_shared` (routed by key, ordered within the same key).

---
description: ArkFlow documentation page.
---

# Pulsar

The Pulsar output publishes messages to an Apache Pulsar topic. It supports token and OAuth2 authentication and uses a single shared producer per output.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"pulsar"` |
| service_url | string | yes | — | Pulsar service URL (e.g. `pulsar://localhost:6650`). |
| topic | object | yes | — | Destination topic (expression; see below). |
| auth | object | no | — | Authentication configuration (see below). |
| value_field | string | no | — | Record field used as the message payload. |

### topic

`topic` is an `Expr<String>` object with one of these shapes:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `value` (static) or `expr` (SQL expression). |
| value | string | yes (`value`) | Static topic name (e.g. `persistent://tenant/namespace/topic`). |
| expr | string | yes (`expr`) | SQL expression evaluated per message. |

### auth

`auth` is a tagged object (selected by its `type` field). Supported variants: `token` and `oauth2`.

#### token

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `token`. |
| token | string | yes | Authentication token. |

#### oauth2

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `oauth2`. |
| issuer_url | string | yes | OAuth2 issuer URL. |
| credentials_url | string | yes | URL to the client credentials file. |
| audience | string | yes | OAuth2 audience. |

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

## Notes

- The output validates the service URL and auth fields at build and connect time; misconfiguration fails fast.
- Pulsar authentication supports `token` and `oauth2` (client credentials). Basic username/password authentication is not supported.

---
sidebar_position: 1
---

# Top-level configuration

An ArkFlow configuration describes the engine: logging, the health-check /
control-plane server, and the list of streams to run. The file format is
selected by extension — `.yaml`/`.yml`, `.json`, or `.toml` are all accepted.

```yaml
logging:
  level: info

health_check:
  enabled: true
  address: "127.0.0.1:8080"

streams:
  - id: orders
    input:    { ... }
    pipeline: { ... }
    output:   { ... }
```

## Top-level fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `streams` | array&lt;[stream](#stream)&gt; | yes | — | Streams to run. |
| `logging` | object | no | see below | Logging configuration. |
| `health_check` | object | no | see below | Health-check and control-plane server. |

## `logging`

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `level` | string | no | `info` | Log level: `debug`, `info`, `warn`, `error`. |
| `file_path` | string | no | — | Write logs to this file instead of stdout. |
| `format` | string | no | `plain` | Log format: `plain` or `json`. |

## `health_check`

Runs an HTTP server with `/health`, `/readiness`, and `/liveness` endpoints
(useful for Kubernetes). The same server also hosts the optional control-plane
API and the Hub agent when `hub_url` is set (see
[Control plane](../operations/control-plane/1-overview.md)).

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | no | `true` | Start the health-check / control-plane server. |
| `address` | string | no | `127.0.0.1:8080` | Listen address. |
| `health_path` | string | no | `/health` | Overall health endpoint path. |
| `readiness_path` | string | no | `/readiness` | Readiness endpoint path. |
| `liveness_path` | string | no | `/liveness` | Liveness endpoint path. |
| `api_prefix` | string | no | `/api/v1` | Prefix for the versioned control-plane API. |
| `api_token` | string | no | — | Optional Bearer token protecting control-plane operations and configuration. |
| `cors_origins` | array&lt;string&gt; | no | `[]` | Browser origins allowed to call the control API. Empty denies cross-origin calls. |
| `hub_url` | string | no | — | Hub URL for compute-node agent mode. Absent ⇒ standalone mode. |
| `node_id` | string | no | — | Stable identity this process reports to its Hub. |
| `node_token` | string | no | — | Shared node registration credential. Never included in reports. |
| `agent_lease_ttl_ms` | integer | no | `15000` | Lease duration (ms) a compute node advertises to its Hub. |

## stream

Each entry in `streams` is one independent processing pipeline. Stream fields
are documented in depth in the [Components](../category/components) section;
the shape is:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | no | `stream-<index>` | Stable stream identifier (must be unique; used for WAL identity and Hub reporting). |
| `input` | object | yes | — | [Input](../category/components) component (source). |
| `pipeline` | object | yes | — | Processor pipeline. |
| `output` | object | yes | — | Output component (sink). |
| `error_output` | object | no | — | Output that receives batches a processor failed on. |
| `buffer` | object | no | — | Buffer / windowing strategy between input and processors. |
| `durability` | object | no | — | Per-stream WAL durability (see [Delivery semantics](../concepts/4-delivery-semantics.md)). |
| `temporary` | array&lt;object&gt; | no | — | Temporary storage tables for joins. |

### `pipeline`

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `thread_num` | integer | no | `1` | Number of processor worker tasks. |
| `processors` | array&lt;object&gt; | yes | — | Ordered list of processor components. |

## Validate before running

Always validate a config first:

```bash
./target/release/arkflow --config config.yaml --validate
```

Or emit the full JSON Schema and point your editor at it for field-level
completion:

```bash
./target/release/arkflow schema > arkflow.schema.json
```

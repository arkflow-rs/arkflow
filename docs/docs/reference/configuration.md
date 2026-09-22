---
sidebar_position: 1
---

# Top-level configuration

An ArkFlow configuration describes the engine: logging, the health-check /
control-plane server, the list of streams to run, and optional streaming
`jobs` executed by the unified kernel. The file format is selected by
extension — `.yaml`/`.yml`, `.json`, or `.toml` are all accepted.

```yaml validate=full
logging:
  level: info

health_check:
  enabled: true
  address: "127.0.0.1:8080"

streams:
  - id: orders
    input:
      type: memory        # any registered input — see the component pages
    pipeline:
      processors: []      # optional processors
    output:
      type: drop          # any registered output

jobs: []      # optional declarative streaming jobs, see "job" below
```

## Top-level fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `streams` | array&lt;[stream](#stream)&gt; | yes* | — | Streams to run. |
| `jobs` | array&lt;[job](#job)&gt; | no | `[]` | Declarative streaming jobs (DAG + time + state + checkpoint) run by the unified kernel. |
| `logging` | object | no | see below | Logging configuration. |
| `health_check` | object | no | see below | Health-check and control-plane server. |

\* Both `streams` and `jobs` default to empty lists; a jobs-only configuration is valid (declare `streams: []` or omit it).

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
[Control plane](../operate/control-plane/overview.md)).

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
| `agent_session_ttl_ms` | integer | no | `3600000` | Hard lifetime (ms) of a Hub-issued agent session credential; the Agent re-registers transparently when it elapses. |
| `data_port` | integer | no | — | Data-plane listen port for cross-node shuffle. When absent the node runs without a network data plane and never advertises the `network_shuffle` capability. Set together with `data_host` on every node participating in a `split` placement. |
| `data_host` | string | no | — | Routable host peers use to reach this node's data plane (e.g. a LAN IP). Required together with `data_port` for split placement. |
| `observability` | object | no | see below | Process-level Prometheus metrics and health probes (see [Observability](../operate/observability.md)). |

### `health_check.observability`

Exports process-level observability endpoints. They stay available even when
`health_check.enabled` is `false` — a pure data-plane deployment (no
control-plane API, no Hub) still exposes metrics and probes. The listener
binds loopback by default; for production, set an explicit address or rely on
host/firewall policy. When the control-plane server is enabled, its router
serves the same endpoints and no second listener is started.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | no | `true` | Start the observability listener (unless the control-plane server already serves the endpoints). |
| `address` | string | no | `127.0.0.1:8081` | Listen address. |
| `metrics_path` | string | no | `/metrics` | Prometheus exposition endpoint (format 0.0.4). |
| `ready_path` | string | no | `/ready` | Readiness probe: success once the engine finished starting the configured Streams and Jobs. |
| `live_path` | string | no | `/live` | Liveness probe: success while the process is running. |

### Standalone Hub startup safety

The standalone `arkflow-server` control plane is fail-closed by default. Set
`ARKFLOW_HUB_STORAGE` to a durable SQLite path, `ARKFLOW_OPERATOR_TOKEN` for
operator APIs, and `ARKFLOW_NODE_TOKEN` for Agent registration before binding
the Hub beyond loopback. For explicitly local development only, set
`ARKFLOW_HUB_INSECURE_LOCAL=1` while keeping `ARKFLOW_HUB_ADDRESS` on a
loopback address; this permits volatile state and omitted credentials. The Hub
restores durable state before binding its listener, so a recovery error leaves
the server unavailable rather than serving a partial view.

:::note
When the control-plane server is enabled, `/ready` and `/live` are also
mounted on the server address next to the legacy `/health`, `/readiness`, and
`/liveness` endpoints (which keep their original semantics).
:::

## Secret references

Sensitive values (passwords, tokens, key material) do not have to be written
into the configuration file. Any **string value** may embed a reference that
is resolved once, when the configuration is materialized — for file configs
(`--config`), for `--validate`, and for configurations applied through the
control plane. The stored/applied configuration keeps the reference text;
only the running process holds the resolved value in memory.

| Syntax | Meaning |
|--------|---------|
| `${env:VAR}` | Value of environment variable `VAR`; error if unset. |
| `${env:VAR:-default}` | Value of `VAR`, or `default` when unset **or empty** (`${env:VAR:-}` allows an explicit empty value). |
| `${file:/path}` | Content of the file at `/path` with trailing newlines stripped (e.g. a mounted Kubernetes secret). |
| `$${` | Escape for a literal `${`. |

Rules and guarantees:

- References may appear anywhere inside a string value, including nested
  maps/arrays (`host=${env:HOST};port=${env:PORT}` works).
- `${...}` forms with unknown schemes are left verbatim (forward
  compatibility); keys and non-string values are never touched.
- Resolved values are not rescanned: a secret whose value contains `${...}`
  stays literal (no injection).
- Resolution errors name the configuration path and the reference — never
  the secret value.

```yaml validate=full
logging:
  level: info

health_check:
  api_token: "${env:ARKFLOW_API_TOKEN:-}"

streams: []
```

If `ARKFLOW_API_TOKEN` is unset, the process fails at startup with an error
like `Failed to resolve secret reference at health_check.api_token:
environment variable 'ARKFLOW_API_TOKEN' is not set (reference:
${env:ARKFLOW_API_TOKEN})`.

:::note
Resolution is strict everywhere a configuration is materialized. In a
Hub–Agent deployment, a configuration referencing node-local secrets must be
validated/applied where those secrets resolve (the standalone
single-process deployment is unaffected). A central secret store is planned
for a later Hub release.
:::

## stream

Each entry in `streams` is one independent processing pipeline. Stream fields
are documented in depth in the [Components](./component-inventory.md) section;
the shape is:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | no | `stream-<index>` | Stable stream identifier (must be unique; used for WAL identity and Hub reporting). |
| `input` | object | yes | — | [Input](./component-inventory.md) component (source). |
| `pipeline` | object | yes | — | Processor pipeline. |
| `output` | object | yes | — | Output component (sink). |
| `error_output` | object | no | — | Output that receives batches a processor failed on. |
| `buffer` | object | no | — | Buffer / windowing strategy between input and processors. |
| `durability` | object | no | — | Per-stream WAL durability (see [Delivery semantics](/docs/build/delivery-semantics)). |
| `state` | object | no | — | State contract for legacy windows. Declare `durability: ephemeral` for a non-recoverable window, or use a `jobs` entry with checkpoints for durable state. |
| `temporary` | array&lt;object&gt; | no | — | Temporary storage tables for joins. |

### `pipeline`

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `thread_num` | integer | no | `1` | Number of processor worker tasks. |
| `processors` | array&lt;object&gt; | yes | — | Ordered list of processor components. |

## job

Each entry in `jobs` is a declarative streaming job: an operator DAG with
explicit event-time, state, checkpoint, and recovery settings. Jobs run
locally through the same unified kernel as streams, and the same job shape is
what the Hub distributes to compute nodes (see
[Distributed jobs](/docs/build/distributed-jobs)).

```yaml validate=full
jobs:
  - id: local-job
    version: 1
    parallelism: 1
    max_parallelism: 128
    operators:
      - { id: source, kind: source }
      - { id: sink, kind: sink }
    edges:
      - { id: e1, from: source, to: sink, partitioned: true }
    sources:
      - operator_id: source
        input_type: generate
        config: { type: generate, context: '{"value": 1}', interval: 1s, batch_size: 10 }
        time:
          mode: processing_time
    sinks:
      - operator_id: sink
        output_type: stdout
    recovery: latest_checkpoint
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | yes | — | Stable job identifier; must be unique across `jobs`. |
| `version` | integer | yes | — | Job version; state-format compatibility on recovery is evaluated against it. |
| `parallelism` | integer | no | `1` | Default task parallelism. |
| `max_parallelism` | integer | no | `128` | Upper bound used for key-group partitioning. |
| `operators` | array&lt;object&gt; | yes | — | DAG nodes: `id`, `kind` (`source`, `map`, `filter`, `aggregate`, `window`, `sink`, `udf`); `join` is reserved but rejected until a distributed multi-input runtime exists. |
| `edges` | array&lt;object&gt; | no | `[]` | DAG edges: `id`, `from`, `to`, `partitioned` (key-group routing instead of same-subtask). |
| `sources` | array&lt;object&gt; | no | `[]` | Attach a component input to a `source` operator: `operator_id`, `input_type`, `config`, `time`. |
| `sinks` | array&lt;object&gt; | no | `[]` | Attach a component output to a `sink` operator: `operator_id`, `output_type`, `config`. |
| `state` | object | no | — | `backend` (e.g. `embedded_kv`), `durability` (`durable` by default or explicit `ephemeral`), optional stable `root` (or `ARKFLOW_STATE_ROOT`), `namespace`, `ttl_ms`, `format_version`, `max_pending_transactions` (positive; default 4096; raise it when a window sees very high per-window key cardinality, since one transaction is held per open window group or unacknowledged output), optional positive `max_bytes` live-state budget. Required by stateful operators; durable state also requires `checkpoint`. |
| `checkpoint` | object | no | — | `interval_ms`, `retention`, `object_store_uri` (e.g. `file://...` or `s3://...`). |
| `recovery` | string | no | `latest_checkpoint` | `latest_checkpoint`, `latest_savepoint`, or `fail`. |

### `time` (source event-time declaration)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `mode` | string | yes | — | `event_time` or `processing_time`. |
| `timestamp_field` | string | no | — | Field read as the event timestamp when `mode: event_time`. |
| `watermark` | object | no | — | `strategy` (`bounded_out_of_orderness` (default) or `monotonous`), `out_of_orderness_ms`, `idle_timeout_ms`. |
| `allowed_lateness_ms` | integer | no | `0` | How far past the watermark late events are still accepted. |
| `late_event_policy` | string | no | `drop` | What happens to late events: `drop`, `route`, or `update`. |
| `late_event_route` | string | no | — | Operator receiving routed late events when the policy is `route`. |

:::note
Intermediate operator kinds (`map`, `filter`, `aggregate`, `window`, `join`,
`udf`) are primarily produced by the streaming SQL compiler and the console
DAG orchestrator today. Always run `--validate` before deploying: it performs
the same deep build checks as startup and rejects unsupported operators or
state backends explicitly. `./target/release/arkflow schema` emits the
authoritative JSON Schema, including the `jobs` fields, for editor completion.
:::

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

A pre-generated schema ships with the documentation at
[`/config-schema.json`](/config-schema.json); see
[IDE auto-completion](./ide-schema.md) for editor setup.

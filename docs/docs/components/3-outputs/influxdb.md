

# InfluxDB

The InfluxDB output writes time-series data to InfluxDB 2.x using the Line Protocol. It maps columns to tags and fields, buffers writes in configurable batches, and retries failures with exponential backoff.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: output-influxdb-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| batch_size | integer | no | — | no | Batch size for write requests. |
| bucket | string | yes | — | no | Destination bucket. |
| fields | array | yes | — | no | Field mappings (value fields). |
| flush_interval | string | no | — | no | Maximum time to wait before flushing a partial batch. |
| measurement | string | yes | — | no | Measurement name. |
| org | string | yes | — | no | Organization name. |
| tags | array | no | — | no | Tag mappings (label fields). |
| timestamp_field | string | no | — | no | Source field for the point timestamp. |
| token | string | yes | — | no | Authentication token. |
| url | string | yes | — | yes | InfluxDB server URL (e.g. http://localhost:8086). |
<!-- END AUTO -->

## Examples

### Basic InfluxDB Output

```yaml
output:
  type: "influxdb"
  url: "http://localhost:8086"
  org: "my-org"
  bucket: "sensor-data"
  token: "${INFLUXDB_TOKEN}"
  measurement: "temperature"
  fields:
    - field: "temp"
      field_name: "value"
      field_type: "float"
```

### With Tags and Timestamp

```yaml
output:
  type: "influxdb"
  url: "http://localhost:8086"
  org: "production"
  bucket: "metrics"
  token: "${INFLUXDB_TOKEN}"
  measurement: "system_metrics"
  tags:
    - field: "hostname"
      tag_name: "host"
    - field: "region"
      tag_name: "region"
  fields:
    - field: "cpu_percent"
      field_name: "cpu_usage"
      field_type: "float"
    - field: "memory_mb"
      field_name: "memory_usage"
      field_type: "integer"
    - field: "status_message"
      field_name: "status"
      field_type: "string"
  timestamp_field: "timestamp"
```

### With Batching and Retry

```yaml
output:
  type: "influxdb"
  url: "https://influxdb.example.com:8086"
  org: "enterprise"
  bucket: "telemetry"
  token: "${INFLUXDB_TOKEN}"
  measurement: "iot_readings"
  tags:
    - field: "device_type"
      tag_name: "device_type"
  fields:
    - field: "temp"
      field_name: "temperature"
      field_type: "float"
    - field: "battery"
      field_name: "battery_level"
      field_type: "integer"
  timestamp_field: "event_time"
  batch_size: 5000
  flush_interval: 10
  retry_count: 5
  timeout_ms: 10000
```

## Input schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.

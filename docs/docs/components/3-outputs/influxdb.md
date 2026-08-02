# InfluxDB

The InfluxDB output writes time-series data to InfluxDB 2.x using the Line Protocol. It maps columns to tags and fields, buffers writes in configurable batches, and retries failures with exponential backoff.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"influxdb"` |
| url | string | yes | — | InfluxDB server URL (e.g. `http://localhost:8086`). |
| org | string | yes | — | Organization name. |
| bucket | string | yes | — | Destination bucket. |
| token | string | yes | — | Authentication token. |
| measurement | string | yes | — | Measurement name. |
| tags | `array<object>` | no | — | Tag mappings (indexed fields). |
| fields | `array<object>` | yes | — | Field mappings (value fields). |
| timestamp_field | string | no | — | Source column for the point timestamp (nanoseconds). Defaults to current time. |
| batch_size | integer | no | `1000` | Number of lines to buffer before flushing. |
| flush_interval | integer | no | — | Flush interval in seconds. |
| retry_count | integer | no | `3` | Number of retry attempts on failure. |
| timeout_ms | integer | no | `5000` | HTTP request timeout in milliseconds. |

### tags[]

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| field | string | yes | — | Source column name in the message batch. |
| tag_name | string | yes | — | Tag name written to InfluxDB. |

### fields[]

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| field | string | yes | — | Source column name in the message batch. |
| field_name | string | yes | — | Field name written to InfluxDB. |
| field_type | string | no | `string` | One of `float`, `integer`, `boolean`, `string`. |

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

## Notes

- Measurement, tag, and field identifiers are escaped per Line Protocol rules automatically.
- When `field_type` is omitted it is treated as a string field.
- Retries use exponential backoff starting at 100 ms.

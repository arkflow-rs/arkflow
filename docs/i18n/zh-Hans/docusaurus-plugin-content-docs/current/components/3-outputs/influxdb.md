---
components: [influxdb]
description: ArkFlow 文档页面。
---

# InfluxDB

InfluxDB 输出(Output)使用行协议(Line Protocol)将时间序列数据写入 InfluxDB 2.x。它将列映射为 tag 与 field,以可配置的批次(Batch)缓冲写入,并在失败时按指数退避重试。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | 固定值 `"influxdb"` |
| url | string | yes | — | InfluxDB 服务器 URL(例如 `http://localhost:8086`)。 |
| org | string | yes | — | 组织名称。 |
| bucket | string | yes | — | 目标存储桶(bucket)。 |
| token | string | yes | — | 认证令牌。 |
| measurement | string | yes | — | 测量(measurement)名称。 |
| tags | `array<object>` | no | — | 标签(tag)映射(带索引的字段)。 |
| fields | `array<object>` | yes | — | 字段(field)映射(值字段)。 |
| timestamp_field | string | no | — | 数据点时间戳(纳秒)的源列。默认为当前时间。 |
| batch_size | integer | no | `1000` | 刷新前缓冲的行数。 |
| flush_interval | integer | no | — | 刷新间隔(秒)。 |
| retry_count | integer | no | `3` | 失败时的重试次数。 |
| timeout_ms | integer | no | `5000` | HTTP 请求超时时间(毫秒)。 |

### tags[]

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| field | string | yes | — | 消息批次中的源列名。 |
| tag_name | string | yes | — | 写入 InfluxDB 的 tag 名称。 |

### fields[]

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| field | string | yes | — | 消息批次中的源列名。 |
| field_name | string | yes | — | 写入 InfluxDB 的 field 名称。 |
| field_type | string | no | `string` | `float`、`integer`、`boolean`、`string` 之一。 |

## 示例

### 基础 InfluxDB 输出

```yaml validate=fragment wrap=output
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

### 使用 tag 与时间戳

```yaml validate=fragment wrap=output
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

### 使用批量与重试

```yaml validate=fragment wrap=output
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

## 注意事项

- measurement、tag 与 field 标识符会自动按行协议规则转义。
- 省略 `field_type` 时,该字段按字符串 field 处理。
- 重试采用指数退避,起始间隔 100 毫秒。

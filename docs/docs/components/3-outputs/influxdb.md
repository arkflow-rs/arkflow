# InfluxDB

The InfluxDB output component writes time-series data to an InfluxDB 2.x server using the Line Protocol. It supports batching, automatic field type detection, and configurable retry logic.

## Configuration

### **url**

InfluxDB server URL.

type: `string`

example: `"http://localhost:8086"`

optional: `false`

### **org**

Organization name in InfluxDB 2.x.

type: `string`

optional: `false`

### **bucket**

Bucket name to write data to.

type: `string`

optional: `false`

### **token**

Authentication token for InfluxDB 2.x API.

type: `string`

optional: `false`

### **measurement**

Measurement name for the time-series data.

type: `string`

optional: `false`

### **tags** (optional)

Tag mappings for indexed fields. Tags are indexed in InfluxDB and used for fast queries.

type: `array` of `object`

optional: `true`

#### **tags[].name**

Tag name in InfluxDB.

type: `string`

optional: `false` (when tags is specified)

#### **tags[].value**

Column name in the data to map to the tag.

type: `string`

optional: `false` (when tags is specified)

### **fields**

Field mappings for value fields. Fields contain the actual data values.

type: `array` of `object`

optional: `false`

#### **fields[].name**

Field name in InfluxDB.

type: `string`

optional: `false`

#### **fields[].value**

Column name in the data to map to the field.

type: `string`

optional: `false`

#### **fields[].value_type**

Data type of the field.

Supported values: `float`, `integer`, `boolean`, `string`

type: `string`

optional: `false`

### **timestamp_field** (optional)

Column name to use as the timestamp. If not specified, InfluxDB will use the current time.

type: `string`

optional: `true`

### **batch_size** (optional)

Number of points to batch before writing to InfluxDB.

type: `integer`

default: `1000`

optional: `true`

### **flush_interval** (optional)

Maximum time to wait before flushing batched data.

type: `string`

default: `"5s"`

optional: `true`

### **retry_count** (optional)

Number of retry attempts for failed writes.

type: `integer`

default: `3`

optional: `true`

### **retry_delay** (optional)

Initial delay between retries in milliseconds.

type: `integer`

default: `1000`

optional: `true`

## Examples

### Basic InfluxDB Output

```yaml
- output:
    type: "influxdb"
    url: "http://localhost:8086"
    org: "my-org"
    bucket: "sensor-data"
    token: "my-token"
    measurement: "temperature"
    fields:
      - name: "value"
        value: "temperature"
        value_type: "float"
      - name: "sensor"
        value: "sensor_id"
        value_type: "string"
```

### With Tags and Timestamp

```yaml
- output:
    type: "influxdb"
    url: "http://localhost:8086"
    org: "production"
    bucket: "metrics"
    token: "${INFLUXDB_TOKEN}"
    measurement: "system_metrics"
    tags:
      - name: "location"
        value: "datacenter"
      - name: "host"
        value: "hostname"
      - name: "region"
        value: "region"
    fields:
      - name: "cpu_usage"
        value: "cpu_percent"
        value_type: "float"
      - name: "memory_usage"
        value: "memory_mb"
        value_type: "integer"
      - name: "is_active"
        value: "active"
        value_type: "boolean"
      - name: "status"
        value: "status_message"
        value_type: "string"
    timestamp_field: "timestamp"
```

### With Batching and Retry

```yaml
- output:
    type: "influxdb"
    url: "https://influxdb.example.com:8086"
    org: "enterprise"
    bucket: "telemetry"
    token: "${INFLUXDB_TOKEN}"
    measurement: "iot_readings"
    tags:
      - name: "device_type"
        value: "device_type"
      - name: "location"
        value: "location"
    fields:
      - name: "temperature"
        value: "temp"
        value_type: "float"
      - name: "humidity"
        value: "humidity"
        value_type: "float"
      - name: "battery_level"
        value: "battery"
        value_type: "integer"
      - name: "online"
        value: "is_online"
        value_type: "boolean"
    timestamp_field: "event_time"
    batch_size: 5000
    flush_interval: 10s
    retry_count: 5
    retry_delay: 2000
```

### Sensor Data Example

```yaml
- output:
    type: "influxdb"
    url: "http://influxdb:8086"
    org: "manufacturing"
    bucket: "sensor-data"
    token: "${INFLUXDB_TOKEN}"
    measurement: "production_line"
    tags:
      - name: "line"
        value: "production_line"
      - name: "machine"
        value: "machine_id"
      - name: "shift"
        value: "shift_id"
    fields:
      - name: "temperature"
        value: "temp_celsius"
        value_type: "float"
      - name: "pressure"
        value: "pressure_psi"
        value_type: "float"
      - name: "vibration"
        value: "vibration_hz"
        value_type: "float"
      - name: "cycle_count"
        value: "cycles"
        value_type: "integer"
      - name: "maintenance_required"
        value: "needs_maintenance"
        value_type: "boolean"
      - name: "operator"
        value: "operator_name"
        value_type: "string"
    timestamp_field: "reading_time"
    batch_size: 1000
    flush_interval: 5s
```

## Features

- **Line Protocol**: Native InfluxDB 2.x Line Protocol format
- **Automatic Escaping**: Special characters in tags and fields are automatically escaped
- **Type Support**: Float, Integer, Boolean, and String field types
- **Batching**: Efficient batch writes with configurable batch size and flush interval
- **Retry Logic**: Automatic retry with exponential backoff for failed writes
- **Timestamp Handling**: Optional timestamp field for precise time-series data
- **Tags and Fields**: Proper separation between indexed tags and value fields

## Best Practices

1. **Use Tags for Filtering**: Tags are indexed in InfluxDB. Use them for fields you frequently filter or group by.
2. **Use Fields for Values**: Store actual measurements and values in fields.
3. **Optimize Batch Size**: Larger batch sizes (1000-10000) generally improve performance but increase memory usage.
4. **Set Appropriate Flush Interval**: Balance between latency and throughput with `flush_interval`.
5. **Timestamp Precision**: Ensure your timestamp field is in nanoseconds for InfluxDB precision.
6. **Cardinality Management**: Be careful with high-cardinality tags (tags with many unique values).

## Field Types

### Float
Floating-point numbers. Use for measurements with decimal precision.

```yaml
- name: "temperature"
  value: "temp"
  value_type: "float"
```

### Integer
Whole numbers. Use for counts and discrete values.

```yaml
- name: "count"
  value: "event_count"
  value_type: "integer"
```

### Boolean
True/false values.

```yaml
- name: "active"
  value: "is_active"
  value_type: "boolean"
```

### String
Text values.

```yaml
- name: "status"
  value: "status_message"
  value_type: "string"
```

## Error Handling

The output component will:
- Retry failed writes up to `retry_count` times
- Use exponential backoff starting at `retry_delay` milliseconds
- Log errors for debugging
- Continue processing on non-fatal errors

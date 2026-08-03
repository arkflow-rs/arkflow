---
sidebar_label: File
---

# File

The File input reads JSON / CSV / Parquet / Avro / Arrow files via DataFusion. It supports local paths and cloud object storage (S3, GCS, Azure, HTTP, HDFS), with optional SQL over the file data or integration with the Ballista distributed engine.

## Status

Stable

## When to use

Use this component when its role matches the surrounding stream topology. Choose another component when the workload requires a different transport, state boundary, or delivery contract.

## Common fields

The `type` field selects this component. The fields marked `common?` are the fields most often tuned in a first deployment.

## Full reference

<!-- BEGIN AUTO: input-file-fields -->
| Field | Type | Required | Default | common? | Description |
|-------|------|----------|---------|---------|-------------|
| ballista | object | no | — | no | Optional Ballista distributed compute configuration. |
| input_type | object | yes | — | no | Format-specific input settings (type: csv/json/parquet/avro/arrow). |
| query | object | no | — | yes | Optional SQL query and table name to filter the read. |
<!-- END AUTO -->

## Examples

```yaml
input:
  type: "json"
  path: "/data/sensor_data.json"
```

```yaml
input:
  type: "parquet"
  path: "s3://my-bucket/data/sensor_readings.parquet"
  store:
    type: "s3"
    region: "us-west-2"
    bucket_name: "my-bucket"
    access_key_id: "${AWS_ACCESS_KEY_ID}"
    secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```

```yaml
input:
  type: "csv"
  path: "/data/sensors.csv"
  query:
    query: "SELECT sensor_id, AVG(temperature) as avg_temp FROM flow GROUP BY sensor_id"
    table: "sensor_data"
```

```yaml
input:
  type: "parquet"
  path: "s3://analytics/data.parquet"
  store:
    type: "s3"
    endpoint: "http://localhost:9000"
    region: "us-east-1"
    bucket_name: "analytics"
    access_key_id: "minioadmin"
    secret_access_key: "minioadmin"
    allow_http: true
```

```yaml
input:
  type: "csv"
  path: "az://my-container/data/input.csv"
  store:
    type: "az"
    account: "mystorageaccount"
    container_name: "my-container"
    access_key: "${AZURE_STORAGE_ACCESS_KEY}"
```

## Output schema

The component preserves ArkFlow message metadata and uses the batch schema documented by the surrounding input or output.

## Error handling

Configuration errors are reported during validation. Runtime connection, decoding, or processing errors are logged with the component name; use the Troubleshooting guide to identify the failing boundary.

## Metrics

Monitor throughput, errors, retries, and end-to-end acknowledgement latency for this component. The deployment's metrics endpoint exposes the runtime counters when the control plane is enabled.

## See also

Use the generated reference as the source of truth for configuration. Validate a complete stream configuration before deployment.

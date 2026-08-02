---
sidebar_label: File
---

# File

The File input reads JSON / CSV / Parquet / Avro / Arrow files via DataFusion. It supports local paths and cloud object storage (S3, GCS, Azure, HTTP, HDFS), with optional SQL over the file data or integration with the Ballista distributed engine.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | File format: `json` / `csv` / `parquet` / `avro` / `arrow` |
| path | string | yes | — | File path or object storage URL |
| store | object | no | — | Object storage configuration (tagged enum), see table below |
| query | object | no | — | SQL to run over the file data, see table below |
| ballista | object | no | — | Distributed query configuration, see table below |

> Note: the field name in the code is `store` (not `object_store` as in the old docs). The format is specified by the top-level `type` field.

### store

`store` is a tagged enum (distinguished by the `type` field).

#### S3

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `"s3"` |
| bucket_name | string | yes | — | S3 bucket name |
| access_key_id | string | yes | — | AWS access key ID |
| secret_access_key | string | yes | — | AWS secret access key |
| endpoint | string | no | — | Custom endpoint (MinIO, etc.) |
| region | string | no | — | AWS region |
| allow_http | boolean | no | `false` | Whether to allow HTTP (non-TLS) connections |

#### GCS (gs)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"gs"` |
| bucket_name | string | yes | GCS bucket name |
| url | string | no | Custom endpoint |
| service_account_path | string | no | Path to the service account JSON key file |
| service_account_key | string | no | Raw service account JSON content |

#### Azure (az)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"az"` |
| account | string | yes | Storage account name |
| container_name | string | yes | Container name |
| endpoint | string | no | Endpoint |
| url | string | no | Blob endpoint URL |
| access_key | string | no | Storage access key |

#### HTTP

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"http"` |
| url | string | yes | HTTP endpoint URL |

#### HDFS

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | yes | `"hdfs"` |
| url | string | yes | HDFS namenode URL |
| ha_config | map&lt;string, string&gt; | no | High-availability configuration |

### query

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| query | string | yes | — | SQL query statement |
| table | string | no | `"flow"` | Table name under which the file data is registered |

### ballista

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| remote_url | string | yes | Ballista server URL |

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

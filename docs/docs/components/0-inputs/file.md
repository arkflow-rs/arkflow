---
components: [file]
sidebar_label: File
---

# File

The File input reads JSON / CSV / Parquet / Avro / Arrow files via DataFusion. It supports local paths and cloud object storage (S3, GCS, Azure, HTTP, HDFS), with optional SQL over the file data or integration with the Ballista distributed engine.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"file"` (the input kind) |
| input_type | object | yes | — | Format and location, see below: `type` (`json` / `csv` / `parquet` / `avro` / `arrow`), `path` (file path or object storage URL), optional `store` (object storage configuration, tagged enum) |
| query | object | no | — | SQL to run over the file data, see table below (top-level field) |
| ballista | object | no | — | Distributed query configuration, see table below (top-level field) |

> Note: `type`, `path`, and `store` live **inside** `input_type`; `query` and `ballista` are siblings of `input_type` (top-level fields of the file input). The field name in the code is `store` (not `object_store` as in the old docs).

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
| url | string | no | Custom endpoint: a `gs://bucket` URL for the same bucket, or an `http(s)://` endpoint for an emulator (the bucket stays `bucket_name`) |
| service_account_path | string | see note | Path to the service account JSON key file |
| service_account_key | string | see note | Raw service account JSON content |

> Exactly **one** of `service_account_path` / `service_account_key` is required: providing neither fails with "GCS auth is missing", and providing both fails with "Specify either … not both".

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
| url | string | yes | Store origin (`scheme://host[:port]`); any path prefix belongs in the data `path` (e.g. `path: http://host/prefix/file.parquet`) |

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

```yaml validate=fragment wrap=input
input:
  type: "file"
  input_type:
    type: "json"
    path: "/data/sensor_data.json"
```

```yaml validate=fragment wrap=input
input:
  type: "file"
  input_type:
    type: "parquet"
    path: "s3://my-bucket/data/sensor_readings.parquet"
    store:
      type: "s3"
      region: "us-west-2"
      bucket_name: "my-bucket"
      access_key_id: "${AWS_ACCESS_KEY_ID}"
      secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```

```yaml validate=fragment wrap=input
input:
  type: "file"
  input_type:
    type: "csv"
    path: "/data/sensors.csv"
  query:
    query: "SELECT sensor_id, AVG(temperature) as avg_temp FROM flow GROUP BY sensor_id"
    table: "sensor_data"
```

```yaml validate=fragment wrap=input
input:
  type: "file"
  input_type:
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

```yaml validate=fragment wrap=input
input:
  type: "file"
  input_type:
    type: "csv"
    path: "az://my-container/data/input.csv"
    store:
      type: "az"
      account: "mystorageaccount"
      container_name: "my-container"
      access_key: "${AZURE_STORAGE_ACCESS_KEY}"
```

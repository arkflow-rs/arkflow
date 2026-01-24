# File

The File input component reads data from various file formats with support for cloud object storage. It provides powerful SQL querying capabilities on file data using Apache Arrow DataFusion.

## Configuration

### **type**

File format type.

Supported formats:
- `json`: JSON files (newline-delimited JSON)
- `csv`: CSV files with customizable delimiters
- `parquet`: Apache Parquet columnar format
- `avro`: Apache Avro format
- `arrow`: Apache Arrow format

type: `string`

optional: `false`

### **path**

File path to read from. Can be a local path or cloud storage URL.

- Local path: `/path/to/file.json`
- S3: `s3://bucket/path/file.parquet`
- GCS: `gs://bucket/path/file.csv`
- Azure: `az://container/path/file.json`
- HTTP: `http://example.com/data.json`
- HDFS: `hdfs://namenode/path/file.csv`

type: `string`

optional: `false`

### **object_store** (optional)

Cloud object store configuration for accessing remote files.

type: `object`

optional: `true`

#### **object_store.type**

Object store type.

Supported values: `s3`, `gs`, `az`, `http`, `hdfs`

type: `string`

optional: `false` (when object_store is specified)

#### **object_store.endpoint** (S3, Azure, GCS)

Custom endpoint URL for object store.

type: `string`

optional: `true`

#### **object_store.region** (S3)

AWS region for S3.

type: `string`

optional: `true`

#### **object_store.bucket_name** (S3, GCS, Azure)

Bucket or container name.

type: `string`

optional: `false`

#### **object_store.access_key_id** (S3)

AWS access key ID.

type: `string`

optional: `false`

#### **object_store.secret_access_key** (S3)

AWS secret access key.

type: `string`

optional: `false`

#### **object_store.allow_http** (S3)

Allow HTTP connections (for testing with MinIO, etc.).

type: `boolean`

default: `false`

optional: `true`

#### **object_store.service_account_path** (GCS)

Path to GCS service account JSON key file.

type: `string`

optional: `true`

#### **object_store.service_account_key** (GCS)

Raw GCS service account key JSON.

type: `string`

optional: `true`

#### **object_store.account** (Azure)

Azure storage account name.

type: `string`

optional: `false`

#### **object_store.access_key** (Azure)

Azure storage access key.

type: `string`

optional: `true`

#### **object_store.container_name** (Azure)

Azure blob container name.

type: `string`

optional: `false`

#### **object_store.url** (HTTP, HDFS)

URL for HTTP endpoint or HDFS namenode.

type: `string`

optional: `false`

### **query** (optional)

SQL query to execute on the file data.

type: `object`

optional: `true`

#### **query.query**

SQL query string. The table name is specified in the `query.table` field.

type: `string`

optional: `false` (when query is specified)

#### **query.table**

Table name to register the file data as.

type: `string`

default: `"flow"`

optional: `true`

### **ballista** (optional)

Remote Ballista cluster configuration for distributed query execution.

type: `object`

optional: `true`

#### **ballista.remote_url**

Ballista server URL.

type: `string`

optional: `false` (when ballista is specified)

## Examples

### Read JSON from Local File

```yaml
- input:
    type: "json"
    path: "/data/sensor_data.json"
```

### Read Parquet from S3

```yaml
- input:
    type: "parquet"
    path: "s3://my-bucket/data/sensor_readings.parquet"
    object_store:
      type: "s3"
      region: "us-west-2"
      bucket_name: "my-bucket"
      access_key_id: "${AWS_ACCESS_KEY_ID}"
      secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```

### Read CSV with SQL Query

```yaml
- input:
    type: "csv"
    path: "/data/sensors.csv"
    query:
      query: "SELECT sensor_id, AVG(temperature) as avg_temp FROM flow GROUP BY sensor_id"
      table: "sensor_data"
```

### Read from MinIO (S3-compatible)

```yaml
- input:
    type: "parquet"
    path: "s3://analytics/data.parquet"
    object_store:
      type: "s3"
      endpoint: "http://localhost:9000"
      region: "us-east-1"
      bucket_name: "analytics"
      access_key_id: "minioadmin"
      secret_access_key: "minioadmin"
      allow_http: true
```

### Read from Google Cloud Storage

```yaml
- input:
    type: "json"
    path: "gs://my-project-bucket/data/*.json"
    object_store:
      type: "gs"
      bucket_name: "my-project-bucket"
      service_account_path: "/path/to/service-account.json"
```

### Read from Azure Blob Storage

```yaml
- input:
    type: "csv"
    path: "az://my-container/data/input.csv"
    object_store:
      type: "az"
      account: "mystorageaccount"
      container_name: "my-container"
      access_key: "${AZURE_STORAGE_ACCESS_KEY}"
```

### Read from HTTP Endpoint

```yaml
- input:
    type: "json"
    path: "https://example.com/data.json"
    object_store:
      type: "http"
      url: "https://example.com"
```

### Distributed Query with Ballista

```yaml
- input:
    type: "parquet"
    path: "s3://data-lake/analytics/*.parquet"
    object_store:
      type: "s3"
      region: "us-west-2"
      bucket_name: "data-lake"
      access_key_id: "${AWS_ACCESS_KEY_ID}"
      secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
    query:
      query: "SELECT category, COUNT(*) as count, AVG(value) as avg_value FROM flow WHERE timestamp > NOW() - INTERVAL '1' DAY GROUP BY category"
      table: "events"
    ballista:
      remote_url: "http://ballista-cluster:50050"
```

## Features

- **Multiple Formats**: Support for JSON, CSV, Parquet, Avro, and Arrow formats
- **Cloud Storage**: Native integration with AWS S3, Google Cloud Storage, Azure Blob Storage
- **SQL Queries**: Execute SQL queries directly on file data using DataFusion
- **Distributed Processing**: Optional Ballista cluster support for large-scale data processing
- **Schema Inference**: Automatic schema detection for supported formats
- **Wildcards**: Use glob patterns like `s3://bucket/data/*.parquet` to read multiple files
- **Columnar Processing**: Efficient columnar data processing with Apache Arrow

## Performance Tips

1. **Use Parquet**: For best performance, use Parquet format with compression
2. **Partition Data**: Organize data in partitions by date/time for efficient querying
3. **Filter Pushdown**: Use SQL WHERE clauses to leverage DataFusion's query optimization
4. **Column Pruning**: Only select required columns in SQL queries to reduce I/O
5. **Batch Size**: Parquet files with larger row groups (100MB-1GB) typically perform better

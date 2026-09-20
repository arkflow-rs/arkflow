---
components: [file]
sidebar_label: File
---

# 文件(File)

文件输入(Input)通过 DataFusion 读取 JSON / CSV / Parquet / Avro / Arrow 文件。它支持本地路径与云对象存储(S3、GCS、Azure、HTTP、HDFS),并可选地对文件数据执行 SQL,或与 Ballista 分布式引擎集成。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 文件格式: `json` / `csv` / `parquet` / `avro` / `arrow` |
| path | string | yes | — | 文件路径或对象存储 URL |
| store | object | no | — | 对象存储配置(带标签的枚举),见下表 |
| query | object | no | — | 对文件数据执行的 SQL,见下表 |
| ballista | object | no | — | 分布式查询配置,见下表 |

> 注意:代码中的字段名是 `store`(而不是旧文档中的 `object_store`)。文件格式由顶层的 `type` 字段指定。

### store

`store` 是一个带标签的枚举(tagged enum,通过 `type` 字段区分)。

#### S3

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `"s3"` |
| bucket_name | string | yes | — | S3 存储桶名称 |
| access_key_id | string | yes | — | AWS 访问密钥 ID |
| secret_access_key | string | yes | — | AWS 秘密访问密钥 |
| endpoint | string | no | — | 自定义端点(MinIO 等) |
| region | string | no | — | AWS 区域 |
| allow_http | boolean | no | `false` | 是否允许 HTTP(非 TLS)连接 |

#### GCS (gs)

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"gs"` |
| bucket_name | string | yes | GCS 存储桶名称 |
| url | string | no | 自定义端点 |
| service_account_path | string | no | 服务账号 JSON 密钥文件的路径 |
| service_account_key | string | no | 服务账号 JSON 原始内容 |

#### Azure (az)

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"az"` |
| account | string | yes | 存储账号名称 |
| container_name | string | yes | 容器名称 |
| endpoint | string | no | 端点 |
| url | string | no | Blob 端点 URL |
| access_key | string | no | 存储访问密钥 |

#### HTTP

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"http"` |
| url | string | yes | HTTP 端点 URL |

#### HDFS

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | `"hdfs"` |
| url | string | yes | HDFS namenode URL |
| ha_config | map&lt;string, string&gt; | no | 高可用配置 |

### query

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| query | string | yes | — | SQL 查询语句 |
| table | string | no | `"flow"` | 文件数据注册所使用的表名 |

### ballista

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| remote_url | string | yes | Ballista 服务器 URL |

## 示例

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

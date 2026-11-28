---
components: [output/iceberg]
description: ArkFlow 文档页面。
---

# Iceberg

Iceberg 输出通过 REST catalog 把行追加到一张已存在的 [Apache Iceberg](https://iceberg.apache.org/) 表。一次 `write_batch` 调用就是**一次原子表追加**：输出先提交快照，源确认随后才发生，确认永远不会跑在表前面。调用之间输出不持有缓冲、不跑定时器——提交节奏与数据文件大小由管道投递的批次决定，实践中就是在本输出前面放一个 [`batch`](/components/2-processors/batch) 处理器。

## 前置条件

- 一个从 ArkFlow 所在主机可达的 REST catalog（已用 [Lakekeeper](https://docs.lakekeeper.io/) 验证）。
- 表必须已存在。输出从不建表、从不演进 schema：表的 schema 与 `write.*` 属性就是契约。
- 一个从主机可达的 S3（或兼容）端点——数据文件由 ArkFlow 进程上传，不是由 catalog 上传。
- 流上保留 durability 块（S3 WAL），让未确认的行在重启后可以重放。

## 配置

| 字段 | 类型 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `catalog.uri` | string | 是 | — | REST catalog 端点，如 `http://lakekeeper:8181/catalog`。 |
| `catalog.warehouse` | string | 是 | — | Warehouse（catalog）名。 |
| `catalog.token` | string | 否 | — | 静态 bearer token。支持 `${env:VAR}`、`${file:/path}`、`${secret:...}` 引用。 |
| `namespace` | string | 是 | — | 表所在 namespace；多级 namespace 用 `.` 分隔。 |
| `table` | string | 是 | — | 表名。 |
| `exactly_once` | bool | 否 | `false` | 丢弃重放中低于已提交 Kafka 位点水位线的行。 |
| `writer_id` | string | 仅 exactly-once | — | 水位线写者身份。恰在 `exactly_once: true` 时必填；用 Kafka consumer group id。 |
| `write.target_file_size_bytes` | int | 否 | 表的 `write.target-file-size-bytes`（512 MiB） | 数据文件滚动阈值。只在一次提交内部切分文件；不决定提交节奏。 |
| `write.compression` | enum | 否 | 表的 `write.parquet.compression-codec`（snappy） | Parquet 编码：`snappy`、`gzip`、`zstd`、`lz4`。 |
| `s3.endpoint` | string | 否 | catalog 提供 | S3 端点。作为客户端属性发送、覆盖服务端默认——运行 ArkFlow 的主机必须可达。 |
| `s3.region` | string | 否 | catalog 提供 | S3 region。 |
| `s3.access_key_id` / `s3.secret_access_key` | string | 否 | 环境变量 / 实例角色 | 静态签名凭据。设置后不再查找环境；支持 secret 引用。 |
| `s3.path_style_access` | bool | 否 | `true` | 路径式寻址（MinIO 必需）。 |

## 示例

```yaml validate=fragment wrap=output
output:
  type: "iceberg"
  catalog:
    uri: "http://lakekeeper:8181/catalog"
    warehouse: "demo"
  namespace: "analytics"
  table: "user_events"
  exactly_once: true
  writer_id: "arkflow-iceberg-ingest"
  write:
    target_file_size_bytes: 33554432
    compression: "zstd"
  s3:
    endpoint: "http://minio:9000"
    region: "local-01"
    access_key_id: "${AWS_ACCESS_KEY_ID}"
    secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
    path_style_access: true
```

## 提交模型

一次调用把幸存的行编码为 Parquet 数据文件（按表的分区 spec 路由）、上传，并作为一个 fast-append 快照提交。暂态传输失败按有界预算重试；每次重试前输出会从 catalog 重载表与水位线——因为结果未知的提交（超时）可能已被服务端接受。失败的尝试会留下孤儿数据文件，由标准 Iceberg `remove_orphan_files` 维护回收。

输出在两秒内提交十次时发出限频警告：请在本输出前放 `batch` 处理器。提交时延由 catalog 往返主导；容量估算为吞吐上限 `batch.count ÷ 提交时延`、延迟下限 `batch.timeout_ms + 提交时延`。

## Exactly-once 投递

`exactly_once: true` 时，每次提交都会把按 `(topic, partition)` 的位点水位线写进快照 summary（属性 `arkflow.writer.<writer_id>.offsets`），重放中低于已恢复水位线的行被丢弃。因此"表提交之后、源确认之前"的崩溃不会产生重复。

保证仅在以下条件全部成立时有效；否则为 at-least-once：

- 每行带着 `__meta_partition`、`__meta_offset`、`__meta_ext.topic` 到达输出——SQL 投影里要保留它们，丢掉它们的投影会让写入显式失败；
- 从 Kafka input 到本输出是每分区有序的单链；
- `writer_id` 恰好对应一个 consumer group。

多个写者可以并发提交同一张表（Iceberg 乐观并发处理）；每个写者追踪自己的水位线。

## Schema 与分区

列按名字与既有表匹配。`__meta_*` 列永不写入。批次中出现不是表列的列报错；缺可选表列允许（Iceberg 数据文件可省略可选列）；缺必需列报错。值只经无损转换——整数拓宽、时间戳单位变宽、`f32 → f64`——其余失败并指明列名与两侧类型。`Timestamp(ns)` 映射到 Iceberg 的 `timestamp_ns` 类型，需要 format-v3 表。分区变换 `identity`、`void`、`year`、`month`、`day`、`hour`、`bucket(N)`、`truncate(W)` 受支持；其余在 connect 时失败。

## 表维护

与所有 Iceberg 写入端一样，本输出依赖 ArkFlow 之外的表维护：小文件合并、快照过期、孤儿文件清理。按你现有的周期执行即可。

---
components: [output/iceberg]
description: ArkFlow documentation page.
---

# Iceberg

The Iceberg output appends rows to an existing [Apache Iceberg](https://iceberg.apache.org/) table through a REST catalog. One `write_batch` call is **one atomic table append**: the output commits a snapshot and only then is the source acknowledged, so the acknowledgement never outruns the table. Between calls the output holds no buffer and runs no timer — commit cadence and data-file size are decided by the batches the pipeline delivers, in practice a [`batch`](/components/2-processors/batch) processor placed in front of this output.

## Prerequisites

- A REST catalog reachable from the host running ArkFlow (tested with [Lakekeeper](https://docs.lakekeeper.io/)).
- The table must already exist. The output never creates tables and never evolves schemas: the table's schema and its `write.*` properties are the contract.
- An S3 (or S3-compatible) endpoint reachable from the host — data files are uploaded by the ArkFlow process, not by the catalog.
- Keep a durability block (an S3 WAL) on the stream so unacknowledged rows survive restarts.

## Configuration

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `catalog.uri` | string | yes | — | REST catalog endpoint, e.g. `http://lakekeeper:8181/catalog`. |
| `catalog.warehouse` | string | yes | — | Warehouse (catalog) name. |
| `catalog.token` | string | no | — | Static bearer token. Supports `${env:VAR}`, `${file:/path}`, `${secret:...}` references. |
| `namespace` | string | yes | — | Namespace of the table; split on `.` for multi-level namespaces. |
| `table` | string | yes | — | Table name. |
| `exactly_once` | bool | no | `false` | Drop replayed rows below the committed Kafka offset watermark. |
| `writer_id` | string | exactly-once only | — | Watermark writer identity. Required exactly when `exactly_once` is `true`; use the Kafka consumer group id. |
| `write.target_file_size_bytes` | int | no | table's `write.target-file-size-bytes` (512 MiB) | Roll the data file after this many bytes. Splits files inside one commit only; it does not drive commit cadence. |
| `write.compression` | enum | no | table's `write.parquet.compression-codec` (snappy) | Parquet codec: `snappy`, `gzip`, `zstd`, `lz4`. |
| `s3.endpoint` | string | no | catalog-provided | S3 endpoint. Sent as a client property, overriding the server default — the host running ArkFlow must be able to reach it. |
| `s3.region` | string | no | catalog-provided | S3 region. |
| `s3.access_key_id` / `s3.secret_access_key` | string | no | environment / instance profile | Static signing credentials. Set them to avoid environment credential lookup; supports secret references. |
| `s3.path_style_access` | bool | no | `true` | Path-style addressing (required by MinIO). |

## Example

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

## Commit model

One call to the output encodes the surviving rows into Parquet data files (routed by the table's partition spec), uploads them, and commits them as one fast-append snapshot. Transient transport failures are retried with a bounded budget; before every retry the output reloads the table and its watermark from the catalog, because a commit whose outcome is unknown (timeout) may have been accepted server-side. A failed attempt can leave orphan data files behind — standard Iceberg `remove_orphan_files` maintenance reclaims them.

The output emits a rate-limited warning when it commits ten times within two seconds: put a `batch` processor in front of the output. Commit latency is dominated by the catalog round trip; sizing guidance is `batch.count ÷ commit latency` for the throughput ceiling and `batch.timeout_ms + commit latency` for the latency floor.

## Exactly-once delivery

With `exactly_once: true` every commit stores a per-`(topic, partition)` offset watermark in the snapshot summary (property `arkflow.writer.<writer_id>.offsets`), and rows whose offset is below the recovered watermark are dropped on replay. A crash between the table commit and the source acknowledgement therefore produces no duplicates.

The guarantee holds only when all of the following are true; otherwise delivery is at-least-once:

- every row reaches the output with `__meta_partition`, `__meta_offset`, and `__meta_ext.topic` — keep them in any SQL projection, a projection that drops them fails the write explicitly;
- the path from the Kafka input to this output is an ordered single chain per partition;
- `writer_id` identifies exactly one consumer group.

Multiple writers may commit to the same table concurrently (Iceberg's optimistic concurrency handles it); each writer tracks its own watermark.

## Schema and partitions

Columns are matched by name against the existing table. `__meta_*` columns are never written. A batch column that is not a table column is an error; a missing optional table column is allowed (Iceberg data files may omit optional columns); a missing required column is an error. Values convert only through lossless casts — widening integers, widening timestamp units, `f32 → f64` — and anything else fails with the column and both types named. `Timestamp(ns)` maps to the Iceberg `timestamp_ns` type, which requires a format-v3 table. Partition transforms `identity`, `void`, `year`, `month`, `day`, `hour`, `bucket(N)`, and `truncate(W)` are supported; anything else fails at connect.

## Table maintenance

Like every Iceberg writer, this output relies on table maintenance performed outside ArkFlow: compaction for small files, snapshot expiry, and orphan-file cleanup. Run them on your usual schedule.

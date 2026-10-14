# Baseline（优化前基线，2026-10-13）

## 环境

- Apple M1 Pro（10 核），32 GB RAM，macOS 14.8.9（23J631）
- rustc 1.97.1（8bab26f4f 2026-07-14），release profile（lto=true, codegen-units=1）
- 仓库 HEAD：`66a2142e`（main，改动落地前）
- 命令：`cargo test --release -p arkflow-plugin --lib -- --ignored --nocapture`（仅取计时测试；同批 `--ignored` 触发的 influxdb/pgvector live-service 测试因无真实服务失败，与本基线无关）；`cargo run -p arkflow --release --example benchmark`

## 计时测试（200k 行，取 3 轮最优）

### json_decode_timing

| 路径 | 耗时 | 吞吐 |
| --- | --- | --- |
| 新管线 4 列 | 133.08 ms | 1,502,911 rows/s |
| 旧路径 4 列 | 212.36 ms | 941,790 rows/s |
| 新管线 200 列宽 | 6.485 s | 30,840 rows/s |
| 旧路径 200 列宽 | 8.381 s | 23,865 rows/s |

### kafka_batch_assembly_timing

| 路径 | 耗时 | 吞吐 |
| --- | --- | --- |
| 批组装（decode_and_attach） | 23.12 ms | 8,651,611 rows/s |
| 逐条元数据链（oracle） | 1.522 s | 131,365 rows/s |

### protobuf_batch_decode_timing

| 路径 | 耗时 | 吞吐 |
| --- | --- | --- |
| 逐条 + normalize_and_concat | 3.366 s | 59,425 rows/s |
| 列式 batch converter | 163.83 ms | 1,220,789 rows/s |

## benchmark 套件（count=200000, warmup=1, runs=3，取最优）

| 场景 | 吞吐 |
| --- | --- |
| linear-sql | 670,686 rows/s |
| groupby-sql | 817,196 rows/s |
| filter-project-sql | 767,643 rows/s |
| codec-json | 2,380 batches/s |
| json-decode | 2,610 batches/s |
| state-backend | 397 ops/s |
| avro-decode-w5 | 1,217,609 rows/s |
| avro-decode-w25 | 391,440 rows/s |
| avro-decode-w100 | 108,467 rows/s |

## 备注

- 本 change 新增的 `protobuf_batch_encode_timing`（任务 2.3）在优化前尚不存在，其"每 cell set_field_by_name 基线"档位落地后以同机同口径补采，作为 encode 侧对照。
- 前端 generate 路径（linear/groupby/filter-project 三场景）当前为逐条 clone + 逐条解码形态，D4 落地后预期前端失真消除，三场景数字应上移。

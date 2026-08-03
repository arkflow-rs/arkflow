---
sidebar_position: 1
---

# Processing guides

ArkFlow transforms data inside a pipeline through an ordered chain of
processors. This section groups in-depth guides for each processing path:

- **SQL** — query, filter, aggregate, and join batches with DataFusion.
- **VRL** — safe, fast field-level transformation with Vector Remap Language.
- **Python UDFs** — custom logic over Arrow `RecordBatch` via PyO3.
- **Codecs** — decode/encode byte payloads (JSON, Protobuf, Debezium CDC,
  Schema Registry) at the input/output boundary.
- **Joins & windows** — correlate multiple sources and windowed aggregation.

Start with [VRL](./vrl.md), [Python UDFs](./python-udf.md), [SQL
processing](./sql-processing.md), or [codecs and joins](./codecs-and-joins.md).

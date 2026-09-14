---
sidebar_position: 3
---

# Message metadata

Inputs may attach **metadata columns** to every `MessageBatch` so that
downstream processors (especially SQL) can inspect where a message came from.
All metadata columns are prefixed with `__meta_` and are regular Arrow columns —
you select or filter on them like any other field.

| Column | Description |
|--------|-------------|
| `__meta_source` | Source identifier (which input produced the message). |
| `__meta_partition` | Partition number, for partitioned sources (e.g. Kafka). |
| `__meta_offset` | Offset / position within the partition. |
| `__meta_key` | Message key (where the source carries one, e.g. Kafka). |
| `__meta_timestamp` | Timestamp from the source. |
| `__meta_ingest_time` | When ArkFlow ingested the message. |
| `__meta_ext` | Extended key/value metadata as a `Map<String, String>` column. |

## Using metadata in SQL

Because metadata columns are ordinary columns, you can project and filter on
them directly:

```yaml
pipeline:
  processors:
    - type: "sql"
      query: |
        SELECT
          *,
          __meta_source    AS source,
          __meta_partition AS partition,
          __meta_offset    AS offset,
          __meta_timestamp AS message_time
        FROM flow
```

Filtering on partition/offset is useful for replay or auditing:

```sql
SELECT id, name
FROM flow
WHERE __meta_partition = 0 AND __meta_offset >= 100
```

## Notes

- Not every input populates every column; a source that has no notion of
  partition (HTTP, file, generate) leaves the irrelevant columns unset.
- `__meta_ext` is a `MapArray`, so individual keys are accessed with the
  appropriate map access in SQL rather than as plain columns.
- Metadata columns are also the basis for the at-least-once ack boundary
  (offsets are committed only after the output confirms the write).

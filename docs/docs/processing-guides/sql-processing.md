---
sidebar_position: 4
---

# SQL processing

The SQL processor evaluates DataFusion SQL against the current Arrow batch.
Use it for projection, filtering, aggregation, window functions, and joins.

```yaml
pipeline:
  processors:
    - type: sql
      query: 'SELECT sensor, AVG(value) AS mean FROM input GROUP BY sensor'
```

For cross-source joins, combine a window buffer or temporary resource with the
[SQL reference](../sql/2-select.md) and [SQL processor](../components/2-processors/sql.md).

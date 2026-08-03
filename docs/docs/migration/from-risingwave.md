---
sidebar_position: 5
---

# From RisingWave

Use ArkFlow when a pipeline and sink are sufficient, rather than a distributed
streaming database with continuously maintained materialized views. Convert a
source query into an input plus SQL processor, then choose an explicit output.

```yaml
pipeline:
  processors:
    - type: sql
      query: "SELECT customer_id, SUM(amount) AS total FROM input GROUP BY customer_id"
output: {type: stdout}
```

Retain RisingWave when distributed state, horizontal scaling, or database
semantics are part of the requirement; ArkFlow deliberately stays single-node.

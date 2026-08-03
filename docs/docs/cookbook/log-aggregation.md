---
sidebar_position: 4
---

# Log aggregation

Use a file or HTTP input, parse JSON with `json_to_arrow`, enrich with VRL,
then route to Kafka or stdout. Keep source and service fields so downstream
queries can group logs without reparsing the payload.

```yaml
pipeline:
  processors:
    - type: json_to_arrow
    - type: vrl
      statement: '.ingest_source = "edge"; .'
```

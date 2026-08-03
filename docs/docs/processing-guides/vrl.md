---
sidebar_position: 2
---

# VRL processing

Use the `vrl` processor for safe field-level enrichment and reshaping.

```yaml
pipeline:
  processors:
    - type: vrl
      statement: '.severity = "info"; .value = to_int!(.value); .'
```

Keep the final expression as the record to emit and validate the complete
pipeline with `arkflow --config pipeline.yaml --validate`. See the [VRL
component reference](../components/2-processors/vrl.md).

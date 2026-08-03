---
sidebar_position: 3
---

# From Vector

Vector's sources and sinks map naturally to ArkFlow inputs and outputs. Move
VRL transforms into a `vrl` processor and use `json_to_arrow` when the source
payload is JSON.

```yaml
pipeline:
  processors:
    - type: json_to_arrow
    - type: vrl
      statement: '.service = "orders"; .'
```

ArkFlow's columnar batch model enables SQL projections and joins in addition
to row-level VRL transforms.

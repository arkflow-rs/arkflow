---
sidebar_position: 3
---

# Python UDF processing

The Python processor runs a user function over Arrow batches. Keep the
function deterministic and avoid network calls in the hot path.

```yaml
pipeline:
  processors:
    - type: python
      module: examples.udf
      function: enrich
```

```python
def enrich(batch):
    batch["normalized"] = batch["value"] * 2
    return batch
```

Use the [Python component reference](../components/2-processors/python.md) for
the supported module and function configuration.

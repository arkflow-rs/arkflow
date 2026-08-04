---
description: ArkFlow documentation page.
---

# Drop

The Drop output discards every message it receives without performing any I/O. It is useful for performance benchmarks, dead-end pipelines, and testing.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | Fixed value `"drop"` |

The Drop output accepts no other fields; a codec may be attached but is ignored.

## Examples

```yaml
output:
  type: "drop"
```

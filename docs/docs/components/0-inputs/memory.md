---
sidebar_label: Memory
---

# Memory

The Memory input reads messages from an in-memory queue that can be pre-seeded with initial messages in configuration. Mainly used for testing and development.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | Constant value `"memory"` |
| messages | array&lt;string&gt; | no | — | Initial list of messages enqueued at startup |

## Examples

```yaml
input:
  type: "memory"
  messages:
    - "Hello"
    - "World"
```

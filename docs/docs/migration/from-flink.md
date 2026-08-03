---
sidebar_position: 4
---

# From Flink

Use ArkFlow buffers for local tumbling, sliding, and session windows and use
the SQL processor for batch queries. A direct translation is intentionally
limited: ArkFlow is a single-node process and does not provide Flink's
distributed checkpoints, rescaling, or cluster-wide state.

```yaml
buffer: {type: tumbling_window, interval: 10s}
pipeline: {processors: [{type: sql, query: "SELECT key, COUNT(*) FROM input GROUP BY key"}]}
```

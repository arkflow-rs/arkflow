---
components: [batch]
description: ArkFlow 文档页面。
---

# 批次(Batch)

批次(Batch)处理器会累积传入的消息批次,当达到所配置的消息数量或超时时间到达时,将它们作为单个合并后的批次刷出。它适用于将小批次归并在一起,以提升下游吞吐量。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `batch` |
| count | integer | yes | — | 刷出前需要累积的消息批次数量。 |
| timeout_ms | integer | yes | — | 刷出不完整批次前等待的最长时间(毫秒)。 |

## 示例

```yaml validate=fragment wrap=processors
- type: "batch"
  count: 1000
  timeout_ms: 1000
```

```yaml validate=full
streams:
  - input:
      type: "memory"
      messages:
        - '{ "value": 1 }'
        - '{ "value": 2 }'
    pipeline:
      thread_num: 4
      processors:
        - type: "batch"
          count: 100
          timeout_ms: 500
    output:
      type: "stdout"
```

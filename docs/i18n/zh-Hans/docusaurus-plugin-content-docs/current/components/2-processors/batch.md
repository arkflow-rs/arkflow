---
components: [batch]
description: ArkFlow 文档页面。
---

# 批次(Batch)

批次(Batch)处理器会累积传入的消息批次,当达到所配置的消息数量或超时时间到达时,将它们作为单个合并后的批次刷出。它适用于将小批次归并在一起,以提升下游吞吐量。

超时触发不依赖下一条消息到达:`timeout_ms` 到期且无新输入时,部分批次会在下一个空闲 tick 被刷出。当上游结束(优雅关停或有界源到达末尾)时,不足 `count` 的部分批次会被排空并继续下发,而不是被丢弃;若管线在中途被取消,仍滞留在缓冲中的消息无法送达,将以警告日志记录后丢弃。

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

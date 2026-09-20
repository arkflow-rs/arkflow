---
components: [buffer/memory]
description: ArkFlow 文档页。
---

# 内存缓冲(Memory)

内存缓冲(Memory buffer)是一种内存中的消息(Message)队列:它累积传入的消息批次(Batch),并在达到容量阈值或超时后,把它们作为一个合并后的批次一起释放。它能平滑流量峰值,并在下游处理跟不上时提供背压(Backpressure)。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `memory` |
| capacity | integer | yes | — | 刷新前允许累积的最大消息批次数。 |
| timeout | duration | yes | — | 刷新已累积批次前的最长等待时间,即使尚未达到 `capacity` 也会触发刷新。示例:`1ms`、`1s`、`1m`、`1h`。 |

## 示例

```yaml validate=fragment wrap=buffer
buffer:
  type: "memory"
  capacity: 100
  timeout: "1s"
```

```yaml validate=full
streams:
  - input:
      type: "generate"
      context: '{ "value": 1 }'
      interval: 100ms
      batch_size: 1
    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
    buffer:
      type: "memory"
      capacity: 100
      timeout: "1s"
    output:
      type: "stdout"
```

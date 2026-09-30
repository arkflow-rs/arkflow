---
components: [buffer/memory]
description: ArkFlow 文档页。
---

# 内存缓冲(Memory)

内存缓冲(Memory buffer)是一种内存中的消息(Message)队列:它累积传入的消息批次(Batch),并在达到容量阈值或超时后,把它们作为一个合并后的批次一起释放。它能平滑流量峰值,并在下游处理跟不上时提供背压(Backpressure):当缓冲持有 `capacity` 条消息后,后续写入会等待读者排空再继续,慢消费沿链路向上游传播,而不是让缓冲无界增长。

合并失败不会丢弃已持有的消息:若累积的批次无法合并(例如 schema 不兼容),队列会原样保留,读取以错误返回,后续仍可重试。关闭时读者仍会先排空余量再结束流;`flush` 只唤醒读者,超时释放在此之后仍然有效。

> **注意:** 在当前的统一执行内核中,流配置里的 `buffer: { type: "memory" }` 出于兼容性被接受,但会被编译为 no-op——阶段间的有界通道已经提供了缓冲与背压。本页描述的语义是组件本身的契约,适用于通过插件注册表直接使用 memory buffer 的场景。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `memory` |
| capacity | integer | yes | — | 允许累积的最大消息条数。持有量达到该值后,写入会等待(背压),直到读者释放。 |
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

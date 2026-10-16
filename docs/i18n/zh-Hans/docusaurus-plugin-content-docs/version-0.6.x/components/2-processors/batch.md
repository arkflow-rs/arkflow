---
components: [batch]
description: ArkFlow 文档页面。
---

# 批次(Batch)

批次(Batch)处理器会累积传入的消息批次,当达到所配置的消息**行数**或超时时间到达时,将它们作为单个合并后的批次刷出。它适用于将小批次归并在一起,以提升下游吞吐量。

超时触发不依赖下一条消息到达:`timeout_ms` 到期且无新输入时,部分批次会在下一个空闲 tick 被刷出。当上游结束(优雅关停或有界源到达末尾)时,不足 `count` 的部分批次会被排空并继续下发,而不是被丢弃。

## 投递确认

缓冲期间投递的确认是延迟结算的,而非到达即结算:行驻留缓冲时处理器返回 `Deferred`,源位点(Kafka offset、WAL 序列)不会提交。flush 发射合并批时,该次发射携带覆盖全部缓冲投递的组合确认,只有下游成功写出后源位点才提交(at-least-once:崩溃后未提交窗口会被重放)。写出失败时,失败补偿(`undo`/`abort`)会传导至每个缓冲投递。管线中途被取消时,仍滞留缓冲的消息无法送达:其确认会被 abort,恢复后由源重放(并记录警告日志)。

由于提交点后移到 flush,`count` 大于源在途窗口时会与背压交互:上游投递会被(有界通道)节流,而不是无感地持续缓冲——这正是持久性正确的方向。

## 合并语义

flush 合并采用 schema 归一:输出列取输入列名的并集,缺失列以 null 填充,行按列名对齐(而非按位置)。同名列出现两种类型(例如 `Int64` 与 `Utf8`)时,flush 以显式错误失败,不会静默强转或错列。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `batch` |
| count | integer | yes | — | 刷出前需要累积的消息**行数**(而非输入批数)。 |
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

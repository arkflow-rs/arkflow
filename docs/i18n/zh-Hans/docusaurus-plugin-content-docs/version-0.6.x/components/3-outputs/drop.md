---
components: [drop]
description: ArkFlow 文档页面。
---

# Drop

Drop 输出(Output)丢弃它收到的每条消息(Message),不执行任何 I/O。它适用于性能基准测试、死端流水线(Pipeline)与测试。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | 固定值 `"drop"` |

Drop 输出不接受其他字段;可以附加编解码器(Codec),但会被忽略。

## 示例

```yaml validate=fragment wrap=output
output:
  type: "drop"
```

---
components: [generate]
sidebar_label: Generate
---

# 生成(Generate)

生成输入(Input)以固定的时间间隔产生合成文本消息(Message),主要用于测试、演示和基准测试。每次读取返回 `batch_size` 份相同的 `context` 字符串。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"generate"` |
| context | string | yes | — | 每条消息的载荷文本 |
| interval | duration | yes | — | 两次读取之间的间隔,如 `1ms`、`1s`、`1m`(首次读取立即执行) |
| count | integer | no | — | 消息总数;达到后返回 EOF。未设置表示无限制 |
| batch_size | integer | no | `1` | 每次读取返回的消息数 |

## 示例

```yaml validate=fragment wrap=input
input:
  type: "generate"
  context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
  interval: 1ms
  batch_size: 1000
  count: 10000
```

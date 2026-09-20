---
components: [stdout]
description: ArkFlow 文档页面。
---

# Stdout

Stdout 输出(Output)将每条消息(Message)的载荷写入标准输出。它适用于调试、演示与小型本地流水线(Pipeline)。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type  | string | yes | — | 固定值 `"stdout"` |
| append_newline | boolean | no | `true` | 是否在每条消息后追加换行符。 |

## 示例

```yaml validate=fragment wrap=output
output:
  type: "stdout"
  append_newline: true
```

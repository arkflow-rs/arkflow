---
components: [input/memory]
sidebar_label: Memory
---

# 内存(Memory)

内存输入(Input)从内存队列读取消息(Message),该队列可以在配置中预先填充初始消息。主要用于测试和开发。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"memory"` |
| messages | array&lt;string&gt; | no | — | 启动时入队的初始消息列表 |

## 示例

```yaml validate=fragment wrap=input
input:
  type: "memory"
  messages:
    - "Hello"
    - "World"
```

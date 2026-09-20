---
components: [multiple_inputs]
sidebar_label: Multiple Inputs
---

# 多输入(Multiple Inputs)

多输入将多个独立的输入(Input)组件合并为一条逻辑流(Stream)。所有子输入并发读取,消息(Message)按到达顺序进入同一条流水线(Pipeline)。每个子输入可以携带一个 `name`,该名称会写入 `__meta_source`,以便下游阶段区分消息来源。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | 常量值 `"multiple_inputs"` |
| inputs | array&lt;object&gt; | yes | — | 子输入配置数组;元素结构见下文 |

### inputs[]

每个元素都是一个标准的输入配置(带有自己的 `type` 和字段),外加一个可选的 `name`:

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | 子输入类型,如 `kafka`、`http` |
| name | string | no | 该数据源的逻辑名称;非空且全局唯一时,会写入 `__meta_source` |
| ... | ... | ... | 该输入类型特有的配置字段 |

## 示例

```yaml validate=fragment wrap=input
input:
  type: "multiple_inputs"
  inputs:
    - name: "kafka_source"
      type: "kafka"
      brokers: ["localhost:9092"]
      topics: ["topic1"]
      consumer_group: "group1"
      start_from_latest: false
    - name: "http_api"
      type: "http"
      address: "0.0.0.0:8080"
      path: "/webhook"
```

## 说明

- 每个非空的 `name` 必须唯一;名称重复或为空会导致构建失败。
- 若任一子输入返回 `EOF` 或 `Disconnection`,该子流即结束,其余子输入继续运行。

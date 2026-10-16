---
components: [multiple_inputs]
sidebar_label: Multiple Inputs
---

# 多输入(Multiple Inputs)

多输入将多个独立的输入(Input)组件合并为一条逻辑流(Stream)。所有子输入并发读取,消息(Message)按到达顺序进入同一条流水线(Pipeline)。每个子输入可以携带一个 `name`,该名称会写入 `__meta_source`,以便下游阶段区分消息来源。

消息经一条有界的内部通道转发(容量 1024,与流水线阶段间通道同一上限):当下游处理跟不上时,子输入读任务在发送处等待,慢消费沿链路向源传导背压,而不是让内存无界增长。

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
- 若任一子输入返回 `EOF`,整个合并输入按流结束处理(流水线正常收尾);任一子输入返回 `Disconnection` 则触发整个输入的重连——所有子输入都会重连并重启读任务。
- 引擎重连本输入(如收到 `Disconnection` 后)时,会先停止并等待上一代子读任务退出,再生成新的一代——任意时刻每个子输入至多被一个任务读取,重连不会造成重复投递。
- 子输入的错误会单次上浮给流水线(该子输入的读任务随即退出);重试与重连的决策由引擎的输入错误处理负责。

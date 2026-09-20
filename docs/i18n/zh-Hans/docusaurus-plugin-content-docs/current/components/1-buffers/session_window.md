---
components: [session_window]
description: ArkFlow 文档页。
---

# 会话窗口(Session Window)

会话窗口(Session Window)缓冲按活动间隙把消息(Message)划分成会话。新消息会延长当前会话;若在配置的 `gap` 时长内没有消息到达,该会话即关闭,其累积的全部消息会作为一个批次(Batch)发出。可选的 `join` 配置允许你在发出时跨多个输入(Input)源执行 SQL 连接(Join)。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `session_window` |
| gap | duration | yes | — | 会话内消息之间的最大空闲时间。当 `gap` 时长耗尽且没有新消息时,会话被刷新。示例:`1ms`、`1s`、`1m`、`1h`。 |
| join | object | no | — | 应用于发出批次的可选 SQL 连接配置。 |

### `join`

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| query | string | yes | — | 对来自不同输入源的批次数据执行连接的 SQL 查询。 |
| value_field | string | no | — | 保存消息载荷的二进制字段的名称。默认为引擎默认的二进制值字段。 |
| codec | object | yes | — | 在连接前用于解码消息批次的编解码器(Codec)。 |
| thread_num | integer | no | — | 连接期间用于并行解码的工作线程数。 |

`codec` 字段是一个 `CodecConfig` 对象:一个选择编解码器的 `type` 字符串,外加编解码器专属的其他字段。

## 示例

### 基本配置

```yaml validate=fragment wrap=buffer
buffer:
  type: "session_window"
  gap: "5s"
```

### 带 Join 配置

```yaml validate=foreign reason="legacy join configuration — join buffers cannot compile to the unified kernel; declare a Job DAG with an explicit join operator"
buffer:
  type: "session_window"
  gap: "10s"
  join:
    query: "SELECT a.user_id, a.event_type, b.metadata FROM events a JOIN metadata b ON a.user_id = b.user_id"
    codec:
      type: "json"
```

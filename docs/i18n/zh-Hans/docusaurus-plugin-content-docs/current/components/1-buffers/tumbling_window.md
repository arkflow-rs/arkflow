---
components: [tumbling_window]
description: ArkFlow 文档页。
---

# 滚动窗口(Tumbling Window)

滚动窗口(Tumbling Window)缓冲把消息(Message)划分到固定大小、互不重叠的时间窗口(Window)中。每当配置的 `interval` 到期,当前窗口内累积的全部消息都会作为一个批次(Batch)一起发出;每条消息恰好属于一个窗口。旧式 `join` 选项仍然存在,但流编译器会拒绝声明了它的窗口缓冲(见下方警告)。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `tumbling_window` |
| interval | duration | yes | — | 每个窗口的固定时长。时长耗尽时,发出所有累积的消息。示例:`1ms`、`1s`、`1m`、`1h`。 |
| join | object | no | — | 旧式 SQL 连接配置;会被流编译器拒绝(见下方警告)。 |

:::warning 旧式 `join` —— 被流编译器拒绝

在 `tumbling_window` 缓冲上声明 `join` 会在编译时报错
`legacy window buffers with 'join' cannot compile`。Stream 配置不支持流-流
连接;请改用 SQL 处理器针对临时表的逐批次连接。

:::

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
  type: "tumbling_window"
  interval: "1s"
```

### 带 Join 配置

```yaml validate=foreign reason="legacy join configuration — stream-stream join is not yet supported; use the SQL processor's per-batch joins against temporary tables instead"
buffer:
  type: "tumbling_window"
  interval: "5s"
  join:
    query: "SELECT a.id, a.name, b.value FROM input1 a JOIN input2 b ON a.id = b.id"
    codec:
      type: "json"
```

---
components: [mongodb]
description: ArkFlow MongoDB 输出组件。
---

# MongoDB

MongoDB 输出(Output)将每一行 Arrow 数据写为已配置 MongoDB 集合(Collection)中的一个 BSON 文档。列名即文档字段名,标量值与 null 均被保留。

## 配置

| Field | Type | Required | 描述 |
|-------|------|----------|-------------|
| type | string | yes | 固定值 `"mongodb"`。 |
| uri | string | yes | MongoDB 连接 URI,按需包含认证/选项。 |
| database | string | yes | 目标数据库名称。 |
| collection | string | yes | 目标集合名称。 |

该输出支持 UTF-8 字符串、可放入 BSON int64 的有符号与无符号整数、浮点值、布尔值、二进制值以及 null。初始版本不支持嵌套 Arrow 值与编解码器(Codec)。

## 示例

```yaml validate=fragment wrap=output
output:
  type: "mongodb"
  uri: "mongodb://localhost:27017"
  database: "arkflow"
  collection: "events"
```

该输出在接受写入前会先连接并 ping MongoDB。每个非空消息批次(Batch)通过一次批量插入操作发送。

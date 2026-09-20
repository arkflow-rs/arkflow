---
sidebar_position: 1
description: 从 Kafka 消费 JSON 消息,并追加写入 SQL 数据库。
---

# 实战:消费 Kafka 并写入 SQL

用一条 ArkFlow 流(Stream),把 Kafka 主题中的 JSON 事件搬进 MySQL(或 PostgreSQL/SQLite)表。

**前提条件**

- Kafka 可达 `localhost:9092`,且存在主题 `app.events`,其中包含 JSON 消息(Message)
- MySQL 可达,已有数据库 `arkflow` 以及与事件列匹配的表(ArkFlow 只追加行,不会建表)
- 已[安装](/zh-Hans/docs/get-started/install) ArkFlow

## 配置

保存为 `kafka-to-sql.yaml`(CI 中同时以 `examples/case_order_stream_sql.yaml` 完成校验):

```yaml validate=fragment wrap=engine
logging:
  level: info

streams:
  - id: events-to-mysql
    input:
      type: kafka
      brokers:
        - localhost:9092
      topics:
        - app.events
      consumer_group: arkflow-events
      start_from_latest: true

    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow"
        - type: arrow_to_json

    output:
      type: sql
      output_type:
        type: "mysql"
        uri: "mysql://user:password@localhost:3306/arkflow"
      table_name: "events"

    error_output:
      type: stdout
```

处理链永远是同样的三步:把 JSON 负载**解码**(decode)为 Arrow(`json_to_arrow`),按需用 SQL **变换**(transform),再为下游**重新编码**(`arrow_to_json`)。`error_output` 会捕获格式错误的消息,这样一条坏记录不会让整条流停下来。

## 运行与验证

```bash
./target/release/arkflow --config kafka-to-sql.yaml
```

生产一条测试消息:

```bash
echo '{"order_id": 1, "amount": 42}' | kafka-console-producer \
  --bootstrap-server localhost:9092 --topic app.events
```

预期结果:`arkflow.events` 表中出现一行 `order_id = 1` 的记录。格式错误的 JSON(试试生产一条 `not-json`)会经由错误输出进入引擎日志,而不会让流停止。

## 故障排查

- **收不到数据** —— 检查消费者组里是否还有其他活跃成员占着分区,以及 `start_from_latest` 是否在跳过既有消息。
- **MySQL 连接被拒** —— URI 的格式是 `mysql://user:password@host:port/database`;先用任意 SQL 客户端确认可达性。
- 关于下游失败时的投递保证,参见[投递语义](/zh-Hans/docs/build/delivery-semantics)。

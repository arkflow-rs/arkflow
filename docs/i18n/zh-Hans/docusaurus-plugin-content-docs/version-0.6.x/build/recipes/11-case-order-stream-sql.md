---
sidebar_position: 11
description: 端到端案例——来自 Kafka 的店铺订单事件经过滤后追加写入 MySQL。
---

# 案例:订单流写入 MySQL

一家在线商店在 Kafka 主题 `shop.orders` 上以 JSON 发出订单事件(创建、支付、发货)。分析团队希望把已支付的订单追加写入一张 MySQL 表,供 BI 看板使用。测试事件(`status != 'PAID'`)与格式错误的消息绝不能进表——也绝不能让流水线停摆。

## 需求

- 在事件抵达数据库之前完成过滤
- 坏消息进入错误路径,而不是让流失败
- 行以接近实时的方式写入;崩溃后的重复可以容忍(下游对目标表按主键 upsert)

## 架构

```
┌──────────┐  JSON   ┌───────────┐   filter    ┌───────────┐  append  ┌───────┐
│  Shop    │────────▶│   Kafka   │────────────▶│  ArkFlow  │─────────▶│ MySQL │
│ services │         │  topic    │  + decode   │ sql + json│          │ table │
└──────────┘         └───────────┘             └───────────┘          └───────┘
                                                        │ malformed
                                                        ▼
                                                   ┌────────┐
                                                   │ stdout │  (error path)
                                                   └────────┘
```

## 配置

已校验示例:[`examples/case_order_stream_sql.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/case_order_stream_sql.yaml)

```yaml validate=full
streams:
  - id: orders-to-mysql
    input:
      type: kafka
      brokers:
        - localhost:9092
      topics:
        - shop.orders
      consumer_group: arkflow-orders
      start_from_latest: true

    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow WHERE status = 'PAID'"
        - type: arrow_to_json

    output:
      type: sql
      output_type:
        type: "mysql"
        uri: "mysql://root:1234@localhost:3306/arkflow"
      table_name: "orders"

    error_output:
      type: stdout
```

## 运行与预期结果

```bash
./target/release/arkflow --config examples/case_order_stream_sql.yaml --validate
./target/release/arkflow --config examples/case_order_stream_sql.yaml
```

向 `shop.orders` 生产 `{"order_id": 7, "status": "PAID", "amount": 42}`:`arkflow.orders` 中会出现一行 `order_id = 7`。生产 `{"order_id": 8, "status": "TEST"}`:没有行出现(被过滤)。生产垃圾数据:什么都不会停;该记录经由错误输出记入日志。

## 权衡与变体

- 在写入端之前加一个[窗口缓冲](./3-windowed-aggregation.md),把插入批量化,减少数据库往返。
- 同一条流水线也可对接 PostgreSQL 或 SQLite——只需修改 `output_type`(参见[SQL 输出参考](/zh-Hans/docs/components/outputs/sql))。
- 若要严格不重复的写入,可使用 Kafka 的事务性[精确一次](/zh-Hans/docs/build/exactly-once)输出,并配合 UPSERT 目标表。

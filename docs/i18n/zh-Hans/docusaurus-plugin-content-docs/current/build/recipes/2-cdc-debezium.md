---
sidebar_position: 2
description: 通过 Debezium 采集数据库变更事件,支持 JSON 或 Schema Registry 编码。
---

# 实战:通过 Debezium 采集变更数据捕获(CDC)数据

借助 [Debezium](https://debezium.io/),把数据库的行级变更以流的方式送入 ArkFlow:Debezium 把变更事件写到 Kafka,ArkFlow 再把每个信封(envelope)解码成可以用 SQL 查询的列式批次(Batch)。

**前提条件**

- 一个 Debezium Kafka Connect 连接器,向 `shop.users` 之类的主题发布变更(参见 [Debezium 连接器文档](https://debezium.io/documentation/))
- Kafka 可达 `localhost:9092`
- 可选:一个位于 `localhost:8081` 的 Confluent Schema Registry(仅用于 Protobuf 消息)

## JSON 编码的信封

流水线就是一个带 `debezium_json` **输入编解码器(Codec)** 的 Kafka 输入,信封在流水线运行之前就被解码。完整且经过 CI 校验的配置见 [`examples/cdc_debezium.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/cdc_debezium.yaml):

```yaml validate=full
streams:
  - input:
      type: kafka
      brokers:
        - localhost:9092
      topics:
        - shop.users
      consumer_group: arkflow-cdc
      start_from_latest: true
      codec:
        type: debezium_json

    pipeline:
      thread_num: 4
      processors:
        - type: sql
          query: |
            SELECT op, id, name, source_db, source_table, ts_ms FROM flow

    output:
      type: stdout
```

解码后的批次包含业务列(取自 `after`,删除时取自 `before`),外加 `op`、`ts_ms`、`source_db`、`source_table`、`before` 和 `source`。

## 经由 Schema Registry 的 Protobuf 信封

当 Debezium 配置了 Confluent Protobuf 转换器时,改用 `schema_registry` 编解码器——对应 `examples/howto_cdc_schema_registry.yaml`,已通过校验:

```yaml validate=fragment wrap=codec
      codec:
        type: schema_registry
        registry_url: "http://localhost:8081"
        message_type: "shop.Users"
```

## 运行与验证

```bash
./target/release/arkflow --config cdc.yaml
```

在源数据库中更新一行。预期结果:stdout 打印一条 `op: u`(更新)记录,带有新的列值;删除操作产生 `op: d`,值来自 `before`。

## 投递语义

CDC 的偏移量就是 Kafka 输入由确认(ack)把关的偏移量——**至少一次(at-least-once)**。崩溃后事件可能被重放,因此下游要保持幂等(例如按主键 UPSERT)。参见[投递语义](/zh-Hans/docs/build/delivery-semantics)。

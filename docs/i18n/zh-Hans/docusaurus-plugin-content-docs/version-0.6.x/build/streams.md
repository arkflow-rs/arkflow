---
sidebar_position: 2
title: 流(Streams)
description: ArkFlow 流的解剖——输入、处理器、输出与错误输出。
---

# 流(Streams)

一条流是对一条数据路径的声明式 YAML 描述:**输入**消费记录,可选的**流水线**对记录做变换,**输出**投递结果。一切都运行在统一执行内核上——同一份 YAML 会编译为 `JobSpec`,因此本文所讲的内容同样适用于[作业](./jobs.md)。

## 解剖

```yaml validate=full
streams:
  - id: orders-to-mysql        # unique stream id
    input:
      type: kafka              # registered input type
      brokers: [localhost:9092]
      topics: [shop.orders]
      consumer_group: arkflow-orders
      start_from_latest: false
    pipeline:
      thread_num: 4            # parallel processing chains
      processors:
        - type: json_to_arrow  # decode into columnar batches
        - type: sql            # DataFusion SQL over the batch
          query: "SELECT * FROM flow WHERE status = 'PAID'"
        - type: arrow_to_json  # re-encode for the sink
    output:
      type: sql
      output_type:
        type: mysql
        uri: mysql://root@localhost:3306/arkflow
      table_name: orders
    error_output:              # side sink for poisoned records
      type: stdout
```

它今天就能跑:仓库中的 `examples/case_order_stream_sql.yaml` 正是这条流水线,并且由 CI 校验。

## 四个阶段

### 输入

`input` 块通过 `type` 选择一个已注册的输入组件,并传入该组件的专属选项。ArkFlow 内置 13 种输入——Kafka、HTTP、MQTT、NATS、Pulsar、Redis、SQL、文件、WebSocket、Modbus 等。每种输入在自己的组件页上记录配置 schema,所有 schema 都可以通过 `arkflow components show input <type>` 离线获取。

### 流水线

`processors` 是在融合算子链内执行的有序链条。记录是列式的 `MessageBatch`(Arrow `RecordBatch`);需要字节的处理器在边界处借助[编解码器](/zh-Hans/docs/components/codecs/json)。典型的链条是:解码、用 [SQL](/zh-Hans/docs/components/processors/sql) 或 [VRL](/zh-Hans/docs/components/processors/vrl) 变换、再重新编码。`thread_num` 控制并行处理批次的链条数;顺序保证见[背压与有序投递](/zh-Hans/docs/build/backpressure)。

窗口(滚动、滑动、会话)以缓冲形式挂载——参见[缓冲组件](/zh-Hans/docs/components/buffers/tumbling_window)与[窗口聚合配方](/zh-Hans/docs/build/recipes/windowed-aggregation)。窗口缓冲是有状态的:旧式流在可接受状态丢失时应声明 `state.durability: ephemeral`;若窗口必须在重启后恢复,则使用带检查点策略的 `jobs` 条目。

### 输出

`output` 块投递每个处理完的批次。Kafka 输出支持精确一次事务;SQL 输出批量插入;所有输出见[组件目录](/zh-Hans/docs/components)。投递保证按流设置——从[投递语义](/zh-Hans/docs/build/delivery-semantics)开始了解。

### 错误输出

`error_output` 是畸形或失败记录的去处。毒消息(poison message)永远不会阻塞流:它被路由到这个旁路输出,处理继续进行——这就是"有韧性的流水线"与"凌晨三点的故障"之间的区别。

## 持久化

为流加上流级持久化,让它具备抗崩溃能力:

```yaml validate=full
streams:
  - id: webhooks
    input:
      type: http
      address: "0.0.0.0:8080"
      path: "/webhook"
    pipeline:
      processors: []
    output:
      type: drop
    durability:
      enabled: true
      path: "./data/wal"
      sync: "group_commit"   # per_entry | group_commit | periodic
```

启用持久化后,每条消息在进入流水线前都会 fsync 到 WAL,数据源只有在输出确认后才提交——跨崩溃的至少一次投递。这对不可重放的数据源(HTTP webhook、Modbus、文件)最为重要:Kafka 这类可重放的数据源可以重新投递,但无论数据源如何配置,WAL 都保证不丢。完整的保证阶梯见 [WAL 持久化](/zh-Hans/docs/build/wal)与[精确一次](/zh-Hans/docs/build/exactly-once)。

## 运行前先校验

```bash
./arkflow --config config.yaml --validate
```

深度校验覆盖流 id、作业图与配置规则——参见 [CLI 参考](/zh-Hans/docs/reference/cli)。

## 下一步

- [流式作业](./jobs.md) —— 带事件时间与状态的多步骤 DAG。
- [组件](/zh-Hans/docs/components) —— 浏览全部 41 个已注册组件。
- [顶层配置](/zh-Hans/docs/reference/configuration) —— 每一个引擎选项。

---
sidebar_position: 1
description: 体验 WAL 支撑的持久化接入——在崩溃中不丢失任何消息。
---

# 教程:构建一条持久化流水线

在[快速入门](./2-quickstart.md)里你已经跑起了一条流水线。真实世界的流水线面临更严苛的要求:*读取*消息与*写出*消息之间发生崩溃时,消息不能丢。本教程为流水线加上 WAL 持久化,并证明它能扛住一次硬杀。

**耗时:** 约 15 分钟。
**前置条件:** 已完成[快速入门](./2-quickstart.md);无需其他服务(数据源是模拟的,输出是 stdout)。

## 你将构建什么

```
┌────────┐   persist    ┌─────┐   process   ┌────────┐  ack   ┌────────┐
│ Input  │─────────────▶│ WAL │────────────▶│ Output │───────▶│ Replay │
└────────┘   first      └─────┘             └────────┘  only  └────────┘
                                              confirms        on crash
```

每条消息在进入流水线**之前**都会先持久化到本地预写日志(WAL)。输出确认之后,数据源才会把消息标记为完成。如果进程在中途死掉,未被确认的消息会在重启时从 WAL 重放——这就是*至少一次*(at-least-once)投递。

## 1. 编写配置

创建 `durable.yaml`:

```yaml validate=fragment wrap=engine
logging:
  level: info

streams:
  - input:
      type: generate
      context: '{ "value": 10, "sensor": "temp_1" }'
      interval: 1ns
      batch_size: 1
      count: 1000

    durability:
      enabled: true
      path: "./data/wal"
      sync: "group_commit"

    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: arrow_to_json

    output:
      type: stdout
```

与快速入门相比,唯一的新东西是 `durability` 块:`path` 是 WAL 的存放位置,`sync: group_commit` 将磁盘同步批量执行以换取吞吐(若要求每条消息立即落盘,使用 `per_entry`)。

## 2. 校验,然后运行

```bash
./target/release/arkflow --config durable.yaml --validate
./target/release/arkflow --config durable.yaml
```

你会在 stdout 上看到源源不断的 JSON 消息,并出现一个 `./data/wal` 目录。这个示例文件随仓库以
[`examples/durability_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/durability_example.yaml)
发布,并由 CI 校验。

## 3. 狠狠地杀掉它

在另一个终端里找到进程,毫不留情地杀掉:

```bash
pkill -9 arkflow
```

重新启动引擎:

```bash
./target/release/arkflow --config durable.yaml
```

启动时,ArkFlow 检测到未确认的 WAL 条目并重放它们。`count: 1000` 条消息全部到达输出——可能少量重复(崩溃瞬间在途的消息可能被再次投递,这正是至少一次投递的含义)。

## 4. 检验学到的知识

- **持久化在哪里配置?** 在流上,`durability:` 块下。
- **输入何时被确认?** 只有在输出确认写入之后。
- **崩溃时在途消息会怎样?** 它们会从 WAL 重放,因此下游可能看到重复。

## 接下来去哪里

- [投递语义](/zh-Hans/docs/build/delivery-semantics) —— 至少一次与面向事务输出的可选精确一次。
- [WAL 优化](/zh-Hans/docs/build/wal) —— `sync` 模式如何在延迟与吞吐之间取舍。
- [案例:持久化 webhook 采集](/zh-Hans/docs/build/recipes/case-webhook-durable) —— 同样的模式,搭配真实的 HTTP 源与 Kafka 输出。

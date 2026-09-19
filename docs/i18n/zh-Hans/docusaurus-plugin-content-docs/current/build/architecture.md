---
sidebar_position: 1
---

# 架构

ArkFlow 是一个流处理引擎:它从数据源读取数据,经过一串处理器变换,再写入下游——全部由单一的 YAML 配置文件定义。本页介绍运行时核心概念,文档的其余部分都建立在这些概念之上。

## 引擎(Engine)

**引擎**是顶层进程。它加载一份配置,并发地运行一条或多条**流**,外加一个 HTTP 健康检查 / 控制平面服务。每条流相互独立;它们只共享进程本身及其组件注册表(输入、输出、处理器、缓冲、编解码器)。

## 流(Stream)

**流**是数据处理的单元。它把以下部件连接在一起:

```
Input → Buffer → [Processor → Processor → …] → Output
                                              ↘ Error output (optional)
```

- **输入(Input)** —— 从数据源读取原始消息(Kafka、MQTT、HTTP、文件等)。
- **缓冲(Buffer)** —— 在输入与处理器之间保存批次。可以是简单的内存队列,也可以是窗口策略(滚动、滑动、会话),还可选地连接多个数据源。
- **流水线(Pipeline)** —— 应用于每个批次的**处理器**有序列表,由 `thread_num` 个工作任务并行执行。
- **输出(Output)** —— 将每个处理完的批次写到下游。可选的 `error_output` 接收处理器处理失败的批次。
- **持久化(Durability)** —— 可选的流级预写日志(WAL),在输入边界持久化消息,使其在崩溃后幸存(参见[投递语义](/zh-Hans/docs/build/delivery-semantics))。

## 消息模型

数据以 `MessageBatch` 的形式流经一条流——它是对 [Apache Arrow](https://arrow.apache.org/) `RecordBatch` 的薄封装。列式 Arrow 让引擎获得高吞吐,并让 SQL 处理器直接在数据上执行而无需重新序列化。输入还可以附加标准化的**元数据列**(参见[元数据](/zh-Hans/docs/build/metadata))。

## 并发模型

每条流作为一组协作式异步任务运行在 Tokio 多线程运行时上:

- 一个**输入工作者**从数据源读取,
- 若干**处理器工作者**(通过 `pipeline.thread_num` 配置),
- 一个**输出工作者**按有序投递写到下游。

它们通过 `flume` 通道通信,并通过 `CancellationToken` 一同关闭。关闭时由 `TaskTracker` 等待所有任务结束。

继续阅读[背压与有序投递](/zh-Hans/docs/build/backpressure)。

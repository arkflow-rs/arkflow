---
sidebar_position: 1
---

# 简介

> **规范入口:** 当前文档采用按领域划分的导航,请从[文档主页](./index.md)开始。本页作为稳定的兼容性入口保留,以便既有链接继续可用。

![ArkFlow logo](pathname:///docs/logo.svg)

ArkFlow 是一个用 Rust 编写、运行在 Tokio 异步运行时之上的高性能流处理引擎。它从多种数据源(Kafka、MQTT、HTTP、文件、SQL、Pulsar、NATS、Redis、Modbus、WebSocket……)摄取数据,使用 SQL、VRL、Python UDF、JSON/Protobuf 编解码器与窗口连接进行变换,再写入一个或多个下游——全部由单个 YAML 文件驱动。

## 核心特性

- **高性能** —— Rust + Tokio,列式 [Apache Arrow](https://arrow.apache.org/) 数据,多线程流水线。
- **可靠投递** —— 默认通过每条流的 WAL 持久化保证至少一次(at-least-once);事务型下游可选择精确一次(exactly-once)。参见[投递语义](/zh-Hans/docs/build/delivery-semantics)。
- **丰富的数据源与下游** —— Kafka、MQTT、HTTP、文件(支持 S3/GCS/Azure/HDFS)、Pulsar、NATS、Redis、SQL、Modbus、WebSocket、InfluxDB 等。
- **强大的处理能力** —— SQL(DataFusion)、VRL、Python UDF、JSON、Protobuf、批处理、窗口化,以及多源连接。
- **流式编解码器** —— JSON、Protobuf、Debezium CDC 信封,以及 Confluent Schema Registry 传输格式(wire format)。
- **控制平面** —— 可选的 Hub 与控制台,用于以机群(Fleet)方式观测、配置和运维多个 ArkFlow 节点。
- **可扩展** —— 为输入、输出、处理器、缓冲和编解码器提供统一的插件模型。

## 后续步骤

- [快速入门](/zh-Hans/docs/get-started/quickstart) —— 安装并在几分钟内运行你的第一条流水线。
- [核心概念](/zh-Hans/docs/build/architecture) —— 引擎、流、流水线、背压与元数据如何协同工作。
- [配置参考](/zh-Hans/docs/reference/configuration) —— 顶层 YAML 结构。
- [组件](/zh-Hans/docs/reference/component-inventory) —— 全部输入、输出、处理器、缓冲与编解码器。
- [SQL 参考](/zh-Hans/docs/sql/select) —— 查询语法与函数。
- [控制平面](/zh-Hans/docs/operate/control-plane/overview) —— 以机群方式运维 ArkFlow。

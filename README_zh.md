# ArkFlow

<p align="center">
<img align="center" width="150px" src="./logo.svg">
<p align="center">

[English](README.md) | 中文

[![Rust](https://github.com/arkflow-rs/arkflow/actions/workflows/rust.yml/badge.svg)](https://github.com/arkflow-rs/arkflow/actions/workflows/rust.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/arkflow-rs/arkflow)

<a href="https://www.producthunt.com/posts/arkflow?embed=true&utm_source=badge-featured&utm_medium=badge&utm_souce=badge-arkflow" target="_blank"><img src="https://api.producthunt.com/widgets/embed-image/v1/featured.svg?post_id=942804&theme=light&t=1743136262336" alt="ArkFlow - High&#0045;performance&#0032;rust&#0032;stream&#0032;processing&#0032;engine | Product Hunt" style="width: 250px; height: 54px;" width="250" height="54" /></a>

高性能Rust流处理引擎，提供强大的数据流处理能力，支持多种输入输出源和处理器。

## 特性

- **高性能**：基于Rust和Tokio异步运行时构建，提供卓越的性能和低延迟
- **多种数据源**：支持Kafka、MQTT、HTTP、文件等多种输入输出源
- **强大的处理能力**：内置SQL查询、JSON处理、Protobuf编解码、批处理等多种处理器
- **可扩展**：模块化设计，易于扩展新的输入、缓冲区、输出和处理器组件

## 安装

### 从源码构建

```bash
# 克隆仓库
git clone https://github.com/arkflow-rs/arkflow.git
cd arkflow

# 构建项目
cargo build --release

# 运行测试
cargo test
```

## 快速开始

1. 创建配置文件 `config.yaml`：

```yaml
logging:
  level: info
streams:
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1s
      batch_size: 10

    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
        - type: "sql"
          query: "SELECT * FROM flow WHERE value >= 10"

    output:
      type: "stdout"
    error_output:
      type: "stdout"
```

2. 运行ArkFlow：

```bash
./target/release/arkflow --config config.yaml
```

## 配置说明

ArkFlow使用YAML格式的配置文件，支持以下主要配置项：

### 顶级配置

```yaml
logging:
  level: info  # 日志级别：debug, info, warn, error

streams: # 流定义列表
  - input:      # 输入配置
    # ...
    pipeline:   # 处理管道配置
    # ...
    output:     # 输出配置
    # ...
    error_output: # 错误输出配置
    # ...
    buffer:     # 缓冲配置
    # ...
```

### 输入组件

ArkFlow支持多种输入源：

- **Kafka**：从Kafka主题读取数据
- **MQTT**：从MQTT主题订阅消息
- **HTTP**：通过HTTP接收数据
- **文件**：使用SQL从文件(Csv、Json、Parquet、Avro、Arrow)读取数据
- **生成器**：生成测试数据
- **数据库**：从数据库(MySQL、PostgreSQL、SQLite、Duckdb)查询数据
- **Nats**: 订阅来自 Nats 主题的消息
- **Redis**: 订阅来自 Redis 频道或列表的消息
- **Websocket**: 订阅来自 WebSocket 连接的消息
- **Modbus**: 从 Modbus 设备读取数据

示例：

```yaml
input:
  type: kafka
  brokers:
    - localhost:9092
  topics:
    - test-topic
  consumer_group: test-group
  client_id: arkflow
  start_from_latest: true
```

### 处理器

ArkFlow提供多种数据处理器：

- **JSON**：JSON数据处理和转换
- **SQL**：使用SQL查询处理数据
- **Protobuf**：Protobuf编解码
- **批处理**：将消息批量处理
- **Vrl**: 使用[VRL](https://vector.dev/docs/reference/vrl/)进行处理数据

示例：

```yaml
pipeline:
  thread_num: 4
  processors:
    - type: json_to_arrow
    - type: sql
      query: "SELECT * FROM flow WHERE value >= 10"
```

### 输出组件

ArkFlow支持多种输出目标：

- **Kafka**：将数据写入Kafka主题
- **MQTT**：将消息发布到MQTT主题
- **HTTP**：通过HTTP发送数据
- **标准输出**：将数据输出到控制台
- **Drop**: 丢弃数据
- **Nats**: 将消息发布到 Nats 主题

示例：

```yaml
output:
  type: kafka
  brokers:
    - localhost:9092
  topic:
    type: value
    value: output-topic
  client_id: arkflow-producer
```

### 错误输出组件

- **Kafka**：将错误数据写入 Kafka 主题
- **MQTT**：将错误消息发布到 MQTT 主题
- **HTTP**：通过 HTTP 发送错误数据
- **标准输出**：将错误数据输出到控制台
- **丢弃**：丢弃错误数据
- **Nats**: 将消息发布到 Nats 主题

示例：

```yaml
error_output:
  type: kafka
  brokers:
    - localhost:9092
  topic:
    type: value
    value: error-topic
  client_id: error-arkflow-producer
```

### 缓冲组件

ArkFlow 提供缓冲能力，以处理消息的背压和临时存储:

- **内存缓冲**: 内存缓冲区，用于高吞吐量场景和窗口聚合。
- **会话窗口 (Session Window)**：会话窗口缓冲组件提供了一种基于会话的消息分组机制，其中消息根据活动间隙进行分组。它实现了一个会话窗口，在可配置的非活动期后关闭。
- **滑动窗口 (Sliding Window)**：滑动窗口缓冲组件提供了一种基于时间的分批处理消息的窗口机制。它实现了一种滑动窗口算法，具有可配置的窗口大小、滑动间隔和滑动大小。
- **滚动窗口 (Tumbling Window)**：滚动窗口缓冲组件提供了一种固定大小、不重叠的批处理消息的窗口机制。它实现了一种滚动窗口算法，具有可配置的间隔设置。

示例：

```yaml
buffer:
  type: memory
  capacity: 10000  # Maximum number of messages to buffer
  timeout: 10s  # Maximum time to buffer messages
```

## 示例

### Kafka到Kafka的数据处理

```yaml
streams:
  - input:
      type: kafka
      brokers:
        - localhost:9092
      topics:
        - test-topic
      consumer_group: test-group

    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow WHERE value > 100"

    output:
      type: kafka
      brokers:
        - localhost:9092
      topic:
        type: value
        value: processed-topic
```

### 生成测试数据并处理

```yaml
streams:
  - input:
      type: "generate"
      context: '{ "timestamp": 1625000000000, "value": 10, "sensor": "temp_1" }'
      interval: 1ms
      batch_size: 10000

    pipeline:
      thread_num: 4
      processors:
        - type: "json_to_arrow"
        - type: "sql"
          query: "SELECT count(*) FROM flow WHERE value >= 10 group by sensor"

    output:
      type: "stdout"
```

## 用户

- Conalog(国家: 韩国)

## ArkFlow 插件

[ArkFlow 插件示例](https://github.com/arkflow-rs/arkflow-plugin-examples)

## 许可证

ArkFlow 使用 [Apache License 2.0](LICENSE) 许可证。

## 社区

Discord: https://discord.gg/CwKhzb8pux

如果你喜欢或正在使用这个项目来学习或开始你的解决方案，请给它一个star⭐。谢谢！
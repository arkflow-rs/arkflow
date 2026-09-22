# ArkFlow

<p align="center">
<img align="center" width="150px" src="images/logo.svg">
<p align="center">

[English](README.md) | 中文

[![Rust](https://github.com/arkflow-rs/arkflow/actions/workflows/rust.yml/badge.svg)](https://github.com/arkflow-rs/arkflow/actions/workflows/rust.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

[文档](https://arkflow-rs.com/docs/) | [0.5 发布版文档](https://arkflow-rs.com/docs/0.5.x/intro)

<a href="https://www.producthunt.com/posts/arkflow?embed=true&utm_source=badge-featured&utm_medium=badge&utm_souce=badge-arkflow" target="_blank"><img src="https://api.producthunt.com/widgets/embed-image/v1/featured.svg?post_id=942804&theme=light&t=1743136262336" alt="ArkFlow - High&#0045;performance&#0032;rust&#0032;stream&#0032;processing&#0032;engine | Product Hunt" style="width: 250px; height: 54px;" width="250" height="54" /></a>

高性能Rust流处理引擎，无缝集成AI能力，提供强大的实时数据处理与智能分析。
它不仅支持多种输入/输出源和处理器，更能轻松加载和执行机器学习模型，实现流式数据和推理、异常检测和复杂事件处理。

##  CNCF 云原生技术全景图

<p float="left">
<img src="images/cncf-logo.svg" width="200"/>&nbsp;&nbsp;&nbsp;
<img src="images/cncf-landscape-logo.svg" width="150"/>
</p>

ArkFlow 已收录在 [CNCF Cloud Native 云原生技术全景图](https://landscape.cncf.io/?item=app-definition-and-development--streaming-messaging--arkflow)中。

## 特性

- **高性能**：基于Rust和Tokio异步运行时构建，提供卓越的性能和低延迟
- **可靠投递**：默认通过每流 WAL 持久化提供 at-least-once，可选地为事务型 sink 启用 exactly-once
- **多种数据源**：支持Kafka、MQTT、HTTP、文件、SQL 数据库等多种输入输出源
- **强大的处理能力**：内置SQL查询、Python UDF、JSON处理、Protobuf编解码、批处理和 VRL 等处理器
- **流编解码**：JSON 与 Protobuf 编解码，以及 Debezium CDC 信封和 Confluent Schema Registry 线格式
- **控制面**：可选的 Hub 与 Web 控制台，可作为一个集群对多个 ArkFlow 计算节点进行观测、配置与运维
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

<!-- README_COMPONENTS:input START -->

- **Kafka** (`kafka`)：从Kafka主题读取数据
- **MQTT** (`mqtt`)：从MQTT主题订阅消息
- **HTTP** (`http`)：通过HTTP接收数据
- **文件** (`file`)：从文件（CSV、JSON、Parquet、Avro、Arrow）读取数据，支持云存储
- **生成器** (`generate`)：生成测试数据
- **SQL** (`sql`)：从 SQL 数据库（MySQL、PostgreSQL、SQLite）查询数据
- **NATS** (`nats`)：订阅来自 NATS 主题的消息，支持 JetStream
- **Pulsar** (`pulsar`)：订阅来自 Pulsar 主题的消息
- **Redis** (`redis`)：订阅来自 Redis 流、列表或发布/订阅频道的消息
- **WebSocket** (`websocket`)：订阅来自 WebSocket 连接的消息
- **Modbus** (`modbus`)：从 Modbus 设备读取数据
- **内存** (`memory`)：用于测试的内存数据源
- **多输入** (`multiple_inputs`)：将多个输入流合并到一个管道

<!-- README_COMPONENTS:input END -->

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

<!-- README_COMPONENTS:processor START -->

- **JSON** (`json_to_arrow` / `arrow_to_json`)：JSON数据处理和转换
- **SQL** (`sql`)：使用SQL查询处理数据
- **Protobuf** (`arrow_to_protobuf` / `protobuf_to_arrow`)：Protobuf编解码
- **批处理** (`batch`)：将消息批量处理
- **VRL** (`vrl`)：使用[VRL](https://vector.dev/docs/reference/vrl/)进行处理数据
- **Python** (`python`)：对每个批次运行用户自定义的 Python 函数
- **Embedding** (`embedding`)：通过 OpenAI 兼容 API 批量向量化文本列并追加向量列
- **LLM** (`llm`)：以有界并发将每行文本发送到 OpenAI 兼容对话补全 API 并追加补全列
- **Vector Search** (`vector_search`)：在 Qdrant 集合中检索每行向量的 top-k 近邻并以 JSON 追加匹配结果

<!-- README_COMPONENTS:processor END -->

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

<!-- README_COMPONENTS:output START -->

- **Kafka** (`kafka`)：将数据写入Kafka主题
- **MQTT** (`mqtt`)：将消息发布到MQTT主题
- **HTTP** (`http`)：通过HTTP发送数据
- **InfluxDB** (`influxdb`)：将时序数据写入 InfluxDB 2.x
- **MongoDB** (`mongodb`)：将文档写入 MongoDB 集合
- **NATS** (`nats`)：将消息发布到 NATS 主题
- **Pulsar** (`pulsar`)：将消息发布到 Pulsar 主题
- **Redis** (`redis`)：写入 Redis 流、列表或发布/订阅频道
- **SQL** (`sql`)：写入 SQL 数据库（MySQL、PostgreSQL），支持批量插入与 UPSERT
- **标准输出** (`stdout`)：将数据输出到控制台
- **Drop** (`drop`)：丢弃数据
- **Qdrant** (`qdrant`)：将向量与负载写入 Qdrant 集合
- **pgvector** (`pgvector`)：将向量与 JSON 负载写入启用了 pgvector 扩展的 PostgreSQL 表
- **Milvus** (`milvus`)：通过 REST v2 API 将向量与 JSON 负载写入 Milvus 集合

<!-- README_COMPONENTS:output END -->

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

`error_output` 接受上述任意一种[输出组件](#输出组件)，用于接收处理失败的消息。最常用的是 Kafka、HTTP 和标准输出（便于调试）。

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

<!-- README_COMPONENTS:buffer START -->

- **内存缓冲** (`memory`)：内存缓冲区，用于高吞吐量场景和窗口聚合。编译后的 Stream 中它退化为直通，capacity/timeout 选项被忽略。
- **会话窗口** (`session_window`)：会话窗口缓冲组件提供了一种基于会话的消息分组机制，其中消息根据活动间隙进行分组。它实现了一个会话窗口，在可配置的非活动期后关闭。编译为处理时间窗口算子。
- **滑动窗口（已弃用）** (`sliding_window`)：已弃用且在 Stream 配置中不可达——流编译器会拒绝滑动窗口缓冲。请改用 Job DAG 中的事件时间滑动窗口算子。
- **滚动窗口** (`tumbling_window`)：滚动窗口缓冲组件提供了一种固定大小、不重叠的批处理消息的窗口机制。它实现了一种滚动窗口算法，具有可配置的间隔设置。编译为处理时间窗口算子。

<!-- README_COMPONENTS:buffer END -->

示例：

```yaml
buffer:
  type: memory
```

旧版 join 缓冲同样已弃用且在 Stream 配置中不可达——流编译器会拒绝 join 缓冲，并给出指向 Job DAG 配置的迁移提示；它不是独立的缓冲类型。

### 编解码组件（Codec）

编解码组件通过 `codec` 配置挂载到输入/输出上，负责消息负载的编码与解码：

<!-- README_COMPONENTS:codec START -->

- **JSON** (`json`)：将 Arrow RecordBatch 编码/解码为 JSON 负载
- **Protobuf** (`protobuf`)：使用 Protobuf 描述符编码/解码 Arrow RecordBatch
- **Debezium JSON** (`debezium_json`)：解码来自 Kafka 变更事件主题的 Debezium CDC 信封 JSON
- **Schema Registry** (`schema_registry`)：通过 Schema Registry 解析模式，解码 Confluent 线路格式的 Protobuf 与 Avro 消息

<!-- README_COMPONENTS:codec END -->

### 临时状态组件（Temporary）

临时状态组件提供处理器可在运行期查询的外部查找状态：

<!-- README_COMPONENTS:temporary START -->

- **Redis** (`redis`)：基于 Redis 的临时查找存储（单机或集群），通过编解码器读取

<!-- README_COMPONENTS:temporary END -->

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

微信社区群：

<img src="./images/wx-group.png" alt="wx" width="300" />

您可以在群内提出任何需要改进的地方，我们会考虑合理性并尽快修改。
如果您发现 bug 请及时提 [issue](https://github.com/arkflow-rs/arkflow/issues/new?template=bug_report.md)，我们会尽快确认并修改。

如果你喜欢或正在使用这个项目来学习或开始你的解决方案，请给它一个star⭐。谢谢！

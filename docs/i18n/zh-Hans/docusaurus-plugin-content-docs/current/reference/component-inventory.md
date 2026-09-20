---
description: 供文档检查使用的受支持组件清单(inventory)。
---

# 组件清单(Component inventory)

本页由 [`component-inventory.json`](https://github.com/arkflow-rs/arkflow/blob/main/docs/reference/component-inventory.json) 生成。它是组件覆盖度的审查入口,不能替代各组件的详细页面。

<!-- COMPONENT_INVENTORY_START -->

| 类别 | 组件 | 描述 | 文档 |
| --- | --- | --- | --- |
| buffer | `memory` | 内存缓冲:达到容量上限或超时后释放一个批次。 | [参考](/zh-Hans/docs/components/buffers/memory) |
| buffer | `session_window` | 依据消息间最大间隔把消息划分为会话。支持跨数据源的 SQL join。 | [参考](/zh-Hans/docs/components/buffers/session_window) |
| buffer | `sliding_window` | 按固定间隔向前滑动的时间重叠窗口。 | [参考](/zh-Hans/docs/components/buffers/sliding_window) |
| buffer | `tumbling_window` | 固定大小、互不重叠的时间窗口。支持跨数据源的 SQL join。 | [参考](/zh-Hans/docs/components/buffers/tumbling_window) |
| codec | `debezium_json` | 把 Debezium CDC Envelope JSON(before/after/op/source/ts_ms)解码为列式 Arrow 批次;附加到消费 Debezium 主题的 Kafka 输入上。CDC 偏移量即 Kafka 输入经确认门控的偏移量。 | [参考](/zh-Hans/docs/components/codecs/debezium) |
| codec | `json` | 将 Arrow RecordBatch 编码/解码为 JSON 字节负载。 | [参考](/zh-Hans/docs/components/codecs/json) |
| codec | `protobuf` | 使用 Protobuf 描述符对 Arrow RecordBatch 进行编码/解码。 | [参考](/zh-Hans/docs/components/codecs/protobuf) |
| codec | `schema_registry` | 通过从 Schema Registry 解析 schema id,解码 Confluent 线路格式的 Protobuf 与 Avro 消息,并支持可选的主题兼容性门控。 | [参考](/zh-Hans/docs/components/codecs/schema-registry) |
| input | `file` | 以 CSV/JSON/Parquet/Avro/Arrow 格式读取本地或远程对象存储(S3、GCS、Azure、HDFS)中的记录。 | [参考](/zh-Hans/docs/components/inputs/file) |
| input | `generate` | 按固定间隔生成合成文本消息(适用于测试与负载模拟)。 | [参考](/zh-Hans/docs/components/inputs/generate) |
| input | `http` | 通过 HTTP 接收数据。可作为服务器运行(在 `path` 上接受 POST/PUT),也可轮询远程端点。 | [参考](/zh-Hans/docs/components/inputs/http) |
| input | `kafka` | 以消费者组从 Apache Kafka 主题消费消息。 | [参考](/zh-Hans/docs/components/inputs/kafka) |
| input | `memory` | 用初始消息列表填充的内存输入队列。主要用于测试与演示。 | [参考](/zh-Hans/docs/components/inputs/memory) |
| input | `modbus` | 按固定间隔轮询 Modbus TCP 设备,读取线圈、离散输入或寄存器。 | [参考](/zh-Hans/docs/components/inputs/modbus) |
| input | `mqtt` | 订阅 MQTT broker 并转发所配置主题的消息。 | [参考](/zh-Hans/docs/components/inputs/mqtt) |
| input | `multiple_inputs` | 把多个输入源组合为一条流。每个数据源都会打上 __meta_source 标签。 | [参考](/zh-Hans/docs/components/inputs/multiple_inputs) |
| input | `nats` | 从 NATS 消费消息,同时支持普通 subject 与 JetStream 消费者。 | [参考](/zh-Hans/docs/components/inputs/nats) |
| input | `pulsar` | 订阅 Apache Pulsar 主题,可配置订阅类型与认证。 | [参考](/zh-Hans/docs/components/inputs/pulsar) |
| input | `redis` | 从 Redis 读取:list 阻塞弹出、pub/sub 订阅或 stream 消费者组。 | [参考](/zh-Hans/docs/components/inputs/redis) |
| input | `sql` | 用 SELECT 语句轮询 SQL 数据库(MySQL / PostgreSQL / SQLite / DuckDB),并以批次发出行。 | [参考](/zh-Hans/docs/components/inputs/sql) |
| input | `websocket` | 连接 WebSocket 服务器,把每条到达的消息作为一个批次转发。 | [参考](/zh-Hans/docs/components/inputs/websocket) |
| output | `drop` | 丢弃所有消息。适用于性能基准测试与尽头管道。 | [参考](/zh-Hans/docs/components/outputs/drop) |
| output | `http` | 把每个批次 POST 到一个 HTTP 端点。支持自定义头部、重试与认证。 | [参考](/zh-Hans/docs/components/outputs/http) |
| output | `influxdb` | 使用 Line Protocol 把时间序列数据写入 InfluxDB v2.x。 | [参考](/zh-Hans/docs/components/outputs/influxdb) |
| output | `kafka` | 向 Apache Kafka 生产消息。支持按键分区与压缩。 | [参考](/zh-Hans/docs/components/outputs/kafka) |
| output | `mongodb` | 把 Arrow 行以 BSON 文档写入 MongoDB。 | [参考](/zh-Hans/docs/components/outputs/mongodb) |
| output | `mqtt` | 向 MQTT broker 主题发布消息。 | [参考](/zh-Hans/docs/components/outputs/mqtt) |
| output | `nats` | 发布到 NATS:普通 subject 或 JetStream 流。 | [参考](/zh-Hans/docs/components/outputs/nats) |
| output | `pulsar` | 向 Apache Pulsar 主题生产消息。 | [参考](/zh-Hans/docs/components/outputs/pulsar) |
| output | `redis` | 把消息写入 Redis:stream、list 或 pub/sub 通道。 | [参考](/zh-Hans/docs/components/outputs/redis) |
| output | `sql` | 把记录批量插入 MySQL 或 PostgreSQL 数据库,支持可选的 upsert(ON DUPLICATE KEY UPDATE / ON CONFLICT DO UPDATE)以实现幂等写入。 | [参考](/zh-Hans/docs/components/outputs/sql) |
| output | `stdout` | 把每条消息写到控制台。适用于调试与演示。 | [参考](/zh-Hans/docs/components/outputs/stdout) |
| processor | `arrow_to_json` | 把 Arrow RecordBatch 转换为 JSON 字节负载(每行一个)。 | [参考](/zh-Hans/docs/components/processors/json) |
| processor | `arrow_to_protobuf` | 把 Arrow RecordBatch 序列化为 Protobuf 线路格式字节。 | [参考](/zh-Hans/docs/components/processors/protobuf) |
| processor | `batch` | 在转发前按条数、大小或时间间隔对消息分批。 | [参考](/zh-Hans/docs/components/processors/batch) |
| processor | `json_to_arrow` | 把 JSON 字节负载解析为推断出 schema 的 Arrow RecordBatch。 | [参考](/zh-Hans/docs/components/processors/json) |
| processor | `protobuf_to_arrow` | 把 Protobuf 线路格式字节解码为 Arrow RecordBatch。 | [参考](/zh-Hans/docs/components/processors/protobuf) |
| processor | `python` | 对每个批次运行用户自定义的 Python 函数(带 PyArrow)。 | [参考](/zh-Hans/docs/components/processors/python) |
| processor | `sql` | 对每个批次运行 DataFusion SQL 查询。支持窗口函数以及与临时表的 join。 | [参考](/zh-Hans/docs/components/processors/sql) |
| processor | `vrl` | 对每个批次运行 Vector Remap Language(VRL)程序,实现安全的变换与富化。 | [参考](/zh-Hans/docs/components/processors/vrl) |
| temporary | `redis` | 以 Redis 为后端的临时查找存储(单节点或集群),通过 codec 读取。 | [参考](/zh-Hans/docs/components/temporary/redis) |

<!-- COMPONENT_INVENTORY_END -->

新增或移除组件时,请在同一个 pull request 中同时更新 JSON 清单与其详细页面。

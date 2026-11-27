---
sidebar_position: 1
---

# 安装

## 前置条件

- **Rust ≥ 1.97**(工具链,用于从源码构建)

无需安装 protobuf 编译器:Protobuf 编解码器的 `.proto` 文件由内置的纯 Rust `protox` 编译器在**运行时**编译,构建过程也不调用 `protoc`(仓库没有 `build.rs`)。

## 从源码构建

```bash
git clone https://github.com/arkflow-rs/arkflow.git
cd arkflow

# 优化的 release 构建
cargo build --release

# (可选)运行测试套件
cargo test
```

二进制位于 `./target/release/arkflow`。

## 构建特性裁剪(feature gating)

连接器家族由 cargo feature 门控。默认构建启用全部家族;最小构建只编译核心引擎与轻量组件集——适合自定义镜像与边缘部署。

| Feature | 组件 |
| --- | --- |
| `connectors-sql` | `sql` 输入/输出(DuckDB、Ballista、MySQL/PostgreSQL/SQLite 表提供器) |
| `connectors-kafka` | `kafka` 输入/输出 |
| `connectors-python` | `python` 处理器 |
| `connectors-mongodb` | `mongodb` 输出 |
| `connectors-pulsar` | `pulsar` 输入/输出 |
| `connectors-hdfs` | `file` 输入的 `hdfs` 对象存储 |
| `connectors-mqtt` | `mqtt` 输入/输出 |
| `connectors-nats` | `nats` 输入/输出 |
| `connectors-amqp09` | `amqp09`（RabbitMQ）输入/输出 |
| `connectors-redis` | `redis` 输入/输出/temporary |
| `connectors-websocket` | `websocket` 输入 |
| `connectors-modbus` | `modbus` 输入 |
| `ai-vector` | `embedding` / `llm` / `vector_search` / `milvus_search` / `pgvector_search` 处理器,`qdrant` / `milvus` / `pgvector` 输出 |
| `processors-vrl` | `vrl` 处理器 |
| `codecs-protobuf` | `protobuf` / `schema_registry` 编解码,`arrow_to_protobuf` / `protobuf_to_arrow` 处理器 |

```bash
# 默认构建:全部家族启用(与无 feature 层的构建行为完全一致)
cargo build --release

# 最小构建:仅核心引擎 + 轻量组件。
# Cargo feature 只加不减,engine-minimal 必须配合 --no-default-features 才有裁剪效果。
cargo build --release --no-default-features --features engine-minimal

# 最小构建加一个家族
cargo build --release --no-default-features --features engine-minimal,connectors-kafka
```

无论 feature 如何都始终编译:`file` / `generate` / `http` / `memory` / `multiple_inputs` 输入,`drop` / `http` / `influxdb` / `stdout` 输出,`lookup` / `batch` / `arrow_to_json` / `json_to_arrow` / `sql` 处理器,全部 buffer 类型,以及 `json` / `debezium_json` 编解码。在未启用某家族的构建中,配置引用其类型时校验会失败并指名缺失的 feature,例如 `Unknown input type: kafka (requires the 'connectors-kafka' feature)`。

## 运行

```bash
# 使用配置文件启动
./target/release/arkflow --config config.yaml

# 仅校验配置,不启动引擎
./target/release/arkflow --config config.yaml --validate
```

## 组件发现与 Schema

ArkFlow 提供 CLI 命令,列出所有已注册组件并打印其配置 schema。它们为编辑器自动补全提供数据,在编写配置时也非常顺手。

```bash
# 列出所有 input / output / processor / buffer / codec
./target/release/arkflow components list
./target/release/arkflow components list --kind input

# 查看某个组件的配置 schema(文本或 JSON)
./target/release/arkflow components show input kafka
./target/release/arkflow components show processor sql --format json

# 导出整个引擎配置的 JSON Schema
./target/release/arkflow schema > arkflow.schema.json
```

将编辑器的 YAML language server 指向生成的 schema,即可获得字段级补全与校验。

继续阅读[快速入门](./2-quickstart.md)。

---
description: 精选的 ArkFlow 端到端示例。
---

# 示例目录

这些示例随仓库一起维护,并由 `pnpm docs:check` 检查。依赖服务的示例需要相应的本地服务;初次学习配置结构时请先使用 quickstart。

| 工作流 | 示例 | 适用场景 |
| --- | --- | --- |
| 快速开始 | [`generate_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/generate_example.yaml) | 本地的从数据源到 stdout 的流水线 |
| Kafka | [`kafka_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/kafka_example.yaml) | Kafka 输入配置 |
| SQL 输出 | [`sql_output_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/sql_output_example.yaml) | 数据库 sink 配置 |
| 持久化 | [`durability_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/durability_example.yaml) | 流级 WAL 持久化 |
| S3 持久化 | [`durability_example_s3.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/durability_example_s3.yaml) | 基于对象存储的恢复 |
| 控制平面节点 | [`control_plane_node.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/control_plane_node.yaml) | 计算节点(Agent)模式:通过 `hub_urls`、`node_id`、`node_token` 向 Hub 注册 |
| 本地控制 API | [`control_plane_example.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/control_plane_example.yaml) | 暴露本地控制 API 的独立引擎 |
| 健康检查服务 | [`control_plane_hub.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/control_plane_hub.yaml) | 无数据流的最小健康检查 / 控制平面端点配置 |
| 本地流式作业 | [`jobs_local.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/jobs_local.yaml) | 带事件时间、状态与检查点的声明式 `jobs` DAG |
| Debezium CDC | [`cdc_debezium.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/cdc_debezium.yaml) | CDC envelope 解码 |

对于每个依赖服务的示例,在共享环境中运行之前请先校验凭据与端点。

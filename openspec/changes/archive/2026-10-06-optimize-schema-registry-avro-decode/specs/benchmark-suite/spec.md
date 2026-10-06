## MODIFIED Requirements

### Requirement: 场景覆盖内核主路径

基准 SHALL 至少覆盖：线性 SQL 聚合管道、GROUP BY 有状态聚合、过滤投影、JSON 编码往返、状态后端读写、Avro schema-registry 解码。流式场景 SHALL 走统一内核公开入口（compile_stream + run_job），与生产执行路径一致；Avro 解码场景 SHALL 走真实 `SchemaRegistryCodec::decode` 路径（离线 in-memory `SchemaResolver`，无网络、无外部服务），并 SHALL 覆盖至少三种 schema 宽度以暴露宽度相关的每消息成本。

#### Scenario: 全部场景产出有限正吞吐

- **WHEN** 以极小行数运行全部场景（smoke）
- **THEN** 每个场景返回有限耗时且吞吐大于零，报告包含全部场景名

#### Scenario: Avro 解码场景自包含

- **WHEN** 在无网络环境运行 avro-decode 场景
- **THEN** 场景经离线 resolver 完成 Confluent wire-format 消息的解码并产出有限耗时与正吞吐，全程不发起任何网络请求

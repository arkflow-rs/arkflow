## MODIFIED Requirements

### Requirement: 场景覆盖内核主路径

基准 SHALL 至少覆盖：线性 SQL 聚合管道、GROUP BY 有状态聚合、过滤投影、JSON 编码往返与宽 schema JSON 解码、状态后端读写、Avro schema-registry 解码、窗口 journaled 持久化、SQL 缓存命中稳态。流式场景 SHALL 走统一内核公开入口（compile_stream + run_job），与生产执行路径一致；Avro 解码场景 SHALL 走真实 `SchemaRegistryCodec::decode` 路径（离线 in-memory `SchemaResolver`，无网络、无外部服务），并 SHALL 覆盖至少三种 schema 宽度以暴露宽度相关的每消息成本；窗口 journaled 持久化场景 SHALL 走生产编译路径构建 journaled 窗口 operator 并覆盖至少三种键基数（N=1/100/1_000 三档，跨三个数量级）以暴露每批成本随键基数的增长；SQL 缓存命中稳态场景 SHALL 在 plan 缓存全命中的稳态下计时，以暴露每批与查询计划构建无关的固定开销。

#### Scenario: 全部场景产出有限正吞吐

- **WHEN** 以极小行数运行全部场景（smoke）
- **THEN** 每个场景返回有限耗时且吞吐大于零，报告包含全部场景名

#### Scenario: Avro 解码场景自包含

- **WHEN** 在无网络环境运行 avro-decode 场景
- **THEN** 场景经离线 resolver 完成 Confluent wire-format 消息的解码并产出有限耗时与正吞吐，全程不发起任何网络请求

#### Scenario: 窗口 journaled 场景分键基数档位报告

- **WHEN** 以键基数 N=1/100/1_000 运行窗口 journaled 场景
- **THEN** 报告为每个基数档位给出独立场景行（独立耗时与吞吐），档位间差异可由开发者自行对比；报告不对档位间数值做通过/失败断言

#### Scenario: SQL 缓存命中稳态场景只计稳态批次

- **WHEN** 运行 sql-hot-cache 场景
- **THEN** plan 缓存预热发生在计时窗口之外，报告的耗时仅包含缓存全命中后的批次处理

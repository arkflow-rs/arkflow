## Why

AI 切片已完成 embedding processor 与 qdrant output（2026-09-23），PLANNING.md 7.3-3 剩余的向量库扩展中 **pgvector 是覆盖面最大的下一步**：PostgreSQL + pgvector 是「够用型」向量库事实标准，大量用户已有 Postgres 而不愿为 PoC 部署独立向量库；给它一个 output，embedding → Postgres 的 RAG 摄取即刻成立。仓库已具备全部基建先例：`sqlx` 0.8（postgres feature）在 workspace（`Cargo.toml:65`）与 arkflow-plugin（`Cargo.toml:78`），`sql.rs` 已建立 Postgres `QueryBuilder` + `ON CONFLICT` upsert 模式（`crates/arkflow-plugin/src/output/sql.rs:113-195`），qdrant output 已确立 vector/payload 列语义。**选型对比**：OTel trace 需引入全新 opentelemetry 依赖栈（workspace 零 otel 依赖）且 span 切面设计面大，独立立项更合适；Milvus 依赖 gRPC SDK，成本高于 REST/SQL 型后端，本变更不做。

## What Changes

- 新增 output `pgvector`（`crates/arkflow-plugin/src/output/pgvector.rs`）：
  - 把每批行 upsert 进 Postgres 表：`id_field`（可选，配置即启用 `ON CONFLICT ... DO UPDATE`）、`vector_field`（默认 `embedding`，接受 FixedSizeList/List(Float32)）、`payload_field`（默认 `payload`，除 id/vector 外的列打包为该 jsonb 列的 JSON 对象；显式置空可禁用）；
  - 连接 `url`（支持 secret 引用）、`max_connections`（默认 4）、`timeout_ms`（默认 30000）；
  - 向量与 payload 以文本参数绑定 + 显式 `::vector` / `::jsonb` cast——零新增依赖；
  - 列缺失、null 向量、空向量返回 `Error::Process`；连接错误返回 `Error::Connection`。
- 组件以 `pgvector` 注册 builder + metadata schema，接入文档体系。

## Capabilities

### New Capabilities

- `pgvector-output`: pgvector output 的配置形状、SQL 生成与 upsert 语义、向量/payload 列映射、错误语义。

### Modified Capabilities

<!-- 无既有能力需求级变更：sql output 不动。 -->

## Impact

- `crates/arkflow-plugin/src/output/`：新增 `pgvector.rs`，`mod.rs` init 接线；无新增依赖。
- 文档：组件页、双 README 组件清单、示例 YAML + manifest 注册、inventory 重新生成。
- 测试：SQL 生成/向量序列化/payload 打包的离线单测（断言生成的 SQL 文本）；带 `#[ignore]` 的真库集成测试（需 pgvector/pgvector Docker 镜像，文档说明运行方式）。

## Non-goals

- 不做 Milvus / Weaviate / Qdrant Cloud 之外的向量库（Qdrant 已有）。
- 不做向量索引创建/迁移（DDL 由用户管理；组件只做 INSERT/UPSERT）。
- 不做 embedding 在线补齐、批量查询侧语义检索。
- 不做 MySQL 侧向量输出（MySQL 无原生向量类型）。
- 不引入 pgvector Rust SDK 或 sqlx json feature——用文本绑定 + cast 等价实现。

## Context

`sql.rs` 已有 Postgres QueryBuilder 模式（引号标识符、`push_values`、`ON CONFLICT ... DO UPDATE` 子句、PgConnectOptions/SSL），但它面向「平铺列」关系表且用单 Connection。pgvector 需要三类特殊值：向量列（`vector` 类型，文本字面量可显式 cast）、jsonb payload、以及可选的 upsert 主键。qdrant output 已确立 vector_field/id_field/payload 语义，pgvector 沿用同一心智模型。

约束：不新增依赖（不用 pgvector SDK、不给 sqlx 加 json feature）；Output trait 的 `connect` 可用于建连接池；真库不可进 CI——离线单测断言生成的 SQL 文本（`QueryBuilder::build().sql()`），真库验证放 `#[ignore]` 集成测试。

## Goals / Non-Goals

**Goals:**
- embedding processor → pgvector 的一步式摄取；已有 Postgres 的用户零新增部署。
- SQL 生成层完全离线可测（文本断言），不依赖真库。
- 向量/payload 语义与 qdrant output 对齐（vector_field/id_field/payload）。

**Non-Goals:** 见 proposal（Milvus、DDL、查询侧、MySQL、SDK）。

## Decisions

1. **独立 `pgvector` 组件而非扩展 `sql` output**。仓库惯例是「一后端一组件」（influxdb/mongodb/redis/sql 各自独立）；pgvector 的配置形状（vector_field/payload_field）与 sql 的平铺列模型差异明显，塞进 sql output 会让两边配置都变复杂。
2. **零依赖的参数 cast 方案**：向量序列化为 pgvector 文本格式 `[1.0,2.0]`，payload 序列化为 JSON 文本，SQL 里对应占位符后跟 `::vector` / `::jsonb`。`QueryBuilder` 的 `separated` + `push_unseparated` 手工拼行元组以在特定占位符后插 cast。备选 pgvector crate（sqlx feature）类型化绑定——拒绝：新增依赖只为省两个 cast，供应链成本不值。
3. **PgPool 而非单 Connection**：`connect()` 建 `PgPoolOptions`（max_connections/默认 4、acquire_timeout=timeout_ms）。output 的 write 可能被内核并发调用，池化避免串行瓶颈；sql.rs 的单连接模式在其平铺低频场景够用，pgvector 摄取是大批量路径。
4. **列布局固定为 `[id?, vector, payload?]`**：`id_field` 配置时首列（upsert 冲突键），`payload_field` 显式置空时第三列省略。UPSERT 更新所有非键列（`EXCLUDED."col"`，复用 sql.rs 的 `postgres_upsert_clause` 语义）。
5. **payload 打包**：除 vector/id 外全部列经 arrow-json `LineDelimitedWriter`（过滤后的 batch）逐行转 JSON 对象，字段名即列名——与 qdrant 的 payload 语义一致；空 payload 列集写入 `{}`。
6. **错误模型**：向量列缺失/类型错/null 向量/空向量 → `Error::Process`（含行号）；payload 列缺失 → `Error::Process`；池获取/执行失败 → `Error::Connection`/`Error::Process`（sqlx 错误文本原样透传，不重试——与 embedding/llm 决策一致）。
7. **表/列名注入面**：标识符直接取配置并双引号包裹（与 sql.rs 同一信任模型：配置由操作员提供）；值一律参数绑定，无注入面。
8. **测试分层**：a) 离线单测断言生成 SQL 全文（列清单、`$N::vector`/`$M::jsonb` cast 位置、ON CONFLICT 子句、禁用 payload 时的形状）；b) 向量序列化（Fixed/List、维度一致性、空向量报错）与 payload 打包单测；c) `#[ignore]` 真库集成（docker pgvector/pgvector:pg16，建表 DDL + 插入 + 冲突更新断言），README 运行说明。

## Risks / Trade-offs

- [用户表缺少唯一约束导致 ON CONFLICT 报错] → 错误信息透传 Postgres 提示；DDL 是用户责任（Non-goal 已声明）。
- [vector 维度与表列不匹配] → Postgres 报错透传（`::vector` cast 失败信息含维度）。
- [文本 cast 的性能损失（相比二进制协议）] → pgvector 文本解析在插入路径占比很小；换取零依赖与可测试性。
- [QueryBuilder 占位符格式假设（Postgres 为 `$N`）] → 离线单测直接断言 SQL 文本，假设被测试钉住。

## Migration Plan

纯新增组件；回滚 = revert。

## Open Questions

无。

## Context

两组件提供全部可复用模式：output `pgvector`（sqlx PgPool、`::vector` 文本 cast、SQL 文本断言测试、`#[ignore]` 真库测试）与 processor `vector_search`（逐行检索 + `buffered` 有界保序并发 + 结果 JSON 列）。pgvector 查询是「每行一条 SELECT + ORDER BY 距离算子 LIMIT k」——需要真实连接才能取回行，因此离线单测覆盖 SQL 生成与行→JSON 映射，真库路径走 `#[ignore]`。

约束：不新增依赖；processor 无 connect 钩子，但连接池建在构建期会阻塞（首次 SELECT 才真正建连，sqlx Pool 是惰性的——构建期 `PgPoolOptions::connect_lazy` 即可，无网络 I/O）。

## Goals / Non-Goals

**Goals:**
- pgvector 成为摄取+检索双完整后端；表形状与 output `pgvector` 写入的表（id, embedding, payload jsonb）直接对称。
- SQL 生成层完全离线可测；检索行序与输入行序一致。

**Non-Goals:** 见 proposal（混合检索、过滤器、索引管理、OTel 本体、分数换算）。

## Decisions

1. **组件名 `pgvector_search`**：与 output `pgvector` 同前缀成对；仓库注册名为 snake_case（README parity 校验强制）。qdrant 后端检索组件是 `vector_search`——两后端两组件，命名各自表意，不做统一泛化接口（避免配置形状互相牵制）。
2. **表侧列名独立配置**：`id_column`（默认 `id`）、`vector_column`（默认 `embedding`）、`payload_column`（默认 `payload`，jsonb；置空禁用）——与 output 写入的表默认形状直接对称。批侧输入列 `vector_field`（默认 `embedding`）。
3. **度量算子映射**：`metric: cosine(默认)|l2|inner_product` → `<=>`|`<->`|`<#>`。返回 `distance` 为算子原始值（pgvector 语义：cosine 距离 ∈[0,2]、inner_product 返回负内积——三种度量下 ORDER BY 升序均为「最近优先」）；不做 1-d 的相似度换算（文档说明，避免口径错误）。
4. **SELECT 形状**：`SELECT "<id_column>"::text AS "id", "<payload_column>"::text AS "payload" FROM "<table>" ORDER BY "<vector_column>" <op> $1::vector LIMIT <top_k>`。id 取文本（bigint/uuid/text 统一，下游 JSON 兼容）；payload 以 `::text` 取回再解析为对象（sqlx 未开 json feature 的等价路径）。禁用 payload 时列与键省略。
5. **连接：构建期 `PgPoolOptions::connect_lazy`**（无网络 I/O，符合 processor 无 connect 钩子的现实），`max_connections`（默认 4）/`acquire_timeout`=timeout_ms。逐行查询经 `buffered(concurrency)` 保序收集。
6. **错误模型**：向量列缺失/类型错/null/空 → `Error::Process`（列名+行号）；行查询执行失败 → `Error::Process`（sqlx 错误透传）；payload JSON 解析失败 → `Error::Process`；空 batch → `ProcessResult::None` 不发查询；构建期拒绝空 `url`/`table`/`vector_field` 与 `top_k: 0`/`concurrency: 0`。
7. **测试**：a) SQL 文本断言（三度量算子、payload 禁用形状、LIMIT）；b) 行→JSON 映射纯函数单测（id/payload/缺失 payload）；c) 向量序列化与错误路径；d) `#[ignore]` 真库往返（建表→output 写入→search 断言最近邻与行序）。

## Risks / Trade-offs

- [逐行 SELECT 在大 batch 下放大往返] → concurrency 可配；真库同机延迟低，跨机场景建议上游控 batch。
- [distance 语义因度量而异易误用] → 文档明示三种度量口径；不做魔法换算。
- [id 全部文本化损失类型] → 下游 JSON/LLM 消费场景文本最兼容；需类型时用户可另查。

## Migration Plan

纯新增组件；回滚 = revert。

## Open Questions

无。

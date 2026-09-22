## Context

三个既有 AI 组件提供模式：milvus output（REST v2 端点形状、code!=0 判败、Bearer/headers/loopback 绕过、mock 测试）、`vector_search`/`pgvector_search`（结果 JSON 列语义、top_k、target_field）。Milvus v2 search：`POST /v2/vectordb/entities/search`，body `{"collectionName", "data": [{"vector": [...]}...], "limit", "outputFields": [...], "searchParams": {"metricType": "COSINE|L2|IP", "params": {}}}`，响应 `{"code": 0, "data": [[命中...], [命中...]]}`——`data[i]` 与输入第 i 个向量对应，且每批只需一次请求。

约束：不新增依赖；schema 字段名由用户 collection 决定（与 output 同一套 field 配置）；离线 mock 可测。

## Goals / Non-Goals

**Goals:**
- Milvus 用户获得流内检索能力，与 output 组成同后端完整 RAG 闭环。
- 批量单请求：N 行向量一次 search，响应按序映射回 N 行（比 qdrant/pgvector 检索的逐行模式更优）。

**Non-Goals:** 见 proposal（filter、partition、radius、混合检索、Qdrant 批量化、gRPC）。

## Decisions

1. **批量单请求而非逐行并发**：v2 search 的 `data` 数组天然多向量，一次往返完成整批——不需要 `buffered` 并发（与 qdrant/pgvector 检索的实现分叉是 API 形状差异使然，组件行为对用户一致：按行序回填）。`limit` 全批共用 `top_k`。
2. **响应映射契约**：`data` 必须为数组的数组且长度等于输入行数，否则 `Error::Process`（防 Milvus 兼容变体静默错位）；每个命中取 `id_field`（配置时）、`distance`、`payload_field`（配置时，原样对象）组成紧凑 JSON——键名归一化为 `id`/`distance`/`payload`，与 `vector_search`/`pgvector_search` 的结果形状对齐，下游 llm prompt 无需按后端区分。
3. **metricType 配置**：`metric: COSINE(默认)|L2|IP`（Milvus 命名，大写枚举），放入 `searchParams.metricType`；必须与 collection schema 声明的度量一致，不一致由 Milvus 报 code!=0 透传。
4. **outputFields**：`id_field` 配置时含之、`payload_field` 非空时含之；命中对象的键名即 collection 字段名——映射时重命名为归一化键。`id_field` 未配置的 auto-id 集合：outputFields 仅 payload，命中对象无 `id` 键。
5. **判败复用**：HTTP 非 2xx（状态码+截断体）与 HTTP 200 + `code != 0`（code+message）均返回 `Error::Process`；`code` 缺失宽容（与 milvus output 一致）。
6. **错误模型**：向量列缺失/类型错/null/空、`data` 形状错位 → `Error::Process`（含行号或原因）；空 batch → `ProcessResult::None` 不发请求；构建期拒绝空 `url`/`collection` 与 `top_k: 0`。
7. **测试**：mock server 断言请求体（collectionName/data/limit/outputFields/metricType 有无）与响应映射（两行 × 两命中、归一化键名）、code!=0、HTTP 404、null 向量、空 batch、构建期校验——全部离线。

## Risks / Trade-offs

- [批量响应体上限（大 batch × 大 top_k）] → 与 output 同理，上游 batch 控制责；文档说明。
- [Milvus v2 search 响应字段兼容性（entities 嵌套形态的历史变体）] → 按 `data[i][j]` 顶层扁平形态实现（2.4+ 行为），旧版本/网关变体报形状错透传，不做多形态兼容。
- [metricType 与 schema 不一致] → Milvus 报错透传（code!=0 路径）。

## Migration Plan

纯新增组件；回滚 = revert。

## Open Questions

无。

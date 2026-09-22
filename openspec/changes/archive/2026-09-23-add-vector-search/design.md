## Context

摄取侧组件（embedding processor、qdrant/pgvector output）与生成侧（llm processor）已确立全部工程模式：builder + metadata 注册、`parse_config` 校验、Bearer/headers/timeout、loopback 代理绕过、`futures_util::stream::buffered` 有界保序并发、`Error::Process` 错误语义、进程内手写 HTTP/1.1 mock 测试。qdrant 检索端点为 `POST /collections/{collection}/points/search`（body `{vector, limit, with_payload, score_threshold?}`，响应 `result: [{id, score, payload?}]` 按得分降序）。

约束：不新增依赖；processor 无 connect 钩子，Client 构建期创建；检索结果需进入列式批次且能被 llm processor 的 `prompt_template` 直接消费。

## Goals / Non-Goals

**Goals:**
- 补全流式 RAG 闭环：与 embedding（向量化）、llm（生成）组合即可在引擎内跑通「检索增强生成」。
- 结果列即 JSON 文本——下游 llm/sql/vrl 均可直接消费，避免跨组件的嵌套 Arrow 类型兼容问题。

**Non-Goals:** 见 proposal（pgvector 查询侧、filter 下推、混合检索、rerank、结果展开多行、本地相似度计算）。

## Decisions

1. **结果形态：单 Utf8 列（`target_field`，默认 `matches`），内容为 JSON 数组文本**（每元素 `{id, score, payload}`）。备选 Arrow `List<Struct>`——列式上更纯，但 vrl/arrow_to_json/sql 对嵌套类型支持参差，且 RAG 主消费方（llm prompt）要的恰是 JSON 文本；文档明示形状。id 保留 qdrant 返回类型（整数或字符串），score 为 number，payload 原样嵌入。
2. **逐行请求 + `buffered(concurrency)` 有界保序**——与 llm processor 完全同构（chat 协议与 search 端点都是一行一请求）。默认并发 4。
3. **查询向量来源**：`vector_field` 列（上游 embedding processor 产出）。不在本组件内做文本向量化——组合优于内置（用户可换 embedding 后端）。
4. **`score_threshold` 可选透传**：配置了才出现在请求体（部分后端版本对显式 null 报错，与 llm 的 temperature 处理同因）。`with_payload` 固定 true（v1 无关闭场景）。
5. **错误模型**：非 2xx → `Error::Process`（状态码 + 512 字符截断响应体）；向量列缺失/非 Float32 列表/null 向量/空向量 → `Error::Process`（含列名与行号）；空 batch → `ProcessResult::None` 不发请求；构建期拒绝空 `url`/`collection`/`vector_field` 与 `top_k: 0`/`concurrency: 0`。
6. **响应解析**：`result` 数组原样保留为 `serde_json::Value` 后整体序列化（不逐字段结构化——v1 不需要强类型，qdrant 兼容变体的容错更好）；`result` 键缺失视为错误。
7. **鉴权/headers/loopback 代理绕过/测试**：逐字复用既有模式；mock server 断言请求体（limit/threshold）与响应回填行序。

## Risks / Trade-offs

- [JSON 列牺牲列式纯度] → 换取全下游组件可消费性与嵌套类型兼容性；后续可按需增加结构化列选项。
- [大 top_k 放大响应体] → top_k 默认 5 且可配；无上限截断（用户自担）。
- [并发触发后端限流] → 同 llm：concurrency 可配 + 429 透传走 error_output。

## Migration Plan

纯新增组件；回滚 = revert。

## Open Questions

无。

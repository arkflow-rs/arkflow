## Context

qdrant output 已确立向量/ID/payload 三列语义与全部工程模式（builder+metadata、Bearer/headers/timeout、loopback 代理绕过、`Error::Process`、mock server 测试）。Milvus 与 Qdrant 的关键 API 差异有三：REST 端点为 `/v2/vectordb/entities/upsert`（2.4+）；**失败以 HTTP 200 + 响应体 `{"code": <非0>, "message": ...}` 表达**（HTTP 层常为 200）；鉴权是 `Authorization: Bearer <user>:<password>`。

约束：不新增依赖（不用 gRPC SDK/tonic）；schema/DDL 由用户管理；离线可测（mock server）。

## Goals / Non-Goals

**Goals:**
- embedding processor → Milvus 的一步式摄取，配置形状与 qdrant output 保持同构（vector_field/id_field/payload_field）。
- 覆盖 Milvus REST 的特有语义（code!=0 判败、Bearer 凭据形式）。

**Non-Goals:** 见 proposal（OTel 本体、gRPC、DDL、partition、查询侧、拆批）。

## Decisions

1. **API 版本：REST v2 vectordb（`/v2/vectordb/entities/upsert`）**，目标 Milvus 2.4+。备选 v1（`/v1/vector/upsert`）已属 legacy；gRPC SDK 需 tonic 全家桶——拒绝。URL 前缀可经 `url` 配置整体覆盖（自建网关场景）。
2. **行对象布局**：每行 `{"<id_field>": id, "<vector_field>": [...], "<payload_field>": {...}}`，字段名与用户 collection schema 对齐；`id_field` 缺省时省略 id 键（Milvus auto-int64）。payload 是 JSON 对象——要求用户 schema 中该字段为 JSON 类型或启用 dynamic field（文档说明）。
3. **判败规则（本组件的核心语义）**：HTTP 非 2xx → 错误（状态码+截断体）；HTTP 2xx 且 `code != 0` → 错误（含 code 与 message）；`code` 缺失视为成功（宽容老版本）。这条与 qdrant 的差异必须有独立测试钉住。
4. **鉴权**：`api_key` → `Authorization: Bearer <api_key>`；Milvus 约定凭据值为 `<username>:<password>`，组件不感知其内部结构，原样放 Bearer。未配置不发鉴权头。
5. **请求频率**：一 batch 一请求（与 qdrant 一致）；不自动拆批——Milvus REST 体上限由用户以 input `batch_size`/buffer 控制，文档说明。
6. **向量序列化**：与 pgvector/qdrant 相同的 Float32 列表提取 → JSON 数组绑定（serde_json 直接嵌套进行对象，非文本 cast——Milvus REST 是纯 JSON API）。
7. **错误模型**：向量列缺失/类型错/null/空 → `Error::Process`（列名+行号）；请求失败/判败 → `Error::Process`；构建期拒绝空 `url`/`collection` 与空 `vector_field`。不重试（与全组件一致）。
8. **测试**：mock server 断言路径、行对象形状、auto-id 省略、Bearer 有无；专测「HTTP 200 + code=1」判败路径与「HTTP 200 + code=0」成功路径；HTTP 错误、null 向量、空 batch、构建期校验。loopback 代理绕过沿用。

## Risks / Trade-offs

- [REST v2 仅覆盖 2.4+] → 文档声明最低版本；gRPC 留给未来按需。
- [payload 需 JSON 字段或 dynamic field] → DDL 是用户责任（文档给建表示例）。
- [单请求大批次触体上限] → 文档警示 + 上游控制。

## Migration Plan

纯新增组件；回滚 = revert。

## Open Questions

无。

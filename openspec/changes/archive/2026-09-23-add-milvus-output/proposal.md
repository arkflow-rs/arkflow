## Why

PLANNING.md 7.3-3 剩余的向量库扩展中，Milvus 是 Qdrant 之后开源向量库的第二主流选择（AI 应用部署量大）。本轮先对 OTel trace 做了完整立项评估（依赖选型：`opentelemetry_sdk` + `opentelemetry-otlp` http-json + `tracing-opentelemetry`；span 切面：统一内核 chain/task 循环 + Job 根 span；OTLP 配置挂 `observability` 节）——**结论：插桩点位于统一内核最核心路径（`unified-execution-kernel` spec 保护范围），高回归风险，应作为内核插桩专项单独立项并精读内核代码后再动**，不适合作为本轮快速切片。故转而立项 `add-milvus-output`：走 Milvus 2.4+ 的 REST v2 vectordb API（`POST /v2/vectordb/entities/upsert`），`reqwest` 已在依赖中，与 qdrant output（`crates/arkflow-plugin/src/output/qdrant.rs`）同构——向量/ID/payload 语义、鉴权、错误模型、mock 测试模式全部复用，风险低、价值直接（AI 用户第二大向量库）。

## What Changes

- 新增 output `milvus`（`crates/arkflow-plugin/src/output/milvus.rs`）：
  - 每批一次 `POST {url}/v2/vectordb/entities/upsert`，`data` 数组每行一个对象：`{vector_field: [...], payload_field: {...}, id_field: 值?}`（id 缺省时省略，由 Milvus auto-id 生成）；
  - 配置 `url`/`collection`/`vector_field`（默认 `embedding`）/`id_field`（可选）/`payload_field`（默认 `payload`，置空禁用）/`api_key`（Bearer，Milvus REST 的 `<user>:<password>` 凭据形式）/`timeout_ms`/`headers`；
  - **Milvus 特有语义**：HTTP 200 + 响应体 `code != 0` 仍视为失败（错误含 `code` 与 `message`）；
  - 向量列缺失/类型错/null/空向量返回 `Error::Process`；空 batch 直通。
- 组件以 `milvus` 注册 builder + metadata schema，接入文档体系。

## Capabilities

### New Capabilities

- `milvus-output`: milvus output 的配置形状、REST v2 upsert 语义（含 code!=0 失败判定）、向量/payload/ID 列映射、错误语义与鉴权。

### Modified Capabilities

<!-- 无既有能力需求级变更。 -->

## Impact

- `crates/arkflow-plugin/src/output/`：新增 `milvus.rs`，`mod.rs` init 接线；无新增依赖。
- 文档：组件页、双 README 组件清单、示例 YAML + manifest 注册、inventory 重新生成。
- PLANNING.md 记录 OTel trace 立项评估结论（依赖/span 切面/OTLP 配置），供专项立项时直接取用。

## Non-goals

- 不做 OTel trace 本体（评估结论见上，专项立项）。
- 不做 Milvus gRPC SDK（REST v2 覆盖 2.4+，避免 tonic 依赖树）。
- 不做 collection/DDL 管理（schema 由用户创建，组件只 upsert）；不做分区/partition 指定、RBAC 管理。
- 不做查询侧（qdrant 已有 `vector_search`；milvus 查询侧按需后补）。
- 不做单批超限拆分（请求体上限由上游 batch_size 控制，文档说明）。

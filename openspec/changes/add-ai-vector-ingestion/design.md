## Context

Processor 侧模式参照 `vrl.rs`（builder + metadata 注册、`MessageBatchRef → ProcessResult`、空 batch → `ProcessResult::None`、错误走 `Error::Process` 由引擎 error_output 语义承接）；Output 侧模式参照 `http.rs`（connect 惰性建 reqwest Client、`write` 逐批、timeout/retry 配置、codec 编码钩子）。数据模型是 Arrow `RecordBatch`，追加列 = 重建 schema fields + arrays。 Secret 引用（`secret-references` spec）保证 `api_key` 不必明文。

## Goals / Non-Goals

**Goals:**
- 最小可用的「文本流 → embedding → Qdrant」摄取管道，两个组件各自独立可用。
- 批量语义：一个 batch 一次（或分块多次）HTTP 调用，发挥列式优势。
- 全部配置走既有体系：serde default、metadata schema、secret 引用兼容。

**Non-Goals:**
- 本地推理、LLM 补全、其他向量库、embedding 缓存（见 proposal Non-goals）。

## Decisions

1. **OpenAI 兼容协议为唯一 wire 协议**（`POST {api_base}/embeddings`，body `{model, input: [..]}`，响应 `data[i].embedding`）。自托管（vLLM/Ollama/TEI 的 OpenAI 兼容层）与云服务都讲这个协议，覆盖面最大；Azure 的特殊鉴权头用 `headers` 覆盖解决。备选：多 provider 适配层——拒绝，v1 不背多协议复杂度。
2. **向量列类型 `FixedSizeList(Float32, dim)`**：Qdrant/主流向量库语义即定长向量；每批内 dim 必须一致（API 契约保证），不一致报 `Error::Process`。备选 `List(Float32)`（可变长）——拒绝，丢失定长约束。dim 在响应时确定，构建期不预校验。
3. **embedding 请求分块**：`batch_size`（默认 32）切分输入行，顺序发送、失败即停。不分块（单请求全批）在超大 batch 下有请求体上限风险；逐行发送则丧失批量折扣与吞吐。
4. **输入列要求 `Utf8`（或 `LargeUtf8`）**：非字符串列在构建后首次 process 时报错（构建期不知道 batch schema；与 SQL processor 运行期校验同一哲学）。null 行报错——embedding 无语义上的 null，宁 fail 不投毒。
5. **qdrant upsert 走 `PUT /collections/{collection}/points`**（`{"points": [{id, vector, payload}]}`，`wait=true`）。id 缺省用「批序号 + 行号」不稳定——改为：`id_field` 缺省时让 Qdrant 自动生成（省略 id 字段），但 `id_field` 配置时必须指向 integer/utf8 列。
6. **vector 列提取**：`FixedSizeList(Float32)` 与 `List(Float32)` 都接受（pipeline 中间可能有 SQL 加工）；`value(i)` 逐行取 `f32` slice。
7. **鉴权与 headers**：`api_key` → `Authorization: Bearer`；`headers` map 允许覆盖（Azure `api-key`）。api_key 为空则不发鉴权头（本地无鉴权部署）。
8. **重试语义**：qdrant output 沿用 http.rs 的 retry_count（连接类错误重试、4xx 不重试）；embedding processor v1 不做重试（有 error_output，且 embedding 请求贵，重试策略留给用户侧）——文档明示。
9. **测试无网络依赖**：进程内 mock server（`tokio::net::TcpListener` + 手写最小 HTTP/1.1 应答）覆盖 happy path、分块、API 错误透传、dim 不一致、鉴权头断言；不引入 mockito/wiremock 新 dev-dep。

## Risks / Trade-offs

- [无 schema registry 时向量库 dim 漂移] → Qdrant 报错透传，错误信息含响应体（截断）。
- [embedding API 明文计费风险（重试翻倍）] → v1 无重试，文档警示。
- [OutOfOrder/重复摄取导致 upsert 覆盖] → Qdrant upsert 幂等由用户 id 策略决定，文档说明 at-least-once 语义。
- [reqwest TLS 栈特性差异] → 沿用 workspace reqwest 现有 features，不特殊化。

## Migration Plan

纯新增组件，无存量行为变化；回滚 = revert。组件注册后 `components list`/schema/文档由既有生成链路自动覆盖。

## Open Questions

无。

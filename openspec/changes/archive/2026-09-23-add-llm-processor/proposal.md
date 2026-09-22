## Why

PLANNING.md 7.3-3 的 AI 切口第一步 `add-ai-vector-ingestion`（embedding processor + qdrant output，2026-09-23）打通了「文本 → 向量 → 向量库」的 RAG 摄取侧，但「文本 → LLM 补全/改写」这一同样常见的能力仍为零：`crates/arkflow-plugin/src/processor/` 现有 7 种组件（batch/json/protobuf/python/sql/vrl/embedding）无一能调用 LLM。流式管道里的典型场景——内容摘要、翻译、改写、分类标注、结构化抽取——都是一次 chat completions 调用即可完成的，用户目前只能绕道 Python UDF（引入 PyO3 运行时与 HTTP 手写）。`reqwest`/`futures-util` 已在 arkflow-plugin 依赖中，embedding processor（`crates/arkflow-plugin/src/processor/embedding.rs`）已建立鉴权、错误语义、loopback 代理绕过与进程内 mock 测试的完整先例，本变更是同栈的第二个 AI 组件。

## What Changes

- 新增 processor `llm`（`crates/arkflow-plugin/src/processor/llm.rs`）：
  - 调 OpenAI 兼容 `POST {api_base}/chat/completions`（`model`/`messages`/可选 `temperature`/`max_tokens`），取 `choices[0].message.content`；
  - 输入取 `field` 列逐行发起请求（chat 协议无批量形态），`concurrency` 有界并发且结果按行序回填为 `target_field`（Utf8 列，默认 `response`）；
  - `system_prompt`（可选）与 `prompt_template`（可选，`{{value}}` 占位符）控制用户消息构造；均缺省时原文即用户消息；
  - `api_key`（Bearer）/`headers`/`timeout_ms`/loopback 代理绕过与 embedding processor 同语义，`api_key` 支持 secret 引用；
  - 失败（非 2xx、响应缺 content、null 输入）返回 `Error::Process` 走 error_output 语义。
- 组件以 `llm` 注册 builder + metadata schema，接入 `components list/show/schema` 与文档体系。

## Capabilities

### New Capabilities

- `llm-processor`: LLM processor 的配置形状、逐行请求与并发语义、消息构造、错误语义与鉴权（secret 引用兼容）。

### Modified Capabilities

<!-- 无既有能力需求级变更。 -->

## Impact

- `crates/arkflow-plugin/src/processor/`：新增 `llm.rs`，`mod.rs` init 接线；无新增依赖（reqwest/futures-util/tokio 已在）。
- 文档：组件页、双 README 组件清单、示例 YAML + manifest 注册、inventory 重新生成。
- README 的 AI 叙事从「向量化摄取」扩展到「LLM 补全/改写」。

## Non-goals

- 不做流式（SSE）响应、多模态（图像/音频）输入、function calling / tool use、JSON mode——后续按需立项。
- 不做对话记忆 / 会话状态（processor 无状态，每行独立完成）。
- 不做本地推理（ONNX/candle）与 token 计费可观测。
- 不做按行错误路由（行内失败使整批进入 error_output，与 embedding 一致）。

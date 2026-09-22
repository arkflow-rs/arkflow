## Context

embedding processor（同 crate，2026-09-23 合入）已确立 OpenAI 兼容组件的全部工程模式：builder + metadata 注册、`parse_config` 构建期校验、Bearer/headers/timeout、loopback 代理绕过、`Error::Process` 错误语义、进程内手写 HTTP/1.1 mock 测试。差异点只有一个：chat completions 协议没有批量输入形态，一行文本 = 一次请求，因此需要逐行并发而不是分块。

约束：不新增依赖（`futures-util` 已在 arkflow-plugin，`stream::buffered` 提供保序有界并发）；无状态 processor（trait 无 connect 钩子，Client 构建期创建）。

## Goals / Non-Goals

**Goals:**
- 最小可用的流内 LLM 调用：摘要、翻译、改写、分类、抽取一句话配置即可接入。
- 逐行请求、结果按行序回填；并发有界，不因大 batch 打爆上游。
- 与 embedding processor 完全一致的鉴权/错误/测试模式。

**Non-Goals:** 见 proposal（流式 SSE、多模态、tool use、会话状态、本地推理、行级错误路由）。

## Decisions

1. **并发模型：`futures_util::stream::iter(...).map(row_request).buffered(concurrency)`**。备选一：顺序逐行——LLM 延迟数百 ms，128 行 batch 串行不可接受；备选二：`tokio::task::JoinSet` + 自带索引重排—— unordered 需手工回填且每个任务要克隆 client/config。`buffered` 保序（按提交顺序产出）、有界（在途请求 ≤ concurrency）、零新依赖，三者全占。默认 `concurrency: 4`：够快又不会触发大多数 provider 的限流。
2. **消息构造：`system_prompt`（可选）+ 用户消息三态**。`prompt_template` 含 `{{value}}` 时做子串替换；缺省时原文即用户消息。不做多占位符/字段引用模板——多字段场景用户可先用 SQL/VRL 拼好列，保持 processor 单列职责。
3. **可选参数省略**：`temperature`/`max_tokens` 未配置时从请求体中完全省略（`serde_json::json!` 手工装配 + `skip`），不发送 `null`——部分兼容网关对显式 null 报错。
4. **响应提取：`choices[0].message.content`**。`choices` 为空（内容过滤/长度截断极端场景）或 `content` 为 null 时报 `Error::Process` 并注明行号与原因——宁可失败不产出空串投毒下游。`finish_reason` 不解析（Non-goal：不做重试策略）。
5. **null 输入与类型**：`field` 列必须 Utf8/LargeUtf8，null 行报错——与 embedding 完全一致（一致性 > 特殊宽容）。
6. **输出列 `target_field` 追加，批内重建 RecordBatch**——复用 embedding 的 `append` 模式（fields + columns + `RecordBatch::try_new`），类型 Utf8。
7. **鉴权/headers/loopback 代理绕过/错误体截断**：逐字复用 embedding 的实现模式（客户端构建期 `no_proxy()` 判定 loopback host；错误体 512 字符截断）。
8. **重试**：不重试（同 embedding 决策——LLM 调用贵，重试策略交用户）；4xx/5xx 均一次失败。
9. **测试**：进程内 mock server 复用 embedding 的手写 HTTP/1.1 模式；用 handler 内 sleep + 行序断言验证并发下保序；请求体断言覆盖 system_prompt 三态、可选参数省略。

## Risks / Trade-offs

- [并发放大限流 429] → concurrency 默认 4 且可配置；429 透传为批错误，由 error_output/上游重投递承接。
- [大 batch 全部 in-flight 失败浪费 token] → 失败即停（buffered 遇错短路后续不再提交？——不，buffered 已提交的请求仍会完成；文档明示，控制 batch_size 是上游责任）。
- [模板占位符拼写错误静默原文发送] → `{{value}}` 是唯一占位符，文档显著说明；不做模板校验（过度工程）。

## Migration Plan

纯新增组件；回滚 = revert。

## Open Questions

无。

## Why

PLANNING 7.3 方向③遗留的「流式 SSE/tool use（挂起）」解挂：llm processor 目前只支持非流式 chat completions，长补全会撞上 provider/网关的非流式超时；agent 场景需要 tool use（function calling）才能落地。内核的批次模型不变——SSE 增量在处理器内部聚合为整行最终文本，tool_calls 以 JSON 列追加。

## What Changes

- llm processor 新增配置：`stream`（bool，默认 false，请求体带 `"stream": true`，响应按 SSE 帧解析：`data: {json}` 增量聚合 content，`data: [DONE]` 终止）；`tools`（JSON 透传到请求体）；`tool_calls_column`（配置时追加 tool_calls JSON 文本列，无 tool_calls 时为空串）。
- 流式 tool_call 分片按 index 合并（id/name 取首个非空，arguments 字符串拼接），与 OpenAI 流式协议一致。
- v1 从完整响应体解析 SSE 帧（处理器仍按行输出完整文本；流式的价值在绕开非流式超时与网关兼容），不做逐 token 下游发射（内核批次模型的边界，后续如需另立项）。
- 注册 schema 增补三个属性（additionalProperties: false 约束下必须显式加入）。

## Capabilities

### Modified Capabilities

- `ai-vector-ingestion`: llm processor 新增流式与 tool use 需求。

## Impact

- `crates/arkflow-plugin/src/processor/llm.rs`（配置/请求/SSE 解析/tool_calls 合并/列追加 + 新测试）
- 文档：llm.md en/zh；PLANNING 方向③解挂记录。

## Non-goals

- 不做逐 token 下游发射/背压式流式输出（内核批次模型边界）。
- 不做 assistant 消息回传/多轮 tool 执行循环（由上游 agent 编排）。
- 不做其他 provider 的流式协议（仅 OpenAI 兼容 SSE）。

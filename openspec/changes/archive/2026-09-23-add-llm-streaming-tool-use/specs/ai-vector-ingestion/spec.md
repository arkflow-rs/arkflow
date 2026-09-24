# ai-vector-ingestion 变更（Delta）

## ADDED Requirements

### Requirement: llm 流式补全（SSE）

`stream=true` 时 llm processor SHALL 在请求体携带 `"stream": true`，并将响应按 OpenAI 兼容 SSE 帧解析：`data: {json}` 增量的 `choices[0].delta.content` SHALL 按到达顺序拼接为该行最终文本，`data: [DONE]` SHALL 终止；缺失 `[DONE]` 时 SHALL 以响应结束为终止。输出列语义与非流式一致（每行一个完整补全）。非法 SSE 数据 SHALL 以 Error::Process 失败。

#### Scenario: 增量聚合为整行文本

- **WHEN** mock API 以三个 content 增量与 `[DONE]` 回应 stream 请求
- **THEN** 输出行的 response 列为全部增量拼接的完整文本，请求体含 `"stream":true`

### Requirement: llm tool use（function calling）

`tools` 配置 SHALL 原样透传到请求体。配置 `tool_calls_column` 时，processor SHALL 追加该列：非流式响应取 `choices[0].message.tool_calls`，流式响应 SHALL 按 tool call index 合并分片（id/name 取首个非空值，arguments 按序拼接）后序列化为 JSON 数组文本；无 tool_calls 时该列 SHALL 为空字符串。未配置 `tool_calls_column` 时 SHALL 不追加列（向后兼容）。

#### Scenario: 非流式 tool_calls 落列

- **WHEN** 配置 tools 与 tool_calls_column 且响应 message 含 tool_calls
- **THEN** 该列为 tool_calls 的 JSON 文本且请求体透传 tools

#### Scenario: 流式 tool_call 分片按 index 合并

- **WHEN** 流式响应以两个 arguments 分片（同一 index）传递 tool call
- **THEN** tool_calls 列为合并后参数完整 JSON 的数组文本

# Tasks: add-llm-streaming-tool-use

## 1. 实现

- [x] 1.1 配置三字段 + schema 增补（stream/tools/tool_calls_column）
- [x] 1.2 SSE 帧解析器（content 聚合 + [DONE] + 容错）
- [x] 1.3 tool_calls 合并（非流式直取；流式按 index 合并分片）+ 列追加

## 2. 测试与文档

- [x] 2.1 五个测试：流式聚合、tools 透传与非流式落列、流式分片合并、无 [DONE] 容错、未配置列不追加
- [x] 2.2 llm.md en/zh + 全量验证 + 归档 + PLANNING 解挂 + 推送

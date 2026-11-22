## ADDED Requirements

### Requirement: AI 处理器的 HTTP 请求 SHALL 经共享骨架并支持有界重试

`embedding`、`llm`、`vector_search`、`milvus_search` 处理器的 HTTP 客户端构建、请求体截断、URL 基址拼接与 bearer 头应用 SHALL 使用共享工具模块（不再持有逐字符本地副本）；对暂态 HTTP 失败（429/5xx/传输错误）SHALL 按 `retry_count`（可选，默认 3，0=关闭）经共享重试实现执行有界指数退避；4xx 与数据类确定性错误（维度不一致、非 Utf8 列）MUST 保持立即失败。

#### Scenario: 处理器不再持有重复工具副本
- **WHEN** 检查四个 AI 处理器源码中的 `truncate_body`/HTTP client 构建/`headers` 应用
- **THEN** 均引用共享工具模块，无逐字符本地副本

#### Scenario: 处理器 429 重试
- **WHEN** 任一 AI 处理器收到 429 响应且默认配置
- **THEN** 按共享退避重试至多 3 次，耗尽后按既有错误透传语义失败

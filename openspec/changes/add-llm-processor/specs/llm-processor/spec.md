# llm-processor 变更（Delta）

## ADDED Requirements

### Requirement: LLM processor 的配置与请求语义

`llm` processor SHALL 接受配置 `api_base`（必填）、`model`（必填）、`field`（必填，输入 Utf8 列名）、`target_field`（可选，默认 `response`）、`system_prompt`（可选）、`prompt_template`（可选，`{{value}}` 占位符）、`temperature`（可选 number）、`max_tokens`（可选 integer）、`concurrency`（可选，默认 4）、`api_key`（可选，支持 secret 引用）、`timeout_ms`（可选，默认 30000）与 `headers`（可选 map）。processor SHALL 对输入 batch 的每一行发起一次 `POST {api_base}/chat/completions`，请求体含 `model` 与 `messages`（`system_prompt` 配置时含 system 消息；用户消息为 `{{value}}` 替换结果或原文），`temperature`/`max_tokens` 仅在配置时出现在请求体中，并 SHALL 把每行响应的 `choices[0].message.content` 按行序回填为 `target_field` Utf8 列。

#### Scenario: 逐行补全按行序回填

- **WHEN** 输入 batch 含 3 行 `text` 列，LLM 对各行返回内容 `r0`/`r1`/`r2`
- **THEN** 输出 batch 在原列之外含 `response` Utf8 列，第 i 行的值为 `r{i}`，且原始 `text` 列保持不变

#### Scenario: system_prompt 三态构造

- **WHEN** 分别配置「仅 system_prompt」「仅原文」「prompt_template: 'Translate: {{value}}'」
- **THEN** 请求 messages 分别为「system+原文」「仅原文 user 消息」「user 消息为替换后的 'Translate: <行值>'」

#### Scenario: 可选参数缺省时不出现在请求体

- **WHEN** 未配置 `temperature` 与 `max_tokens`
- **THEN** 请求体不含 `temperature` 与 `max_tokens` 键；配置时以数值原样出现

#### Scenario: 有界并发且保序

- **WHEN** 输入 6 行、`concurrency` 为 2、各行响应延迟不同
- **THEN** 在途请求数不超过 2，输出仍按行序回填且全部成功

#### Scenario: api_key 鉴权与 secret 引用

- **WHEN** 配置 `api_key: "${env:LLM_KEY}"`（解析后 `sk-x`）
- **THEN** 每个请求携带 `Authorization: Bearer sk-x`；未配置 api_key 时不携带鉴权头

#### Scenario: 空 batch 直通

- **WHEN** 输入 batch 为 0 行
- **THEN** process 返回 `ProcessResult::None`，不发起 HTTP 请求

### Requirement: LLM processor 的错误语义

以下情形 SHALL 返回指明行号与原因的 `Error::Process`（批次走 error_output 语义）：`field` 列缺失或非 Utf8；null 输入行；API 非 2xx（错误含状态码与截断响应体）；响应 `choices` 为空或 `content` 缺失。构建期 SHALL 拒绝 `api_base`/`model`/`field` 为空与 `concurrency` 为 0 的配置。

#### Scenario: 非 2xx 透传

- **WHEN** LLM API 返回 429
- **THEN** process 返回错误，信息包含 429 与响应体片段

#### Scenario: 响应缺 content

- **WHEN** 响应 `choices[0].message.content` 为 null 或 `choices` 为空
- **THEN** process 返回错误，不产出空串结果

#### Scenario: null 输入行

- **WHEN** `field` 列某行为 null
- **THEN** process 返回错误并指明行号

#### Scenario: 构建期校验

- **WHEN** 配置 `concurrency: 0` 或必填字段为空
- **THEN** builder 返回 `Error::Config`，不创建组件

### Requirement: 组件注册与文档体系一致性

组件 SHALL 以 `llm` 注册 builder 与 metadata schema（`components list/show/schema` 可见），SHALL 有带 `components:` front matter 的文档页、双 README 组件清单条目、注册进 example-manifest 的示例 YAML；生成的 inventory SHALL 与注册一致。

#### Scenario: registry 一致性

- **WHEN** 运行 workspace 测试（registry_consistency、docs_inventory_snapshot）与 `pnpm docs:check`
- **THEN** 全部通过，生成文件无需手工编辑

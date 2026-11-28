## Context

ArkFlow 的 Hub–Agent 控制面（`crates/arkflow-server`）与 React Web 控制台（`console/`）已具备完整的资源治理、RBAC 鉴权、审批流、可观测与可视化 DAG 编排能力；同时数据面已验证基于 `reqwest` 的 OpenAI 兼容 chat completions + tool calling + SSE 消费模式（`crates/arkflow-plugin/src/processor/llm.rs`）。本设计以**最小侵入、零存储表变更、零 Agent 协议变更**为原则，在 Hub 层引入纯 Tool-Calling 编排器与 SSE 端点，并在 Console 中引入提案式（Proposal-only）Copilot 交互闭环。

## Goals / Non-Goals

- **Goals**:
  1. 在 `arkflow-server` Hub 模式提供开箱即用的 OpenAI 兼容 AI Copilot 后端代理，密钥仅保存在服务端并支持 `${env:}` / `${secret:}` 引用。
  2. 提供基于引擎真实插件 Schema 与校验逻辑（`arkflow_core::component`、`parse_and_validate`、`deep_validate_job`）的自纠错生成闭环。
  3. 提供严格受调用者 `OperatorPrincipal::can_scope` 约束、且经敏感信息脱敏的只读遥测排查能力。
  4. 在 Console 提供全局跨路由 Copilot 抽屉、页面上下文触发器与结构化提案卡片（Stream YAML / JobSpec DAG / 诊断建议深链）。
- **Non-Goals**:
  1. 不支持单机 Standalone 控制面；不改 Agent wire 协议；不在 SQLite/Postgres 新增对话持久化表。
  2. 不允许 AI 直接调用任何写接口或绕过 `approval_required` / RBAC。
  3. 不兼容不支持 OpenAI tool calling 的模型（检测不支持或协议异常时显式报错）。

## Decisions

### 1. 配置模型与启停机制

在 `crates/arkflow-server/src/api/mod.rs` 的 `ServerConfig`（或仅挂载于 `HubStartupConfig` + `ServerConfig` 供 `hub_router` 读取，且 `ServerConfig::from_engine` 始终保持关闭）新增 `ai: AiCopilotConfig`（`#[serde(default)]`）：

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiCopilotConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub api_base: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default = "default_ai_timeout_ms")]
    pub timeout_ms: u64, // 默认 90_000ms，覆盖多轮 tool calling 总时长
    #[serde(default = "default_ai_max_tool_rounds")]
    pub max_tool_rounds: usize, // 默认 8
    #[serde(default = "default_ai_max_tokens")]
    pub max_tokens: Option<u32>, // 默认 Some(4096)
    #[serde(default = "default_ai_rate_limit_rpm")]
    pub requests_per_minute: u32, // 默认 20
    #[serde(default = "default_ai_rate_limit_burst")]
    pub burst: u32, // 默认 5
}
```

- **Env 覆盖**（`startup_config.rs:apply_env_overrides`）：`ARKFLOW_AI_ENABLED`、`ARKFLOW_AI_API_BASE`、`ARKFLOW_AI_MODEL`、`ARKFLOW_AI_API_KEY`、`ARKFLOW_AI_TIMEOUT_MS`、`ARKFLOW_AI_MAX_TOOL_ROUNDS`。
- **启动/运行期校验**：当 `ai.enabled = true` 但 `api_base` 或 `model` 为空或非 `http://` / `https://` scheme 时，Hub 启动校验拒绝启动（fail-closed，避免部署静默失效）；`api_key` 可选（适配内网无鉴权 Ollama/vLLM）。
- **Standby 与 Capability 通告**：
  - `hub_router` 的 standby 中间件（`api/mod.rs:845`）自动拦截 standby Hub 上的 `/api/v1/ai/*` 请求并返回 `503 hub_standby`。
  - `hub_system`（`api/nodes.rs:52`）仅在 `ai.enabled == true` 时向 `capabilities` 数组追加 `"ai_copilot"`（可选附带脱敏的 `ai_model` 展示名，绝不返回 `api_base` 或 `api_key`）。

### 2. HTTP 与 SSE 协议设计

- **路由**：`POST /api/v1/ai/chat`（挂载在 `hub_router` 的 `api` Nest 内，自动经过 `operator_auth_middleware` 与 `rate_limit_middleware`）。
- **请求体** (`AiChatRequest`)：
  - `messages: Vec<AiChatMessage>`（`role: "user" | "assistant"`, `content: String`，服务端限制消息条数上限如 40 条、总字符预算上限如 64 KiB，防止恶意膨胀 prompt）。
  - `context?: AiUiContext`（前端当前页面上下文，如 `page: "configuration" | "jobs" | "events" | "overview" | "runtime"`、`node_id?`、`job_id?`、`draft_format?`、`draft_content?`、`error_summary?`；注意前端传入的草稿/ID 仅作为提示线索，服务端一切集群数据以鉴权后的后端工具查询为准）。
- **响应格式与超时豁免**：
  - 遵循 `control-plane-api` 规格中与 `/events/stream` 一致的契约：`request_timeout_middleware`（`api/mod.rs:1523`）仅约束 handler 产生响应头的耗时；`hub_ai_chat` 在完成鉴权、AI 专属限流与请求体基础校验后，立即在后台 `tokio::spawn` 启动编排任务并返回 `axum::response::Sse` 流。
  - 编排任务受 `tokio::time::timeout(Duration::from_millis(ai.timeout_ms), ...)` 有界约束；同时监听前端断连（SSE channel receiver drop 时立即中止上游 LLM 请求与后续工具轮次）。
- **SSE 事件类型**：
  - `event: tool_start` -> `{"call_id": "...", "tool": "validate_job_spec", "summary": "..."}`
  - `event: tool_result` -> `{"call_id": "...", "tool": "validate_job_spec", "ok": true, "summary": "..."}`
  - `event: delta` -> `{"text": "..."}`（流式文本增量）
  - `event: proposal` -> 结构化提案卡片（与前端强类型对齐）：
    - `StreamConfigProposal`: `{ kind: "stream_config", title, summary, format: "yaml" | "json", content, node_id?, validated: true }`
    - `JobSpecProposal`: `{ kind: "job_spec", title, summary, spec: JobSpec, mode?: "create" | "upgrade", target_job_id?, validated: true }`
    - `DiagnosticActionProposal`: `{ kind: "diagnostic_action", severity: "info" | "warn" | "critical", title, summary, route: string, params?: Record<string, string>, action_label: string }`
  - `event: done` -> `{"usage": {"prompt_tokens": ..., "completion_tokens": ...}, "tool_calls": [...]}`
  - `event: error` -> `{"code": "...", "message": "..."}`（用于流开始后上游 LLM 4xx/5xx、超时或不支持 tool calling 的报错）

### 3. 纯 Tool-Calling 编排循环与提案发出机制

- **如何保证提案“必经校验”且结构化输出？**
  - 为大模型提供显式的“提交提案工具”（如 `emit_stream_config_proposal`、`emit_job_spec_proposal`、`emit_diagnostic_action`），或由工具 `validate_stream_config` / `validate_job_spec` 在校验通过后返回规范化结构并允许 `emit_*_proposal` 引用/内部再次校验。
  - **强制门禁**：`emit_stream_config_proposal` 与 `emit_job_spec_proposal` 在推送 SSE `proposal` 事件之前，**服务端必须在工具执行函数内同步调用 `parse_and_validate`（含插件校验）或 `deep_validate_job`**；若校验失败，则直接将结构化校验错误返回给 LLM 让其继续修正，绝不向前端发出未通过校验的提案卡片。
- **内置工具列表与 RBAC 矩阵**：
  1. `list_components(kind?)` / `get_component_schema(kind, name)`：要求 `OperatorAction::Read`；按需懒加载 schema，避免将全量 40+ 组件 JSON Schema 塞入系统提示导致 token 爆炸。
  2. `validate_stream_config(format, content)` / `emit_stream_config_proposal(...)`：要求 `OperatorAction::Configure`（`Viewer` 调用将被 tool dispatcher 拒绝并返回权限提示，且系统提示在检测到 `Viewer` 时仅注册只读工具集）。
  3. `validate_job_spec(spec)` / `emit_job_spec_proposal(...)`：要求 `OperatorAction::Configure`。
  4. `get_fleet_overview()` / `list_nodes(label_selector?)`：要求 `can_scope(Read, "node", None)`；`node` 作用域凭证仅返回其被授权的节点。
  5. `get_node_configuration(node_id)`：要求 `can_scope(Read, "configuration", Some(node_id))`，返回经敏感字段脱敏后的配置。
  6. `list_jobs()` / `get_job_detail(job_id)` / `get_job_plan(job_id)`：要求 `can_scope(Read, "job", ...)`，返回的 `spec_json` 与 `plan` 均经脱敏处理。
  7. `list_events(node_id?, stream_id?, event_type?, limit?)`：要求 `can_scope(Read, "event", ...)`，按调用者可见范围过滤。
  8. `get_metrics_summary(node_id?, job_id?)`：汇总 `hub.metrics_by_node` 与 `hub.job_metrics`（吞吐、延迟、错误数、checkpoint 耗时/失败、watermark lag、late events、WAL flush failures），受 `can_scope(Read, "node" | "job", ...)` 过滤。
  9. `list_audit_events(resource_id?, action?, limit?)`：要求 `OperatorAction::ReadAudit`，无权者不注册此工具。
  10. `emit_diagnostic_action(...)`：要求 `OperatorAction::Read`；仅输出深链建议卡片，不触发后端变更。
- **循环边界与异常处理**：
  - 最多运行 `max_tool_rounds` 轮（默认 8），超出则向 SSE 发送 `error`（`ai_tool_round_limit_exceeded`）或要求模型基于已有结果直接收口总结。
  - 上游返回非流式错误、HTTP 4xx/5xx、或请求携带 `tools` 被上游拒绝（不支持 function calling）时，映射为明确错误码（`ai_provider_error` / `ai_tool_calling_unsupported`）并通过 SSE `error` 事件（或流启动前 HTTP 502/422）返回。
  - 无论 SSE 请求还是非流式工具循环：每轮请求 LLM 使用 `stream: true` 解析 OpenAI SSE（复用 `processor/llm.rs` 的 delta + `tool_calls` 按 `index` 聚合逻辑），若存在 `tool_calls` 则执行工具并继续下一轮；若无 `tool_calls` 则将文本增量实时转发为 SSE `delta` 并最终发 `done`。

### 4. 脱敏策略、AI 专属限流与审计

- **敏感信息脱敏与占位符保留**：
  - `arkflow_core::configuration::redact_secrets` 目前将所有命中 `is_secret_key` 的字段统一替换为 `"******"`（`crates/arkflow-core/src/configuration.rs:403-404`）。对于未展开的 `${secret:NAME}` 与 `${env:VAR}` 引用字符串，本身不含真实密钥值，若保留占位符可让 AI 理解现有配置引用结构并在修改后保留原引用；而明文字段与 URL 密码（`redact_url_password`）必须掩码为 `"******"`。因此：Hub AI 工具层在投影现有配置/Job 时使用脱敏投影（掩码一切明文凭据与 URL 密码，可选保留 `${secret:...}` / `${env:...}` 引用串字面量），同时在事件/审计文本中对常见 `Bearer <token>` 模式做二次掩码。
  - 系统提示明确指示 LLM：生成任何含密码/Token/API Key 的配置时，一律生成 `${secret:NAME}` 或 `${env:VAR}` 占位符，不得虚构明文凭据。
- **AI 专属限流**：
  - 在 `hub_ai_chat` 内按 `principal.id`（或回退到全局 API credential bucket key）维护独立的令牌桶限流器（容量 `burst`，速率 `requests_per_minute / 60`），超限直接返回 HTTP `429` + `Retry-After` + 标准 problem body（`ai_rate_limit_exceeded`），不消耗上游 LLM 配额。
- **元数据审计**：
  - 每次 `POST /api/v1/ai/chat` 结束（成功、失败或客户端取消）时调用 `hub.record_audit` 写入 `cp_audit_events`：`action = "ai.copilot.chat"`、`resource_type = "ai_copilot"`、`resource_id = context 中的主资源标识或 "session"`、`actor = principal.id`、`detail_json` 仅包含 `{ model, tool_calls: [...], prompt_tokens, completion_tokens, outcome, duration_ms }`，**严禁写入用户自然语言 prompt 正文或 AI 回复正文**。

### 5. Console 架构与编辑器集成细节

- **能力探测与路由隔离**：
  - `console/src/app.tsx` 从 `useSystem()` 读取 `id === 'arkflow-control-hub'` 且 `capabilities.includes('ai_copilot')`；条件不满足时不渲染顶栏 Copilot 触发器及任何页面内 AI 入口。
- **状态管理**：
  - 新建 `CopilotProvider` / `useCopilot()` 上下文包裹 `ConsoleShell` 内部路由树，维护抽屉开关、当前会话消息数组（持久化于 `sessionStorage` 按当前会话/用户隔离，支持“清空会话”）、流式生成中的 `AbortController`、以及当前注入的页面上下文。
- **编辑器联动（关键边界处理）**：
  - **Configuration 页（`console/src/features/configuration.tsx`）**：现状中 Hub 模式下 `editable` 为 `false`（`configuration.tsx:86-103` 仅加载节点脱敏快照；因为 `GET /configuration/draft` 仅存在于 local mode）。当用户点击 Stream YAML 提案卡片的“填入编辑器 / 预览变更”时：
    - 若当前未选中节点，引导用户先选择目标节点（或卡片已携带 `node_id` 时跳转 `/configuration?node_id=...`）；
    - 若目标节点已选中，将编辑器置于**本地未保存候选态**（切换为可编辑的候选内容，**不调用** local-only 的 `/configuration/draft`），展示基于现有节点快照的客户端文本 Diff 预览，并允许用户直接点击 `Validate`（`POST /nodes/{node_id}/configuration/validate`）、`Publish`（`POST /nodes/{node_id}/configuration/apply`，若开启审批则返回 `422 approval_required` 提示走 Rollouts/Approvals）或一键复制 YAML。
  - **JobEditor 页（`console/src/features/job-editor.tsx`）**：当用户点击 `JobSpec` 提案卡片的“导入 DAG 画布”时，跳转到 `/jobs`（若不在该页）并通过 `jobSpecToDag`（`console/src/features/job-dag.ts`）直接渲染节点与连线到 ReactFlow 画布，同时清零 `validated` 状态以遵守既有“导入/修改后必须先过 `/jobs/validate` 才能提交”的不变量（`control-plane-console` spec）。

## Risks / Trade-offs

- **风险 1：Configuration 页在 Hub 模式下填入候选配置后触发 `approval_required` 拦截** → 缓解：UI 上保持与现有 `applyConfig` 一致的行为，当 `approval_required` 开启时明确提示可通过复制 YAML / 版本流程提交审批，不破坏 `configuration-approval` 约束。
- **风险 2：大模型在有界轮次内无法收敛出合法配置** → 缓解：工具返回精确的字段路径与 `get_component_schema` 指引；达到 `max_tool_rounds` 后安全终止并把最后一次校验错误呈现给用户，绝不绕过校验发出提案卡片。
- **风险 3：长 SSE 对话占用并发连接或上游费用失控** → 缓解：AI 专属按身份限流、消息数与字符长度硬上限、`timeout_ms` 总超时、客户端断连自动 abort 上游请求、`max_tokens` 默认封顶。
- **风险 4：测试稳定性** → 缓解：所有 Rust 单元测试与 API 集成测试均使用本地 `tokio::net::TcpListener` Mock OpenAI SSE server（仿现有 `oidc` / `webhooks` 测试模式），零外部网络依赖，可完全离线在 CI 运行。

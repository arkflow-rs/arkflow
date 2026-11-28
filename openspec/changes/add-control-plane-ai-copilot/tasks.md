## 1. 配置与能力通告（Hub-only）

- [x] 1.1 在 `crates/arkflow-server/src/api/mod.rs` 新增 `AiCopilotConfig`（`enabled` 默认 `false`、`api_base`、`model`、`api_key`、`timeout_ms`、`max_tool_rounds`、`max_tokens`、`requests_per_minute`、`burst`），挂载于 `ServerConfig.ai`（`#[serde(default)]`；`ServerConfig::from_engine` 始终关闭）
- [x] 1.2 在 `crates/arkflow-server/src/startup_config.rs` 的 `apply_env_overrides` 支持 `ARKFLOW_AI_*` 环境变量覆盖，确保 `--config` 文件经 `${env:}`/`${secret:}` 解析后生效且脱敏错误语义一致
- [x] 1.3 在 `ServerConfig::validate_hub_startup` 增加 `ai.enabled` 启动门禁（校验 `api_base` 为 http/https 且 `model` 非空），并在 `crates/arkflow-server/src/api/nodes.rs` 的 `hub_system` 中于 `ai.enabled` 时向 `capabilities` 追加 `"ai_copilot"`（绝不输出 `api_key` / `api_base`）
- [x] 1.4 编写单元测试验证：`ai` 缺省关闭向后兼容、非法 `api_base`/`model` 启动失败、`${secret:}` 解析生效、`/system` 不泄露凭证、local 引擎模式不开放 `"ai_copilot"`

## 2. 脱敏、鉴权与内置只读/校验工具集

- [x] 2.1 在 `crates/arkflow-server/src/hub/ai_tools.rs` 实现 AI 上下文脱敏投影助手（复用 `arkflow_core::configuration::redact_secrets`，掩码明文密码与 URL userinfo，保留未展开 `${secret:...}` / `${env:...}` 引用字面量，并对事件/审计文本脱敏 Bearer token）
- [x] 2.2 实现内部工具定义与分派器：组件发现（`list_components`、`get_component_schema`）、集群遥测只读查询（`get_fleet_overview`、`list_nodes`、`get_node_configuration`、`list_jobs`、`get_job_detail`、`get_job_plan`、`list_events`、`get_metrics_summary`、`list_audit_events`）、以及校验与提案工具（`validate_stream_config`、`validate_job_spec`、`emit_stream_config_proposal`、`emit_job_spec_proposal`、`emit_diagnostic_action`）
- [x] 2.3 为所有工具接入 `OperatorPrincipal::can_scope` 严格鉴权（`Read` / `Configure` / `ReadAudit`）及动态工具注册裁剪（如 `Viewer` 不暴露配置校验/提案工具，无 `ReadAudit` 不暴露审计工具）
- [x] 2.4 强制门禁：`emit_stream_config_proposal` 与 `emit_job_spec_proposal` 内部同步调用 `parse_and_validate`（含 `arkflow_plugin::initialize()`）或 `deep_validate_job`，校验失败将错误回传 LLM 且不发提案事件
- [x] 2.5 编写单元测试：覆盖跨节点 scope 隔离、`Viewer` 越权拒绝、审计权限隔离、明文密钥脱敏（含 `${secret:}` 保留）及未通过校验的配置被拦截

## 3. OpenAI Tool-Calling 编排器与 SSE HTTP 端点

- [x] 3.1 在 `crates/arkflow-server/src/hub/ai.rs` 实现 OpenAI 兼容 `/chat/completions` SSE 流式客户端与纯 Tool-Calling 有界循环（`max_tool_rounds` 上限、按 `index` 聚合流式 `tool_calls`、支持无 `api_key` 内网端点、显式识别并报错不支持 tool calling 的响应）
- [x] 3.2 在 `crates/arkflow-server/src/api/ai.rs` 实现 `POST /api/v1/ai/chat`：`hub_router` 注册、Standby Hub 自动拦截（`503 hub_standby`）、请求大小与消息条数校验、独立按 `principal.id` 令牌桶限流（429 + `Retry-After` + `ai_rate_limit_exceeded`）、后台任务 spawn + `Sse` 响应（不受 `request_timeout_middleware` 截断，受 `ai.timeout_ms` 总超时与客户端断连 abort 控制）
- [x] 3.3 接入 `cp_audit_events` 元数据审计（`action: "ai.copilot.chat"`，记录 actor、工具清单、模型/token 用量、终态，严禁写入 prompt/completion 正文）
- [x] 3.4 编写基于本地 `TcpListener` Mock OpenAI SSE 服务的 API 集成测试：覆盖多轮 tool calling、校验自修正闭环、不支持 tool calling 报错、超时与客户端断连中止、401/403/429/503 矩阵及审计记录正文隔离断言

## 4. Console 前端 Copilot 抽屉与编辑器联动

- [x] 4.1 在 `console/src/api.ts` 与 `console/src/features/types.ts` 新增 AI SSE 流客户端（`fetch` + `ReadableStream` 解析，携带 Bearer token / credentials、支持 `AbortController`）及提案/上下文类型定义
- [x] 4.2 新建 `console/src/features/copilot.tsx`（`CopilotProvider` + 右侧滑出抽屉组件）：基于 `system.capabilities.includes('ai_copilot') && id === 'arkflow-control-hub'` 门控可见性；`sessionStorage` 持久化当前会话消息、展示工具执行进度、流式 Markdown 输出、清空会话与停止生成按钮
- [x] 4.3 实现三类提案卡片渲染与联动：
  - `stream_config` 卡片：跳转 `/configuration?node_id=...`，将 YAML/JSON 填入 `Configuration` 本地候选编辑态（不调 `/configuration/draft`），提供与节点快照的 Diff 预览并保留 Validate/Publish 门禁
  - `job_spec` 卡片：跳转 `/jobs`，经 `jobSpecToDag` 导入 `JobEditor` DAG 画布并重置校验状态
  - `diagnostic_action` 卡片：带深链跳转到对应页面，不直接发起变更请求
- [x] 4.4 在 `console/src/app.tsx` 顶栏添加 Copilot 唤醒按钮，并在 `configuration.tsx`、`job-editor.tsx`、`jobs.tsx`、`events.tsx` 添加上下文快捷触发按钮（`Configure` 缺失时隐藏/禁用生成入口）
- [x] 4.5 补齐 `console/src/i18n/en.ts` 与 `console/src/i18n/zh.ts` 全量翻译键；编写 Vitest 前端单测覆盖能力门控隐藏、提案卡片填入 `Configuration` / `JobEditor`（含 `jobSpecToDag` 导入与未校验锁定）、流式事件解析
- [x] 4.6 运行 `pnpm --dir console lint`、`pnpm --dir console test`、`pnpm --dir console build`

## 5. 文档与全量门禁验证

- [x] 5.1 更新 `docs/docs/`（控制面配置/HTTP API/Console 指南）新增 Hub AI Copilot 配置说明（含 `ARKFLOW_AI_*`、`ai` 配置块、`validate=fragment` 或 `validate=foreign` 正确标注）、API 契约与安全脱敏边界说明
- [x] 5.2 同步更新 `docs/i18n/zh-Hans/` 对应中文文档页面
- [x] 5.3 运行全量门禁：`cargo test --workspace --all-targets`、`cargo clippy --workspace --all-targets`、`cargo fmt --check`、`pnpm --dir docs docs:check`（全量 feature 门禁随 GitHub Actions PR CI 一并验证）

## Why

ArkFlow 的数据面已经具备成熟的 OpenAI 兼容大模型与向量生态（`crates/arkflow-plugin/src/processor/llm.rs:39-77` 已实现 chat completions、SSE 流式组装与 tool calling），但**控制面（Hub + Console）目前完全没有大模型辅助能力**：

1. **后端零 AI 接入**：`hub_router`（`crates/arkflow-server/src/api/mod.rs:677-814`）没有任何 `/ai/*` 路由；`HubStartupConfig`（`crates/arkflow-server/src/startup_config.rs:22-32`）没有大模型服务端配置节。
2. **前端学习与排障门槛高**：用户需要手工编写覆盖 40+ 插件的 Stream YAML 或在 `JobEditor`（`console/src/features/job-editor.tsx`）拖拽组装 `JobSpec` DAG，排障时需在 Overview / Runtime / Events / Jobs / Audit 多个页面间人工关联事件与 `KernelMetricsSnapshot`；`ConsoleShell`（`console/src/app.tsx:49-78`）缺少全局 Copilot 入口和上下文诊断动作。
3. **现成底座已齐备**：Hub 已在启动时初始化插件注册表并暴露 `components`/`schema`（`crates/arkflow-server/src/api/diagnostics.rs:495-515`）、内置 `deep_validate_job`（`crates/arkflow-server/src/api/jobs.rs:149-153`）与 `parse_and_validate`（`crates/arkflow-server/src/api/configuration.rs:8`）、`redacted_value` 密钥脱敏（`crates/arkflow-core/src/configuration.rs:534-542`）、`OperatorPrincipal::can_scope` 细粒度鉴权（`crates/arkflow-server/src/api_contract.rs:102-120`）以及 `record_audit` 审计链路。

将这些现成底座通过 Hub 代理的 AI Copilot 串接起来，可以零 Agent 协议改动地为运维与开发人员提供“自然语言配置生成+后端校验闭环”和“基于 Hub 遥测的故障根因诊断”。

## What Changes

- **Hub 服务端 AI 配置（仅 Hub 控制面）**：在 `HubStartupConfig`（`crates/arkflow-server/src/startup_config.rs`）与 `ServerConfig`（`crates/arkflow-server/src/api/mod.rs`）中增加可选 `ai` 配置节及 `ARKFLOW_AI_*` 环境变量覆盖（OpenAI 兼容 `api_base`、`model`、`api_key` 支持 `${env:}` / `${secret:}`、超时/最大工具轮次/速率限制等），默认关闭；启用时在 `GET /api/v1/system` 的 `capabilities` 中通告 `"ai_copilot"`（`crates/arkflow-server/src/api/nodes.rs:52`）。
- **Hub Copilot 编排与 SSE 接口**：新增 `POST /api/v1/ai/chat`（`text/event-stream` SSE），在后台任务中运行**有界纯 Tool-Calling 循环**，实时流式推送 `tool_start` / `tool_result` / `delta` / `proposal` / `done` / `error` 事件（模型不支持 tool calling 时明确报错）；无状态接口，对话历史由前端随请求传入。
- **只读 + 校验型内置工具集（严格 RBAC + 脱敏）**：
  - 组件与校验工具：`list_components`、`get_component_schema`、`validate_stream_config`、`validate_job_spec`（后两者要求调用者具备 `OperatorAction::Configure`）。
  - 集群遥测检索工具：`get_fleet_overview`、`list_nodes`、`get_node_configuration`、`list_jobs`、`get_job_detail`、`list_events`、`get_metrics_summary`、`list_audit_events`（每项工具严格按当前调用者 `OperatorPrincipal::can_scope` 鉴权与裁剪）。
  - 所有配置与 Job Spec 送往 LLM 前强制经 `redacted_value` 脱敏，禁止读取或发送任何数据面消息负载（Arrow `RecordBatch`）。
- **独立限流与元数据审计**：AI 对话入口施加每凭证独立限流，并将每次调用的元数据（actor、模型、调用工具清单、token 用量、终态）记入 `cp_audit_events`，**不落库用户对话正文**。
- **Console 全局 Copilot 抽屉与上下文联动**：
  - 仅在 Hub 模式且 `capabilities` 包含 `ai_copilot` 时展示顶栏 Copilot 按钮与全局右侧滑出抽屉（会话状态保存在浏览器 `sessionStorage`，跨路由保留）。
  - 在 Configuration、Job DAG Editor、Events、Jobs 异常处提供上下文快捷按钮（AI 生成/修复/诊断）。
  - 支持三类结构化提案卡片：**Stream YAML 提案**（填入 Configuration 编辑器候选态并支持 Diff 预览与校验）、**JobSpec 提案**（经 `jobSpecToDag` 直接渲染到 Job Editor 可视化 DAG 画布）与**诊断运维建议卡**（带目标页面深链，由用户人工确认执行）。

## Capabilities

### New Capabilities

- `control-plane-ai-copilot`: Hub 控制面大模型 Copilot 的配置、OpenAI Tool-Calling 编排与 SSE 流式协议、RBAC/Scope 工具鉴权裁剪、敏感信息脱敏、独立限流与元数据审计规范。

### Modified Capabilities

- `control-plane-console`: 新增 Hub 模式（且 `ai_copilot` capability 启用）下的全局 Copilot 抽屉、上下文快捷入口、浏览器会话历史与结构化提案卡片（Stream YAML / JobSpec DAG / 诊断建议深链）交互要求；本地 Standalone 模式与未启用 AI 的 Hub 不展示入口。

## Non-goals

- **不覆盖单机 Standalone 控制面**：`arkflow` 引擎本地控制面（`router()`）不增加大模型配置、HTTP 客户端或 AI 路由；Console 本地模式下完全隐藏 AI 入口。
- **不直接执行任何写操作**：AI 不拥有也不调用变更类工具（不直接触发 publish、rollout、approval、job start/stop/pause/resume/upgrade、node drain/maintenance 等），所有变更均通过提案卡片由用户在既有 UI/RBAC/审批流中手动确认。
- **不做 Hub 端对话持久化**：不在 SQLite / PostgreSQL `StorageBackend` 中新增对话表；不改造 `backup`/`restore`/`migrate` 工具与 HA 同步。
- **不改造 Hub–Agent 协议**：不新增 Agent 进程日志采集通道，故障诊断严格使用 Hub 已聚合的遥测与状态数据。
- **不做无 Tool-Calling 模型的降级适配与多模型路由/Console 模型管理页面**：模型配置仅来自 Hub 启动配置/环境变量，且要求目标模型支持 OpenAI 兼容 function/tool calling。

## Impact

- **配置与服务端**：`crates/arkflow-server/src/api/mod.rs`、`crates/arkflow-server/src/startup_config.rs`、`crates/arkflow-server/src/api/nodes.rs`、`crates/arkflow-server/src/api/rate_limit.rs`、新增 `crates/arkflow-server/src/api/ai.rs` 与 `crates/arkflow-server/src/hub/ai.rs`（或同级模块）。
- **核心脱敏微调（如需要）**：核实 `crates/arkflow-core/src/configuration.rs` 中 `redact_secrets` 对 `${secret:...}` / `${env:...}` 引用占位符的保留语义（或在 Hub AI 工具投影层保留引用语法名、掩码明文）。
- **依赖**：复用 `arkflow-server` 已有 `reqwest`、`futures-util`、`tokio`、`axum::response::Sse`，不新增重型 ML/SDK 依赖。
- **Console**：`console/src/app.tsx`、`console/src/api.ts`、`console/src/features/configuration.tsx`、`console/src/features/job-editor.tsx`、`console/src/features/events.tsx`、`console/src/features/jobs.tsx`、新增 `console/src/features/copilot.tsx`、i18n（`en.ts` / `zh.ts`）。
- **文档**：`docs/docs/` 与 `docs/i18n/zh-Hans/` 新增/更新控制面 AI Copilot 配置指南、API 文档与 Console 使用说明。

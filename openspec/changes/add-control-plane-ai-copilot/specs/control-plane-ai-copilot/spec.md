# control-plane-ai-copilot Specification

## Purpose

Define the Hub-side AI Copilot proxy, OpenAI-compatible tool-calling orchestration, SSE streaming protocol, RBAC/scope enforcement, secret redaction, rate limiting, and metadata audit requirements for the ArkFlow control plane.

## Requirements

### Requirement: Hub-only AI Copilot configuration and startup validation

The system SHALL configure the AI Copilot exclusively on the Hub control-plane configuration (`ServerConfig.ai` / `HubStartupConfig` and `ARKFLOW_AI_*` environment overrides), defaulting to disabled. When enabled, the Hub MUST validate that `api_base` uses an `http://` or `https://` scheme and `model` is non-empty at startup, failing fast on invalid configuration. The Hub MUST resolve secret references (`${env:...}`, `${file:...}`, `${secret:...}`) in `api_key` using the existing startup config resolver and SHALL NOT expose `api_key` or `api_base` in any API response, event, log, or `/system` payload. The standalone local control plane SHALL NOT mount AI routes or advertise `"ai_copilot"` in `/system` capabilities.

#### Scenario: Disabled by default preserves Hub and local behavior

- **WHEN** the Hub or standalone engine starts without enabling `ai`
- **THEN** `/api/v1/system` capabilities do not contain `"ai_copilot"`, `/api/v1/ai/chat` rejects requests with `404` or `503 ai_copilot_disabled`, and all existing routes behave unchanged

#### Scenario: Invalid AI startup configuration fails fast

- **WHEN** `ai.enabled` is `true` and `api_base` is missing, uses a non-HTTP(S) scheme, or `model` is empty
- **THEN** Hub startup fails with a configuration error before binding the listener

#### Scenario: Enabled Hub advertises capability without leaking credentials

- **WHEN** the Hub starts with valid `ai` configuration including an `api_key`
- **THEN** `GET /api/v1/system` includes `"ai_copilot"` in `capabilities` for authenticated callers, and neither `api_key` nor `api_base` appears in the response

### Requirement: Authenticated SSE tool-calling orchestration endpoint

The Hub SHALL expose `POST /api/v1/ai/chat` under the authenticated versioned operator API, gated by leadership (returning `503 hub_standby` on standby Hubs). The endpoint SHALL accept stateless conversation message turns and optional UI context clues, enforce bounded message count and payload size limits, and stream `text/event-stream` SSE events (`tool_start`, `tool_result`, `delta`, `proposal`, `done`, `error`) from a bounded background task whose total execution time is capped by `ai.timeout_ms`. The Hub MUST require OpenAI-compatible tool calling and MUST emit a structured error (`ai_tool_calling_unsupported` or `ai_provider_error`) if the upstream provider rejects tool definitions or returns an HTTP/transport failure. When the client closes the HTTP stream, the Hub MUST abort any in-flight upstream request and remaining tool rounds.

#### Scenario: Multi-round tool calling streams progress and final answer

- **WHEN** an authenticated operator sends a prompt to `POST /api/v1/ai/chat` that triggers one or more internal tool calls followed by a final assistant message
- **THEN** the Hub emits `tool_start` and `tool_result` SSE events for each tool invocation, streams text increments as `delta` events, and terminates with a `done` event

#### Scenario: Standby Hub rejects Copilot requests

- **WHEN** `POST /api/v1/ai/chat` is sent to a standby Hub
- **THEN** the request is rejected with `503` and problem code `hub_standby`

#### Scenario: Upstream provider without tool calling fails explicitly

- **WHEN** the configured LLM provider rejects the `tools` parameter or returns a non-2xx response
- **THEN** the Hub surfaces an explicit error (`ai_tool_calling_unsupported` or `ai_provider_error`) instead of silently falling back to ungrounded output

#### Scenario: Client disconnection aborts upstream orchestration

- **WHEN** the HTTP client disconnects before the SSE stream finishes
- **THEN** the Hub cancels the orchestration task and aborts in-flight upstream LLM calls

### Requirement: Read-only and validation-only tool boundary with RBAC scope enforcement

Every internal tool exposed to the LLM by `POST /api/v1/ai/chat` SHALL be strictly read-only or side-effect-free validation; the Hub MUST NOT expose any mutating control-plane tool (such as configuration apply, rollout creation, approval decision, job start/stop/pause/resume/upgrade, or node drain/maintenance) to the LLM. Every tool execution SHALL enforce the authenticated caller's `OperatorPrincipal` and `can_scope` rules:
- Component discovery (`list_components`, `get_component_schema`), fleet/node/job/event/metrics inspection, and `emit_diagnostic_action` SHALL require `OperatorAction::Read` on the corresponding resource scope.
- Configuration and Job validation/proposal tools (`validate_stream_config`, `validate_job_spec`, `emit_stream_config_proposal`, `emit_job_spec_proposal`) SHALL require `OperatorAction::Configure`.
- Audit inspection (`list_audit_events`) SHALL require `OperatorAction::ReadAudit`.
A tool call targeting a resource outside the caller's authorized roles or scopes MUST be denied by the server and MUST NOT leak cross-scope data to the LLM.

#### Scenario: Viewer cannot invoke configuration authoring tools

- **WHEN** a principal holding only the `viewer` role invokes `POST /api/v1/ai/chat`
- **THEN** diagnostic read tools succeed within `viewer` scope, while `validate_stream_config` / `validate_job_spec` / configuration proposal tools are withheld or denied with an authorization error

#### Scenario: Node-scoped principal cannot inspect other nodes

- **WHEN** a principal scoped to `node:node-a` queries cluster nodes, configurations, or events through Copilot
- **THEN** internal tools return only `node-a` resources and deny or omit resources belonging to `node-b`

#### Scenario: Unprivileged caller cannot read audit history through AI

- **WHEN** an `operator` principal lacking `OperatorAction::ReadAudit` asks Copilot to inspect audit logs
- **THEN** `list_audit_events` is denied and no audit records are disclosed to the LLM

### Requirement: Server-validated configuration and Job proposals

Before emitting any `proposal` SSE event for a Stream configuration (`stream_config`) or distributed Job specification (`job_spec`), the Hub MUST validate the candidate on the server through the real engine validation path (`parse_and_validate` with initialized plugins or `deep_validate_job`). If validation fails, the Hub MUST return the structured path/component validation errors to the LLM within the bounded tool-call loop and MUST NOT emit an unvalidated `proposal` event to the client. When the tool-round limit (`ai.max_tool_rounds`) is reached without a valid proposal, the Hub MUST terminate with an explicit error or explanation rather than emitting an unvalidated configuration.

#### Scenario: Invalid generated config triggers self-correction loop before emission

- **WHEN** the LLM initially calls `emit_stream_config_proposal` or `emit_job_spec_proposal` with an unknown component or invalid field
- **THEN** the server rejects the proposal inside the tool call with the engine validation error, does not emit a `proposal` SSE event, and allows the LLM to retry within `max_tool_rounds`

#### Scenario: Valid generated config emits a verified proposal event

- **WHEN** a candidate passes `parse_and_validate` or `deep_validate_job`
- **THEN** the Hub emits a `proposal` SSE event marked `validated: true` containing the verified YAML/JSON or `JobSpec` payload

### Requirement: Secret redaction, zero data-plane payload leakage, rate limiting, and metadata audit

All configurations, Job specs, execution plans, and events sent to the LLM by Hub tools MUST be redacted so plaintext credentials and URL passwords are masked (`******`) while unexpanded `${secret:...}` / `${env:...}` reference syntax MAY remain visible without ever resolving secret values. Copilot tools MUST NOT read or transmit data-plane message payloads (`MessageBatch` rows). The Hub SHALL enforce a dedicated per-principal token-bucket rate limit on `POST /api/v1/ai/chat`, returning HTTP `429` with `Retry-After` and problem code `ai_rate_limit_exceeded` when exceeded. Every Copilot invocation SHALL record a metadata-only entry in `cp_audit_events` (`action: "ai.copilot.chat"`, actor, model, invoked tool names, token usage, outcome, duration) and MUST NOT persist raw prompt text or assistant replies in the audit table.

#### Scenario: Plaintext secrets are never sent to the LLM

- **WHEN** Copilot inspects a node configuration or Job spec containing connector passwords or API keys
- **THEN** the payload delivered to the upstream LLM contains `******` for literal secrets (and never resolved secret values)

#### Scenario: Exceeding AI rate limit returns 429 before contacting LLM

- **WHEN** a principal exceeds `ai.requests_per_minute` / `ai.burst` on `POST /api/v1/ai/chat`
- **THEN** the Hub responds with HTTP `429`, problem code `ai_rate_limit_exceeded`, and a `Retry-After` header without making an upstream LLM request

#### Scenario: Copilot invocation writes metadata-only audit record

- **WHEN** a `POST /api/v1/ai/chat` request completes or fails after passing authentication and rate limiting
- **THEN** `cp_audit_events` records the actor, tool call list, model, token usage, and outcome, and contains neither the user's prompt text nor the assistant's completion text

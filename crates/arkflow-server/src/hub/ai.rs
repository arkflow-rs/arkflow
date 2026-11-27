//! Hub AI Copilot orchestration service: OpenAI-compatible tool-calling
//! client, bounded multi-round loop, SSE stream events, and per-principal
//! rate limiting.

use crate::api_contract::OperatorPrincipal;
use crate::hub::ai_tools::{
    execute_tool, redact_text_for_ai, tool_definitions_for_principal, AiProposal,
};
use crate::hub::Hub;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub const MAX_CHAT_MESSAGES: usize = 40;
pub const MAX_CHAT_CHARS: usize = 65_536;
const MAX_AI_RATE_BUCKETS: usize = 1024;
const MAX_TOOL_OUTPUT_CHARS: usize = 24_000;

/// Configuration for the Hub-only AI Copilot proxy (`ServerConfig.ai` and
/// `ARKFLOW_AI_*` environment overrides).
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
    pub timeout_ms: u64,
    #[serde(default = "default_ai_max_tool_rounds")]
    pub max_tool_rounds: usize,
    #[serde(default = "default_ai_max_tokens")]
    pub max_tokens: Option<u32>,
    #[serde(default = "default_ai_requests_per_minute")]
    pub requests_per_minute: u32,
    #[serde(default = "default_ai_burst")]
    pub burst: u32,
}

fn default_ai_timeout_ms() -> u64 {
    90_000
}
fn default_ai_max_tool_rounds() -> usize {
    8
}
fn default_ai_max_tokens() -> Option<u32> {
    Some(4096)
}
fn default_ai_requests_per_minute() -> u32 {
    20
}
fn default_ai_burst() -> u32 {
    5
}

impl Default for AiCopilotConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_base: None,
            model: None,
            api_key: None,
            timeout_ms: default_ai_timeout_ms(),
            max_tool_rounds: default_ai_max_tool_rounds(),
            max_tokens: default_ai_max_tokens(),
            requests_per_minute: default_ai_requests_per_minute(),
            burst: default_ai_burst(),
        }
    }
}

impl AiCopilotConfig {
    /// Validate Hub startup invariants when AI Copilot is enabled.
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        let api_base = self
            .api_base
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                "ai.enabled=true requires ai.api_base (or ARKFLOW_AI_API_BASE) to be set"
                    .to_string()
            })?;
        let parsed = reqwest::Url::parse(api_base)
            .map_err(|err| format!("invalid ai.api_base '{api_base}': {err}"))?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(format!(
                "ai.api_base '{api_base}' must be an http:// or https:// URL with a host"
            ));
        }
        let model = self
            .model
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                "ai.enabled=true requires ai.model (or ARKFLOW_AI_MODEL) to be set".to_string()
            })?;
        if model.is_empty() {
            return Err("ai.model must not be empty".to_string());
        }
        if self.timeout_ms < 1_000 {
            return Err("ai.timeout_ms must be at least 1000".to_string());
        }
        if self.max_tool_rounds == 0 {
            return Err("ai.max_tool_rounds must be at least 1".to_string());
        }
        if self.requests_per_minute == 0 {
            return Err("ai.requests_per_minute must be at least 1".to_string());
        }
        if self.burst == 0 {
            return Err("ai.burst must be at least 1".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AiChatRequest {
    pub messages: Vec<AiChatMessage>,
    #[serde(default)]
    pub context: Option<AiUiContext>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AiChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AiUiContext {
    #[serde(default)]
    pub page: Option<String>,
    #[serde(default)]
    pub node_id: Option<String>,
    #[serde(default)]
    pub job_id: Option<String>,
    #[serde(default)]
    pub stream_id: Option<String>,
    #[serde(default)]
    pub draft_format: Option<String>,
    #[serde(default)]
    pub draft_content: Option<String>,
    #[serde(default)]
    pub error_summary: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AiUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", content = "data", rename_all = "snake_case")]
pub enum AiStreamEvent {
    ToolStart {
        call_id: String,
        tool: String,
        summary: String,
    },
    ToolResult {
        call_id: String,
        tool: String,
        ok: bool,
        summary: String,
    },
    Delta {
        text: String,
    },
    Proposal(AiProposal),
    Done {
        usage: AiUsage,
        tool_calls: Vec<String>,
    },
    Error {
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone)]
pub struct AiSessionSummary {
    pub model: String,
    pub tool_calls: Vec<String>,
    pub usage: AiUsage,
    pub outcome: &'static str,
    pub failure_code: Option<String>,
    pub duration_ms: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct BucketKey([u8; 16]);

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

/// Per-principal token bucket for AI Copilot requests, separate from the
/// general API limiter so heavy LLM sessions cannot starve regular control APIs
/// and vice versa.
pub struct AiRateLimiter {
    rps: f64,
    burst: f64,
    buckets: Mutex<HashMap<BucketKey, Bucket>>,
    catch_all: Mutex<Bucket>,
}

impl AiRateLimiter {
    pub fn new(requests_per_minute: u32, burst: u32) -> Self {
        let rpm = requests_per_minute.max(1) as f64;
        let burst = burst.max(1) as f64;
        Self {
            rps: rpm / 60.0,
            burst,
            buckets: Mutex::new(HashMap::new()),
            catch_all: Mutex::new(Bucket {
                tokens: burst,
                last_refill: Instant::now(),
            }),
        }
    }

    pub fn check(&self, principal_id: &str) -> Result<(), u64> {
        let now = Instant::now();
        let key = hash_principal(principal_id);
        let mut table = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        if !table.contains_key(&key) && table.len() >= MAX_AI_RATE_BUCKETS {
            let idle_window = Duration::from_secs_f64((2.0 * self.burst / self.rps).max(60.0));
            table.retain(|_, bucket| now.duration_since(bucket.last_refill) < idle_window);
        }
        if !table.contains_key(&key) && table.len() >= MAX_AI_RATE_BUCKETS {
            drop(table);
            let mut catch_all = self.catch_all.lock().unwrap_or_else(|e| e.into_inner());
            return take_token(&mut catch_all, self.rps, self.burst, now);
        }
        let bucket = table.entry(key).or_insert_with(|| Bucket {
            tokens: self.burst,
            last_refill: now,
        });
        take_token(bucket, self.rps, self.burst, now)
    }
}

fn take_token(bucket: &mut Bucket, rps: f64, burst: f64, now: Instant) -> Result<(), u64> {
    let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
    bucket.tokens = (bucket.tokens + elapsed * rps).min(burst);
    bucket.last_refill = now;
    if bucket.tokens >= 1.0 {
        bucket.tokens -= 1.0;
        Ok(())
    } else {
        let missing = (1.0 - bucket.tokens).max(0.0);
        let retry_after = (missing / rps).ceil().max(1.0) as u64;
        Err(retry_after)
    }
}

fn hash_principal(principal_id: &str) -> BucketKey {
    let mut hasher = Sha256::new();
    hasher.update(b"ai_copilot\0");
    hasher.update(principal_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    BucketKey(bytes)
}

pub struct AiCopilotService {
    config: AiCopilotConfig,
    client: Client,
    limiter: AiRateLimiter,
}

impl AiCopilotService {
    pub fn new(config: AiCopilotConfig) -> Self {
        let limiter = AiRateLimiter::new(config.requests_per_minute, config.burst);
        let mut builder = Client::builder().connect_timeout(Duration::from_secs(10));
        if is_loopback_base(config.api_base.as_deref()) {
            builder = builder.no_proxy();
        }
        let client = builder.build().unwrap_or_else(|_| Client::new());
        Self {
            config,
            client,
            limiter,
        }
    }

    pub fn config(&self) -> &AiCopilotConfig {
        &self.config
    }

    pub fn check_rate_limit(&self, principal_id: &str) -> Result<(), u64> {
        self.limiter.check(principal_id)
    }
}

fn is_loopback_base(api_base: Option<&str>) -> bool {
    let Some(base) = api_base else {
        return false;
    };
    let Ok(url) = reqwest::Url::parse(base) else {
        return false;
    };
    match url.host_str() {
        Some("localhost") | Some("127.0.0.1") | Some("::1") | Some("[::1]") => true,
        Some(host) => host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback()),
        None => false,
    }
}

/// Validate client-supplied conversation turns before dispatching to the LLM.
pub fn validate_chat_request(req: &AiChatRequest) -> Result<(), String> {
    if req.messages.is_empty() {
        return Err("messages must not be empty".to_string());
    }
    if req.messages.len() > MAX_CHAT_MESSAGES {
        return Err(format!(
            "messages exceeds maximum of {MAX_CHAT_MESSAGES} turns"
        ));
    }
    let mut total_chars = 0usize;
    for msg in &req.messages {
        if !matches!(msg.role.as_str(), "user" | "assistant") {
            return Err(format!(
                "unsupported message role '{}'; only 'user' and 'assistant' are allowed",
                msg.role
            ));
        }
        if msg.content.trim().is_empty() {
            return Err("message content must not be empty".to_string());
        }
        total_chars = total_chars.saturating_add(msg.content.len());
    }
    if let Some(ctx) = &req.context {
        if let Some(draft) = &ctx.draft_content {
            total_chars = total_chars.saturating_add(draft.len());
        }
        if let Some(err) = &ctx.error_summary {
            total_chars = total_chars.saturating_add(err.len());
        }
    }
    if total_chars > MAX_CHAT_CHARS {
        return Err(format!(
            "request payload exceeds maximum budget of {MAX_CHAT_CHARS} characters"
        ));
    }
    Ok(())
}

/// Run the bounded OpenAI-compatible tool-calling orchestration loop and stream
/// events to `tx`. Returns metadata summary for audit logging.
pub async fn run_copilot_session(
    service: &AiCopilotService,
    hub: &Hub,
    principal: &OperatorPrincipal,
    request: AiChatRequest,
    tx: mpsc::Sender<AiStreamEvent>,
    cancel: CancellationToken,
) -> AiSessionSummary {
    let started = Instant::now();
    let model = service
        .config
        .model
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let mut invoked_tools = Vec::new();
    let mut total_usage = AiUsage::default();

    let timeout_duration = Duration::from_millis(service.config.timeout_ms.max(1_000));
    let outcome_result = tokio::time::timeout(timeout_duration, async {
        tokio::select! {
            _ = cancel.cancelled() => Err(("ai_cancelled".to_string(), "Client disconnected".to_string())),
            res = run_loop(
                service,
                hub,
                principal,
                request,
                &tx,
                &mut invoked_tools,
                &mut total_usage,
            ) => res,
        }
    })
    .await;

    let duration_ms = started.elapsed().as_millis() as u64;
    match outcome_result {
        Ok(Ok(())) => {
            let _ = tx
                .send(AiStreamEvent::Done {
                    usage: total_usage.clone(),
                    tool_calls: invoked_tools.clone(),
                })
                .await;
            AiSessionSummary {
                model,
                tool_calls: invoked_tools,
                usage: total_usage,
                outcome: "succeeded",
                failure_code: None,
                duration_ms,
            }
        }
        Ok(Err((code, message))) => {
            if code != "ai_cancelled" {
                let _ = tx
                    .send(AiStreamEvent::Error {
                        code: code.clone(),
                        message,
                    })
                    .await;
            }
            AiSessionSummary {
                model,
                tool_calls: invoked_tools,
                usage: total_usage,
                outcome: if code == "ai_cancelled" {
                    "cancelled"
                } else {
                    "failed"
                },
                failure_code: Some(code),
                duration_ms,
            }
        }
        Err(_) => {
            let code = "ai_timeout".to_string();
            let _ = tx
                .send(AiStreamEvent::Error {
                    code: code.clone(),
                    message: format!(
                        "AI Copilot request exceeded {}ms timeout",
                        service.config.timeout_ms
                    ),
                })
                .await;
            AiSessionSummary {
                model,
                tool_calls: invoked_tools,
                usage: total_usage,
                outcome: "failed",
                failure_code: Some(code),
                duration_ms,
            }
        }
    }
}

async fn run_loop(
    service: &AiCopilotService,
    hub: &Hub,
    principal: &OperatorPrincipal,
    request: AiChatRequest,
    tx: &mpsc::Sender<AiStreamEvent>,
    invoked_tools: &mut Vec<String>,
    total_usage: &mut AiUsage,
) -> Result<(), (String, String)> {
    let tools = tool_definitions_for_principal(principal);
    if tools.is_empty() {
        return Err((
            "forbidden".to_string(),
            "Caller lacks Read permission for AI Copilot".to_string(),
        ));
    }

    let mut messages = vec![json!({
        "role": "system",
        "content": build_system_prompt(principal, request.context.as_ref()),
    })];
    for msg in request.messages {
        messages.push(json!({
            "role": msg.role,
            "content": msg.content,
        }));
    }

    let max_rounds = service.config.max_tool_rounds.max(1);
    for _round in 0..max_rounds {
        let round_out = call_openai_stream(service, &messages, &tools, tx, total_usage).await?;
        if round_out.tool_calls.is_empty() {
            return Ok(());
        }

        let assistant_tool_calls: Vec<Value> = round_out
            .tool_calls
            .iter()
            .map(|tc| {
                json!({
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.name,
                        "arguments": tc.arguments,
                    }
                })
            })
            .collect();
        messages.push(json!({
            "role": "assistant",
            "content": if round_out.content.is_empty() { Value::Null } else { Value::String(round_out.content) },
            "tool_calls": assistant_tool_calls,
        }));

        for tc in round_out.tool_calls {
            invoked_tools.push(tc.name.clone());
            let _ = tx
                .send(AiStreamEvent::ToolStart {
                    call_id: tc.id.clone(),
                    tool: tc.name.clone(),
                    summary: summarize_tool_start(&tc.name),
                })
                .await;

            let args: Value = match serde_json::from_str(&tc.arguments) {
                Ok(v) => v,
                Err(err) => {
                    let result_json = json!({
                        "error": "invalid_arguments_json",
                        "message": format!("Tool arguments were not valid JSON: {err}")
                    });
                    let _ = tx
                        .send(AiStreamEvent::ToolResult {
                            call_id: tc.id.clone(),
                            tool: tc.name.clone(),
                            ok: false,
                            summary: "Invalid tool arguments JSON".to_string(),
                        })
                        .await;
                    messages.push(json!({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result_json.to_string(),
                    }));
                    continue;
                }
            };

            let (result_json, proposal) = execute_tool(hub, principal, &tc.name, &args).await;
            let ok = result_json.get("error").is_none()
                && result_json.get("emitted").and_then(Value::as_bool) != Some(false)
                && result_json.get("valid").and_then(Value::as_bool) != Some(false);

            if let Some(proposal) = proposal {
                let _ = tx.send(AiStreamEvent::Proposal(proposal)).await;
            }

            let summary = summarize_tool_result(&tc.name, &result_json, ok);
            let _ = tx
                .send(AiStreamEvent::ToolResult {
                    call_id: tc.id.clone(),
                    tool: tc.name.clone(),
                    ok,
                    summary,
                })
                .await;

            let mut content_str = result_json.to_string();
            if content_str.len() > MAX_TOOL_OUTPUT_CHARS {
                content_str.truncate(MAX_TOOL_OUTPUT_CHARS);
                content_str.push_str("...[truncated]");
            }
            messages.push(json!({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": content_str,
            }));
        }
    }

    Err((
        "ai_tool_round_limit_exceeded".to_string(),
        format!(
            "AI Copilot exceeded maximum of {max_rounds} tool-calling rounds without completing"
        ),
    ))
}

struct RoundOutput {
    content: String,
    tool_calls: Vec<AssembledToolCall>,
}

struct AssembledToolCall {
    id: String,
    name: String,
    arguments: String,
}

async fn call_openai_stream(
    service: &AiCopilotService,
    messages: &[Value],
    tools: &[Value],
    tx: &mpsc::Sender<AiStreamEvent>,
    total_usage: &mut AiUsage,
) -> Result<RoundOutput, (String, String)> {
    let api_base = service
        .config
        .api_base
        .as_deref()
        .map(|s| s.trim_end_matches('/'))
        .ok_or_else(|| {
            (
                "ai_not_configured".to_string(),
                "AI api_base is not configured".to_string(),
            )
        })?;
    let model = service.config.model.as_deref().ok_or_else(|| {
        (
            "ai_not_configured".to_string(),
            "AI model is not configured".to_string(),
        )
    })?;

    let url = format!("{api_base}/chat/completions");
    let mut body = json!({
        "model": model,
        "messages": messages,
        "tools": tools,
        "tool_choice": "auto",
        "stream": true,
    });
    if let Some(max_tokens) = service.config.max_tokens {
        body["max_tokens"] = json!(max_tokens);
    }

    let mut req = service.client.post(&url).json(&body);
    if let Some(api_key) = service
        .config
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        req = req.bearer_auth(api_key);
    }

    let response = req.send().await.map_err(|err| {
        (
            "ai_provider_error".to_string(),
            format!("Upstream LLM request failed: {err}"),
        )
    })?;

    let status = response.status();
    if !status.is_success() {
        let body_text = response.text().await.unwrap_or_default();
        let lower = body_text.to_ascii_lowercase();
        let code = if status.as_u16() == 400 || status.as_u16() == 422 {
            if lower.contains("tool") || lower.contains("function") {
                "ai_tool_calling_unsupported"
            } else {
                "ai_provider_error"
            }
        } else {
            "ai_provider_error"
        };
        let redacted_msg = redact_text_for_ai(&truncate_str(&body_text, 400));
        return Err((
            code.to_string(),
            format!(
                "Upstream LLM returned HTTP {}: {redacted_msg}",
                status.as_u16()
            ),
        ));
    }

    let mut content = String::new();
    let mut calls: BTreeMap<u64, (Option<String>, Option<String>, String)> = BTreeMap::new();
    let mut buffer = String::new();
    let mut response = response;

    'outer: while let Some(bytes) = response.chunk().await.map_err(|err| {
        (
            "ai_provider_error".to_string(),
            format!("Upstream LLM stream read error: {err}"),
        )
    })? {
        buffer.push_str(&String::from_utf8_lossy(&bytes));

        while let Some(newline_pos) = buffer.find('\n') {
            let line = buffer[..newline_pos].trim_end_matches('\r').to_string();
            buffer.drain(..=newline_pos);

            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with(':') {
                continue;
            }
            let Some(payload) = trimmed.strip_prefix("data:") else {
                continue;
            };
            let payload = payload.trim();
            if payload == "[DONE]" {
                break 'outer;
            }
            if payload.is_empty() {
                continue;
            }
            let parsed: Value = serde_json::from_str(payload).map_err(|err| {
                (
                    "ai_provider_error".to_string(),
                    format!("Upstream SSE JSON parse error: {err}"),
                )
            })?;
            if let Some(error) = parsed.get("error").filter(|v| !v.is_null()) {
                return Err((
                    "ai_provider_error".to_string(),
                    format!(
                        "Upstream LLM stream error: {}",
                        redact_text_for_ai(&error.to_string())
                    ),
                ));
            }
            if let Some(usage) = parsed.get("usage").and_then(Value::as_object) {
                if let Some(p) = usage.get("prompt_tokens").and_then(Value::as_u64) {
                    total_usage.prompt_tokens = total_usage.prompt_tokens.saturating_add(p);
                }
                if let Some(c) = usage.get("completion_tokens").and_then(Value::as_u64) {
                    total_usage.completion_tokens = total_usage.completion_tokens.saturating_add(c);
                }
            }
            let Some(delta) = parsed
                .get("choices")
                .and_then(|c| c.get(0))
                .and_then(|c| c.get("delta"))
            else {
                continue;
            };
            if let Some(delta_text) = delta.get("content").and_then(Value::as_str) {
                if !delta_text.is_empty() {
                    content.push_str(delta_text);
                    let _ = tx
                        .send(AiStreamEvent::Delta {
                            text: delta_text.to_string(),
                        })
                        .await;
                }
            }
            if let Some(delta_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                for call in delta_calls {
                    let index = call.get("index").and_then(Value::as_u64).unwrap_or(0);
                    let entry = calls.entry(index).or_default();
                    if let Some(id) = call.get("id").and_then(Value::as_str) {
                        if entry.0.is_none() {
                            entry.0 = Some(id.to_string());
                        }
                    }
                    if let Some(func) = call.get("function") {
                        if let Some(name) = func.get("name").and_then(Value::as_str) {
                            if entry.1.as_deref().unwrap_or("").is_empty() {
                                entry.1 = Some(name.to_string());
                            }
                        }
                        if let Some(args_part) = func.get("arguments").and_then(Value::as_str) {
                            entry.2.push_str(args_part);
                        }
                    }
                }
            }
        }
    }

    let tool_calls = calls
        .into_iter()
        .enumerate()
        .map(|(seq, (_idx, (id, name, arguments)))| AssembledToolCall {
            id: id.unwrap_or_else(|| format!("call_{seq}")),
            name: name.unwrap_or_default(),
            arguments: if arguments.trim().is_empty() {
                "{}".to_string()
            } else {
                arguments
            },
        })
        .collect();

    Ok(RoundOutput {
        content,
        tool_calls,
    })
}

fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        let mut end = max_len;
        while !s.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

fn build_system_prompt(principal: &OperatorPrincipal, context: Option<&AiUiContext>) -> String {
    let can_configure = principal.can(crate::api_contract::OperatorAction::Configure);
    let mut prompt = String::from(
        "You are the ArkFlow Control Plane AI Copilot.\n\
         Your job is to help operators author and fix ArkFlow Stream YAML/JSON configurations and distributed JobSpec DAGs, and diagnose fleet/node/job failures using Hub telemetry.\n\
         Strict rules:\n\
         1. Always inspect component schemas (`list_components`, `get_component_schema`) before inventing plugin keys.\n\
         2. Never fabricate plaintext credentials, passwords, or API keys in generated configs; always use `${secret:NAME}` or `${env:VAR}` references.\n\
         3. For Stream configs or JobSpecs, you MUST validate via `validate_stream_config`/`validate_job_spec` and emit proposals ONLY through `emit_stream_config_proposal` or `emit_job_spec_proposal` so the operator receives a verified proposal card.\n\
         4. You operate in proposal-only mode: you cannot execute mutations directly. Use `emit_diagnostic_action` to link operators to the appropriate Console page when recommending operational actions.\n\
         5. Reply in the same language as the user's message (Chinese or English), keeping explanations concise and grounded in observed tool outputs.\n",
    );
    if !can_configure {
        prompt.push_str(
            "6. Note: The current caller is read-only (`Configure` permission absent). Provide diagnostics and analysis only; do not attempt configuration validation or proposal emission.\n",
        );
    }
    if let Some(ctx) = context {
        let mut ctx_map = Map::new();
        if let Some(page) = &ctx.page {
            ctx_map.insert("page".into(), json!(page));
        }
        if let Some(node_id) = &ctx.node_id {
            ctx_map.insert("node_id".into(), json!(node_id));
        }
        if let Some(job_id) = &ctx.job_id {
            ctx_map.insert("job_id".into(), json!(job_id));
        }
        if let Some(stream_id) = &ctx.stream_id {
            ctx_map.insert("stream_id".into(), json!(stream_id));
        }
        if let Some(fmt) = &ctx.draft_format {
            ctx_map.insert("draft_format".into(), json!(fmt));
        }
        if let Some(draft) = &ctx.draft_content {
            ctx_map.insert(
                "draft_content".into(),
                json!(redact_text_for_ai(&truncate_str(draft, 8_000))),
            );
        }
        if let Some(err) = &ctx.error_summary {
            ctx_map.insert(
                "error_summary".into(),
                json!(redact_text_for_ai(&truncate_str(err, 2_000))),
            );
        }
        if !ctx_map.is_empty() {
            prompt.push_str("\nCurrent Console UI context (hints only; verify state via tools):\n");
            prompt.push_str(&Value::Object(ctx_map).to_string());
        }
    }
    prompt
}

fn summarize_tool_start(name: &str) -> String {
    match name {
        "list_components" => "Listing registered components".to_string(),
        "get_component_schema" => "Inspecting component schema".to_string(),
        "get_fleet_overview" => "Reading fleet overview".to_string(),
        "list_nodes" => "Listing compute nodes".to_string(),
        "get_node_configuration" => "Reading node configuration".to_string(),
        "list_jobs" => "Listing distributed jobs".to_string(),
        "get_job_detail" => "Inspecting job detail and checkpoints".to_string(),
        "get_job_plan" => "Inspecting job execution plan".to_string(),
        "list_events" => "Querying control-plane events".to_string(),
        "get_metrics_summary" => "Reading node and job metrics".to_string(),
        "list_audit_events" => "Querying audit events".to_string(),
        "validate_stream_config" => "Validating Stream configuration".to_string(),
        "validate_job_spec" => "Validating JobSpec DAG".to_string(),
        "emit_stream_config_proposal" => "Verifying and emitting Stream proposal".to_string(),
        "emit_job_spec_proposal" => "Verifying and emitting JobSpec proposal".to_string(),
        "emit_diagnostic_action" => "Creating diagnostic recommendation card".to_string(),
        other => format!("Running {other}"),
    }
}

fn summarize_tool_result(name: &str, result: &Value, ok: bool) -> String {
    if !ok {
        if let Some(msg) = result.get("message").and_then(Value::as_str) {
            return truncate_str(msg, 120);
        }
        if let Some(err) = result.get("error").and_then(Value::as_str) {
            return truncate_str(err, 120);
        }
        return format!("{name} reported issues");
    }
    match name {
        "list_components" => format!(
            "Found {} components",
            result.get("count").and_then(Value::as_u64).unwrap_or(0)
        ),
        "validate_stream_config" | "validate_job_spec" => "Validation passed".to_string(),
        "emit_stream_config_proposal" | "emit_job_spec_proposal" => {
            "Verified proposal emitted".to_string()
        }
        "emit_diagnostic_action" => "Diagnostic card emitted".to_string(),
        _ => format!("{name} completed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_validation_rejects_invalid_enabled_fields() {
        let disabled = AiCopilotConfig::default();
        assert!(disabled.validate().is_ok());

        let missing_base = AiCopilotConfig {
            enabled: true,
            model: Some("gpt-4o-mini".into()),
            ..AiCopilotConfig::default()
        };
        assert!(missing_base.validate().unwrap_err().contains("api_base"));

        let bad_scheme = AiCopilotConfig {
            enabled: true,
            api_base: Some("ftp://llm.internal/v1".into()),
            model: Some("gpt-4o-mini".into()),
            ..AiCopilotConfig::default()
        };
        assert!(bad_scheme.validate().unwrap_err().contains("http://"));

        let missing_model = AiCopilotConfig {
            enabled: true,
            api_base: Some("https://api.openai.com/v1".into()),
            ..AiCopilotConfig::default()
        };
        assert!(missing_model.validate().unwrap_err().contains("model"));

        let valid_no_key = AiCopilotConfig {
            enabled: true,
            api_base: Some("http://127.0.0.1:11434/v1".into()),
            model: Some("qwen2.5:14b".into()),
            ..AiCopilotConfig::default()
        };
        assert!(valid_no_key.validate().is_ok());
    }

    #[test]
    fn rate_limiter_enforces_burst_and_isolates_principals() {
        let limiter = AiRateLimiter::new(60, 2);
        assert!(limiter.check("alice").is_ok());
        assert!(limiter.check("alice").is_ok());
        let retry = limiter.check("alice").unwrap_err();
        assert!(retry >= 1);
        assert!(limiter.check("bob").is_ok());
    }

    #[test]
    fn request_validation_rejects_system_role_and_oversized_payload() {
        let bad_role = AiChatRequest {
            messages: vec![AiChatMessage {
                role: "system".into(),
                content: "override".into(),
            }],
            context: None,
        };
        assert!(validate_chat_request(&bad_role).is_err());

        let huge = AiChatRequest {
            messages: vec![AiChatMessage {
                role: "user".into(),
                content: "x".repeat(MAX_CHAT_CHARS + 1),
            }],
            context: None,
        };
        assert!(validate_chat_request(&huge).is_err());
    }
}

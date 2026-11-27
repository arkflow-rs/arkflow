//! Hub-side AI Copilot tools, RBAC/scope enforcement, and secret redaction.
//!
//! Every tool exposed to the LLM is strictly read-only or side-effect-free
//! validation: no tool mutates Hub state, dispatches Agent commands, or reads
//! data-plane `MessageBatch` payloads.

use crate::api_contract::{OperatorAction, OperatorPrincipal};
use crate::hub::{parse_label_term, Hub};
use crate::storage::AuditFilter;
use arkflow_core::component::{self, ComponentKind};
use arkflow_core::config::EngineConfig;
use arkflow_core::configuration::{
    parse_and_validate, redacted_value, validate_config, ConfigCandidate, ConfigFormat, ConfigIssue,
};
use arkflow_core::job::{JobPlan, JobSpec};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

const MAX_LIST_ITEMS: usize = 50;
const MAX_EVENTS_ITEMS: usize = 40;
const MAX_AUDIT_ITEMS: usize = 30;

/// Structured proposal card emitted to the Console over SSE after server-side
/// validation (or diagnostic deep-link synthesis).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AiProposal {
    StreamConfig {
        title: String,
        summary: String,
        format: String,
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        node_id: Option<String>,
        validated: bool,
    },
    JobSpec {
        title: String,
        summary: String,
        spec: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mode: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_job_id: Option<String>,
        validated: bool,
    },
    DiagnosticAction {
        severity: String,
        title: String,
        summary: String,
        route: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        params: Option<Map<String, Value>>,
        action_label: String,
    },
}

/// Redact sensitive fields in JSON payloads sent to an upstream LLM.
///
/// Mirrors `arkflow_core::configuration::redacted_value` so literal credentials
/// and URL userinfo/query passwords are masked as `******`, while preserving
/// unexpanded secret-reference placeholders (`${secret:...}`, `${env:...}`,
/// `${file:...}`) when the entire string is a pure reference placeholder.
pub fn redact_for_ai(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut out = Map::with_capacity(object.len());
            for (key, child) in object {
                if is_secret_key(key) {
                    if let Some(placeholder) = pure_secret_reference(child) {
                        out.insert(key.clone(), Value::String(placeholder.to_owned()));
                    } else {
                        out.insert(key.clone(), Value::String("******".to_string()));
                    }
                } else {
                    out.insert(key.clone(), redact_for_ai(child));
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(redact_for_ai).collect()),
        Value::String(_) => redacted_value(value),
        other => other.clone(),
    }
}

/// Mask `Bearer <token>` fragments and embedded `scheme://user:password@` URLs
/// in free-text strings before sending them to an upstream LLM.
pub fn redact_text_for_ai(text: &str) -> String {
    let bearer_masked = mask_bearer_tokens(text);
    let url_masked = mask_inline_url_credentials(&bearer_masked);
    if let Value::String(final_masked) = redacted_value(&Value::String(url_masked.clone())) {
        final_masked
    } else {
        url_masked
    }
}

/// Redact YAML/JSON config fragments structurally when parseable, and apply
/// line-level key/URL/Bearer redaction for partial snippets or plain text while
/// keeping `${secret:...}` / `${env:...}` / `${file:...}` placeholders intact.
pub fn redact_config_or_text_for_ai(text: &str) -> String {
    let trimmed = text.trim();
    if (trimmed.starts_with('{') || trimmed.starts_with('[') || trimmed.contains(':'))
        && !trimmed.is_empty()
    {
        if let Ok(val) = serde_yaml::from_str::<Value>(text) {
            if matches!(val, Value::Object(_) | Value::Array(_)) {
                let redacted = redact_for_ai(&val);
                if trimmed.starts_with('{') || trimmed.starts_with('[') {
                    if let Ok(json_out) = serde_json::to_string_pretty(&redacted) {
                        return redact_text_for_ai(&json_out);
                    }
                } else if let Ok(yaml_out) = serde_yaml::to_string(&redacted) {
                    return redact_text_for_ai(&yaml_out);
                }
            }
        }
    }
    let line_masked = mask_secret_key_lines(text);
    redact_text_for_ai(&line_masked)
}

fn mask_bearer_tokens(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.to_ascii_lowercase().find("bearer ") {
        result.push_str(&rest[..pos + 7]);
        let after = &rest[pos + 7..];
        let token_end = after
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ',' | ';'))
            .unwrap_or(after.len());
        if token_end > 0 {
            result.push_str("******");
        }
        rest = &after[token_end..];
    }
    result.push_str(rest);
    result
}

fn mask_inline_url_credentials(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(scheme_pos) = rest.find("://") {
        let head = &rest[..scheme_pos + 3];
        out.push_str(head);
        let after_scheme = &rest[scheme_pos + 3..];
        let url_end = after_scheme
            .find(|c: char| {
                c.is_whitespace() || matches!(c, '"' | '\'' | ',' | ';' | ')' | ']' | '}')
            })
            .unwrap_or(after_scheme.len());
        let url_body = &after_scheme[..url_end];
        if let Some(at_pos) = url_body.find('@') {
            let userinfo = &url_body[..at_pos];
            if let Some(colon_pos) = userinfo.find(':') {
                out.push_str(&userinfo[..colon_pos + 1]);
                out.push_str("******");
                out.push_str(&url_body[at_pos..]);
            } else {
                out.push_str(url_body);
            }
        } else {
            out.push_str(url_body);
        }
        rest = &after_scheme[url_end..];
    }
    out.push_str(rest);
    out
}

fn mask_secret_key_lines(text: &str) -> String {
    let mut lines = Vec::new();
    for line in text.lines() {
        let trimmed_start = line.trim_start();
        let leading = &line[..line.len() - trimmed_start.len()];
        let candidate = trimmed_start.strip_prefix("- ").unwrap_or(trimmed_start);
        let prefix_dash = if trimmed_start.starts_with("- ") {
            "- "
        } else {
            ""
        };
        if let Some((raw_key, raw_val)) = candidate.split_once(':') {
            let clean_key = raw_key.trim().trim_matches(|c| c == '"' || c == '\'');
            let val_trimmed = raw_val.trim();
            if is_secret_key(clean_key) && !val_trimmed.is_empty() {
                let unquoted = val_trimmed
                    .trim_end_matches(',')
                    .trim()
                    .trim_matches(|c| c == '"' || c == '\'');
                if !unquoted.starts_with("${secret:")
                    && !unquoted.starts_with("${env:")
                    && !unquoted.starts_with("${file:")
                    && unquoted != "******"
                {
                    let trailing_comma = if val_trimmed.ends_with(',') { "," } else { "" };
                    lines.push(format!(
                        "{leading}{prefix_dash}{raw_key}: \"******\"{trailing_comma}"
                    ));
                    continue;
                }
            }
        }
        lines.push(line.to_string());
    }
    let mut joined = lines.join("\n");
    if text.ends_with('\n') && !joined.ends_with('\n') {
        joined.push('\n');
    }
    joined
}

/// Return true if `name` is a known built-in Copilot tool name.
pub fn is_known_tool_name(name: &str) -> bool {
    matches!(
        name,
        "list_components"
            | "get_component_schema"
            | "get_fleet_overview"
            | "list_nodes"
            | "get_node_configuration"
            | "list_jobs"
            | "get_job_detail"
            | "get_job_plan"
            | "list_events"
            | "get_metrics_summary"
            | "list_audit_events"
            | "validate_stream_config"
            | "validate_job_spec"
            | "emit_stream_config_proposal"
            | "emit_job_spec_proposal"
            | "emit_diagnostic_action"
    )
}

fn pure_secret_reference(value: &Value) -> Option<&str> {
    let Value::String(s) = value else {
        return None;
    };
    let trimmed = s.trim();
    if trimmed.starts_with("${")
        && trimmed.ends_with('}')
        && (trimmed.starts_with("${secret:")
            || trimmed.starts_with("${env:")
            || trimmed.starts_with("${file:"))
        && !trimmed[2..trimmed.len() - 1].contains("${")
    {
        Some(trimmed)
    } else {
        None
    }
}

fn is_secret_key(key: &str) -> bool {
    let normalized: String = key
        .to_ascii_lowercase()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect();
    [
        "password",
        "passwd",
        "token",
        "secret",
        "credential",
        "apikey",
        "accesskey",
        "privatekey",
        "accountkey",
    ]
    .iter()
    .any(|part| normalized.contains(part))
}

/// Build the OpenAI-compatible `tools` schema array filtered by the caller's
/// `OperatorPrincipal` permissions.
pub fn tool_definitions_for_principal(principal: &OperatorPrincipal) -> Vec<Value> {
    let mut tools = Vec::new();
    if !principal.can(OperatorAction::Read) {
        return tools;
    }

    tools.extend([
        tool_def(
            "list_components",
            "List registered ArkFlow components (input, output, processor, buffer, codec, temporary) with short descriptions.",
            json!({
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": ["input", "output", "processor", "buffer", "codec", "temporary"],
                        "description": "Optional component kind filter."
                    }
                },
                "additionalProperties": false
            }),
        ),
        tool_def(
            "get_component_schema",
            "Fetch the JSON Schema and example configuration for a specific registered ArkFlow component.",
            json!({
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": ["input", "output", "processor", "buffer", "codec", "temporary"]
                    },
                    "name": { "type": "string" }
                },
                "required": ["kind", "name"],
                "additionalProperties": false
            }),
        ),
        tool_def(
            "get_fleet_overview",
            "Read Hub operational status, HA leadership state, node counts, and stream summary within the caller's authorized scope.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        ),
        tool_def(
            "list_nodes",
            "List compute nodes registered on the Hub (optionally filtered by label selector like 'env=prod,region=us'), scoped to the caller.",
            json!({
                "type": "object",
                "properties": {
                    "label_selector": {
                        "type": "string",
                        "description": "Optional comma-separated key=value label selector."
                    }
                },
                "additionalProperties": false
            }),
        ),
        tool_def(
            "get_node_configuration",
            "Read the redacted configuration snapshot reported by a specific node.",
            json!({
                "type": "object",
                "properties": {
                    "node_id": { "type": "string" }
                },
                "required": ["node_id"],
                "additionalProperties": false
            }),
        ),
        tool_def(
            "list_jobs",
            "List distributed Jobs managed by the Hub within the caller's scope.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        ),
        tool_def(
            "get_job_detail",
            "Read a Job's redacted spec, state, convergence, last error, task placement, and recent checkpoints.",
            json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string" }
                },
                "required": ["job_id"],
                "additionalProperties": false
            }),
        ),
        tool_def(
            "get_job_plan",
            "Compile and inspect the redacted execution plan (chains/tasks/edges) of an existing Job.",
            json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string" }
                },
                "required": ["job_id"],
                "additionalProperties": false
            }),
        ),
        tool_def(
            "list_events",
            "List recent Hub control-plane events, optionally filtered by node_id, stream_id, or event_type.",
            json!({
                "type": "object",
                "properties": {
                    "node_id": { "type": "string" },
                    "stream_id": { "type": "string" },
                    "event_type": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 40 }
                },
                "additionalProperties": false
            }),
        ),
        tool_def(
            "get_metrics_summary",
            "Read node metrics and per-Job KernelMetricsSnapshot readings (throughput, errors, latency, checkpoint failures, watermark lag, WAL flush failures).",
            json!({
                "type": "object",
                "properties": {
                    "node_id": { "type": "string" },
                    "job_id": { "type": "string" }
                },
                "additionalProperties": false
            }),
        ),
        tool_def(
            "emit_diagnostic_action",
            "Emit a structured deep-link recommendation card in the Console pointing the operator to the relevant page/resource without mutating state.",
            json!({
                "type": "object",
                "properties": {
                    "severity": { "type": "string", "enum": ["info", "warn", "critical"] },
                    "title": { "type": "string" },
                    "summary": { "type": "string" },
                    "route": {
                        "type": "string",
                        "enum": ["/", "/runtime", "/events", "/jobs", "/rollouts", "/approvals", "/configuration", "/components", "/audit", "/settings"]
                    },
                    "params": {
                        "type": "object",
                        "additionalProperties": { "type": "string" }
                    },
                    "action_label": { "type": "string" }
                },
                "required": ["severity", "title", "summary", "route", "action_label"],
                "additionalProperties": false
            }),
        ),
    ]);

    if principal.can_scope(OperatorAction::ReadAudit, "audit", None) {
        tools.push(tool_def(
            "list_audit_events",
            "List recent control-plane audit records (requires ReadAudit permission).",
            json!({
                "type": "object",
                "properties": {
                    "resource_id": { "type": "string" },
                    "action": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 30 }
                },
                "additionalProperties": false
            }),
        ));
    }

    if principal.can(OperatorAction::Configure) {
        tools.extend([
            tool_def(
                "validate_stream_config",
                "Validate an ArkFlow Stream configuration (YAML/JSON/TOML) using the real engine parser and component builders.",
                json!({
                    "type": "object",
                    "properties": {
                        "format": { "type": "string", "enum": ["yaml", "json", "toml"] },
                        "content": { "type": "string" }
                    },
                    "required": ["format", "content"],
                    "additionalProperties": false
                }),
            ),
            tool_def(
                "validate_job_spec",
                "Validate a distributed JobSpec JSON object through the engine graph/component deep validator.",
                json!({
                    "type": "object",
                    "properties": {
                        "spec": { "type": "object" }
                    },
                    "required": ["spec"],
                    "additionalProperties": false
                }),
            ),
            tool_def(
                "emit_stream_config_proposal",
                "Validate and emit a Stream configuration proposal card to the Console so the operator can fill it into the Configuration editor. Fails if validation does not pass.",
                json!({
                    "type": "object",
                    "properties": {
                        "title": { "type": "string" },
                        "summary": { "type": "string" },
                        "format": { "type": "string", "enum": ["yaml", "json"] },
                        "content": { "type": "string" },
                        "node_id": { "type": "string" }
                    },
                    "required": ["title", "summary", "format", "content"],
                    "additionalProperties": false
                }),
            ),
            tool_def(
                "emit_job_spec_proposal",
                "Validate and emit a distributed JobSpec proposal card to the Console so the operator can import it into the visual DAG editor. Fails if validation does not pass.",
                json!({
                    "type": "object",
                    "properties": {
                        "title": { "type": "string" },
                        "summary": { "type": "string" },
                        "spec": { "type": "object" },
                        "mode": { "type": "string", "enum": ["create", "upgrade"] },
                        "target_job_id": { "type": "string" }
                    },
                    "required": ["title", "summary", "spec"],
                    "additionalProperties": false
                }),
            ),
        ]);
    }

    tools
}

fn tool_def(name: &str, description: &str, parameters: Value) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": parameters
        }
    })
}

/// Execute one internal Hub Copilot tool call under `principal` RBAC/scope rules.
pub async fn execute_tool(
    hub: &Hub,
    principal: &OperatorPrincipal,
    name: &str,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    match name {
        "list_components" => exec_list_components(principal, args),
        "get_component_schema" => exec_get_component_schema(principal, args),
        "get_fleet_overview" => exec_get_fleet_overview(hub, principal).await,
        "list_nodes" => exec_list_nodes(hub, principal, args).await,
        "get_node_configuration" => exec_get_node_configuration(hub, principal, args).await,
        "list_jobs" => exec_list_jobs(hub, principal).await,
        "get_job_detail" => exec_get_job_detail(hub, principal, args).await,
        "get_job_plan" => exec_get_job_plan(hub, principal, args).await,
        "list_events" => exec_list_events(hub, principal, args).await,
        "get_metrics_summary" => exec_get_metrics_summary(hub, principal, args).await,
        "list_audit_events" => exec_list_audit_events(hub, principal, args).await,
        "validate_stream_config" => exec_validate_stream_config(principal, args),
        "validate_job_spec" => exec_validate_job_spec(principal, args),
        "emit_stream_config_proposal" => exec_emit_stream_config_proposal(principal, args),
        "emit_job_spec_proposal" => exec_emit_job_spec_proposal(principal, args),
        "emit_diagnostic_action" => exec_emit_diagnostic_action(principal, args),
        _ => (
            json!({"error": "unknown_tool", "message": format!("Unknown tool '{name}'")}),
            None,
        ),
    }
}

fn denied(action: OperatorAction, resource_type: &str) -> (Value, Option<AiProposal>) {
    (
        json!({
            "error": "forbidden",
            "message": format!("Caller lacks {action:?} permission on {resource_type}")
        }),
        None,
    )
}

fn invalid_args(msg: impl Into<String>) -> (Value, Option<AiProposal>) {
    (
        json!({"error": "invalid_arguments", "message": msg.into()}),
        None,
    )
}

fn exec_list_components(
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    if !principal.can(OperatorAction::Read) {
        return denied(OperatorAction::Read, "component");
    }
    if let Err(err) = arkflow_plugin::initialize() {
        return (
            json!({"error": "plugin_init_failed", "message": err.to_string()}),
            None,
        );
    }
    let kind_filter = match args.get("kind").and_then(Value::as_str) {
        Some(raw) => match raw.parse::<ComponentKind>() {
            Ok(kind) => Some(kind),
            Err(_) => return invalid_args(format!("Unknown component kind '{raw}'")),
        },
        None => None,
    };
    let items: Vec<Value> = component::list_components()
        .into_iter()
        .filter(|(kind, _)| kind_filter.is_none_or(|expected| *kind == expected))
        .map(|(kind, meta)| {
            json!({
                "kind": kind,
                "name": meta.name,
                "description": meta.description,
            })
        })
        .collect();
    (json!({"count": items.len(), "components": items}), None)
}

fn exec_get_component_schema(
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    if !principal.can(OperatorAction::Read) {
        return denied(OperatorAction::Read, "component");
    }
    if let Err(err) = arkflow_plugin::initialize() {
        return (
            json!({"error": "plugin_init_failed", "message": err.to_string()}),
            None,
        );
    }
    let Some(kind_str) = args.get("kind").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'kind'");
    };
    let Some(name) = args.get("name").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'name'");
    };
    let Ok(kind) = kind_str.parse::<ComponentKind>() else {
        return invalid_args(format!("Unknown component kind '{kind_str}'"));
    };
    match component::get_component_metadata(kind, name) {
        Some(meta) => (
            json!({
                "kind": kind,
                "name": meta.name,
                "description": meta.description,
                "schema": meta.config_schema,
                "example": meta.config_example,
            }),
            None,
        ),
        None => (
            json!({"error": "component_not_found", "message": format!("Unknown component {kind_str}/{name}")}),
            None,
        ),
    }
}

async fn exec_get_fleet_overview(
    hub: &Hub,
    principal: &OperatorPrincipal,
) -> (Value, Option<AiProposal>) {
    if !principal.can(OperatorAction::Read) {
        return denied(OperatorAction::Read, "node");
    }
    let nodes: Vec<_> = hub
        .nodes()
        .await
        .into_iter()
        .filter(|node| principal.can_scope(OperatorAction::Read, "node", Some(&node.id)))
        .collect();
    let total_nodes = nodes.len();
    let online_nodes = nodes
        .iter()
        .filter(|n| n.state == crate::hub::NodeConnectionState::Online)
        .count();
    let streams_total: usize = nodes.iter().map(|n| n.streams_total).sum();
    let streams_running: usize = nodes.iter().map(|n| n.streams_running).sum();
    let streams_failed: usize = nodes.iter().map(|n| n.streams_failed).sum();

    let mut payload = json!({
        "nodes_total": total_nodes,
        "nodes_online": online_nodes,
        "streams_total": streams_total,
        "streams_running": streams_running,
        "streams_failed": streams_failed,
        "approval_required": hub.approval_required(),
    });
    if principal.can_scope(OperatorAction::Read, "node", None) {
        if let Ok(status) = hub.operational_status().await {
            payload["operational_status"] = serde_json::to_value(status).unwrap_or(Value::Null);
        }
        let leadership = hub.leadership().await;
        payload["ha"] = json!({
            "enabled": hub.ha_config().enabled,
            "role": leadership.role(),
            "epoch": leadership.epoch(),
        });
    }
    (payload, None)
}

async fn exec_list_nodes(
    hub: &Hub,
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    if !principal.can(OperatorAction::Read) {
        return denied(OperatorAction::Read, "node");
    }
    let mut selector = Vec::new();
    if let Some(raw) = args.get("label_selector").and_then(Value::as_str) {
        for term in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            match parse_label_term(term) {
                Ok(pair) => selector.push(pair),
                Err(err) => return invalid_args(err),
            }
        }
    }
    let mut nodes: Vec<_> = hub
        .nodes()
        .await
        .into_iter()
        .filter(|node| principal.can_scope(OperatorAction::Read, "node", Some(&node.id)))
        .filter(|node| {
            selector
                .iter()
                .all(|(k, v)| node.labels.get(k).is_some_and(|actual| actual == v))
        })
        .collect();
    let total = nodes.len();
    nodes.truncate(MAX_LIST_ITEMS);
    (json!({"total": total, "items": nodes}), None)
}

async fn exec_get_node_configuration(
    hub: &Hub,
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    let Some(node_id) = args.get("node_id").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'node_id'");
    };
    if !principal.can_scope(OperatorAction::Read, "configuration", Some(node_id)) {
        return denied(OperatorAction::Read, "configuration");
    }
    match hub.configuration(node_id).await {
        Some(cfg) => (
            json!({
                "node_id": node_id,
                "configuration": redact_for_ai(&cfg),
            }),
            None,
        ),
        None => (
            json!({"error": "node_configuration_unavailable", "message": format!("Node '{node_id}' has not reported configuration")}),
            None,
        ),
    }
}

async fn exec_list_jobs(hub: &Hub, principal: &OperatorPrincipal) -> (Value, Option<AiProposal>) {
    if !principal.can(OperatorAction::Read) {
        return denied(OperatorAction::Read, "job");
    }
    match hub.jobs().await {
        Ok(jobs) => {
            let mut items: Vec<Value> = jobs
                .into_iter()
                .filter(|job| principal.can_scope(OperatorAction::Read, "job", Some(&job.job_id)))
                .map(|job| {
                    json!({
                        "job_id": job.job_id,
                        "version": job.version,
                        "desired_state": job.desired_state,
                        "observed_state": job.observed_state,
                        "convergence": job.convergence,
                        "generation": job.generation,
                        "node_ids": job.node_ids,
                        "checkpoint_id": job.checkpoint_id,
                        "pause_phase": job.pause_phase,
                        "last_error": job.last_error.as_deref().map(redact_text_for_ai),
                        "updated_at_ms": job.updated_at_ms,
                    })
                })
                .collect();
            let total = items.len();
            items.truncate(MAX_LIST_ITEMS);
            (json!({"total": total, "items": items}), None)
        }
        Err(err) => (
            json!({"error": "hub_error", "message": err.to_string()}),
            None,
        ),
    }
}

async fn exec_get_job_detail(
    hub: &Hub,
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    let Some(job_id) = args.get("job_id").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'job_id'");
    };
    if !principal.can_scope(OperatorAction::Read, "job", Some(job_id)) {
        return denied(OperatorAction::Read, "job");
    }
    let job = match hub.job(job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => {
            return (
                json!({"error": "job_not_found", "message": format!("Unknown Job '{job_id}'")}),
                None,
            )
        }
        Err(err) => {
            return (
                json!({"error": "hub_error", "message": err.to_string()}),
                None,
            )
        }
    };
    let redacted_spec = serde_json::from_str::<Value>(&job.spec_json)
        .map(|val| redact_for_ai(&val))
        .unwrap_or_else(|_| json!({"masked": "******"}));
    let checkpoints = hub.job_checkpoints(job_id).await.unwrap_or_default();
    let metrics = hub.job_detail_metrics(job_id).await;
    let tasks = hub.observed_job_tasks(job_id).await;
    (
        json!({
            "job_id": job.job_id,
            "version": job.version,
            "desired_state": job.desired_state,
            "observed_state": job.observed_state,
            "convergence": job.convergence,
            "generation": job.generation,
            "node_ids": job.node_ids,
            "checkpoint_id": job.checkpoint_id,
            "pause_phase": job.pause_phase,
            "last_error": job.last_error.as_deref().map(redact_text_for_ai),
            "spec": redacted_spec,
            "checkpoints": checkpoints.into_iter().take(10).collect::<Vec<_>>(),
            "metrics": metrics,
            "observed_tasks": tasks,
        }),
        None,
    )
}

async fn exec_get_job_plan(
    hub: &Hub,
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    let Some(job_id) = args.get("job_id").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'job_id'");
    };
    if !principal.can_scope(OperatorAction::Read, "job", Some(job_id)) {
        return denied(OperatorAction::Read, "job");
    }
    let job = match hub.job(job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => {
            return (
                json!({"error": "job_not_found", "message": format!("Unknown Job '{job_id}'")}),
                None,
            )
        }
        Err(err) => {
            return (
                json!({"error": "hub_error", "message": err.to_string()}),
                None,
            )
        }
    };
    let spec: JobSpec = match serde_json::from_str(&job.spec_json) {
        Ok(spec) => spec,
        Err(err) => {
            return (
                json!({"error": "invalid_persisted_job", "message": err.to_string()}),
                None,
            )
        }
    };
    match JobPlan::compile(spec) {
        Ok(plan) => {
            let value = serde_json::to_value(plan).unwrap_or(Value::Null);
            (
                json!({"job_id": job_id, "plan": redact_for_ai(&value)}),
                None,
            )
        }
        Err(err) => (
            json!({"error": "invalid_job_plan", "message": err.to_string()}),
            None,
        ),
    }
}

async fn exec_list_events(
    hub: &Hub,
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    if !principal.can(OperatorAction::Read) {
        return denied(OperatorAction::Read, "event");
    }
    let node_id = args.get("node_id").and_then(Value::as_str);
    if let Some(target_node) = node_id {
        if !principal.can_scope(OperatorAction::Read, "event", Some(target_node)) {
            return denied(OperatorAction::Read, "event");
        }
    }
    let stream_id = args.get("stream_id").and_then(Value::as_str);
    let event_type = args.get("event_type").and_then(Value::as_str);
    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .map(|n| (n as usize).clamp(1, MAX_EVENTS_ITEMS))
        .unwrap_or(20);

    let mut events: Vec<Value> = hub
        .events(node_id)
        .await
        .into_iter()
        .filter(|item| principal.can_scope(OperatorAction::Read, "event", Some(&item.node_id)))
        .filter(|item| stream_id.is_none_or(|s| item.event.stream_id.as_deref() == Some(s)))
        .filter(|item| event_type.is_none_or(|t| item.event.event_type == t))
        .take(limit)
        .map(|item| {
            let mut value = serde_json::to_value(&item).unwrap_or(Value::Null);
            if let Some(msg) = value.get("message").and_then(Value::as_str) {
                let masked = redact_text_for_ai(msg);
                value["message"] = Value::String(masked);
            }
            redact_for_ai(&value)
        })
        .collect();
    events.truncate(limit);
    (json!({"count": events.len(), "items": events}), None)
}

async fn exec_get_metrics_summary(
    hub: &Hub,
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    if !principal.can(OperatorAction::Read) {
        return denied(OperatorAction::Read, "node");
    }
    let node_filter = args.get("node_id").and_then(Value::as_str);
    let job_filter = args.get("job_id").and_then(Value::as_str);

    if let Some(node_id) = node_filter {
        if !principal.can_scope(OperatorAction::Read, "node", Some(node_id)) {
            return denied(OperatorAction::Read, "node");
        }
    }
    if let Some(job_id) = job_filter {
        if !principal.can_scope(OperatorAction::Read, "job", Some(job_id)) {
            return denied(OperatorAction::Read, "job");
        }
    }

    let node_metrics: Vec<_> = hub
        .metrics_by_node(node_filter)
        .await
        .into_iter()
        .filter(|m| principal.can_scope(OperatorAction::Read, "node", Some(&m.node_id)))
        .take(MAX_LIST_ITEMS)
        .collect();

    let mut job_metrics_out = Map::new();
    for (node_id, jobs) in hub.job_metrics().await {
        if node_filter.is_some_and(|expected| expected != node_id) {
            continue;
        }
        if !principal.can_scope(OperatorAction::Read, "node", Some(&node_id)) {
            continue;
        }
        let mut node_jobs = Map::new();
        for (job_id, snapshot) in jobs {
            if job_filter.is_some_and(|expected| expected != job_id) {
                continue;
            }
            if !principal.can_scope(OperatorAction::Read, "job", Some(&job_id)) {
                continue;
            }
            node_jobs.insert(
                job_id,
                serde_json::to_value(snapshot).unwrap_or(Value::Null),
            );
        }
        if !node_jobs.is_empty() {
            job_metrics_out.insert(node_id, Value::Object(node_jobs));
        }
    }

    (
        json!({
            "node_metrics": node_metrics,
            "job_metrics": Value::Object(job_metrics_out),
        }),
        None,
    )
}

async fn exec_list_audit_events(
    hub: &Hub,
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    let resource_id = args
        .get("resource_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if !principal.can_scope(OperatorAction::ReadAudit, "audit", resource_id.as_deref()) {
        return denied(OperatorAction::ReadAudit, "audit");
    }
    let action = args
        .get("action")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .map(|n| (n as usize).clamp(1, MAX_AUDIT_ITEMS))
        .unwrap_or(20);
    let filter = AuditFilter {
        resource_id,
        actor: None,
        action,
        from_ms: None,
        to_ms: None,
    };
    match hub.audit_filtered(filter).await {
        Ok(records) => {
            let items: Vec<Value> = records
                .into_iter()
                .take(limit)
                .map(|mut rec| {
                    rec.message = rec.message.as_deref().map(redact_text_for_ai);
                    serde_json::to_value(rec).unwrap_or(Value::Null)
                })
                .collect();
            (json!({"count": items.len(), "items": items}), None)
        }
        Err(crate::hub::HubError::StorageUnavailable) => (
            json!({"count": 0, "items": [], "note": "Hub has no durable audit store configured"}),
            None,
        ),
        Err(err) => (
            json!({"error": "hub_error", "message": err.to_string()}),
            None,
        ),
    }
}

fn validate_stream_candidate_content(
    format: ConfigFormat,
    content: &str,
) -> Result<Vec<ConfigIssue>, ConfigIssue> {
    if content.trim().is_empty() {
        return Err(ConfigIssue {
            path: "document".into(),
            message: "Configuration content must not be empty".into(),
        });
    }
    arkflow_plugin::initialize().map_err(|err| ConfigIssue {
        path: "document".into(),
        message: err.to_string(),
    })?;

    // Parse without resolving `${env:}` / `${secret:}` on the Hub process so
    // authoring configs referencing node-local env vars or not-yet-provisioned
    // Hub secrets still validate their component schemas and execution graphs.
    let parsed_direct: Result<EngineConfig, ConfigIssue> = match format {
        ConfigFormat::Yaml => serde_yaml::from_str(content).map_err(|e| ConfigIssue {
            path: e
                .location()
                .map(|loc| format!("line {}, column {}", loc.line(), loc.column()))
                .unwrap_or_else(|| "document".into()),
            message: e.to_string(),
        }),
        ConfigFormat::Json => serde_json::from_str(content).map_err(|e| ConfigIssue {
            path: format!("line {}, column {}", e.line(), e.column()),
            message: e.to_string(),
        }),
        ConfigFormat::Toml => toml::from_str(content).map_err(|e| ConfigIssue {
            path: "document".into(),
            message: e.to_string(),
        }),
    };

    match parsed_direct {
        Ok(cfg) => {
            let report = validate_config(&cfg);
            Ok(report.errors)
        }
        Err(direct_err) => {
            // If the direct parse failed only because a non-string field holds
            // a `${...}` reference (e.g. a port), fall back to full resolution.
            // Never return raw resolved messages from this fallback so resolved
            // `${secret:...}` / `${env:...}` values cannot reach the upstream LLM.
            if content.contains("${") {
                let candidate = ConfigCandidate {
                    format,
                    content: content.to_string(),
                    content_verbatim: None,
                };
                if let Ok(report) = parse_and_validate(&candidate) {
                    let sanitized = report
                        .errors
                        .into_iter()
                        .map(|err| ConfigIssue {
                            path: err.path,
                            message: "Validation constraint failed after resolving ${...} references (details redacted)".into(),
                        })
                        .collect();
                    return Ok(sanitized);
                }
            }
            Err(direct_err)
        }
    }
}

fn parse_config_format(raw: &str) -> Result<ConfigFormat, String> {
    match raw {
        "yaml" => Ok(ConfigFormat::Yaml),
        "json" => Ok(ConfigFormat::Json),
        "toml" => Ok(ConfigFormat::Toml),
        other => Err(format!("Unsupported format '{other}'")),
    }
}

fn exec_validate_stream_config(
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    if !principal.can(OperatorAction::Configure) {
        return denied(OperatorAction::Configure, "configuration");
    }
    let Some(format_str) = args.get("format").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'format'");
    };
    let Some(content) = args.get("content").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'content'");
    };
    let format = match parse_config_format(format_str) {
        Ok(f) => f,
        Err(err) => return invalid_args(err),
    };
    match validate_stream_candidate_content(format, content) {
        Ok(errors) if errors.is_empty() => (json!({"valid": true, "errors": []}), None),
        Ok(errors) => (json!({"valid": false, "errors": errors}), None),
        Err(issue) => (json!({"valid": false, "errors": [issue]}), None),
    }
}

fn exec_validate_job_spec(
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    if !principal.can(OperatorAction::Configure) {
        return denied(OperatorAction::Configure, "job");
    }
    let Some(spec_val) = args.get("spec") else {
        return invalid_args("Missing required object field 'spec'");
    };
    let spec: JobSpec = match serde_json::from_value(spec_val.clone()) {
        Ok(spec) => spec,
        Err(err) => {
            return (
                json!({"valid": false, "error": format!("Invalid JobSpec structure: {err}")}),
                None,
            )
        }
    };
    match crate::deep_validate_job(&spec) {
        Ok(()) => (json!({"valid": true}), None),
        Err(err) => (json!({"valid": false, "error": err}), None),
    }
}

fn exec_emit_stream_config_proposal(
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    if !principal.can(OperatorAction::Configure) {
        return denied(OperatorAction::Configure, "configuration");
    }
    let Some(title) = args.get("title").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'title'");
    };
    let Some(summary) = args.get("summary").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'summary'");
    };
    let Some(format_str) = args.get("format").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'format'");
    };
    if !matches!(format_str, "yaml" | "json") {
        return invalid_args("Stream config proposal format must be 'yaml' or 'json'");
    }
    let Some(content) = args.get("content").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'content'");
    };
    let node_id = args
        .get("node_id")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_owned);
    if let Some(ref target_node) = node_id {
        if !principal.can_scope(
            OperatorAction::Configure,
            "configuration",
            Some(target_node),
        ) {
            return denied(OperatorAction::Configure, "configuration");
        }
    }
    let format = parse_config_format(format_str).expect("yaml/json");
    match validate_stream_candidate_content(format, content) {
        Ok(errors) if errors.is_empty() => (
            json!({"emitted": true, "validated": true}),
            Some(AiProposal::StreamConfig {
                title: title.to_string(),
                summary: summary.to_string(),
                format: format_str.to_string(),
                content: content.to_string(),
                node_id,
                validated: true,
            }),
        ),
        Ok(errors) => (
            json!({
                "emitted": false,
                "validated": false,
                "errors": errors,
                "message": "Proposal rejected because server validation failed; fix the errors and retry."
            }),
            None,
        ),
        Err(issue) => (
            json!({
                "emitted": false,
                "validated": false,
                "errors": [issue],
                "message": "Proposal rejected because configuration parsing failed; fix the errors and retry."
            }),
            None,
        ),
    }
}

fn exec_emit_job_spec_proposal(
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    if !principal.can(OperatorAction::Configure) {
        return denied(OperatorAction::Configure, "job");
    }
    let Some(title) = args.get("title").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'title'");
    };
    let Some(summary) = args.get("summary").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'summary'");
    };
    let Some(spec_val) = args.get("spec") else {
        return invalid_args("Missing required object field 'spec'");
    };
    let mode = match args.get("mode") {
        None | Some(Value::Null) => None,
        Some(Value::String(raw)) if matches!(raw.as_str(), "create" | "upgrade") => {
            Some(raw.clone())
        }
        Some(Value::String(other)) => {
            return invalid_args(format!(
                "Unsupported mode '{other}'; expected 'create' or 'upgrade'"
            ));
        }
        Some(_) => return invalid_args("Field 'mode' must be a string ('create' or 'upgrade')"),
    };
    let target_job_id = args
        .get("target_job_id")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_owned);
    if let Some(ref job_id) = target_job_id {
        if !principal.can_scope(OperatorAction::Configure, "job", Some(job_id)) {
            return denied(OperatorAction::Configure, "job");
        }
    }
    let spec: JobSpec = match serde_json::from_value(spec_val.clone()) {
        Ok(spec) => spec,
        Err(err) => {
            return (
                json!({
                    "emitted": false,
                    "validated": false,
                    "error": format!("Invalid JobSpec structure: {err}")
                }),
                None,
            )
        }
    };
    match crate::deep_validate_job(&spec) {
        Ok(()) => (
            json!({"emitted": true, "validated": true}),
            Some(AiProposal::JobSpec {
                title: title.to_string(),
                summary: summary.to_string(),
                spec: serde_json::to_value(&spec).unwrap_or_else(|_| spec_val.clone()),
                mode,
                target_job_id,
                validated: true,
            }),
        ),
        Err(err) => (
            json!({
                "emitted": false,
                "validated": false,
                "error": err,
                "message": "JobSpec proposal rejected because deep_validate_job failed; fix the error and retry."
            }),
            None,
        ),
    }
}

fn exec_emit_diagnostic_action(
    principal: &OperatorPrincipal,
    args: &Value,
) -> (Value, Option<AiProposal>) {
    if !principal.can(OperatorAction::Read) {
        return denied(OperatorAction::Read, "diagnostic");
    }
    let Some(severity) = args.get("severity").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'severity'");
    };
    if !matches!(severity, "info" | "warn" | "critical") {
        return invalid_args("severity must be one of info, warn, critical");
    }
    let Some(title) = args.get("title").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'title'");
    };
    let Some(summary) = args.get("summary").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'summary'");
    };
    let Some(route) = args.get("route").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'route'");
    };
    if !matches!(
        route,
        "/" | "/runtime"
            | "/events"
            | "/jobs"
            | "/rollouts"
            | "/approvals"
            | "/configuration"
            | "/components"
            | "/audit"
            | "/settings"
    ) {
        return invalid_args("Invalid Console route");
    }
    let Some(action_label) = args.get("action_label").and_then(Value::as_str) else {
        return invalid_args("Missing required string field 'action_label'");
    };
    let params = args.get("params").and_then(Value::as_object).cloned();

    (
        json!({"emitted": true}),
        Some(AiProposal::DiagnosticAction {
            severity: severity.to_string(),
            title: title.to_string(),
            summary: summary.to_string(),
            route: route.to_string(),
            params,
            action_label: action_label.to_string(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_contract::{OperatorRole, ResourceScope};
    use crate::hub::{HubConfig, RegisterRequest};

    fn principal_with(role: OperatorRole, scopes: Vec<ResourceScope>) -> OperatorPrincipal {
        OperatorPrincipal {
            id: "test-user".into(),
            roles: vec![role],
            scopes,
        }
    }

    fn hub_config() -> HubConfig {
        HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 15_000,
            poll_interval_ms: 1_000,
            session_ttl_ms: 3_600_000,
            approval_required: false,
            approval_ttl_ms: 60_000,
        }
    }

    #[test]
    fn redacts_plaintext_secrets_and_preserves_secret_refs() {
        let input = json!({
            "streams": [{
                "input": {
                    "type": "kafka",
                    "password": "my-raw-password",
                    "api_key": "${secret:OPENAI_KEY}",
                    "token": "${env:TOKEN_VAR}",
                    "dsn": "postgres://alice:s3cret@db.internal:5432/app"
                }
            }]
        });
        let redacted = redact_for_ai(&input);
        let inp = &redacted["streams"][0]["input"];
        assert_eq!(inp["password"], "******");
        assert_eq!(inp["api_key"], "${secret:OPENAI_KEY}");
        assert_eq!(inp["token"], "${env:TOKEN_VAR}");
        assert_eq!(inp["dsn"], "postgres://alice:******@db.internal:5432/app");
    }

    #[test]
    fn redacts_bearer_tokens_in_free_text() {
        let text = "auth failed with Bearer ark_super_secret_12345 on upstream";
        assert_eq!(
            redact_text_for_ai(text),
            "auth failed with Bearer ****** on upstream"
        );
    }

    #[test]
    fn tool_definitions_filter_by_role() {
        let viewer = principal_with(OperatorRole::Viewer, vec![]);
        let viewer_tools: Vec<_> = tool_definitions_for_principal(&viewer)
            .into_iter()
            .filter_map(|t| t["function"]["name"].as_str().map(str::to_owned))
            .collect();
        assert!(viewer_tools.contains(&"list_components".to_string()));
        assert!(viewer_tools.contains(&"list_audit_events".to_string()));
        assert!(!viewer_tools.contains(&"validate_stream_config".to_string()));
        assert!(!viewer_tools.contains(&"emit_stream_config_proposal".to_string()));

        let operator = principal_with(OperatorRole::Operator, vec![]);
        let op_tools: Vec<_> = tool_definitions_for_principal(&operator)
            .into_iter()
            .filter_map(|t| t["function"]["name"].as_str().map(str::to_owned))
            .collect();
        assert!(op_tools.contains(&"validate_stream_config".to_string()));
        assert!(op_tools.contains(&"emit_job_spec_proposal".to_string()));
        assert!(!op_tools.contains(&"list_audit_events".to_string()));
    }

    #[tokio::test]
    async fn node_scoped_principal_cannot_inspect_other_nodes() {
        let hub = Hub::new(hub_config());
        for id in ["node-a", "node-b"] {
            hub.register(RegisterRequest {
                node_id: id.into(),
                node_token: String::new(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: None,
                data_address: None,
            })
            .await
            .unwrap();
        }
        let scoped = principal_with(
            OperatorRole::Operator,
            vec![ResourceScope {
                resource_type: "node".into(),
                resource_id: Some("node-a".into()),
            }],
        );
        let (list_res, _) = execute_tool(&hub, &scoped, "list_nodes", &json!({})).await;
        assert_eq!(list_res["total"], 1);
        assert_eq!(list_res["items"][0]["id"], "node-a");

        let (denied_cfg, _) = execute_tool(
            &hub,
            &scoped,
            "get_node_configuration",
            &json!({"node_id": "node-b"}),
        )
        .await;
        assert_eq!(denied_cfg["error"], "forbidden");
    }

    #[tokio::test]
    async fn proposal_emission_rejects_invalid_and_accepts_valid_configs() {
        let hub = Hub::new(hub_config());
        let admin = principal_with(OperatorRole::Admin, vec![]);

        let (bad_res, bad_prop) = execute_tool(
            &hub,
            &admin,
            "emit_stream_config_proposal",
            &json!({
                "title": "Bad",
                "summary": "Broken",
                "format": "yaml",
                "content": "streams:\n  - input:\n      type: nonexistent_input\n    pipeline:\n      processors: []\n    output:\n      type: drop\n"
            }),
        )
        .await;
        assert_eq!(bad_res["emitted"], false);
        assert!(bad_prop.is_none());

        let valid_yaml = r#"
streams:
  - input:
      type: generate
      context: '{"msg":"hi"}'
      interval: 1s
    pipeline:
      processors:
        - type: json_to_arrow
    output:
      type: stdout
"#;
        let (ok_res, ok_prop) = execute_tool(
            &hub,
            &admin,
            "emit_stream_config_proposal",
            &json!({
                "title": "Valid Stream",
                "summary": "Generate to stdout",
                "format": "yaml",
                "content": valid_yaml,
            }),
        )
        .await;
        assert_eq!(ok_res["emitted"], true, "{ok_res}");
        assert!(matches!(
            ok_prop,
            Some(AiProposal::StreamConfig {
                validated: true,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn viewer_cannot_emit_stream_or_job_proposals() {
        let hub = Hub::new(hub_config());
        let viewer = principal_with(OperatorRole::Viewer, vec![]);
        let (res, prop) = execute_tool(
            &hub,
            &viewer,
            "emit_stream_config_proposal",
            &json!({"title": "t", "summary": "s", "format": "yaml", "content": "streams: []"}),
        )
        .await;
        assert_eq!(res["error"], "forbidden");
        assert!(prop.is_none());
    }

    #[test]
    fn redacts_multiline_yaml_and_inline_urls() {
        let draft = "streams:\n  - input:\n      password: plain-secret\n      api_key: ${secret:KEEP_ME}\n      dsn: postgres://u:urlsecret@db.internal:5432/app\n";
        let masked = redact_config_or_text_for_ai(draft);
        assert!(!masked.contains("plain-secret"), "{masked}");
        assert!(!masked.contains("urlsecret"), "{masked}");
        assert!(masked.contains("${secret:KEEP_ME}"), "{masked}");

        let partial = "password: raw-pw\nurl: https://alice:s3cr3t@example.com/hook";
        let partial_masked = redact_config_or_text_for_ai(partial);
        assert!(!partial_masked.contains("raw-pw"), "{partial_masked}");
        assert!(!partial_masked.contains("s3cr3t"), "{partial_masked}");

        let prose = "failed connecting to https://alice:pw123@hub.local/api with Bearer tok123";
        let prose_masked = redact_text_for_ai(prose);
        assert!(!prose_masked.contains("pw123"), "{prose_masked}");
        assert!(!prose_masked.contains("tok123"), "{prose_masked}");
    }

    #[tokio::test]
    async fn node_scoped_viewer_cannot_list_audit_events_and_invalid_job_mode_fails() {
        let hub = Hub::new(hub_config());
        let scoped_viewer = principal_with(
            OperatorRole::Viewer,
            vec![ResourceScope {
                resource_type: "node".into(),
                resource_id: Some("node-a".into()),
            }],
        );
        let tools: Vec<_> = tool_definitions_for_principal(&scoped_viewer)
            .into_iter()
            .filter_map(|t| t["function"]["name"].as_str().map(str::to_owned))
            .collect();
        assert!(!tools.contains(&"list_audit_events".to_string()));

        let (res, _) = execute_tool(&hub, &scoped_viewer, "list_audit_events", &json!({})).await;
        assert_eq!(res["error"], "forbidden");

        let admin = principal_with(OperatorRole::Admin, vec![]);
        let (bad_mode, prop) = execute_tool(
            &hub,
            &admin,
            "emit_job_spec_proposal",
            &json!({
                "title": "Job",
                "summary": "Summary",
                "mode": "replace",
                "spec": {}
            }),
        )
        .await;
        assert_eq!(bad_mode["error"], "invalid_arguments");
        assert!(prop.is_none());
    }
}

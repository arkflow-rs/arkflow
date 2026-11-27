//! HTTP API and mock-LLM SSE integration tests for Hub AI Copilot.

use super::tests::{get_json, post_json};
use super::{hub, hub_router, router, ServerConfig};
use crate::api_contract::OperatorRole;
use crate::hub::ai::AiCopilotConfig;
use crate::hub::{HubConfig, HubHaConfig};
use crate::storage::{AuditFilter, ControlPlaneStore, StorageActor};
use arkflow_core::config::EngineConfig;
use arkflow_core::control_plane::ControlPlane;
use arkflow_core::runtime::RuntimeManager;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tower::ServiceExt;

fn test_hub_config(operator_token: &str) -> HubConfig {
    HubConfig {
        operator_token: Some(operator_token.into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: hub::default_session_ttl_ms(),
        approval_required: false,
        approval_ttl_ms: hub::default_approval_ttl_ms(),
    }
}

async fn spawn_mock_openai_sse<F>(handler: F) -> (String, Arc<AtomicUsize>)
where
    F: Fn(usize, String) -> (u16, String) + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_clone = Arc::clone(&hits);
    let handler = Arc::new(handler);

    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let turn = hits_clone.fetch_add(1, Ordering::SeqCst);
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                let mut content_length = 0usize;
                let mut header_end = None;
                loop {
                    let Ok(n) = socket.read(&mut tmp).await else {
                        return;
                    };
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if header_end.is_none() {
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            header_end = Some(pos + 4);
                            let headers = String::from_utf8_lossy(&buf[..pos]);
                            for line in headers.lines() {
                                if let Some((k, v)) = line.split_once(':') {
                                    if k.trim().eq_ignore_ascii_case("content-length") {
                                        content_length = v.trim().parse().unwrap_or(0);
                                    }
                                }
                            }
                        }
                    }
                    if let Some(end) = header_end {
                        if buf.len() >= end + content_length {
                            break;
                        }
                    }
                }
                let body = header_end
                    .map(|end| String::from_utf8_lossy(&buf[end..]).into_owned())
                    .unwrap_or_default();
                let (status, resp_body) = handler(turn, body);
                let content_type = if status == 200 {
                    "text/event-stream"
                } else {
                    "application/json"
                };
                let reply = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp_body}",
                    resp_body.len()
                );
                let _ = socket.write_all(reply.as_bytes()).await;
            });
        }
    });

    (format!("http://{addr}/v1"), hits)
}

fn sse_chunks(chunks: Vec<Value>) -> String {
    let mut out = String::new();
    for chunk in chunks {
        out.push_str("data: ");
        out.push_str(&chunk.to_string());
        out.push_str("\n\n");
    }
    out.push_str("data: [DONE]\n\n");
    out
}

fn parse_sse_events(raw: &str) -> Vec<(String, Value)> {
    let mut events = Vec::new();
    for frame in raw.split("\n\n") {
        let mut name = String::from("message");
        let mut data = String::new();
        for line in frame.lines() {
            if let Some(ev) = line.strip_prefix("event:") {
                name = ev.trim().to_string();
            } else if let Some(d) = line.strip_prefix("data:") {
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(d.trim());
            }
        }
        if !data.is_empty() {
            if let Ok(val) = serde_json::from_str::<Value>(&data) {
                events.push((name, val));
            }
        }
    }
    events
}

#[tokio::test]
async fn disabled_copilot_is_absent_from_system_and_rejects_chat() {
    let store = ControlPlaneStore::contract("ai_disabled").await;
    let storage = StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(test_hub_config("root|admin|admin-secret"), storage);
    let app = hub_router(hub, &ServerConfig::default());

    let (status, sys) = get_json(&app, "/api/v1/system", "admin-secret").await;
    assert_eq!(status, StatusCode::OK);
    let caps = sys["capabilities"].as_array().unwrap();
    assert!(!caps.iter().any(|c| c == "ai_copilot"));

    let (status, body) = post_json(
        &app,
        "/api/v1/ai/chat",
        "admin-secret",
        json!({"messages": [{"role": "user", "content": "hello"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], "ai_copilot_disabled");

    // Local standalone router never exposes /api/v1/ai/chat or ai_copilot.
    let local_engine_cfg: EngineConfig = serde_json::from_str(r#"{"streams":[]}"#).unwrap();
    let local_app = router(
        ControlPlane::new(local_engine_cfg, RuntimeManager::new()),
        &ServerConfig::default(),
    );
    let resp = local_app
        .clone()
        .oneshot(
            Request::post("/api/v1/ai/chat")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"messages": [{"role": "user", "content": "hi"}]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn enabled_hub_advertises_capability_without_leaking_credentials() {
    let store = ControlPlaneStore::contract("ai_enabled_sys").await;
    let storage = StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(test_hub_config("root|admin|admin-secret"), storage);
    let cfg = ServerConfig {
        ai: AiCopilotConfig {
            enabled: true,
            api_base: Some("https://secret-llm.internal/v1".into()),
            model: Some("gpt-4o-mini".into()),
            api_key: Some("sk-super-secret-value".into()),
            ..AiCopilotConfig::default()
        },
        ..ServerConfig::default()
    };
    let app = hub_router(hub, &cfg);

    let (status, sys) = get_json(&app, "/api/v1/system", "admin-secret").await;
    assert_eq!(status, StatusCode::OK);
    let caps = sys["capabilities"].as_array().unwrap();
    assert!(caps.iter().any(|c| c == "ai_copilot"));
    let raw = sys.to_string();
    assert!(!raw.contains("sk-super-secret-value"));
    assert!(!raw.contains("secret-llm.internal"));
}

#[tokio::test]
async fn startup_validation_rejects_invalid_enabled_ai_config() {
    let hub = hub::Hub::new(HubConfig {
        operator_token: None,
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: hub::default_session_ttl_ms(),
        approval_required: false,
        approval_ttl_ms: hub::default_approval_ttl_ms(),
    });
    let bad = ServerConfig {
        address: "127.0.0.1:0".into(),
        insecure_local: true,
        ai: AiCopilotConfig {
            enabled: true,
            api_base: Some("ftp://bad-scheme/v1".into()),
            model: Some("gpt-4o".into()),
            ..AiCopilotConfig::default()
        },
        ..ServerConfig::default()
    };
    let err = bad.validate_hub_startup(&hub).unwrap_err();
    assert!(err.to_string().contains("http://"), "{err}");
}

#[tokio::test]
async fn multi_round_tool_calling_validates_and_audits_without_prompt_leakage() {
    let seen_bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = Arc::clone(&seen_bodies);

    let (api_base, hits) = spawn_mock_openai_sse(move |turn, body| {
        seen_clone.lock().unwrap().push(body);
        match turn {
            0 => (
                200,
                sse_chunks(vec![json!({
                    "choices": [{
                        "delta": {
                            "tool_calls": [{
                                "index": 0,
                                "id": "call_bad",
                                "type": "function",
                                "function": {
                                    "name": "emit_stream_config_proposal",
                                    "arguments": serde_json::json!({
                                        "title": "Broken",
                                        "summary": "Bad component",
                                        "format": "yaml",
                                        "content": "streams:\n  - input:\n      type: bogus_input\n    pipeline:\n      processors: []\n    output:\n      type: drop\n"
                                    }).to_string()
                                }
                            }]
                        }
                    }]
                })]),
            ),
            1 => (
                200,
                sse_chunks(vec![json!({
                    "choices": [{
                        "delta": {
                            "tool_calls": [{
                                "index": 0,
                                "id": "call_good",
                                "type": "function",
                                "function": {
                                    "name": "emit_stream_config_proposal",
                                    "arguments": serde_json::json!({
                                        "title": "Valid Stream",
                                        "summary": "Generate to stdout",
                                        "format": "yaml",
                                        "content": "streams:\n  - input:\n      type: generate\n      context: '{\"x\":1}'\n      interval: 1s\n    pipeline:\n      processors:\n        - type: json_to_arrow\n    output:\n      type: stdout\n"
                                    }).to_string()
                                }
                            }]
                        }
                    }]
                })]),
            ),
            _ => (
                200,
                sse_chunks(vec![
                    json!({"choices": [{"delta": {"content": "Stream validated and ready."}}]}),
                    json!({"usage": {"prompt_tokens": 111, "completion_tokens": 42}}),
                ]),
            ),
        }
    })
    .await;

    let store = ControlPlaneStore::contract("ai_tool_loop").await;
    let storage = StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(test_hub_config("root|admin|admin-secret"), storage);
    let cfg = ServerConfig {
        ai: AiCopilotConfig {
            enabled: true,
            api_base: Some(api_base),
            model: Some("mock-gpt".into()),
            api_key: Some("sk-mock".into()),
            ..AiCopilotConfig::default()
        },
        ..ServerConfig::default()
    };
    let app = hub_router(hub.clone(), &cfg);

    let secret_prompt = "UNIQUE_PROMPT_TEXT_MUST_NOT_BE_AUDITED";
    let resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/ai/chat")
                .header("authorization", "Bearer admin-secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"messages": [{"role": "user", "content": secret_prompt}]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let raw_sse = String::from_utf8_lossy(&bytes);
    let events = parse_sse_events(&raw_sse);

    assert_eq!(hits.load(Ordering::SeqCst), 3);
    // Only ONE proposal event was emitted (the invalid one was rejected before emission).
    let proposals: Vec<_> = events.iter().filter(|(n, _)| n == "proposal").collect();
    assert_eq!(proposals.len(), 1, "{events:?}");
    assert_eq!(proposals[0].1["validated"], true);
    assert_eq!(proposals[0].1["title"], "Valid Stream");

    let deltas: String = events
        .iter()
        .filter(|(n, _)| n == "delta")
        .filter_map(|(_, v)| v["text"].as_str())
        .collect();
    assert_eq!(deltas, "Stream validated and ready.");
    assert!(events.iter().any(|(n, _)| n == "done"));

    // Verify metadata-only audit event (no prompt or completion text persisted).
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let audits = loop {
        let rows = hub
            .audit_filtered(AuditFilter {
                action: Some("ai.copilot.chat".into()),
                ..AuditFilter::default()
            })
            .await
            .unwrap();
        if !rows.is_empty() || tokio::time::Instant::now() >= deadline {
            break rows;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].actor.as_deref(), Some("root"));
    assert_eq!(audits[0].outcome, "succeeded");
    let msg = audits[0].message.as_deref().unwrap();
    assert!(!msg.contains(secret_prompt));
    assert!(!msg.contains("Stream validated and ready."));
    assert!(msg.contains("emit_stream_config_proposal"));
}

#[tokio::test]
async fn rate_limit_standby_and_unsupported_tool_calling_errors() {
    let (api_base, hits) = spawn_mock_openai_sse(|_turn, _body| {
        (
            400,
            json!({"error": {"message": "This model does not support tool_choice or tools"}})
                .to_string(),
        )
    })
    .await;

    let store = ControlPlaneStore::contract("ai_errors").await;
    let storage = StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(test_hub_config("root|admin|admin-secret"), storage);
    let cfg = ServerConfig {
        ai: AiCopilotConfig {
            enabled: true,
            api_base: Some(api_base.clone()),
            model: Some("no-tools-model".into()),
            burst: 1,
            requests_per_minute: 1,
            ..AiCopilotConfig::default()
        },
        ..ServerConfig::default()
    };
    let app = hub_router(hub, &cfg);

    // Unauthenticated -> 401
    let (status, _) = post_json(
        &app,
        "/api/v1/ai/chat",
        "wrong-secret",
        json!({"messages": [{"role": "user", "content": "hi"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // First request hits upstream and receives ai_tool_calling_unsupported via SSE error
    let resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/ai/chat")
                .header("authorization", "Bearer admin-secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"messages": [{"role": "user", "content": "hi"}]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let raw = String::from_utf8_lossy(
        &axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .into_owned();
    let events = parse_sse_events(&raw);
    assert!(
        events
            .iter()
            .any(|(n, v)| n == "error" && v["code"] == "ai_tool_calling_unsupported"),
        "{events:?}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    // Second immediate request exceeds AI burst=1 -> 429 + Retry-After before hitting LLM
    let resp2 = app
        .clone()
        .oneshot(
            Request::post("/api/v1/ai/chat")
                .header("authorization", "Bearer admin-secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"messages": [{"role": "user", "content": "hi again"}]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(resp2.headers().get("retry-after").is_some());
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    // Standby Hub rejects /api/v1/ai/chat with 503 hub_standby
    let standby = hub::Hub::new(test_hub_config("root|admin|admin-secret")).with_ha(HubHaConfig {
        enabled: true,
        ..HubHaConfig::default()
    });
    standby.enter_election().await;
    let standby_app = hub_router(standby, &cfg);
    let (status, body) = post_json(
        &standby_app,
        "/api/v1/ai/chat",
        "admin-secret",
        json!({"messages": [{"role": "user", "content": "hi"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["code"], "hub_standby");
}

#[tokio::test]
async fn tool_round_limit_terminates_cleanly() {
    let (api_base, _hits) = spawn_mock_openai_sse(|turn, _body| {
        (
            200,
            sse_chunks(vec![json!({
                "choices": [{
                    "delta": {
                        "tool_calls": [{
                            "index": 0,
                            "id": format!("c_{turn}"),
                            "type": "function",
                            "function": {
                                "name": "list_components",
                                "arguments": "{}"
                            }
                        }]
                    }
                }]
            })]),
        )
    })
    .await;

    let mut cfg_hub = test_hub_config("spectator|viewer|viewer-secret");
    cfg_hub.operator_token = Some("spectator|viewer|viewer-secret".into());
    let hub = hub::Hub::new(cfg_hub);
    let cfg = ServerConfig {
        ai: AiCopilotConfig {
            enabled: true,
            api_base: Some(api_base),
            model: Some("mock".into()),
            max_tool_rounds: 2,
            ..AiCopilotConfig::default()
        },
        ..ServerConfig::default()
    };
    let app = hub_router(hub, &cfg);

    let resp = app
        .oneshot(
            Request::post("/api/v1/ai/chat")
                .header("authorization", "Bearer viewer-secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"messages": [{"role": "user", "content": "loop"}]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let raw = String::from_utf8_lossy(
        &axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .into_owned();
    let events = parse_sse_events(&raw);
    assert!(
        events
            .iter()
            .any(|(n, v)| n == "error" && v["code"] == "ai_tool_round_limit_exceeded"),
        "{events:?}"
    );
    let _ = OperatorRole::Viewer;
}

#[tokio::test]
async fn incomplete_stream_fails_and_unknown_tool_is_masked_in_audit() {
    // Turn 0 emits an unknown provider tool name; Turn 1 ends abruptly without [DONE]/finish_reason.
    let (api_base, _hits) = spawn_mock_openai_sse(|turn, _body| {
        if turn == 0 {
            (
                200,
                sse_chunks(vec![json!({
                    "choices": [{
                        "finish_reason": "tool_calls",
                        "delta": {
                            "tool_calls": [{
                                "index": 0,
                                "id": "c_unknown",
                                "type": "function",
                                "function": {
                                    "name": "leaked_completion_tool_name",
                                    "arguments": "{}"
                                }
                            }]
                        }
                    }]
                })]),
            )
        } else {
            (
                200,
                format!(
                    "data: {}\n\n",
                    json!({"choices": [{"delta": {"content": "truncated"}}]})
                ),
            )
        }
    })
    .await;

    let store = ControlPlaneStore::contract("ai_incomplete_audit").await;
    let actor = StorageActor::start(store, 8);
    let mut cfg_hub = test_hub_config("root|admin|admin-secret");
    cfg_hub.operator_token = Some("root|admin|admin-secret".into());
    let hub = hub::Hub::with_storage(cfg_hub, actor);
    let cfg = ServerConfig {
        ai: AiCopilotConfig {
            enabled: true,
            api_base: Some(api_base),
            model: Some("mock".into()),
            ..AiCopilotConfig::default()
        },
        ..ServerConfig::default()
    };
    let app = hub_router(hub.clone(), &cfg);

    let resp = app
        .oneshot(
            Request::post("/api/v1/ai/chat")
                .header("authorization", "Bearer admin-secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"messages": [{"role": "user", "content": "hi"}]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let raw = String::from_utf8_lossy(
        &axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .into_owned();
    let events = parse_sse_events(&raw);
    assert!(
        events
            .iter()
            .any(|(n, v)| n == "error" && v["code"] == "ai_provider_error"),
        "{events:?}"
    );
    assert!(!events.iter().any(|(n, _)| n == "done"), "{events:?}");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let audits = loop {
        let rows = hub
            .audit_filtered(AuditFilter {
                action: Some("ai.copilot.chat".into()),
                ..AuditFilter::default()
            })
            .await
            .unwrap();
        if !rows.is_empty() || tokio::time::Instant::now() >= deadline {
            break rows;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].outcome, "failed");
    let msg = audits[0].message.as_deref().unwrap();
    assert!(!msg.contains("leaked_completion_tool_name"), "{msg}");
    assert!(msg.contains("<unknown_tool>"), "{msg}");
}

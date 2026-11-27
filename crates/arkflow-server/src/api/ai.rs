//! HTTP SSE endpoint for the Hub AI Copilot (`POST /api/v1/ai/chat`).

use super::{bearer, operator_denied, problem};
use crate::api_contract::OperatorAction;
use crate::hub;
use crate::hub::ai::{
    run_copilot_session, validate_chat_request, AiChatRequest, AiCopilotService, AiStreamEvent,
};
use crate::storage::AuditRecord;
use axum::extract::{Extension, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures_util::stream::Stream;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct CancelOnDropStream {
    rx: mpsc::Receiver<AiStreamEvent>,
    cancel: CancellationToken,
}

impl Drop for CancelOnDropStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Stream for CancelOnDropStream {
    type Item = Result<SseEvent, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.rx).poll_recv(cx) {
            Poll::Ready(Some(event)) => Poll::Ready(Some(Ok(to_sse_event(&event)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn to_sse_event(event: &AiStreamEvent) -> SseEvent {
    let (name, payload) = match event {
        AiStreamEvent::ToolStart { .. } => ("tool_start", serde_json::to_value(event)),
        AiStreamEvent::ToolResult { .. } => ("tool_result", serde_json::to_value(event)),
        AiStreamEvent::Delta { .. } => ("delta", serde_json::to_value(event)),
        AiStreamEvent::Proposal(_) => ("proposal", serde_json::to_value(event)),
        AiStreamEvent::Done { .. } => ("done", serde_json::to_value(event)),
        AiStreamEvent::Error { .. } => ("error", serde_json::to_value(event)),
    };
    let data = payload
        .ok()
        .and_then(|v| v.get("data").cloned())
        .unwrap_or(serde_json::Value::Null);
    SseEvent::default()
        .event(name)
        .json_data(data)
        .unwrap_or_else(|_| SseEvent::default().event("error").data("{}"))
}

pub(super) async fn hub_ai_chat(
    State(hub): State<hub::Hub>,
    Extension(ai): Extension<Arc<AiCopilotService>>,
    headers: HeaderMap,
    Json(request): Json<AiChatRequest>,
) -> Response {
    if !ai.config().enabled {
        return problem(
            StatusCode::NOT_FOUND,
            "ai_copilot_disabled",
            "AI Copilot is not enabled on this Hub".into(),
        );
    }

    let supplied = bearer(&headers);
    let principal_opt = hub.operator_principal(supplied.as_deref()).await;
    let Some(principal) = principal_opt
        .as_ref()
        .filter(|p| p.can(OperatorAction::Read))
        .cloned()
    else {
        let authenticated = principal_opt.is_some();
        let correlation_id = headers
            .get("x-correlation-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        hub.record_audit_best_effort(
            "auth_denial",
            AuditRecord {
                event_id: 0,
                actor: principal_opt.map(|p| p.id),
                action: "operator.read".into(),
                resource_type: "ai_copilot".into(),
                resource_id: Some("session".into()),
                node_id: None,
                stream_id: None,
                correlation_id,
                outcome: "rejected".into(),
                failure_code: Some(if authenticated {
                    "forbidden".into()
                } else {
                    "unauthorized".into()
                }),
                message: None,
                occurred_at_ms: hub::now_ms_for_metrics(),
            },
        )
        .await;
        return operator_denied(authenticated, OperatorAction::Read);
    };

    if let Err(retry_after) = ai.check_rate_limit(&principal.id) {
        let mut resp = problem(
            StatusCode::TOO_MANY_REQUESTS,
            "ai_rate_limit_exceeded",
            format!("AI Copilot rate limit exceeded; retry after {retry_after}s"),
        );
        if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
            resp.headers_mut().insert(header::RETRY_AFTER, value);
        }
        return resp;
    }

    if let Err(message) = validate_chat_request(&request) {
        return problem(StatusCode::BAD_REQUEST, "invalid_ai_request", message);
    }

    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let node_id = request.context.as_ref().and_then(|c| c.node_id.clone());
    let stream_id = request.context.as_ref().and_then(|c| c.stream_id.clone());
    let resource_id = request
        .context
        .as_ref()
        .and_then(|c| c.job_id.clone().or_else(|| c.node_id.clone()))
        .or_else(|| Some("session".to_string()));

    let (tx, rx) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let task_hub = hub.clone();
    let task_ai = Arc::clone(&ai);
    let actor_id = principal.id.clone();

    tokio::spawn(async move {
        let summary =
            run_copilot_session(&task_ai, &task_hub, &principal, request, tx, task_cancel).await;

        let metadata = serde_json::json!({
            "model": summary.model,
            "tool_calls": summary.tool_calls,
            "prompt_tokens": summary.usage.as_ref().map(|u| u.prompt_tokens),
            "completion_tokens": summary.usage.as_ref().map(|u| u.completion_tokens),
            "duration_ms": summary.duration_ms,
        })
        .to_string();

        task_hub
            .record_audit_best_effort(
                "ai_copilot",
                AuditRecord {
                    event_id: 0,
                    actor: Some(actor_id),
                    action: "ai.copilot.chat".into(),
                    resource_type: "ai_copilot".into(),
                    resource_id,
                    node_id,
                    stream_id,
                    correlation_id,
                    outcome: summary.outcome.into(),
                    failure_code: summary.failure_code,
                    message: Some(metadata),
                    occurred_at_ms: hub::now_ms_for_metrics(),
                },
            )
            .await;
    });

    Sse::new(CancelOnDropStream { rx, cancel })
        .keep_alive(KeepAlive::default())
        .into_response()
}

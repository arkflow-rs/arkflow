//! Agent-facing Hub endpoints: register, heartbeat, report, job observations,
//! command poll, and command-result delivery.
use super::{hub_problem, problem};
use crate::hub;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;

pub(super) async fn agent_register(
    State(hub): State<hub::Hub>,
    Json(request): Json<hub::RegisterRequest>,
) -> Response {
    match hub.register(request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => hub_problem(error),
    }
}
pub(super) async fn agent_heartbeat(
    State(hub): State<hub::Hub>,
    Json(request): Json<hub::HeartbeatRequest>,
) -> Response {
    match hub.heartbeat(request).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => hub_problem(error),
    }
}
pub(super) async fn agent_report(
    State(hub): State<hub::Hub>,
    Json(request): Json<hub::NodeReport>,
) -> Response {
    match hub.report(request).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => hub_problem(error),
    }
}
pub(super) async fn agent_job_observation(
    State(hub): State<hub::Hub>,
    Json(request): Json<hub::JobObservationRequest>,
) -> Response {
    match hub.report_job_observation(request).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => hub_problem(error),
    }
}
/// Session tokens embedded in URL query strings leak into reverse-proxy and
/// access logs. Prefer the `Authorization: Bearer` header; the query param
/// remains accepted for older Agents.
/// Resolve the Agent session credential from its two accepted transports. The
/// header wins so a client that sends both is never authenticated by the stale
/// query value; the query parameter is the deprecated fallback that keeps older
/// Agents working through the transition window.
pub(super) fn agent_session_token(
    headers: &HeaderMap,
    query_token: Option<String>,
) -> Option<String> {
    bearer_session_token(headers).or(query_token)
}

pub(super) fn bearer_session_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

/// Query parameters for the agent command endpoints. The session token is
/// optional here: it travels in the `Authorization: Bearer` header; the query
/// field remains accepted for older Agents.
#[derive(serde::Deserialize)]
pub(super) struct AgentCommandsQuery {
    node_id: String,
    session_token: Option<String>,
}

pub(super) async fn agent_commands(
    State(hub): State<hub::Hub>,
    headers: HeaderMap,
    Query(query): Query<AgentCommandsQuery>,
) -> Response {
    if query.session_token.is_some() {
        // The query parameter leaks the live session credential into
        // tracing/proxy logs (the trace layer records the full URI).
        tracing::warn!(
            node_id = %query.node_id,
            "agent session token supplied via query parameter; this is deprecated and will be removed - send it in the Authorization: Bearer header"
        );
    }
    let Some(session_token) = agent_session_token(&headers, query.session_token) else {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing session token".into(),
        );
    };
    match hub
        .commands(hub::AgentAuth {
            node_id: query.node_id,
            session_token,
        })
        .await
    {
        Ok(commands) => Json(commands).into_response(),
        Err(error) => hub_problem(error),
    }
}
pub(super) async fn agent_command_result(
    State(hub): State<hub::Hub>,
    Path(_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<AgentCommandsQuery>,
    Json(result): Json<hub::CommandResult>,
) -> Response {
    if query.session_token.is_some() {
        tracing::warn!(
            node_id = %query.node_id,
            "agent session token supplied via query parameter; this is deprecated and will be removed - send it in the Authorization: Bearer header"
        );
    }
    let Some(session_token) = agent_session_token(&headers, query.session_token) else {
        return problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing session token".into(),
        );
    };
    match hub
        .command_result(
            hub::AgentAuth {
                node_id: query.node_id,
                session_token,
            },
            result,
        )
        .await
    {
        Ok(operation) => Json(operation).into_response(),
        Err(error) => hub_problem(error),
    }
}

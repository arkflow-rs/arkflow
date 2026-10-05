//! Health/readiness/liveness probes, system and fleet status, component
//! catalog, schema, metrics exposition, and the OIDC login flow.
use super::{bearer, hub_problem, page_items, problem, PageQuery};
use crate::hub;
use crate::oidc;
use arkflow_core::component::{self, ComponentKind};
use arkflow_core::control::Page;
use arkflow_core::control_plane::ControlPlane;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use subtle::ConstantTimeEq;

#[derive(Deserialize)]
pub(super) struct MetricsQuery {
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    format: Option<String>,
}

pub(super) async fn hub_metrics(
    State(hub): State<hub::Hub>,
    Query(query): Query<MetricsQuery>,
    headers: HeaderMap,
) -> Response {
    // Console clients ask for the JSON aggregate explicitly; anything else
    // (including every Prometheus scraper) keeps the text exposition.
    let wants_json = query.format.as_deref() == Some("json")
        || headers
            .get(axum::http::header::ACCEPT)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|accept| {
                accept
                    .split(',')
                    .any(|part| part.trim().starts_with("application/json"))
            });
    if wants_json {
        let items = hub.metrics_by_node(query.node_id.as_deref()).await;
        let aggregate = hub.metrics(query.node_id.as_deref()).await;
        return Json(serde_json::json!({"items": items, "aggregate": aggregate})).into_response();
    }
    let status = hub.operational_status().await;
    let mut body = String::new();
    if let Ok(status) = status {
        body.push_str(&format!(
            "arkflow_control_plane_ready {}\n",
            status.ready as u8
        ));
        body.push_str(&format!(
            "arkflow_reconciliation_runs_total {}\narkflow_reconciliation_failures_total {}\n",
            status.reconciliation.runs_total, status.reconciliation.failures_total
        ));
        body.push_str(&format!("arkflow_outbox_pending {}\narkflow_outbox_claimed {}\narkflow_stale_nodes {}\narkflow_active_attempts {}\narkflow_non_terminal_intents {}\n", status.outbox_pending, status.outbox_claimed, status.stale_nodes, status.active_attempts, status.non_terminal_intents));
        for (state, count) in status.node_states {
            body.push_str(&format!(
                "arkflow_nodes_state{{state=\"{state}\"}} {count}\n"
            ));
        }
        for (state, count) in status.maintenance_states {
            body.push_str(&format!(
                "arkflow_nodes_maintenance_state{{state=\"{state}\"}} {count}\n"
            ));
        }
        for (state, count) in status.intent_states {
            body.push_str(&format!(
                "arkflow_intents_state{{state=\"{state}\"}} {count}\n"
            ));
        }
        for (state, count) in status.attempt_states {
            body.push_str(&format!(
                "arkflow_attempts_state{{state=\"{state}\"}} {count}\n"
            ));
        }
    }
    if let Ok(rollouts) = hub.rollouts().await {
        let mut states = std::collections::BTreeMap::<String, usize>::new();
        for rollout in rollouts {
            *states.entry(rollout.state).or_default() += 1;
        }
        for (state, count) in states {
            body.push_str(&format!(
                "arkflow_rollouts_state{{state=\"{state}\"}} {count}\n"
            ));
        }
    }
    for node in hub.nodes().await {
        let node_id = prometheus_label(&node.id);
        let protocol = prometheus_label(&node.protocol_version);
        let software = prometheus_label(&node.version);
        let state = prometheus_label(&format!("{:?}", node.state).to_lowercase());
        body.push_str(&format!(
            "arkflow_node_compatibility{{node_id=\"{node_id}\",protocol_version=\"{protocol}\",software_version=\"{software}\",state=\"{state}\"}} 1\n"
        ));
        for capability in node.capabilities {
            body.push_str(&format!(
                "arkflow_node_capability{{node_id=\"{node_id}\",capability=\"{}\"}} 1\n",
                prometheus_label(&capability)
            ));
        }
    }
    for node in hub.metrics_by_node(query.node_id.as_deref()).await {
        let node_id = prometheus_label(&node.node_id);
        for (name, value) in node.metrics {
            body.push_str(&format!(
                "arkflow_node_metric{{node_id=\"{node_id}\",metric=\"{}\"}} {value}\n",
                prometheus_label(&name)
            ));
        }
    }
    body.push_str(&hub.command_metrics().render());
    body.push_str(&crate::metrics::encode_families(
        crate::metrics::storage_families(),
    ));
    // Data-plane series reported by Agents: the kernel Job vocabulary with an
    // extra `node` label. Expired-lease nodes are excluded by the Hub.
    for (node_id, jobs) in hub.job_metrics().await {
        for (job_id, snapshot) in &jobs {
            body.push_str(&crate::metrics::encode_families(
                crate::metrics::kernel_job_families(job_id, snapshot, &[("node", node_id.clone())]),
            ));
        }
    }
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
}

pub(super) async fn hub_operational_status(
    State(hub): State<hub::Hub>,
    _headers: HeaderMap,
) -> Response {
    match hub.operational_status().await {
        Ok(status) => {
            let mut value = serde_json::to_value(status).unwrap_or_default();
            if let Some(object) = value.as_object_mut() {
                let rollouts = hub.rollouts().await.unwrap_or_default();
                object.insert(
                    "rollouts".into(),
                    serde_json::json!({
                        "total": rollouts.len(),
                        "states": rollouts.iter().fold(std::collections::BTreeMap::<String, usize>::new(), |mut states, rollout| { *states.entry(rollout.state.clone()).or_default() += 1; states }),
                    }),
                );
                object.insert(
                    "compatibility".into(),
                    serde_json::json!({
                        "nodes": hub.nodes().await.into_iter().map(|node| serde_json::json!({ "id": node.id, "protocol_version": node.protocol_version, "software_version": node.version, "capabilities": node.capabilities })).collect::<Vec<_>>()
                    }),
                );
            }
            Json(value).into_response()
        }
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_health(State(hub): State<hub::Hub>) -> Response {
    let status = hub.operational_status().await.ok();
    let body = Json(
        serde_json::json!({"status": status.as_ref().map(|s| s.status.as_str()).unwrap_or("degraded"), "nodes":hub.nodes().await.len(), "ready": status.as_ref().is_some_and(|s| s.ready)}),
    );
    if status.is_some_and(|s| s.ready) {
        (StatusCode::OK, body).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, body).into_response()
    }
}
pub(super) async fn hub_readiness(State(hub): State<hub::Hub>) -> Response {
    let leadership = hub.leadership().await;
    let ha = serde_json::json!({
        "enabled": hub.ha_config().enabled,
        "role": leadership.role(),
        "epoch": leadership.epoch(),
    });
    if !leadership.is_leader() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "status": "not_ready",
                "ready": false,
                "reason": "standby",
                "ha": ha,
            })),
        )
            .into_response();
    }
    match hub.operational_status().await {
        Ok(status) if status.ready => (
            StatusCode::OK,
            Json(serde_json::json!({"status":"ready","ready":true,"ha":ha})),
        )
            .into_response(),
        Ok(_status) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status":"not_ready","ready":false,"reason":"startup_recovery","ha":ha})),
        )
            .into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status":"not_ready","ready":false,"reason":"storage_unavailable","ha":ha})),
        )
            .into_response(),
    }
}
pub(super) async fn hub_liveness() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status":"alive","alive":true}))
}

pub(crate) async fn hub_oidc_status(State(hub): State<hub::Hub>, headers: HeaderMap) -> Response {
    let login_enabled = hub.oidc_login().is_some();
    let principal = cookie_value(&headers, "arkflow_session")
        .and_then(|session_id| hub.oidc_session_principal(&session_id));
    let body = serde_json::json!({
        "login_enabled": login_enabled,
        "authenticated": principal.is_some(),
        "principal": principal.map(|principal| serde_json::json!({
            "id": principal.id,
            "roles": principal.roles.iter().map(|role| format!("{role:?}").to_lowercase()).collect::<Vec<_>>(),
        })),
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

pub(crate) async fn hub_oidc_login(State(hub): State<hub::Hub>) -> Response {
    let Some(federation) = hub.oidc_login() else {
        return problem(
            StatusCode::NOT_FOUND,
            "not_found",
            "OIDC login is not enabled".into(),
        );
    };
    fn random_hex() -> Option<String> {
        use rand::TryRngCore;
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.try_fill_bytes(&mut bytes).ok()?;
        Some(bytes.iter().map(|b| format!("{b:02x}")).collect())
    }
    let Some(state) = random_hex() else {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "oidc_random_unavailable",
            "OS random source is unavailable; login cannot start".into(),
        );
    };
    // PKCE S256 verifier + OIDC nonce, round-tripped inside the state cookie
    // (state.verifier.nonce — hex segments, '.'-separated).
    let Some(verifier) = random_hex() else {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "oidc_random_unavailable",
            "OS random source is unavailable; login cannot start".into(),
        );
    };
    let Some(nonce) = random_hex() else {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "oidc_random_unavailable",
            "OS random source is unavailable; login cannot start".into(),
        );
    };
    use sha2::{Digest, Sha256};
    let challenge: String = {
        let digest = Sha256::digest(verifier.as_bytes());
        digest.iter().map(|b| format!("{b:02x}")).collect()
    };
    let location = federation.authorization_redirect(&state, &challenge, &nonce);
    let cookie_value = format!("{state}.{verifier}.{nonce}");
    Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, location)
        .header(
            header::SET_COOKIE,
            format!(
                "arkflow_oidc_state={cookie_value}; HttpOnly; SameSite=Lax; Path=/; Max-Age=600{}",
                if federation.login_is_secure() {
                    "; Secure"
                } else {
                    ""
                }
            ),
        )
        .body(axum::body::Body::empty())
        .unwrap()
}

pub(crate) async fn hub_oidc_callback(
    State(hub): State<hub::Hub>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let Some(federation) = hub.oidc_login() else {
        return problem(
            StatusCode::NOT_FOUND,
            "not_found",
            "OIDC login is not enabled".into(),
        );
    };
    let unauthorized = || {
        problem(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "OIDC login failed".into(),
        )
    };
    let Some(expected_state) = cookie_value(&headers, "arkflow_oidc_state") else {
        return unauthorized();
    };
    let Some(supplied_state) = params.get("state") else {
        return unauthorized();
    };
    // The state cookie packs `state.verifier.nonce` (PKCE S256 + OIDC nonce
    // round-trip).
    let mut cookie_parts = expected_state.split('.');
    let (expected_state, code_verifier, expected_nonce) = (
        cookie_parts.next(),
        cookie_parts.next(),
        cookie_parts.next(),
    );
    let (Some(expected_state), Some(code_verifier), Some(expected_nonce)) =
        (expected_state, code_verifier, expected_nonce)
    else {
        return unauthorized();
    };
    if !bool::from(expected_state.as_bytes().ct_eq(supplied_state.as_bytes())) {
        return unauthorized();
    }
    let Some(code) = params.get("code") else {
        return unauthorized();
    };
    let Some(id_token) = federation.exchange_code(code, code_verifier).await else {
        return unauthorized();
    };
    // Nonce pre-check (raw claim read); signature/iss/aud validation happens
    // in `authenticate` below.
    if oidc::OidcFederation::nonce_claim(&id_token)
        .is_none_or(|claim| !bool::from(claim.as_bytes().ct_eq(expected_nonce.as_bytes())))
    {
        return unauthorized();
    }
    let Some(principal) = federation.authenticate(&id_token).await else {
        return unauthorized();
    };
    let Some(session_id) = federation.create_session(principal) else {
        return problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "session_create_failed",
            "OS random source is unavailable; session cannot be created".into(),
        );
    };
    let max_age = oidc::session_ttl_secs();
    Response::builder()
        .status(StatusCode::SEE_OTHER)
        .header(header::LOCATION, "/")
        .header(
            header::SET_COOKIE,
            format!(
                "arkflow_session={session_id}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age}{}",
                if federation.login_is_secure() {
                    "; Secure"
                } else {
                    ""
                }
            ),
        )
        .body(axum::body::Body::empty())
        .unwrap()
}

pub(crate) async fn hub_oidc_logout(State(hub): State<hub::Hub>, headers: HeaderMap) -> Response {
    if let Some(token) = bearer(&headers) {
        if let Some(session_id) = token.strip_prefix("session:") {
            if let Some(federation) = hub.oidc_login() {
                federation.remove_session(session_id);
            }
        }
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::SET_COOKIE,
            "arkflow_session=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0",
        )
        .body(axum::body::Body::empty())
        .unwrap()
}

pub(super) fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let cookies = headers.get(header::COOKIE)?.to_str().ok()?;
    cookies.split(';').find_map(|pair| {
        let pair = pair.trim();
        pair.strip_prefix(&format!("{name}=")).map(str::to_string)
    })
}

pub(super) fn prometheus_label(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || "._-".contains(*character))
        .take(64)
        .collect()
}

pub(super) async fn system(
    State(cp): State<ControlPlane>,
) -> Json<arkflow_core::control::SystemResource> {
    Json(cp.system().await)
}
pub(super) async fn status(
    State(cp): State<ControlPlane>,
) -> Json<arkflow_core::control::EngineStatus> {
    Json(cp.status().await)
}
pub(super) async fn node(
    State(cp): State<ControlPlane>,
) -> Json<arkflow_core::control::NodeResource> {
    Json(cp.node().await)
}

pub(super) async fn nodes(
    State(cp): State<ControlPlane>,
    Query(query): Query<PageQuery>,
) -> Json<Page<arkflow_core::control::NodeResource>> {
    Json(page_items(vec![cp.node().await], &query))
}

pub(super) async fn components() -> Json<Vec<serde_json::Value>> {
    Json(component::list_components().into_iter().map(|(kind, item)| serde_json::json!({"kind": kind, "name": item.name, "description": item.description, "schema": item.config_schema, "example": item.config_example})).collect())
}

pub(super) async fn component(Path((kind, name)): Path<(String, String)>) -> Response {
    let kind = match kind.parse::<ComponentKind>() {
        Ok(value) => value,
        Err(_) => {
            return problem(
                StatusCode::NOT_FOUND,
                "component_not_found",
                "Unknown component kind".into(),
            )
        }
    };
    match component::get_component_metadata(kind, &name) { Some(item) => Json(serde_json::json!({"kind": kind, "name": item.name, "description": item.description, "schema": item.config_schema, "example": item.config_example})).into_response(), None => problem(StatusCode::NOT_FOUND, "component_not_found", format!("Unknown component: {kind}/{name}")) }
}

pub(super) async fn schema() -> Json<serde_json::Value> {
    Json(component::build_config_schema())
}

pub(super) async fn metrics(State(cp): State<ControlPlane>) -> Response {
    let runtime = cp.runtime_manager();
    let mut streams: Vec<(String, arkflow_core::control::StreamMetricsSnapshot)> = runtime
        .snapshots()
        .await
        .into_iter()
        .map(|status| (status.id, status.metrics))
        .collect();
    streams.sort_by(|left, right| left.0.cmp(&right.0));
    let jobs = runtime.job_metrics().snapshots();
    let body = crate::metrics::data_plane_exposition(&streams, &jobs);
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
}

pub(super) async fn health(State(cp): State<ControlPlane>) -> Response {
    let healthy = cp.health().is_running();
    let body = serde_json::json!({"status": if healthy {"healthy"} else {"unhealthy"}, "running": healthy});
    (
        if healthy {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(body),
    )
        .into_response()
}
pub(super) async fn readiness(State(cp): State<ControlPlane>) -> Response {
    ready(State(cp)).await
}
pub(super) async fn liveness() -> Json<serde_json::Value> {
    live().await
}

/// Readiness on the observability surface: success once the engine runtime
/// has finished starting the configured Streams and Jobs (in local mode this
/// is the same health state the control plane reports via `/readiness`).
pub(super) async fn ready(State(cp): State<ControlPlane>) -> Response {
    let ready = cp.health().is_ready();
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(serde_json::json!({"status": if ready {"ready"} else {"not_ready"}, "ready": ready})),
    )
        .into_response()
}
pub(super) async fn live() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status":"alive", "alive":true}))
}

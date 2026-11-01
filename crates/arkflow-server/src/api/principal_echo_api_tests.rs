//! HTTP-surface tests for the `/system` principal echo: the wire contract
//! of `principal { id, roles, actions }`, per-role action fidelity, and
//! rejection of unauthenticated callers (see the `control-plane-api` delta
//! and the `console-rbac-and-pagination` change).

use super::tests::get_json;
use super::{hub, hub_router, router, ServerConfig};
use arkflow_core::config::{ControlApiConfig, EngineConfig, NodeConfig};
use arkflow_core::engine::Engine;

fn echo_config(operator_token: &str) -> hub::HubConfig {
    hub::HubConfig {
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

/// The actions list on the wire is the full v1 contract filtered by the
/// enforcing predicate, pinned per role so a new action cannot silently
/// miss the echo.
#[tokio::test]
async fn hub_system_echoes_role_actions_pinned_per_role() {
    let cases = [
        (
            "root|admin|admin-secret",
            "admin-secret",
            "root",
            serde_json::json!(["admin"]),
            serde_json::json!([
                "read",
                "operate",
                "configure",
                "manage_nodes",
                "manage_rollouts",
                "manage_webhooks",
                "approve_rollouts",
                "read_audit",
                "manage_users",
            ]),
        ),
        (
            "ops|operator|operator-secret",
            "operator-secret",
            "ops",
            serde_json::json!(["operator"]),
            serde_json::json!([
                "read",
                "operate",
                "configure",
                "manage_nodes",
                "manage_rollouts",
                "manage_webhooks",
            ]),
        ),
        (
            "guest|viewer|viewer-secret",
            "viewer-secret",
            "guest",
            serde_json::json!(["viewer"]),
            serde_json::json!(["read", "read_audit"]),
        ),
    ];
    for (credential, secret, expected_id, expected_roles, expected_actions) in cases {
        let app = hub_router(
            hub::Hub::new(echo_config(credential)),
            &ServerConfig::default(),
        );
        let (status, body) = get_json(&app, "/api/v1/system", secret).await;
        assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
        assert_eq!(body["principal"]["id"], expected_id);
        assert_eq!(body["principal"]["roles"], expected_roles);
        assert_eq!(body["principal"]["actions"], expected_actions);
        // The echo never carries credential material or scopes.
        assert!(body["principal"].get("scopes").is_none());
        assert!(body["principal"].get("secret").is_none());
    }
}

/// The echo stays on the enforcement predicate: a caller whose credential
/// the middleware rejects never reaches the handler to see any identity.
#[tokio::test]
async fn hub_system_rejects_unauthenticated_callers() {
    let app = hub_router(
        hub::Hub::new(echo_config("root|admin|admin-secret")),
        &ServerConfig::default(),
    );
    let (status, body) = get_json(&app, "/api/v1/system", "wrong-secret").await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED, "body: {body}");
}

fn local_router_with_token(token: &str) -> axum::Router {
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: Default::default(),
        node: NodeConfig {
            control_api: ControlApiConfig {
                api_token: Some(token.into()),
                ..Default::default()
            },
            ..NodeConfig::default()
        },
    });
    router(engine.control_plane(), &ServerConfig::default())
}

/// The standalone plane has one all-powerful operator; its echo says so
/// instead of fabricating a role model the plane cannot enforce.
#[tokio::test]
async fn local_system_echoes_the_single_admin_operator() {
    let app = local_router_with_token("op-token");
    let (status, body) = get_json(&app, "/api/v1/system", "op-token").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert_eq!(body["principal"]["id"], "operator");
    assert_eq!(body["principal"]["roles"], serde_json::json!(["admin"]));
    assert_eq!(
        body["principal"]["actions"],
        serde_json::json!([
            "read",
            "operate",
            "configure",
            "manage_nodes",
            "manage_rollouts",
            "manage_webhooks",
            "approve_rollouts",
            "read_audit",
            "manage_users",
        ])
    );
    // The local plane's identity stays reachable and unchanged otherwise.
    assert_eq!(body["id"], "arkflow-control-plane");
}

/// A local plane with a configured token still rejects wrong credentials
/// before any identity is echoed.
#[tokio::test]
async fn local_system_rejects_wrong_token() {
    let app = local_router_with_token("op-token");
    let (status, body) = get_json(&app, "/api/v1/system", "wrong-token").await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED, "body: {body}");
}

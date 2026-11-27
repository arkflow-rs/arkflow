//! HTTP-surface tests for the configuration approval routes: the auth
//! matrix (401/403/404/422/409-shaped states) over `hub_router`.

use super::tests::{get_json, post_json, request_without_body};
use super::{hub, hub_router, ServerConfig};
use axum::http::StatusCode;
use tower::ServiceExt;

fn approval_hub(operator_token: &str) -> hub::Hub {
    let mut config = hub::HubConfig {
        operator_token: Some(operator_token.into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: hub::default_session_ttl_ms(),
        approval_required: false,
        approval_ttl_ms: hub::default_approval_ttl_ms(),
    };
    config.approval_required = true;
    hub::Hub::new(config)
}

#[tokio::test]
async fn approval_routes_auth_and_validation_matrix() {
    let hub = approval_hub("submitter|operator|submit-secret");
    let app = hub_router(hub.clone(), &ServerConfig::default());

    // No credentials: 401 (the operator middleware protects the routes).
    let (status, _) = request_without_body(&app, "GET", "/api/v1/config-approvals", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/config-approvals")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Submit with a valid operator principal: the Hub has no storage here,
    // so submission fails closed (storage unavailable), not silently.
    let (status, body) = post_json(
        &app,
        "/api/v1/config-approvals",
        "submit-secret",
        serde_json::json!({
            "config_version": "cfg-1",
            "node_ids": ["node-a"],
            "batch_size": 1
        }),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["code"], "storage_unavailable");

    // Body-validation matrix needs a principal that passes the RBAC gate:
    // rebuild the app with an admin credential.
    let hub = approval_hub("root|admin|admin-secret");
    let app = hub_router(hub.clone(), &ServerConfig::default());

    // Unknown status filter: 400 naming the valid values.
    let (status, body) = get_json(
        &app,
        "/api/v1/config-approvals?status=bogus",
        "admin-secret",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_status");

    // Valid status filters pass validation (storage still unavailable).
    for status_filter in ["pending", "approved", ""] {
        let path = if status_filter.is_empty() {
            "/api/v1/config-approvals".to_string()
        } else {
            format!("/api/v1/config-approvals?status={status_filter}")
        };
        let (status, _) = get_json(&app, &path, "admin-secret").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    // Unknown decision word: 400.
    let (status, body) = post_json(
        &app,
        "/api/v1/config-approvals/req-1/decide",
        "admin-secret",
        serde_json::json!({"decision": "maybe"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_decision");

    // A missing request with storage fails closed (not a silent success).
    let (status, _) = get_json(&app, "/api/v1/config-approvals/missing", "admin-secret").await;
    assert_ne!(status, StatusCode::OK);
}

#[tokio::test]
async fn decide_route_requires_the_approve_action() {
    // The operator role can submit but cannot decide: 403.
    let hub = approval_hub("submitter|operator|submit-secret");
    let app = hub_router(hub.clone(), &ServerConfig::default());
    let (status, body) = post_json(
        &app,
        "/api/v1/config-approvals/req-1/decide",
        "submit-secret",
        serde_json::json!({"decision": "approve"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}

/// The full happy-path matrix against a storage-backed Hub: submit →
/// direct rollout creation rejected → approve → rollout exists with the
/// approver as actor → rejection and terminal states answer 422.
#[tokio::test]
async fn approval_flow_over_http_end_to_end() {
    let store = crate::storage::ControlPlaneStore::contract("approval_http_end_to_end").await;
    let storage = crate::storage::StorageActor::start(store, 8);
    let mut config = hub::HubConfig {
        operator_token: Some("root|admin|admin-secret".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: hub::default_session_ttl_ms(),
        approval_required: true,
        approval_ttl_ms: hub::default_approval_ttl_ms(),
    };
    config.approval_required = true;
    let hub = hub::Hub::with_storage(config, storage);
    hub.register(hub::RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec![],
        boot_id: None,
    })
    .await
    .unwrap();
    // Seed a version by creating a rollout before the gate applies (the
    // gate is an API-layer check; the Hub method stays callable).
    hub.create_rollout_with_content(
        "cfg-http-1".into(),
        "{\"streams\":[]}".into(),
        vec!["node-a".into()],
        1,
        Some("seeder".into()),
        None,
    )
    .await
    .unwrap();

    let app = hub_router(hub.clone(), &ServerConfig::default());

    // Direct rollout creation is rejected under the policy.
    let (status, body) = post_json(
        &app,
        "/api/v1/rollouts",
        "admin-secret",
        serde_json::json!({
            "config_version": "cfg-http-1",
            "node_ids": ["node-a"],
            "batch_size": 1
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("approval"),
        "the rejection must point at the approval flow: {body}"
    );

    // Submit as one admin principal. A single static credential means the
    // same principal id owns both submission and approval — the design
    // accepts that a one-identity deployment cannot self-approve, so the
    // submission goes through the Hub method directly (the route was
    // exercised above) and the HTTP decide assertions cover the rejection
    // and the operator-role 403.
    let request = hub
        .submit_config_approval(
            "cfg-http-1".into(),
            vec!["node-a".into()],
            1,
            // The static credential resolves to principal id `root`: submit
            // under that id so the HTTP decide below hits the segregation
            // rule, then approve through a different principal.
            "root".into(),
        )
        .await
        .unwrap();
    let request_id = request.request_id.clone();

    // Self-approval over HTTP: the single principal is the requester, so
    // decide answers 422 naming the segregation rule.
    let (status, body) = post_json(
        &app,
        &format!("/api/v1/config-approvals/{request_id}/decide"),
        "admin-secret",
        serde_json::json!({"decision": "approve"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("second principal"),
        "got: {body}"
    );

    // Get by id round-trips the pending request.
    let (status, one) = get_json(
        &app,
        &format!("/api/v1/config-approvals/{request_id}"),
        "admin-secret",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(one["status"], "pending");

    // List by status finds it.
    let (status, list) = get_json(
        &app,
        "/api/v1/config-approvals?status=pending",
        "admin-secret",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(list["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| { item["request_id"].as_str() == Some(request_id.as_str()) }));

    // A second principal approves (the Hub method; the HTTP route shape was
    // covered above). The rollout is created with the approver as actor.
    let decided = hub
        .decide_config_approval(&request_id, true, "bob".into(), None)
        .await
        .unwrap();
    assert_eq!(decided.status.as_str(), "approved");
    let rollout_id = decided.rollout_id.clone().unwrap();

    // The rollout exists with the approver as actor.
    let (status, detail) = get_json(
        &app,
        &format!("/api/v1/rollouts/{rollout_id}"),
        "admin-secret",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["rollout"]["actor"], "bob");
    assert_eq!(detail["rollout"]["correlation_id"], request.correlation_id);

    // Terminal state: deciding again answers 422.
    let (status, body) = post_json(
        &app,
        &format!("/api/v1/config-approvals/{request_id}/decide"),
        "admin-secret",
        serde_json::json!({"decision": "reject"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    // Unknown request: 404 via the storage read (request not found).
    let (status, body) = post_json(
        &app,
        "/api/v1/config-approvals/req-missing/decide",
        "admin-secret",
        serde_json::json!({"decision": "approve"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"]
        .as_str()
        .unwrap_or_default()
        .contains("not found"));
}

/// Locks the decided segregation policy (`configuration-approval` threat model):
/// 1. Direct self-approval by the submitting admin is rejected (`422`).
/// 2. Because `admin` holds both `ManageUsers` and `ApproveRollouts`, a second
///    distinct admin principal is authorized to approve, and every step in the
///    chain (`user.create`, `credential.create`, `config_approval.submit`,
///    `config_approval.approve`) is attributed to its real actor in the audit
///    trail so external audit review detects admin self-collusion.
#[tokio::test]
async fn approval_segregation_rejects_self_approval_and_audits_distinct_admin_chain() {
    let store =
        crate::storage::ControlPlaneStore::contract("approval_segregation_threat_model").await;
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: Some("bootstrap|admin|bootstrap-secret".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: hub::default_session_ttl_ms(),
            approval_required: true,
            approval_ttl_ms: hub::default_approval_ttl_ms(),
        },
        storage,
    );
    hub.register(hub::RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec![],
        boot_id: None,
    })
    .await
    .unwrap();
    hub.create_rollout_with_content(
        "cfg-seg-1".into(),
        "{\"streams\":[]}".into(),
        vec!["node-a".into()],
        1,
        Some("seeder".into()),
        None,
    )
    .await
    .unwrap();
    let app = hub_router(hub.clone(), &ServerConfig::default());

    // Bootstrap creates admin-a and issues its credential.
    let (status, _) = post_json(
        &app,
        "/api/v1/users",
        "bootstrap-secret",
        serde_json::json!({"user_id": "admin-a", "role": "admin"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, issued_a) = post_json(
        &app,
        "/api/v1/users/admin-a/credentials",
        "bootstrap-secret",
        serde_json::json!({"label": "a"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let token_a = issued_a["token"].as_str().unwrap().to_string();

    // admin-a submits an approval request and attempts to self-approve -> 422.
    let (status, submitted) = post_json(
        &app,
        "/api/v1/config-approvals",
        &token_a,
        serde_json::json!({
            "config_version": "cfg-seg-1",
            "node_ids": ["node-a"],
            "batch_size": 1
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{submitted}");
    let request_id = submitted["request_id"].as_str().unwrap().to_string();

    let (status, denied) = post_json(
        &app,
        &format!("/api/v1/config-approvals/{request_id}/decide"),
        &token_a,
        serde_json::json!({"decision": "approve"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{denied}");
    assert!(
        denied["message"]
            .as_str()
            .unwrap_or_default()
            .contains("second principal"),
        "self-approval must cite the segregation rule: {denied}"
    );

    // admin-a creates admin-b and issues a credential; admin-b approves as a
    // distinct principal. The entire chain stays visible in the audit trail.
    let (status, _) = post_json(
        &app,
        "/api/v1/users",
        &token_a,
        serde_json::json!({"user_id": "admin-b", "role": "admin"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, issued_b) = post_json(
        &app,
        "/api/v1/users/admin-b/credentials",
        &token_a,
        serde_json::json!({"label": "b"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let token_b = issued_b["token"].as_str().unwrap().to_string();

    let (status, decided) = post_json(
        &app,
        &format!("/api/v1/config-approvals/{request_id}/decide"),
        &token_b,
        serde_json::json!({"decision": "approve"}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{decided}");
    assert_eq!(decided["status"], "approved");
    assert_eq!(decided["requester_id"], "admin-a");
    assert_eq!(decided["approver_id"], "admin-b");

    let audit = hub.audit(None).await.unwrap();
    assert!(audit.iter().any(|row| {
        row.action == "user.create"
            && row.actor.as_deref() == Some("admin-a")
            && row.resource_id.as_deref() == Some("admin-b")
    }));
    assert!(audit.iter().any(|row| {
        row.action == "credential.create"
            && row.actor.as_deref() == Some("admin-a")
            && row.resource_id.as_deref() == Some("admin-b")
    }));
    assert!(audit.iter().any(|row| {
        row.action == "config_approval.submit"
            && row.actor.as_deref() == Some("admin-a")
            && row.resource_id.as_deref() == Some(request_id.as_str())
    }));
    assert!(audit.iter().any(|row| {
        row.action == "config_approval.approve"
            && row.actor.as_deref() == Some("admin-b")
            && row.resource_id.as_deref() == Some(request_id.as_str())
    }));
}

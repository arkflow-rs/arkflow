//! HTTP-surface tests for node labels: management auth/validation matrix,
//! listing filters, re-registration hydration, and rollout target selection
//! (see the `node-labels` capability).

use super::tests::{get_json, post_json, put_json};
use super::{hub, hub_router, ServerConfig};
use axum::http::StatusCode;
use tower::ServiceExt;

fn labels_config(operator_token: &str, approval_required: bool) -> hub::HubConfig {
    hub::HubConfig {
        operator_token: Some(operator_token.into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: hub::default_session_ttl_ms(),
        approval_required,
        approval_ttl_ms: hub::default_approval_ttl_ms(),
    }
}

async fn register_node(hub: &hub::Hub, node_id: &str) {
    hub.register(hub::RegisterRequest {
        data_address: None,
        node_id: node_id.into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec![],
        boot_id: None,
    })
    .await
    .unwrap();
}

/// Auth and validation matrix over the label replacement route.
#[tokio::test]
async fn labels_route_auth_and_validation_matrix() {
    let hub = hub::Hub::new(labels_config("root|admin|admin-secret", false));
    let app = hub_router(hub.clone(), &ServerConfig::default());

    // No credentials: 401 from the operator middleware.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::put("/api/v1/nodes/node-a/labels")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let (status, _) = put_json(
        &app,
        "/api/v1/nodes/node-a/labels",
        "admin-secret",
        serde_json::json!({"labels": {}}),
    )
    .await;
    // No storage attached: the hub fails closed rather than pretending.
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    // RBAC: a viewer (no ManageNodes) is forbidden; operators hold
    // ManageNodes like drain/maintenance, so they may manage labels.
    let hub = hub::Hub::new(labels_config("spectator|viewer|viewer-secret", false));
    let app = hub_router(hub, &ServerConfig::default());
    let (status, body) = put_json(
        &app,
        "/api/v1/nodes/node-a/labels",
        "viewer-secret",
        serde_json::json!({"labels": {"env": "prod"}}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    // Validation matrix (storage-backed so validation runs past 503):
    let store = crate::storage::ControlPlaneStore::contract("labels_validation_matrix").await;
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(labels_config("root|admin|admin-secret", false), storage);
    register_node(&hub, "node-a").await;
    let app = hub_router(hub.clone(), &ServerConfig::default());

    for labels in [
        // 17 entries.
        serde_json::json!({"k1":"v","k2":"v","k3":"v","k4":"v","k5":"v","k6":"v","k7":"v","k8":"v","k9":"v","k10":"v","k11":"v","k12":"v","k13":"v","k14":"v","k15":"v","k16":"v","k17":"v"}),
        // Empty key.
        serde_json::json!({"": "v"}),
        // Leading '-' in value.
        serde_json::json!({"env": "-prod"}),
        // 64-character key.
        serde_json::json!({"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": "v"}),
        // Invalid character.
        serde_json::json!({"env": "prod east"}),
    ] {
        let (status, body) = put_json(
            &app,
            "/api/v1/nodes/node-a/labels",
            "admin-secret",
            serde_json::json!({"labels": labels}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_labels", "{body}");
    }

    // Unknown node: 404.
    let (status, body) = put_json(
        &app,
        "/api/v1/nodes/node-missing/labels",
        "admin-secret",
        serde_json::json!({"labels": {"env": "prod"}}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

    // A valid map replaces fully and the response reports the stored labels.
    let (status, body) = put_json(
        &app,
        "/api/v1/nodes/node-a/labels",
        "admin-secret",
        serde_json::json!({"labels": {"env": "prod"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["labels"]["env"], "prod");
}

/// Labels surface in the fleet listing, filter by equality AND semantics,
/// and survive Agent re-registration (hydration from durable storage).
#[tokio::test]
async fn labels_filter_and_re_registration_hydration() {
    let store = crate::storage::ControlPlaneStore::contract("labels_filter_hydration").await;
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(labels_config("root|admin|admin-secret", false), storage);
    register_node(&hub, "node-a").await;
    register_node(&hub, "node-b").await;
    let app = hub_router(hub.clone(), &ServerConfig::default());

    let (status, _) = put_json(
        &app,
        "/api/v1/nodes/node-a/labels",
        "admin-secret",
        serde_json::json!({"labels": {"env": "prod", "tier": "edge"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = put_json(
        &app,
        "/api/v1/nodes/node-b/labels",
        "admin-secret",
        serde_json::json!({"labels": {"env": "staging"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // The listing carries labels and the filter narrows it.
    let (status, body) = get_json(&app, "/api/v1/nodes?label=env%3Dprod", "admin-secret").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
    assert_eq!(body["items"][0]["id"], "node-a");
    assert_eq!(body["items"][0]["labels"]["tier"], "edge");

    // Multiple terms AND.
    let (status, body) = get_json(
        &app,
        "/api/v1/nodes?label=env%3Dprod%2Ctier%3Dedge",
        "admin-secret",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["items"].as_array().unwrap().len(), 1);

    // A node without the label is excluded.
    let (status, body) = get_json(&app, "/api/v1/nodes?label=env%3Dstaging", "admin-secret").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
    assert_eq!(body["items"][0]["id"], "node-b");

    // Invalid filter expression: 400.
    let (status, body) = get_json(&app, "/api/v1/nodes?label=prod", "admin-secret").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_label_filter");

    // Re-registration keeps the labels (fresh in-memory resource is
    // re-hydrated from storage; the upsert never touches the column).
    register_node(&hub, "node-a").await;
    let (status, body) = get_json(&app, "/api/v1/nodes", "admin-secret").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let node_a = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["id"] == "node-a")
        .unwrap();
    assert_eq!(node_a["labels"]["env"], "prod");

    // Full replacement: the new map wins entirely.
    let (status, _) = put_json(
        &app,
        "/api/v1/nodes/node-a/labels",
        "admin-secret",
        serde_json::json!({"labels": {"env": "dr"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = get_json(&app, "/api/v1/nodes?label=env%3Dprod", "admin-secret").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["items"].as_array().unwrap().is_empty());
}

/// Rollout target selection by label selector: resolved once at creation,
/// empty resolution rejected, both/neither rejected, later label changes do
/// not re-target an existing rollout.
#[tokio::test]
async fn rollout_target_selector_over_http() {
    let store = crate::storage::ControlPlaneStore::contract("rollout_target_selector").await;
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(labels_config("root|admin|admin-secret", false), storage);
    register_node(&hub, "node-a").await;
    register_node(&hub, "node-b").await;
    // Seed the config version via a single-node content rollout (the fleet
    // creation path only needs the version to exist).
    hub.create_rollout_with_content(
        "cfg-selector-1".into(),
        "{\"streams\":[]}".into(),
        vec!["node-a".into()],
        1,
        Some("seeder".into()),
        None,
    )
    .await
    .unwrap();
    let app = hub_router(hub.clone(), &ServerConfig::default());
    let (status, _) = put_json(
        &app,
        "/api/v1/nodes/node-a/labels",
        "admin-secret",
        serde_json::json!({"labels": {"env": "prod"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Both provided: rejected as ambiguous.
    let (status, body) = post_json(
        &app,
        "/api/v1/rollouts",
        "admin-secret",
        serde_json::json!({
            "config_version": "cfg-selector-1",
            "node_ids": ["node-a"],
            "target_selector": {"env": "prod"},
            "batch_size": 1
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_target_selection");

    // Neither: rejected.
    let (status, body) = post_json(
        &app,
        "/api/v1/rollouts",
        "admin-secret",
        serde_json::json!({"config_version": "cfg-selector-1", "batch_size": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_target_selection");

    // Empty selector map: rejected (never an implicit fleet-wide match).
    let (status, body) = post_json(
        &app,
        "/api/v1/rollouts",
        "admin-secret",
        serde_json::json!({
            "config_version": "cfg-selector-1",
            "target_selector": {},
            "batch_size": 1
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_target_selection");

    // Selector matching no node: explicit no_matching_nodes error.
    let (status, body) = post_json(
        &app,
        "/api/v1/rollouts",
        "admin-secret",
        serde_json::json!({
            "config_version": "cfg-selector-1",
            "target_selector": {"env": "nope"},
            "batch_size": 1
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "no_matching_nodes");

    // Happy path: the selector resolves to the labeled node only.
    let (status, body) = post_json(
        &app,
        "/api/v1/rollouts",
        "admin-secret",
        serde_json::json!({
            "config_version": "cfg-selector-1",
            "target_selector": {"env": "prod"},
            "batch_size": 1
        }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["total_targets"], 1);
    let rollout_id = body["rollout_id"].as_str().unwrap().to_owned();

    // Later label changes do not re-target the rollout.
    let (status, _) = put_json(
        &app,
        "/api/v1/nodes/node-a/labels",
        "admin-secret",
        serde_json::json!({"labels": {"env": "staging"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = get_json(
        &app,
        &format!("/api/v1/rollouts/{rollout_id}"),
        "admin-secret",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let targets = body["targets"].as_array().unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0]["node_id"], "node-a");
}

/// Under the approval policy a selector-based submission pins the resolved
/// node set; labels changing between submission and approval do not alter it.
#[tokio::test]
async fn approval_submission_pins_resolved_node_set() {
    let store = crate::storage::ControlPlaneStore::contract("approval_pins_selector").await;
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(labels_config("root|admin|admin-secret", true), storage);
    register_node(&hub, "node-a").await;
    register_node(&hub, "node-b").await;
    hub.create_rollout_with_content(
        "cfg-pin-1".into(),
        "{\"streams\":[]}".into(),
        vec!["node-a".into()],
        1,
        Some("seeder".into()),
        None,
    )
    .await
    .unwrap();
    let app = hub_router(hub.clone(), &ServerConfig::default());
    let (status, _) = put_json(
        &app,
        "/api/v1/nodes/node-a/labels",
        "admin-secret",
        serde_json::json!({"labels": {"env": "prod"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = post_json(
        &app,
        "/api/v1/config-approvals",
        "admin-secret",
        serde_json::json!({
            "config_version": "cfg-pin-1",
            "target_selector": {"env": "prod"},
            "batch_size": 1
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let request_id = body["request_id"].as_str().unwrap().to_owned();
    // The pending request pins the RESOLVED node set, not the selector.
    let pinned: Vec<&str> = body["targets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect();
    assert_eq!(pinned, vec!["node-a"]);

    // Labels change after submission; the pinned request is immutable.
    let (status, _) = put_json(
        &app,
        "/api/v1/nodes/node-a/labels",
        "admin-secret",
        serde_json::json!({"labels": {"env": "staging"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = get_json(
        &app,
        &format!("/api/v1/config-approvals/{request_id}"),
        "admin-secret",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let still: Vec<&str> = body["targets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect();
    assert_eq!(still, vec!["node-a"]);
}

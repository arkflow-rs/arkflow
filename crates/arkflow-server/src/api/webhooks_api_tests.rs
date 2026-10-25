//! HTTP-surface tests for webhook alerting: management auth/validation
//! matrix, secret hygiene, test delivery with signature verification, and
//! leader-gated forwarding (see the `webhook-alerting` capability).

use super::tests::{get_json, put_json, request_without_body};
use super::{hub, hub_router, ServerConfig};
use axum::http::StatusCode;
use tower::ServiceExt;

fn webhooks_config(operator_token: &str) -> hub::HubConfig {
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

async fn storage_backed_router(operator_token: &str, label: &str) -> (hub::Hub, axum::Router) {
    let store = crate::storage::ControlPlaneStore::contract(label).await;
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(webhooks_config(operator_token), storage);
    let app = hub_router(hub.clone(), &ServerConfig::default());
    (hub, app)
}

fn subscription_json(
    id: &str,
    url: &str,
    event_types: &[&str],
    enabled: bool,
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "url": url,
        "event_types": event_types,
        "enabled": enabled,
        "secret": "shared-secret",
    })
}

/// Auth matrix: 401 without credentials, 403 for a viewer (no
/// ManageWebhooks), 503 fail-closed when no storage is attached.
#[tokio::test]
async fn webhook_routes_auth_matrix() {
    let hub = hub::Hub::new(webhooks_config("root|admin|admin-secret"));
    let app = hub_router(hub.clone(), &ServerConfig::default());

    let (status, _) = request_without_body(&app, "GET", "/api/v1/webhooks", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::put("/api/v1/webhooks")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Viewer role: ManageWebhooks is denied on every route.
    let (_, app) =
        storage_backed_router("spectator|viewer|viewer-secret", "webhooks_viewer_denied").await;
    let (status, body) = get_json(&app, "/api/v1/webhooks", "viewer-secret").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, body) = put_json(
        &app,
        "/api/v1/webhooks",
        "viewer-secret",
        serde_json::json!({"subscriptions": []}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/webhooks/alerts/test",
        Some("viewer-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    // Operator role holds ManageWebhooks (storage attached so it gets past
    // the fail-closed guard).
    let (_, app) = storage_backed_router("ops|operator|ops-secret", "webhooks_operator_ok").await;
    let (status, body) = put_json(
        &app,
        "/api/v1/webhooks",
        "ops-secret",
        serde_json::json!({"subscriptions": []}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // No storage: the hub fails closed rather than pretending.
    let hub = hub::Hub::new(webhooks_config("root|admin|admin-secret"));
    let app = hub_router(hub, &ServerConfig::default());
    let (status, body) = put_json(
        &app,
        "/api/v1/webhooks",
        "admin-secret",
        serde_json::json!({"subscriptions": []}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
}

/// Validation rejects over-bound or malformed sets without partially
/// applying, and responses never echo the signing secret.
#[tokio::test]
async fn webhook_replacement_validation_and_secret_hygiene() {
    let (_, app) = storage_backed_router("root|admin|admin-secret", "webhooks_validation").await;

    // Seed one valid subscription.
    let (status, body) = put_json(
        &app,
        "/api/v1/webhooks",
        "admin-secret",
        serde_json::json!({"subscriptions": [
            subscription_json("alerts", "https://example.com/hook", &["rollout."], true),
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.to_string().contains("\"has_secret\":true"));
    assert!(
        !body.to_string().contains("shared-secret"),
        "responses must never echo the signing secret"
    );

    // Rejection matrix: over-bound set, bad scheme, empty filter entry.
    let over_bound = (0..=16)
        .map(|index| {
            subscription_json(
                &format!("hook-{index}"),
                "https://example.com/hook",
                &["x."],
                true,
            )
        })
        .collect::<Vec<_>>();
    for bad in [
        serde_json::json!({"subscriptions": over_bound}),
        serde_json::json!({"subscriptions": [
            subscription_json("alerts", "ftp://example.com/hook", &["rollout."], true),
        ]}),
        serde_json::json!({"subscriptions": [
            subscription_json("alerts", "https://example.com/hook", &[], true),
        ]}),
        serde_json::json!({"subscriptions": [
            subscription_json("alerts", "https://example.com/hook", &["rollout.", ""], true),
        ]}),
    ] {
        let (status, body) = put_json(&app, "/api/v1/webhooks", "admin-secret", bad).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }

    // The stored set is unchanged by rejected submissions.
    let (status, body) = get_json(&app, "/api/v1/webhooks", "admin-secret").await;
    assert_eq!(status, StatusCode::OK);
    let subscriptions = body["subscriptions"].as_array().unwrap().clone();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0]["id"], "alerts");
    assert!(
        !body.to_string().contains("shared-secret"),
        "list responses must never echo the signing secret"
    );

    // Full replacement semantics: a new list wins entirely.
    let (status, _) = put_json(
        &app,
        "/api/v1/webhooks",
        "admin-secret",
        serde_json::json!({"subscriptions": [
            subscription_json("fresh", "https://example.com/other", &["node."], true),
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = get_json(&app, "/api/v1/webhooks", "admin-secret").await;
    let subscriptions = body["subscriptions"].as_array().unwrap().clone();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0]["id"], "fresh");
}

/// Captured request from the local test receiver.
#[derive(Clone)]
struct Captured {
    event_type: String,
    signature: Option<String>,
    body: Vec<u8>,
}

async fn spawn_receiver(status: u16) -> (String, std::sync::Arc<std::sync::Mutex<Vec<Captured>>>) {
    use axum::routing::post;
    let captured: std::sync::Arc<std::sync::Mutex<Vec<Captured>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let state = captured.clone();
    let app = axum::Router::new().route(
        "/hook",
        post(
            move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                let state = state.clone();
                async move {
                    state.lock().expect("receiver lock").push(Captured {
                        event_type: headers
                            .get("x-arkflow-event-type")
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_owned(),
                        signature: headers
                            .get("x-arkflow-signature")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned),
                        body: body.to_vec(),
                    });
                    (axum::http::StatusCode::from_u16(status).unwrap(), "ok")
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    (url, captured)
}

/// Test delivery: one synchronous POST of a synthetic event, signed with
/// the subscription secret, reporting the endpoint outcome without
/// persisting anything or affecting delivery state.
#[tokio::test]
async fn webhook_test_delivery_reports_outcome_with_signature() {
    let (url, captured) = spawn_receiver(200).await;
    let (hub, app) =
        storage_backed_router("root|admin|admin-secret", "webhooks_test_delivery").await;

    let (status, body) = put_json(
        &app,
        "/api/v1/webhooks",
        "admin-secret",
        serde_json::json!({"subscriptions": [
            subscription_json("alerts", &url, &["rollout."], true),
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/webhooks/alerts/test",
        Some("admin-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["delivered"], serde_json::json!(true), "{body}");

    // Exactly one request arrived: synthetic, typed, and signed over the
    // exact body bytes (receiver-side HMAC verification).
    let requests = captured.lock().expect("receiver lock").clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].event_type, "webhook.test");
    let signature = requests[0].signature.as_deref().expect("signature header");
    let expected = {
        use hmac::Mac;
        type HmacSha256 = hmac::Hmac<sha2::Sha256>;
        let mut mac = HmacSha256::new_from_slice(b"shared-secret").unwrap();
        mac.update(&requests[0].body);
        mac.finalize().into_bytes()
    };
    assert_eq!(
        signature,
        expected
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );

    // The synthetic event was not persisted and the subscription keeps its
    // enabled state and empty delivery failure history.
    let (_, body) = get_json(&app, "/api/v1/webhooks", "admin-secret").await;
    assert_eq!(body["subscriptions"][0]["enabled"], serde_json::json!(true));
    let events = hub.events(None).await;
    assert!(
        !events
            .iter()
            .any(|event| event.event.event_type == "webhook.test"),
        "synthetic test events must not persist"
    );

    // Unknown subscription: 404.
    let (status, _) = request_without_body(
        &app,
        "POST",
        "/api/v1/webhooks/missing/test",
        Some("admin-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // A failing endpoint reports the HTTP status without retrying.
    let (failing_url, _guard) = spawn_receiver(500).await;
    let (status, _) = put_json(
        &app,
        "/api/v1/webhooks",
        "admin-secret",
        serde_json::json!({"subscriptions": [
            subscription_json("alerts", &failing_url, &["rollout."], true),
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/webhooks/alerts/test",
        Some("admin-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["delivered"], serde_json::json!(false), "{body}");
    assert_eq!(body["status"], serde_json::json!(500), "{body}");
}

/// Leader-gated forwarding: the forwarder delivers matching broadcast
/// events to enabled subscriptions and stops when the Hub steps down.
#[tokio::test]
async fn webhook_forwarder_delivers_matching_events_and_stops_on_standby() {
    let (url, captured) = spawn_receiver(200).await;
    let (hub, app) = storage_backed_router("root|admin|admin-secret", "webhooks_forwarding").await;

    // A meta subscription filtering `webhook.` — lifecycle events forward.
    let (status, body) = put_json(
        &app,
        "/api/v1/webhooks",
        "admin-secret",
        serde_json::json!({"subscriptions": [
            subscription_json("meta", &url, &["webhook."], true),
            subscription_json("sleeper", "https://example.com/unused", &["never."], false),
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let cancellation = tokio_util::sync::CancellationToken::new();
    let forwarder_hub = hub.clone();
    let forwarder = tokio::spawn(async move {
        forwarder_hub.run_webhook_forwarder(cancellation).await;
    });

    // The forwarder registers its broadcast receiver immediately but loads
    // the subscription set on its first refresh tick (~2s): a broadcast
    // arriving before the tasks exist would be dropped, so wait for the
    // first refresh before triggering the lifecycle event.
    tokio::time::sleep(std::time::Duration::from_millis(2_500)).await;

    // Re-enabling `sleeper` persists a webhook.resumed event and
    // broadcasts it; the forwarder must deliver it to `meta`.
    let (status, _) = put_json(
        &app,
        "/api/v1/webhooks",
        "admin-secret",
        serde_json::json!({"subscriptions": [
            subscription_json("meta", &url, &["webhook."], true),
            subscription_json("sleeper", "https://example.com/unused", &["never."], true),
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let count = captured.lock().expect("receiver lock").len();
        if count >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "forwarder never delivered the resumed event"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let delivered = captured.lock().expect("receiver lock").clone();
    assert!(delivered
        .iter()
        .all(|request| request.event_type.starts_with("webhook.")));
    assert!(delivered
        .iter()
        .any(|request| request.event_type == "webhook.resumed"));

    // Standby: the HTTP gate rejects operator routes outright, and no
    // further deliveries happen even for matching broadcasts.
    hub.step_down("test standby").await;
    let (status, _) = put_json(
        &app,
        "/api/v1/webhooks",
        "admin-secret",
        serde_json::json!({"subscriptions": [
            subscription_json("meta", &url, &["webhook."], true),
            subscription_json("sleeper", "https://example.com/unused", &["never."], false),
        ]}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "standby gate rejects operator routes"
    );
    // A matching lifecycle broadcast while standby: pause_webhook works
    // through storage (not the HTTP gate) and broadcasts `webhook.paused`,
    // which the standby forwarder must NOT deliver.
    assert!(hub.pause_webhook("sleeper", "standby probe").await);
    let events = hub.events(None).await;
    assert!(
        events
            .iter()
            .any(|event| event.event.event_type == "webhook.paused"),
        "the paused event is durable even on standby"
    );
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let steady = captured.lock().expect("receiver lock").len();
    assert_eq!(steady, delivered.len(), "standby must not forward");

    forwarder.abort();
}

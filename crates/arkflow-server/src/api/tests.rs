/// Stale-leader fencing surfaces as 503 with an explicit code, in the
/// same retry-against-the-elected-leader family as standby.
#[tokio::test]
async fn hub_problem_maps_stale_leader_to_503() {
    let response = hub_problem(hub::HubError::StaleLeader {
        claimed: 3,
        current: 4,
    });
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(
        text.contains("\"stale_leader\""),
        "body carries the code: {text}"
    );
}

use super::*;
use crate::hub::AgentOperation;

fn headers_with(authorization: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Some(value) = authorization {
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(value).unwrap(),
        );
    }
    headers
}

/// Regression: the Hub must authenticate the Agent's session from BOTH
/// transports for one transition window. An Agent that only sends the
/// legacy query parameter keeps working (Hub upgraded first), and an Agent
/// that sends both is authenticated from the header, never from the stale
/// query value.
#[test]
fn agent_session_token_is_accepted_from_the_header_or_the_query() {
    // Header only: the new transport.
    assert_eq!(
        agent_session_token(&headers_with(Some("Bearer header-token")), None).as_deref(),
        Some("header-token")
    );
    // Query only: an older Agent against a newer Hub.
    assert_eq!(
        agent_session_token(&headers_with(None), Some("query-token".into())).as_deref(),
        Some("query-token")
    );
    // Both: the header wins over the deprecated query credential.
    assert_eq!(
        agent_session_token(
            &headers_with(Some("Bearer header-token")),
            Some("query-token".into())
        )
        .as_deref(),
        Some("header-token"),
        "the header transport takes precedence over the query parameter"
    );
    // Neither: the handler answers 401.
    assert_eq!(agent_session_token(&headers_with(None), None), None);
    // A malformed or empty header credential never falls through.
    assert_eq!(bearer_session_token(&headers_with(Some("Basic x"))), None);
    assert_eq!(bearer_session_token(&headers_with(Some("Bearer   "))), None);
}

use arkflow_core::config::{
    AgentConfig, ControlApiConfig, EngineConfig, HealthEndpointsConfig, LoggingConfig, NodeConfig,
};
use arkflow_core::engine::Engine;
use futures_util::StreamExt;
use tower::ServiceExt;

#[tokio::test]
async fn hub_component_catalogue_exposes_registered_job_components() {
    arkflow_plugin::initialize().unwrap();
    let app = hub_router(
        hub::Hub::new(hub::HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        }),
        &ServerConfig::default(),
    );
    let response = app
        .oneshot(
            axum::http::Request::get("/api/v1/components")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let components: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert!(components.iter().any(|item| item["kind"] == "input"));
    assert!(components.iter().any(|item| item["kind"] == "output"));
    assert!(components.iter().any(|item| item["kind"] == "processor"));
}

#[tokio::test]
async fn configuration_validation_requires_the_operator_token() {
    // The validate endpoint resolves secret references and constructs
    // every component in the candidate, so it must be as guarded as
    // apply: an open endpoint would hand unauthenticated callers a
    // node-local env/file oracle and a per-request construction load.
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: NodeConfig {
            control_api: ControlApiConfig {
                api_token: Some("op-token".into()),
                ..Default::default()
            },
            ..NodeConfig::default()
        },
    });
    let cp = engine.control_plane();
    let app = router(cp, &ServerConfig::default());
    let candidate = serde_json::json!({
        "format": "yaml",
        "content": "not: [a valid config"
    });
    assert_eq!(
        app.clone()
            .oneshot(
                axum::http::Request::post("/api/v1/configuration/validate")
                    .header(axum::http::header::AUTHORIZATION, "Bearer wrong")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(candidate.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.oneshot(
            axum::http::Request::post("/api/v1/configuration/validate")
                .header(axum::http::header::AUTHORIZATION, "Bearer op-token")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(candidate.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn resource_router_exposes_system_nodes_streams_and_health() {
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: NodeConfig::default(),
    });
    let cp = engine.control_plane();
    let app = router(cp, &ServerConfig::default());
    assert_eq!(
        app.clone()
            .oneshot(
                axum::http::Request::get("/api/v1/system")
                    .body(axum::body::Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone()
            .oneshot(
                axum::http::Request::get("/api/v1/nodes")
                    .body(axum::body::Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/nodes?page=1&page_size=1")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let nodes: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(nodes["total"], 1);
    assert_eq!(nodes["items"].as_array().unwrap().len(), 1);
    assert_eq!(nodes["items"][0]["id"], "local-node");
    assert_eq!(
        app.oneshot(
            axum::http::Request::get("/health")
                .body(axum::body::Body::empty())
                .unwrap()
        )
        .await
        .unwrap()
        .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn resource_contract_includes_pagination_and_correlation_id() {
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: NodeConfig::default(),
    });
    let app = router(engine.control_plane(), &ServerConfig::default());
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/streams?page=2&page_size=1")
                .header("x-correlation-id", "test-correlation")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-correlation-id").unwrap(),
        "test-correlation"
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["page"], 2);
    assert_eq!(value["page_size"], 1);
    assert!(value["items"].is_array());

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/nodes?page=not-a-number")
                .header("x-correlation-id", "invalid-page")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(error["code"], "invalid_query");
    assert_eq!(error["correlation_id"], "invalid-page");

    let response = app
        .oneshot(
            axum::http::Request::get("/api/v1/streams/missing")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["code"], "stream_not_found");
}

#[tokio::test]
async fn protected_routes_reject_missing_credentials() {
    let health = NodeConfig {
        control_api: ControlApiConfig {
            api_token: Some("secret".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: health,
    });
    let app = router(engine.control_plane(), &ServerConfig::default());
    let response = app
        .oneshot(
            axum::http::Request::get("/api/v1/configuration")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn ha_enabled_without_storage_fails_before_binding() {
    let hub = hub::Hub::new(hub::HubConfig {
        operator_token: None,
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 1_000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
    })
    .with_ha(hub::HubHaConfig {
        enabled: true,
        ..hub::HubHaConfig::default()
    });
    let config = ServerConfig {
        address: "127.0.0.1:0".into(),
        insecure_local: true,
        ..ServerConfig::default()
    };
    let cancellation = tokio_util::sync::CancellationToken::new();
    let error = serve_hub(hub, config, cancellation).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("HA election requires durable storage"),
        "{error}"
    );
}

#[tokio::test]
async fn hub_startup_policy_covers_local_external_storage_and_credentials() {
    let local_hub = hub::Hub::new(hub::HubConfig {
        operator_token: None,
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 1_000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
    });
    let local = ServerConfig {
        address: "127.0.0.1:8080".into(),
        insecure_local: true,
        ..ServerConfig::default()
    };
    assert!(local.validate_hub_startup(&local_hub).is_ok());

    let mut external_insecure = local.clone();
    external_insecure.address = "0.0.0.0:8080".into();
    assert!(external_insecure
        .validate_hub_startup(&local_hub)
        .unwrap_err()
        .to_string()
        .contains("loopback"));

    let secure = ServerConfig {
        address: "0.0.0.0:8080".into(),
        ..ServerConfig::default()
    };
    let credentialed_without_storage = hub::Hub::new(hub::HubConfig {
        operator_token: Some("operator".into()),
        node_token: Some("node".into()),
        insecure_local: false,
        lease_ttl_ms: 1_000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
    });
    assert!(secure
        .validate_hub_startup(&credentialed_without_storage)
        .unwrap_err()
        .to_string()
        .contains("durable storage"));

    let temp = tempfile::tempdir().unwrap();
    let store = storage::ControlPlaneStore::open(temp.path().join("hub.sqlite").to_str().unwrap())
        .await
        .unwrap();
    let missing_credentials = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: false,
            lease_ttl_ms: 1_000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
        },
        storage::StorageActor::start(store.clone(), 8),
    );
    assert!(secure
        .validate_hub_startup(&missing_credentials)
        .unwrap_err()
        .to_string()
        .contains("credentials"));

    let valid_hub = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node".into()),
            insecure_local: false,
            lease_ttl_ms: 1_000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
        },
        storage::StorageActor::start(store, 8),
    );
    assert!(secure.validate_hub_startup(&valid_hub).is_ok());
}

/// The API-server-disabled process still exposes observability endpoints:
/// `/metrics` returns a valid exposition and `/ready` flips from 503 to
/// 200 once the engine runtime reports readiness.
#[tokio::test]
async fn observability_router_serves_metrics_ready_and_live_without_the_api_server() {
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: NodeConfig::default(),
    });
    let control_plane = engine.control_plane();
    control_plane.health().set_ready(false);
    let app = observability_router(control_plane.clone(), &ServerConfig::default());

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/ready")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["status"], "not_ready");
    assert_eq!(value["ready"], false);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/live")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    control_plane.health().set_ready(true);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/ready")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            axum::http::Request::get("/metrics")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/plain; version=0.0.4"
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    // No streams and no jobs: a valid (empty) exposition.
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(!text.lines().any(|line| !line.starts_with('#')));
}

/// The API-server router gains `/ready` and `/live` while the legacy
/// `/health`, `/readiness`, and `/liveness` paths keep their semantics.
#[tokio::test]
async fn local_router_keeps_legacy_health_paths_and_adds_ready_live() {
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: NodeConfig::default(),
    });
    let control_plane = engine.control_plane();
    control_plane.health().set_ready(true);
    control_plane.health().set_running(true);
    let app = router(control_plane, &ServerConfig::default());
    for path in ["/ready", "/live", "/readiness", "/liveness", "/health"] {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get(path)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success(), "path failed: {path}");
    }
}

#[tokio::test]
async fn resource_api_integration_covers_routes_filters_redaction_and_aliases() {
    let health = NodeConfig {
        control_api: ControlApiConfig {
            api_token: Some("secret-token".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: health,
    });
    let control_plane = engine.control_plane();
    control_plane.health().set_ready(true);
    control_plane.health().set_running(true);
    let app = router(control_plane, &ServerConfig::default());
    let auth = "Bearer secret-token";

    for path in [
        "/api/v1/system",
        "/api/v1/status",
        "/api/v1/nodes",
        "/api/v1/streams?page=1&page_size=10",
        "/api/v1/operations?page=1&page_size=10",
        "/api/v1/events?page=1&page_size=10",
        "/api/v1/components",
        "/api/v1/schema",
        "/api/v1/metrics",
        "/health",
        "/readiness",
        "/liveness",
    ] {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get(path)
                    .header("authorization", auth)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(!response.status().is_server_error(), "route failed: {path}");
    }

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/operations?state=not-a-real-state")
                .header("authorization", auth)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/streams/missing")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/configuration")
                .header("authorization", auth)
                .header("x-correlation-id", "resource-contract")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-correlation-id").unwrap(),
        "resource-contract"
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let configuration = String::from_utf8(body.to_vec()).unwrap();
    assert!(configuration.contains("******"));
    assert!(!configuration.contains("secret-token"));

    let draft = serde_json::json!({"format":"json","content":"{\"streams\":[]}"});
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::put("/api/v1/configuration/draft")
                .header("authorization", auth)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(draft.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/configuration/draft")
                .header("authorization", auth)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/configuration/validate")
                .header("authorization", auth)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"format":"json","content":"not-json"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let validation: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(validation["valid"], false);
    for path in ["/api/v1/config", "/api/v1/config/versions"] {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get(path)
                    .header("authorization", auth)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "alias failed: {path}");
    }
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/config/validate")
                .header("authorization", auth)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"format":"json","content":"{\"streams\":[]}"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/configuration/diff?from=missing&to=missing")
                .header("authorization", auth)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/components")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/components/input/generate")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        response.status(),
        StatusCode::OK | StatusCode::NOT_FOUND
    ));

    let response = app
        .oneshot(
            axum::http::Request::get("/api/v1/unknown-resource")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn hub_http_contract_registers_and_routes_a_targeted_command() {
    let hub = hub::Hub::new(hub::HubConfig {
        operator_token: Some("operator".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: default_session_ttl_ms(),
    });
    let app = hub_router(hub, &ServerConfig::default());
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/agent/register")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"node_id":"node-a","node_token":"node-secret"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let session: hub::RegisterResponse = serde_json::from_slice(&body).unwrap();

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/start")
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .oneshot(
            axum::http::Request::get(format!(
                "/api/v1/agent/commands?node_id=node-a&session_token={}",
                session.session_token
            ))
            .body(axum::body::Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let commands: Vec<hub::AgentCommand> = serde_json::from_slice(&body).unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].resource_id, "orders");
}

#[tokio::test]
async fn hub_routes_distinguish_viewer_from_operator_actions() {
    let hub = hub::Hub::new(hub::HubConfig {
        operator_token: Some("readonly|viewer|viewer-secret".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: default_session_ttl_ms(),
    });
    let app = hub_router(hub, &ServerConfig::default());
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/nodes")
                .header("authorization", "Bearer viewer-secret")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/start")
                .header("authorization", "Bearer viewer-secret")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = app
        .oneshot(
            axum::http::Request::get("/api/v1/events/stream")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn job_workbench_routes_validate_create_detail_and_versions() {
    let hub = hub::Hub::new(hub::HubConfig {
        operator_token: Some("operator-secret".into()),
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: default_session_ttl_ms(),
    });
    let app = hub_router(hub, &ServerConfig::default());
    let spec = serde_json::json!({
        "id": "workbench-job",
        "version": 1,
        "operators": [
            {"id": "source", "kind": "source"},
            {"id": "sink", "kind": "sink"}
        ],
        "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
        "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
        "sinks": [{"operator_id": "sink", "output_type": "drop"}]
    });
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/jobs/validate")
                .header("authorization", "Bearer operator-secret")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"spec": spec, "node_ids": []}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/jobs")
                .header("authorization", "Bearer operator-secret")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"spec": spec, "desired_state": "stopped"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/jobs/workbench-job/detail")
                .header("authorization", "Bearer operator-secret")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let detail: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(detail["job"]["job_id"], "workbench-job");
    assert!(detail["plan"].is_object());
    let response = app
        .oneshot(
            axum::http::Request::get("/api/v1/jobs/workbench-job/versions")
                .header("authorization", "Bearer operator-secret")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn job_action_route_leaves_an_audit_trail() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: Some("operator-secret".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        },
        storage::StorageActor::start(store, 8),
    );
    hub.register(hub::RegisterRequest {
        data_address: None,
        node_id: "compute-1".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec!["job_runtime".into(), "state_backend".into()],
        boot_id: None,
    })
    .await
    .unwrap();
    let app = hub_router(hub.clone(), &ServerConfig::default());
    let spec = serde_json::json!({
        "id": "audit-job",
        "version": 1,
        "operators": [
            {"id": "source", "kind": "source"},
            {"id": "sink", "kind": "sink"}
        ],
        "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
        "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
        "sinks": [{"operator_id": "sink", "output_type": "drop"}]
    });
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/jobs")
                .header("authorization", "Bearer operator-secret")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"spec": spec, "desired_state": "stopped"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    // The start action funnels into the audited enqueue path.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/jobs/audit-job/actions/start")
                .header("authorization", "Bearer operator-secret")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/audit?resource_id=audit-job")
                .header("authorization", "Bearer operator-secret")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let page: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let actions: Vec<&str> = page["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item["action"].as_str())
                .collect()
        })
        .unwrap_or_default();
    assert!(
        actions.contains(&"job.start"),
        "the start action must be audited, got {actions:?}"
    );
    // The hub-level funnel keeps working for direct dispatches too.
    assert!(hub
        .audit(Some("audit-job"))
        .await
        .unwrap()
        .iter()
        .any(|record| record.action == "job.start" && record.outcome == "accepted"));
    // The operator-triggered checkpoint is audited exactly once at the
    // trigger, not per dispatch and not by the periodic scheduler.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/jobs/audit-job/checkpoints")
                .header("authorization", "Bearer operator-secret")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let checkpoint_audits = hub
        .audit(Some("audit-job"))
        .await
        .unwrap()
        .iter()
        .filter(|record| record.action == "job.checkpoint")
        .count();
    assert_eq!(checkpoint_audits, 1, "the trigger is audited exactly once");
}

#[tokio::test]
async fn denied_mutation_is_visible_in_audit_history() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: Some("viewer|viewer|viewer-secret".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        },
        storage::StorageActor::start(store, 8),
    );
    let app = hub_router(hub, &ServerConfig::default());
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                .header("authorization", "Bearer viewer-secret")
                .header("content-type", "application/json")
                .header("x-correlation-id", "audit-test")
                .body(axum::body::Body::from(r#"{"state":"running"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = app
        .oneshot(
            axum::http::Request::get("/api/v1/audit?resource_id=node-a%2Forders")
                .header("authorization", "Bearer viewer-secret")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["items"][0]["outcome"], "rejected");
    assert_eq!(value["items"][0]["correlation_id"], "audit-test");
}

#[tokio::test]
async fn sse_replays_durable_events_by_last_event_id() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_events (node_id, event_type, outcome, occurred_at_ms) VALUES ('node-a', 'rollout_changed', 'accepted', 10)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
    let hub = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        },
        storage::StorageActor::start(store, 8),
    );
    let response = hub_router(hub, &ServerConfig::default())
        .oneshot(
            axum::http::Request::get("/api/v1/events/stream")
                .header("authorization", "Bearer operator")
                .header("last-event-id", "0")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let chunk = tokio::time::timeout(std::time::Duration::from_secs(1), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let text = String::from_utf8(chunk.to_vec()).unwrap();
    assert!(text.contains("id: 1"));
    assert!(text.contains("event: rollout_changed"));
}

#[tokio::test]
async fn desired_state_route_persists_offline_intent_and_rejects_stale_generation() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        },
        storage::StorageActor::start(store, 8),
    );
    let app = hub_router(hub, &ServerConfig::default());
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                .header("authorization", "Bearer operator")
                .header("content-type", "application/json")
                .header("idempotency-key", "desired-orders-1")
                .body(axum::body::Body::from(r#"{"state":"running"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert!(response.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .starts_with("/api/v1/operations/intent-"));
    assert_eq!(
        response.headers().get(header::ETAG).unwrap(),
        "\"generation-1\""
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["generation"], 1);
    assert_eq!(value["convergence"], "pending");
    assert_eq!(value["desired_state"], "running");

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/nodes/node-a/streams/orders")
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let resource: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(resource["desired"]["state"], "running");
    assert_eq!(resource["desired"]["generation"], 1);
    assert_eq!(resource["state"], "unknown");

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/events?node_id=node-a")
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let events: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(events["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["event_type"] == "intent_created"));

    let operation_id = value["operation_id"].as_str().unwrap().to_owned();
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get(format!("/api/v1/operations/{operation_id}"))
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let operation: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(operation["intent_id"], value["operation_id"]);
    assert_eq!(operation["intent_state"], "accepted");
    assert_eq!(operation["convergence_state"], "pending");
    assert_eq!(operation["retry_count"], 0);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                .header("authorization", "Bearer operator")
                .header("content-type", "application/json")
                .header("idempotency-key", "desired-orders-1")
                .body(axum::body::Body::from(r#"{"state":"stopped"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let replay_error: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(replay_error["code"], "idempotency_key_reused");

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                .header("authorization", "Bearer operator")
                .header("content-type", "application/json")
                .header("idempotency-key", "desired-orders-1")
                .body(axum::body::Body::from(r#"{"state":"running"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let replay: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(replay["generation"], 1);
    assert_eq!(replay["desired_state"], "running");

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/restart")
                .header("authorization", "Bearer operator")
                .header("if-match", "\"generation-1\"")
                .header("idempotency-key", "restart-orders-1")
                .header("x-action-id", "restart-action-1")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let restart: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(restart["generation"], 2);
    assert_eq!(restart["desired_state"], "running");
    assert_eq!(restart["action_id"], "restart-action-1");

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/actions/restart")
                .header("authorization", "Bearer operator")
                .header("if-match", "\"generation-2\"")
                .header("idempotency-key", "restart-orders-2")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    r#"{"action_id":"restart-action-2"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let canonical_restart: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(canonical_restart["generation"], 3);
    assert_eq!(canonical_restart["action_id"], "restart-action-2");

    let response = app
        .oneshot(
            axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                .header("authorization", "Bearer operator")
                .header("x-correlation-id", "generation-conflict-test")
                .header("if-match", "\"generation-0\"")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(r#"{"state":"stopped"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(error["code"], "generation_conflict");
    assert_eq!(error["correlation_id"], "generation-conflict-test");
    assert_eq!(error["details"]["current_generation"], 3);
}

#[tokio::test]
async fn configuration_apply_persists_target_and_reconciles_offline_write() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let storage = storage::StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        },
        storage.clone(),
    );
    let app = hub_router(hub.clone(), &ServerConfig::default());
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/nodes/node-a/configuration/apply")
                .header("authorization", "Bearer operator")
                .header("idempotency-key", "config-apply-1")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    r#"{"format":"json","content":"{\"streams\":[]}"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let intent: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(intent["generation"], 1);
    assert_eq!(intent["desired_state"], "configured");
    let config_version = intent["config_version"].as_str().unwrap().to_owned();
    assert!(intent["rollout_id"].as_str().is_some());

    let session = hub
        .register(hub::RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["configuration".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    hub.reconcile_rollouts().await.unwrap();
    let operation = hub
        .reconcile_once("config-reconciler")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.operation.as_str(), "apply_configuration");
    assert_eq!(
        operation.config_version_id.as_deref(),
        Some(config_version.as_str())
    );
    let commands = hub
        .commands(hub::AgentAuth {
            node_id: "node-a".into(),
            session_token: session.session_token.clone(),
        })
        .await;
    let commands = commands.unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].operation.as_str(), "apply_configuration");
    assert_eq!(
        commands[0].config_version_id.as_deref(),
        Some(config_version.as_str())
    );
    assert!(commands[0].payload.is_some());

    hub.report(hub::NodeReport {
        auth: hub::AgentAuth {
            node_id: "node-a".into(),
            session_token: session.session_token.clone(),
        },
        version: "test".into(),
        state: "online".into(),
        capabilities: vec!["configuration".into()],
        streams: vec![],
        operations: vec![],
        events: vec![],
        metrics: Default::default(),
        jobs: Default::default(),
        configuration: None,
        configuration_version: Some(config_version),
        config_versions: Vec::new(),
        job_tasks: Default::default(),
        boot_id: Some(session.session_token.clone()),
        connected_hub: None,
        report_seq: 1,
    })
    .await
    .unwrap();
    let rollout_id = intent["rollout_id"].as_str().unwrap();
    let (_, targets) = hub.rollout(rollout_id).await.unwrap().unwrap();
    let intent_id = targets[0].attempt_id.clone().unwrap();
    let persisted = storage.get_intent(intent_id).await.unwrap().unwrap();
    assert_eq!(persisted.state, "converged");
}

#[tokio::test]
async fn node_level_rollback_returns_a_single_node_rollout() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-rollback', 'digest', '{}', 'json', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
    let storage = storage::StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        },
        storage,
    );
    let app = hub_router(hub.clone(), &ServerConfig::default());
    let response = app
        .oneshot(
            axum::http::Request::post("/api/v1/nodes/node-a/configuration/rollback/cfg-rollback")
                .header("authorization", "Bearer operator")
                .header("content-type", "application/json")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let rollout: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(rollout["rollout_id"].as_str().is_some());
    assert_eq!(rollout["config_version_id"], "cfg-rollback");
    assert_eq!(rollout["total_targets"], 1);
    let (_, targets) = hub
        .rollout(rollout["rollout_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(targets[0].node_id, "node-a");
}

#[tokio::test]
async fn operation_resource_exposes_retry_superseded_and_blocked_states() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let storage = storage::StorageActor::start(store, 8);
    let hub = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: default_session_ttl_ms(),
        },
        storage.clone(),
    );
    let app = hub_router(hub, &ServerConfig::default());
    let first = app
        .clone()
        .oneshot(
            axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                .header("authorization", "Bearer operator")
                .header("content-type", "application/json")
                .header("idempotency-key", "state-matrix-1")
                .body(axum::body::Body::from(r#"{"state":"running"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(first.into_body(), usize::MAX)
        .await
        .unwrap();
    let first: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let first_id = first["intent_id"].as_str().unwrap().to_owned();
    let attempt = storage.claim_attempt(&first_id).await.unwrap().unwrap();
    storage
        .complete_attempt(
            &attempt.attempt_id,
            "timed_out",
            Some("temporary_execution".into()),
        )
        .await
        .unwrap();
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get(format!("/api/v1/operations/{first_id}"))
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let retrying: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(retrying["intent_state"], "retrying");
    assert_eq!(retrying["convergence_state"], "degraded");
    assert_eq!(retrying["retry_count"], 1);
    assert_eq!(retrying["failure_class"], "temporary_execution");

    let second = app
        .clone()
        .oneshot(
            axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                .header("authorization", "Bearer operator")
                .header("content-type", "application/json")
                .header("if-match", "\"generation-1\"")
                .header("idempotency-key", "state-matrix-2")
                .body(axum::body::Body::from(r#"{"state":"stopped"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(second.into_body(), usize::MAX)
        .await
        .unwrap();
    let second: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let second_id = second["intent_id"].as_str().unwrap().to_owned();
    let attempt = storage.claim_attempt(&second_id).await.unwrap().unwrap();
    storage
        .complete_attempt(
            &attempt.attempt_id,
            "failed",
            Some("permanent_execution".into()),
        )
        .await
        .unwrap();
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get(format!("/api/v1/operations/{first_id}"))
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let superseded: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(superseded["intent_state"], "superseded");
    assert_eq!(superseded["superseded_generation"], 2);

    let response = app
        .oneshot(
            axum::http::Request::get(format!("/api/v1/operations/{second_id}"))
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let blocked: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(blocked["intent_state"], "blocked");
    assert_eq!(blocked["convergence_state"], "blocked");
    assert_eq!(blocked["failure_class"], "permanent_execution");
}

#[tokio::test]
async fn hub_end_to_end_two_nodes_aggregate_target_reconnect_and_drain() {
    let hub = hub::Hub::new(hub::HubConfig {
        operator_token: Some("operator".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
    });
    let app = hub_router(hub.clone(), &ServerConfig::default());

    let node_a = register_hub_node(&app, "node-a").await;
    let node_b = register_hub_node(&app, "node-b").await;
    report_hub_node(&app, &node_a, "orders", "online").await;
    report_hub_node(&app, &node_b, "orders", "online").await;

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/system")
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let system: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(system["node_count"], 2);
    assert_eq!(system["online_nodes"], 2);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/streams")
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let streams: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let stream_nodes: Vec<&str> = streams["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|stream| stream["node_id"].as_str().unwrap())
        .collect();
    assert_eq!(stream_nodes, vec!["node-a", "node-b"]);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/nodes?page=2&page_size=1")
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let nodes_page: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(nodes_page["total"], 2);
    assert_eq!(nodes_page["items"].as_array().unwrap().len(), 1);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/start")
                .header("authorization", "Bearer operator")
                .header("x-correlation-id", "two-node-start")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let operation: hub::HubOperation = serde_json::from_slice(&body).unwrap();
    assert_eq!(operation.node_id, "node-a");
    assert_eq!(operation.correlation_id.as_deref(), Some("two-node-start"));

    let commands_a = agent_commands(&app, &node_a).await;
    let commands_b = agent_commands(&app, &node_b).await;
    assert_eq!(commands_a.len(), 1);
    assert!(commands_b.is_empty());
    assert_eq!(commands_a[0].node_id, "node-a");

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post(format!(
                "/api/v1/agent/commands/{}/result?node_id={}&session_token={}",
                commands_a[0].id, node_a.node_id, node_a.session_token
            ))
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                serde_json::to_string(&hub::CommandResult {
                    command_id: commands_a[0].id.clone(),
                    operation_id: operation.id.clone(),
                    state: hub::HubOperationState::Succeeded,
                    progress: 100,
                    error: None,
                    correlation_id: Some("two-node-start".into()),
                    generation: 0,
                    observed_generation: None,
                    action_id: None,
                    failure_class: None,
                    config_version_id: None,
                    rollout_id: None,
                    observed_checkpoint_id: None,
                    checkpoint_manifest_uri: None,
                    result: None,
                })
                .unwrap(),
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let operation_after_result = get_hub_operation(&app, &operation.id).await;
    assert_eq!(
        operation_after_result.state,
        hub::HubOperationState::Succeeded
    );

    let old_session = node_a.session_token.clone();
    let node_a_reconnected = register_hub_node(&app, "node-a").await;
    assert_ne!(old_session, node_a_reconnected.session_token);
    let old_session_response = app
        .clone()
        .oneshot(
            axum::http::Request::get(format!(
                "/api/v1/agent/commands?node_id=node-a&session_token={old_session}"
            ))
            .body(axum::body::Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(old_session_response.status(), StatusCode::UNAUTHORIZED);

    let streams_after_reconnect = get_hub_streams(&app).await;
    assert_eq!(streams_after_reconnect["total"], 2);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/agent/heartbeat")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({
                        "node_id": "node-a",
                        "session_token": node_a_reconnected.session_token,
                        "state": "draining"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let nodes = get_hub_nodes(&app).await;
    assert_eq!(
        nodes.iter().find(|node| node.id == "node-a").unwrap().state,
        hub::NodeConnectionState::Draining
    );
    assert_eq!(
        nodes.iter().find(|node| node.id == "node-b").unwrap().state,
        hub::NodeConnectionState::Online
    );
}

async fn register_hub_node(app: &Router, node_id: &str) -> hub::RegisterResponse {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/agent/register")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"node_id": node_id, "node_token": "node-secret"})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn report_hub_node(
    app: &Router,
    session: &hub::RegisterResponse,
    stream_id: &str,
    state: &str,
) {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/agent/report")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({
                        "node_id": session.node_id,
                        "session_token": session.session_token,
                        "version": "test-node",
                        "state": state,
                        "capabilities": ["stream_lifecycle", "metrics"],
                        "streams": [{
                            "id": stream_id,
                            "state": "running",
                            "desired_state": "running",
                            "started_at_ms": 1,
                            "last_error": null,
                            "metrics": {
                                "input_batches": 1,
                                "input_messages": 3,
                                "processing_errors": 0,
                                "output_batches": 1,
                                "output_messages": 2,
                                "input_errors": 0,
                                "input_reconnects": 0,
                                "output_errors": 0,
                                "restarts": 0
                            }
                        }],
                        "operations": [],
                        "events": [],
                        "metrics": {"input_messages": 3.0},
                        "configuration": {"token": "[REDACTED]"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

async fn agent_commands(app: &Router, session: &hub::RegisterResponse) -> Vec<hub::AgentCommand> {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get(format!(
                "/api/v1/agent/commands?node_id={}&session_token={}",
                session.node_id, session.session_token
            ))
            .body(axum::body::Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn get_hub_operation(app: &Router, id: &str) -> hub::HubOperation {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get(format!("/api/v1/operations/{id}"))
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn get_hub_streams(app: &Router) -> serde_json::Value {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/streams")
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn get_hub_nodes(app: &Router) -> Vec<hub::HubNode> {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/nodes")
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let page: Page<hub::HubNode> = serde_json::from_slice(&body).unwrap();
    page.items
}

fn hub_tls_material() -> (std::path::PathBuf, std::path::PathBuf) {
    use rcgen::CertificateParams;
    use rcgen::KeyPair;
    let params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    let key = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    // A unique directory per call: parallel TLS tests used to share one
    // pid-keyed dir and delete each other's (freshly regenerated)
    // material mid-test.
    static TLS_MATERIAL_SEQUENCE: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);
    let sequence = TLS_MATERIAL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("arkflow-hub-tls-{}-{sequence}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    (cert_path, key_path)
}

#[tokio::test]
async fn hub_tls_listener_serves_readiness_over_https() {
    let (cert_path, key_path) = hub_tls_material();
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let hub = hub::Hub::new(hub::HubConfig {
        operator_token: None,
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 1_000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
    });
    let config = ServerConfig {
        address: format!("127.0.0.1:{port}"),
        insecure_local: true,
        tls_cert: Some(cert_path.to_string_lossy().into_owned()),
        tls_key: Some(key_path.to_string_lossy().into_owned()),
        ..ServerConfig::default()
    };
    let cancellation = tokio_util::sync::CancellationToken::new();
    let shutdown = cancellation.clone();
    tokio::spawn(async move {
        if let Err(error) = serve_hub(hub, config, shutdown).await {
            eprintln!("SERVE_HUB ERROR: {error}");
        }
    });
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let mut served = false;
    for _ in 0..100 {
        if let Ok(response) = client
            .get(format!("https://127.0.0.1:{port}/readiness"))
            .send()
            .await
        {
            // The response status itself is the plaintext readiness
            // semantics (503 without storage); what matters here is
            // that the TLS handshake completed and HTTP was served.
            let _ = response.status();
            served = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(served, "the TLS hub must serve https requests");
    cancellation.cancel();
    let _ = std::fs::remove_dir_all(cert_path.parent().unwrap());
}

#[tokio::test]
async fn half_configured_hub_tls_fails_startup() {
    let (cert_path, _key_path) = hub_tls_material();
    let hub = hub::Hub::new(hub::HubConfig {
        operator_token: None,
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 1_000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
    });
    let config = ServerConfig {
        address: "127.0.0.1:0".into(),
        insecure_local: true,
        tls_cert: Some(cert_path.to_string_lossy().into_owned()),
        tls_key: None,
        ..ServerConfig::default()
    };
    let error = serve_hub(hub, config, tokio_util::sync::CancellationToken::new())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("both ARKFLOW_HUB_TLS_CERT"),
        "{error}"
    );
    let _ = std::fs::remove_dir_all(cert_path.parent().unwrap());
}

fn console_hub() -> hub::Hub {
    hub::Hub::new(hub::HubConfig {
        operator_token: Some("operator-secret".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: default_session_ttl_ms(),
    })
}

async fn post_json(
    app: &Router,
    path: &str,
    bearer: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post(path)
                .header("authorization", format!("Bearer {bearer}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    read_json(response).await
}

async fn get_json(app: &Router, path: &str, bearer: &str) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get(path)
                .header("authorization", format!("Bearer {bearer}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    read_json(response).await
}

async fn read_json(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
    let status = response.status();
    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = if content_type.starts_with("application/json") {
        serde_json::from_slice(&body).unwrap_or_default()
    } else {
        serde_json::Value::String(String::from_utf8_lossy(&body).into_owned())
    };
    (status, value)
}

async fn report_node_raw(app: &Router, body: serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/agent/report")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

fn agent_report_body(session: &hub::RegisterResponse) -> serde_json::Value {
    serde_json::json!({
        "node_id": session.node_id,
        "session_token": session.session_token,
        "version": "test-node",
        "state": "online",
        "capabilities": ["stream_lifecycle", "metrics"],
    })
}

#[tokio::test]
async fn hub_status_aggregates_fleet_gauges() {
    let hub = console_hub();
    let app = hub_router(hub, &ServerConfig::default());
    let session = register_hub_node(&app, "node-a").await;
    let mut report = agent_report_body(&session);
    report["streams"] = serde_json::json!([
        {
            "id": "s1", "state": "running", "desired_state": "running",
            "started_at_ms": 1, "last_error": null, "metrics": {
                "input_batches": 1, "input_messages": 1, "processing_errors": 0,
                "output_batches": 1, "output_messages": 1, "input_errors": 0,
                "input_reconnects": 0, "output_errors": 0, "restarts": 0
            }
        },
        {
            "id": "s2", "state": "failed", "desired_state": "running",
            "started_at_ms": 1, "last_error": null, "metrics": {
                "input_batches": 0, "input_messages": 0, "processing_errors": 1,
                "output_batches": 0, "output_messages": 0, "input_errors": 0,
                "input_reconnects": 0, "output_errors": 0, "restarts": 0
            }
        }
    ]);
    report_node_raw(&app, report).await;

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/status")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let (status, body) = get_json(&app, "/api/v1/status", "operator-secret").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["streams_total"], 2);
    assert_eq!(body["streams_running"], 1);
    assert_eq!(body["streams_failed"], 1);
    assert_eq!(body["state"], "running");
    assert!(body["uptime_seconds"].is_u64());
    assert!(body["version"].is_string());
}

#[tokio::test]
async fn hub_metrics_serves_json_for_console_and_text_for_scrapers() {
    let hub = console_hub();
    let app = hub_router(hub, &ServerConfig::default());
    let session_a = register_hub_node(&app, "node-a").await;
    let mut report = agent_report_body(&session_a);
    report["metrics"] = serde_json::json!({"input_batches": 3.0, "streams_total": 2.0});
    report_node_raw(&app, report).await;
    let session_b = register_hub_node(&app, "node-b").await;
    let mut report = agent_report_body(&session_b);
    report["metrics"] = serde_json::json!({"input_batches": 4.0});
    report_node_raw(&app, report).await;

    // Default branch: Prometheus text, unchanged for scrapers.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/metrics")
                .header("authorization", "Bearer operator-secret")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .unwrap(),
        "text/plain; version=0.0.4"
    );

    // JSON branch via Accept header.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/metrics")
                .header("authorization", "Bearer operator-secret")
                .header("accept", "application/json")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"].as_array().unwrap().len(), 2);
    assert_eq!(body["aggregate"]["input_batches"], 7.0);

    // node_id filter and explicit format override.
    let (status, body) = get_json(
        &app,
        "/api/v1/metrics?node_id=node-a&format=json",
        "operator-secret",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["node_id"], "node-a");
    assert_eq!(body["aggregate"]["input_batches"], 3.0);
}

#[tokio::test]
async fn hub_configuration_validate_and_diff_round_trip_through_commands() {
    let hub = console_hub();
    let app = hub_router(hub, &ServerConfig::default());
    let session = register_hub_node(&app, "node-a").await;

    // Unknown node is rejected, not silently queued.
    let (status, body) = post_json(
        &app,
        "/api/v1/nodes/node-b/configuration/validate",
        "operator-secret",
        serde_json::json!({"format": "yaml", "content": "streams: []\n"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "node_unavailable");

    let (status, operation) = post_json(
        &app,
        "/api/v1/nodes/node-a/configuration/validate",
        "operator-secret",
        serde_json::json!({"format": "yaml", "content": "streams: []\n"}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(operation["operation"], "validate_configuration");

    // The agent receives the read-only command.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get(format!(
                "/api/v1/agent/commands?node_id=node-a&session_token={}",
                session.session_token
            ))
            .body(axum::body::Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let commands: Vec<hub::AgentCommand> = serde_json::from_slice(&body).unwrap();
    let command = commands
        .iter()
        .find(|command| command.operation.as_str() == "validate_configuration")
        .expect("validate command dispatched");
    assert!(command.payload.is_some());

    // Terminal result carries the validation report.
    let (status, settled) = post_json(
        &app,
        &format!(
            "/api/v1/agent/commands/{}/result?node_id=node-a",
            command.id
        ),
        &session.session_token,
        serde_json::json!({
            "command_id": command.id,
            "operation_id": command.operation_id,
            "state": "succeeded",
            "progress": 100,
            "result": {"valid": true, "errors": []}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(settled["result"]["valid"], true);

    let operation_id = operation["id"].as_str().unwrap().to_owned();
    let (status, fetched) = get_json(
        &app,
        &format!("/api/v1/operations/{operation_id}"),
        "operator-secret",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["state"], "succeeded");
    assert_eq!(fetched["result"]["valid"], true);

    // Diff dispatches through the same read-only channel.
    let (status, diff_operation) = get_json(
        &app,
        "/api/v1/nodes/node-a/configuration/diff?from=v1&to=v2",
        "operator-secret",
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(diff_operation["operation"], "diff_configuration");

    // Reported version metadata becomes the node's version list.
    let mut report = agent_report_body(&session);
    report["config_versions"] = serde_json::json!([
        {"id": "v2", "created_at_ms": 2, "format": "yaml"},
        {"id": "v1", "created_at_ms": 1, "format": "json"}
    ]);
    report["configuration"] = serde_json::json!({"streams": []});
    report_node_raw(&app, report).await;
    let (status, versions) = get_json(
        &app,
        "/api/v1/nodes/node-a/configuration/versions",
        "operator-secret",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let versions = versions.as_array().unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0]["id"], "v2");
}

#[tokio::test]
async fn job_detail_reports_scoped_metrics_and_observed_tasks() {
    let hub = hub::Hub::new(hub::HubConfig {
        operator_token: Some("operator-secret".into()),
        node_token: Some("node-secret".into()),
        insecure_local: true,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: default_session_ttl_ms(),
    });
    let app = hub_router(hub, &ServerConfig::default());
    let spec = serde_json::json!({
        "id": "scoped-job",
        "version": 1,
        "operators": [
            {"id": "source", "kind": "source"},
            {"id": "sink", "kind": "sink"}
        ],
        "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
        "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
        "sinks": [{"operator_id": "sink", "output_type": "drop"}]
    });
    let (status, _) = post_json(
        &app,
        "/api/v1/jobs",
        "operator-secret",
        serde_json::json!({"spec": spec, "desired_state": "stopped"}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let session = register_hub_node(&app, "node-a").await;
    let mut report = agent_report_body(&session);
    report["jobs"] = serde_json::json!({
        "scoped-job": {
            "chains": {},
            "checkpoint_duration_ms": 7,
            "checkpoint_failures": 2,
            "watermark_lag_ms": 5,
            "late_events": 0
        }
    });
    report_node_raw(&app, report).await;

    let (status, detail) =
        get_json(&app, "/api/v1/jobs/scoped-job/detail", "operator-secret").await;
    assert_eq!(status, StatusCode::OK);
    let metrics = detail["metrics"].as_object().unwrap();
    assert_eq!(
        metrics
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        [
            "checkpoint_duration_ms",
            "checkpoint_failures",
            "watermark_lag_ms"
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<std::collections::BTreeSet<_>>()
    );
    assert_eq!(detail["metrics"]["watermark_lag_ms"], 5);
    let tasks = detail["tasks"].as_array().unwrap();
    assert!(!tasks.is_empty());
    assert!(tasks.iter().all(|task| task["observed"].is_boolean()));

    // Report one of the planned tasks as executing: it flips to observed
    // running state while the others stay marked not observed.
    let running_task = tasks[0]["task_id"].as_str().unwrap().to_owned();
    let mut report = agent_report_body(&session);
    report["job_tasks"] = serde_json::json!({"scoped-job": [running_task]});
    report_node_raw(&app, report).await;
    let (status, detail) =
        get_json(&app, "/api/v1/jobs/scoped-job/detail", "operator-secret").await;
    assert_eq!(status, StatusCode::OK);
    let tasks = detail["tasks"].as_array().unwrap();
    let observed = tasks
        .iter()
        .find(|task| task["task_id"] == running_task.as_str())
        .unwrap();
    assert_eq!(observed["observed"], true);
    assert_eq!(observed["state"], "running");
    assert!(tasks.len() > 1);
    assert!(tasks
        .iter()
        .filter(|task| task["task_id"] != running_task.as_str())
        .all(|task| task["observed"] == false));
}

// ------------------------------------------------------------------
// Coverage additions: hub HTTP surface, TLS/serve paths, and helpers.
// ------------------------------------------------------------------

fn storage_hub_config() -> hub::HubConfig {
    hub::HubConfig {
        operator_token: Some("operator".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: default_session_ttl_ms(),
    }
}

fn storage_hub() -> hub::Hub {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    hub::Hub::with_storage(storage_hub_config(), storage::StorageActor::start(store, 8))
}

fn scoped_operator_hub_config() -> hub::HubConfig {
    hub::HubConfig {
        operator_token: Some("readonly|viewer|viewer-secret".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: default_session_ttl_ms(),
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn put_json(
    app: &Router,
    path: &str,
    bearer: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::put(path)
                .header("authorization", format!("Bearer {bearer}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    read_json(response).await
}

/// Body-less request (DELETE, POST without payload) returning status+JSON.
async fn request_without_body(
    app: &Router,
    method: &str,
    path: &str,
    bearer: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut builder = axum::http::Request::builder().method(method).uri(path);
    if let Some(bearer) = bearer {
        builder = builder.header("authorization", format!("Bearer {bearer}"));
    }
    let response = app
        .clone()
        .oneshot(builder.body(axum::body::Body::empty()).unwrap())
        .await
        .unwrap();
    read_json(response).await
}

fn job_spec(id: &str, version: u64) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "version": version,
        "operators": [
            {"id": "source", "kind": "source"},
            {"id": "sink", "kind": "sink"}
        ],
        "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
        "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
        "sinks": [{"operator_id": "sink", "output_type": "drop"}]
    })
}

async fn create_job(app: &Router, id: &str) -> serde_json::Value {
    let (status, job) = post_json(
        app,
        "/api/v1/jobs",
        "operator",
        serde_json::json!({"spec": job_spec(id, 1), "desired_state": "stopped"}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "job create failed: {job}");
    job
}

async fn current_generation(app: &Router, job_id: &str) -> u64 {
    let (status, job) = get_json(app, &format!("/api/v1/jobs/{job_id}"), "operator").await;
    assert_eq!(status, StatusCode::OK);
    job["generation"].as_u64().unwrap()
}

/// Register an online node that can execute Jobs (placement needs the
/// runtime capabilities reported, not just the lease).
async fn register_job_node(app: &Router, node_id: &str) -> hub::RegisterResponse {
    let session = register_hub_node(app, node_id).await;
    let mut report = agent_report_body(&session);
    report["capabilities"] = serde_json::json!(["job_runtime", "state_backend", "metrics"]);
    report_node_raw(app, report).await;
    session
}

/// The `from_engine` mapping and startup address validation.
#[test]
fn server_config_from_engine_maps_health_settings_and_rejects_bad_addresses() {
    let health = NodeConfig {
        health: HealthEndpointsConfig {
            enabled: false,
            address: "127.0.0.1:9999".into(),
            health_path: "/hub-health".into(),
            readiness_path: "/hub-readiness".into(),
            liveness_path: "/hub-liveness".into(),
        },
        control_api: ControlApiConfig {
            api_prefix: "/api/v2".into(),
            cors_origins: vec!["https://console.example".into()],
            ..Default::default()
        },
        agent: AgentConfig {
            node_token: Some("node".into()),
            agent_lease_ttl_ms: 12_345,
            agent_session_ttl_ms: 678,
            ..Default::default()
        },
        ..NodeConfig::default()
    };
    let config = ServerConfig::from_engine(&EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: health,
    });
    assert!(!config.enabled);
    assert_eq!(config.address, "127.0.0.1:9999");
    assert_eq!(config.api_prefix, "/api/v2");
    assert_eq!(config.health_path, "/hub-health");
    assert_eq!(config.readiness_path, "/hub-readiness");
    assert_eq!(config.liveness_path, "/hub-liveness");
    assert_eq!(config.node_token.as_deref(), Some("node"));
    assert_eq!(config.lease_ttl_ms, 12_345);
    assert_eq!(config.session_ttl_ms, 678);
    assert_eq!(
        config.cors_origins,
        vec!["https://console.example".to_owned()]
    );
    assert!(config.tls_cert.is_none() && config.tls_key.is_none());
    assert!(!config.insecure_local && config.hub_storage.is_none());

    let local_hub = hub::Hub::new(hub::HubConfig {
        operator_token: None,
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 1_000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
    });
    let bad = ServerConfig {
        address: "not-a-socket-address".into(),
        insecure_local: true,
        ..ServerConfig::default()
    };
    assert!(bad
        .validate_hub_startup(&local_hub)
        .unwrap_err()
        .to_string()
        .contains("invalid Hub bind address"));
}

#[tokio::test]
async fn serve_is_a_no_op_when_disabled_and_serves_until_cancelled() {
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: NodeConfig::default(),
    });
    // Disabled: return before binding anything.
    let disabled = ServerConfig {
        enabled: false,
        ..ServerConfig::default()
    };
    serve(
        engine.control_plane(),
        disabled,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();

    // Enabled: serve until the cancellation token fires.
    let port = free_port();
    let enabled = ServerConfig {
        address: format!("127.0.0.1:{port}"),
        ..ServerConfig::default()
    };
    let cancellation = tokio_util::sync::CancellationToken::new();
    let shutdown = cancellation.clone();
    tokio::spawn(async move {
        serve(engine.control_plane(), enabled, shutdown)
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let mut served = false;
    for _ in 0..100 {
        if let Ok(response) = client
            .get(format!("http://127.0.0.1:{port}/health"))
            .send()
            .await
        {
            let _ = response.status();
            served = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(served, "the local server must answer /health");
    cancellation.cancel();
}

#[tokio::test]
async fn serve_observability_is_optional_and_never_blocks_the_data_plane() {
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: NodeConfig::default(),
    });
    // Disabled: returns immediately.
    let disabled = ServerConfig {
        observability: arkflow_core::config::ObservabilityConfig {
            enabled: false,
            ..Default::default()
        },
        ..ServerConfig::default()
    };
    serve_observability(
        engine.control_plane(),
        disabled,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();

    // Bind conflict: a taken port logs a warning and returns Ok —
    // observability must never block the data plane.
    let port = free_port();
    let _occupied = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
    let conflicted = ServerConfig {
        observability: arkflow_core::config::ObservabilityConfig {
            address: format!("127.0.0.1:{port}"),
            ..Default::default()
        },
        ..ServerConfig::default()
    };
    serve_observability(
        engine.control_plane(),
        conflicted,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();

    // Free port: probes are served until cancellation.
    let port = free_port();
    let enabled = ServerConfig {
        observability: arkflow_core::config::ObservabilityConfig {
            address: format!("127.0.0.1:{port}"),
            ..Default::default()
        },
        ..ServerConfig::default()
    };
    let cancellation = tokio_util::sync::CancellationToken::new();
    let shutdown = cancellation.clone();
    tokio::spawn(async move {
        serve_observability(engine.control_plane(), enabled, shutdown)
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let mut served = false;
    for _ in 0..100 {
        if let Ok(response) = client
            .get(format!("http://127.0.0.1:{port}/live"))
            .send()
            .await
        {
            assert_eq!(response.status(), StatusCode::OK);
            served = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(served, "the observability listener must answer /live");
    cancellation.cancel();
}

#[tokio::test]
async fn routers_attach_cors_headers_for_configured_origins() {
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: NodeConfig::default(),
    });
    let config = ServerConfig {
        cors_origins: vec!["https://console.example".into()],
        ..ServerConfig::default()
    };
    let app = router(engine.control_plane(), &config);
    let response = app
        .oneshot(
            axum::http::Request::get("/api/v1/system")
                .header(header::ORIGIN, "https://console.example")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .unwrap(),
        "https://console.example"
    );

    let app = hub_router(console_hub(), &config);
    let response = app
        .oneshot(
            axum::http::Request::get("/health")
                .header(header::ORIGIN, "https://console.example")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .unwrap(),
        "https://console.example"
    );
}

/// Isolated TLS material: unlike `hub_tls_material` (which shares one
/// pid-keyed directory across tests and cleans it up), each caller owns a
/// unique tempdir so parallel TLS tests cannot race each other's files.
fn isolated_tls_material() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    use rcgen::CertificateParams;
    use rcgen::KeyPair;
    let params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    let key = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    (dir, cert_path, key_path)
}

#[tokio::test]
async fn hub_tls_startup_rejects_unreadable_and_empty_material() {
    let (_dir, cert_path, key_path) = isolated_tls_material();
    let local_hub = || {
        hub::Hub::new(hub::HubConfig {
            operator_token: None,
            node_token: None,
            insecure_local: true,
            lease_ttl_ms: 1_000,
            poll_interval_ms: 10,
            session_ttl_ms: default_session_ttl_ms(),
        })
    };
    let base = || ServerConfig {
        address: "127.0.0.1:0".into(),
        insecure_local: true,
        ..ServerConfig::default()
    };
    // Certificate file that does not exist.
    let config = ServerConfig {
        tls_cert: Some("/nonexistent/cert.pem".into()),
        tls_key: Some(key_path.to_string_lossy().into_owned()),
        ..base()
    };
    let error = serve_hub(
        local_hub(),
        config,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("could not be read"), "{error}");

    // Certificate file without any PEM certificate entries.
    let dir = tempfile::tempdir().unwrap();
    let empty_cert = dir.path().join("empty-cert.pem");
    std::fs::write(&empty_cert, "not a pem file").unwrap();
    let config = ServerConfig {
        tls_cert: Some(empty_cert.to_string_lossy().into_owned()),
        tls_key: Some(key_path.to_string_lossy().into_owned()),
        ..base()
    };
    let error = serve_hub(
        local_hub(),
        config,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("no PEM certificates"), "{error}");

    // Key file that does not exist.
    let config = ServerConfig {
        tls_cert: Some(cert_path.to_string_lossy().into_owned()),
        tls_key: Some("/nonexistent/key.pem".into()),
        ..base()
    };
    let error = serve_hub(
        local_hub(),
        config,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("could not be read"), "{error}");

    // Key file without a PEM private key.
    let empty_key = dir.path().join("empty-key.pem");
    std::fs::write(&empty_key, "still not a pem file").unwrap();
    let config = ServerConfig {
        tls_cert: Some(cert_path.to_string_lossy().into_owned()),
        tls_key: Some(empty_key.to_string_lossy().into_owned()),
        ..base()
    };
    let error = serve_hub(
        local_hub(),
        config,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("no PEM private key"), "{error}");
}

/// A plaintext (or garbage) peer fails the TLS handshake; the listener
/// logs it and keeps accepting instead of tearing down the accept loop.
#[tokio::test]
async fn hub_tls_listener_survives_a_failed_handshake() {
    use axum::serve::Listener as _;
    use tokio::io::AsyncWriteExt;
    let (_dir, cert_path, key_path) = isolated_tls_material();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert_pem = std::fs::read_to_string(&cert_path).unwrap();
    let key_pem = std::fs::read_to_string(&key_path).unwrap();
    let chain = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .map(|item| item.unwrap())
        .collect::<Vec<_>>();
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
        .unwrap()
        .unwrap();
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap();
    let inner = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = inner.local_addr().unwrap();
    let mut listener = HubTlsListener::spawn(
        inner,
        tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_config)),
    )
    .unwrap();
    assert!(listener.local_addr().is_ok());

    let accept_task = tokio::spawn(async move {
        use axum::serve::Listener as _;
        listener.accept().await
    });
    // First peer speaks plaintext HTTP: the handshake rejects it.
    let mut plaintext = tokio::net::TcpStream::connect(address).await.unwrap();
    plaintext
        .write_all(b"GET /readiness HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    drop(plaintext);
    // Second peer completes a real TLS handshake, so accept_one returns.
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap();
    let _ = client
        .get(format!("https://{address}/liveness"))
        .send()
        .await;
    let (_stream, peer) = tokio::time::timeout(std::time::Duration::from_secs(3), accept_task)
        .await
        .expect("accept returns after a healthy handshake")
        .unwrap();
    assert_eq!(peer.ip(), address.ip());
}

#[tokio::test]
async fn hub_job_read_routes_report_missing_and_corrupt_records() {
    let hub = storage_hub();
    let app = hub_router(hub.clone(), &ServerConfig::default());
    let (status, jobs) = get_json(&app, "/api/v1/jobs", "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(jobs.as_array().unwrap().len(), 0);

    create_job(&app, "readable-job").await;
    let (status, plan) = get_json(&app, "/api/v1/jobs/readable-job/plan", "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(plan["job_id"], "readable-job");
    assert!(plan["plan"].is_object());

    for path in [
        "/api/v1/jobs/missing-job",
        "/api/v1/jobs/missing-job/plan",
        "/api/v1/jobs/missing-job/detail",
        "/api/v1/jobs/missing-job/versions",
        "/api/v1/nodes/node-a/streams/not-there",
    ] {
        let (status, body) = get_json(&app, path, "operator").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}: {body}");
    }

    // Corrupt persisted specs surface as 500, never a handler panic.
    hub.upsert_job(storage::JobRecord {
        job_id: "corrupt-job".into(),
        version: 1,
        spec_json: "not json".into(),
        desired_state: "stopped".into(),
        observed_state: "stopped".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: Vec::new(),
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: hub::now_ms_for_metrics(),
    })
    .await
    .unwrap();
    let (status, body) = get_json(&app, "/api/v1/jobs/corrupt-job/plan", "operator").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["code"], "invalid_persisted_job");
    let (status, body) = get_json(&app, "/api/v1/jobs/corrupt-job/detail", "operator").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");

    // A spec that parses but cannot compile its plan fails closed as 422.
    let mut unpluckable = job_spec("planless-job", 1);
    unpluckable["sources"] = serde_json::json!([]);
    hub.upsert_job(storage::JobRecord {
        job_id: "planless-job".into(),
        version: 1,
        spec_json: unpluckable.to_string(),
        desired_state: "stopped".into(),
        observed_state: "stopped".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: Vec::new(),
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: hub::now_ms_for_metrics(),
    })
    .await
    .unwrap();
    let (status, body) = get_json(&app, "/api/v1/jobs/planless-job/plan", "operator").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "invalid_job_plan");
    let (status, body) = get_json(&app, "/api/v1/jobs/planless-job/detail", "operator").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

#[tokio::test]
async fn hub_job_action_and_recovery_artifact_routes_validate_input() {
    let hub = storage_hub();
    let app = hub_router(hub.clone(), &ServerConfig::default());
    let session = register_job_node(&app, "node-a").await;

    create_job(&app, "acted-job").await;
    // Unknown action and unknown Job.
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/acted-job/actions/pause",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_job_action");
    let (status, _) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/missing/actions/start",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/missing/savepoints",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Start converges the desired state and is audited.
    let (status, job) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/acted-job/actions/start",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{job}");
    assert_eq!(job["desired_state"], "running");
    let (status, job) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/acted-job/actions/stop",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{job}");
    assert_eq!(job["desired_state"], "stopped");

    // A savepoint trigger is accepted, audited once, and listed.
    let (status, job) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/acted-job/savepoints",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{job}");
    assert!(hub
        .audit(Some("acted-job"))
        .await
        .unwrap()
        .iter()
        .any(|record| record.action == "job.savepoint" && record.outcome == "accepted"));
    let (status, checkpoints) =
        get_json(&app, "/api/v1/jobs/acted-job/checkpoints", "operator").await;
    assert_eq!(status, StatusCode::OK);
    let checkpoints = checkpoints.as_array().unwrap();
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0]["kind"], "savepoint");
    assert_eq!(checkpoints[0]["status"], "pending");

    // Detail with node-scoped placement filters the node view and marks
    // unobserved tasks.
    let (status, detail) = get_json(&app, "/api/v1/jobs/acted-job/detail", "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["nodes"].as_array().unwrap().len(), 1);

    // Job observation from the executing node flips observed state.
    let (status, _) = post_json(
        &app,
        "/api/v1/agent/job-observations",
        &session.session_token,
        serde_json::json!({
            "node_id": "node-a",
            "session_token": session.session_token,
            "job_id": "acted-job",
            "generation": detail["job"]["generation"],
            "state": "stopped",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, versions) = get_json(&app, "/api/v1/jobs/acted-job/versions", "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(versions.as_array().unwrap().len(), 1);

    // Viewer credentials cannot operate Jobs.
    let viewer_hub = hub::Hub::new(scoped_operator_hub_config());
    let viewer_app = hub_router(viewer_hub, &ServerConfig::default());
    let (status, _) = request_without_body(
        &viewer_app,
        "POST",
        "/api/v1/jobs/acted-job/actions/start",
        Some("viewer-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn hub_job_detail_reports_split_placement_violations() {
    let hub = storage_hub();
    let app = hub_router(hub.clone(), &ServerConfig::default());
    register_hub_node(&app, "node-a").await;
    register_hub_node(&app, "node-b").await;
    // A split placement whose error-route side edge would cross nodes.
    let split_spec = serde_json::json!({
        "id": "split-job",
        "version": 1,
        "placement": "split",
        "operators": [
            {"id": "src", "kind": "source"},
            {"id": "map", "kind": "map"},
            {"id": "err", "kind": "sink", "config": {"__arkflow_error_sink": true}},
            {"id": "out", "kind": "sink"}
        ],
        "edges": [
            {"id": "e1", "from": "src", "to": "map"},
            {"id": "e2", "from": "map", "to": "err"},
            {"id": "e3", "from": "map", "to": "out"}
        ],
        "sources": [{"operator_id": "src", "input_type": "memory", "time": {"mode": "processing_time"}}],
        "sinks": [
            {"operator_id": "out", "output_type": "drop"},
            {"operator_id": "err", "output_type": "drop"}
        ]
    });
    hub.upsert_job(storage::JobRecord {
        job_id: "split-job".into(),
        version: 1,
        spec_json: split_spec.to_string(),
        desired_state: "stopped".into(),
        observed_state: "stopped".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec!["node-a".into(), "node-b".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: hub::now_ms_for_metrics(),
    })
    .await
    .unwrap();
    let (status, body) = get_json(&app, "/api/v1/jobs/split-job/detail", "operator").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "invalid_placement");
}

#[tokio::test]
async fn hub_validate_job_route_reports_spec_errors_and_node_compatibility() {
    let hub = storage_hub();
    let app = hub_router(hub, &ServerConfig::default());
    register_job_node(&app, "node-a").await;

    // A spec that does not parse.
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/validate",
        "operator",
        serde_json::json!({"spec": {"garbage": true}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_job_spec");

    // A spec that parses but violates the Job contract.
    let mut empty = job_spec("validate-job", 1);
    empty["sources"] = serde_json::json!([]);
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/validate",
        "operator",
        serde_json::json!({"spec": empty}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "invalid_job_spec");

    // A structurally valid spec with an unknown component fails deep
    // validation (the same construction path local execution uses).
    let mut unknown = job_spec("validate-job", 1);
    unknown["sources"][0]["input_type"] = serde_json::json!("not-a-real-input");
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/validate",
        "operator",
        serde_json::json!({"spec": unknown}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "invalid_job_plan");

    // Node selection drives the compatibility matrix.
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/validate",
        "operator",
        serde_json::json!({"spec": job_spec("validate-job", 1), "node_ids": ["node-a"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["valid"], true);
    assert_eq!(body["nodes"].as_array().unwrap().len(), 1);
    assert_eq!(body["nodes"][0]["compatible"], true);
    assert_eq!(body["nodes"][0]["node_id"], "node-a");

    // Selecting only unknown nodes leaves an empty matrix with a warning.
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/validate",
        "operator",
        serde_json::json!({"spec": job_spec("validate-job", 1), "node_ids": ["missing"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["valid"], true);
    assert_eq!(body["nodes"].as_array().unwrap().len(), 0);
    assert_eq!(body["warnings"][0], "No online compute nodes selected");

    // An online node without the runtime capabilities is incompatible.
    let session_b = register_hub_node(&app, "node-b").await;
    let mut report = agent_report_body(&session_b);
    report["capabilities"] = serde_json::json!(["metrics"]);
    report_node_raw(&app, report).await;
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/validate",
        "operator",
        serde_json::json!({"spec": job_spec("validate-job", 1), "node_ids": ["node-b"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["valid"], false);
    assert_eq!(body["nodes"][0]["compatible"], false);
}

#[tokio::test]
async fn hub_create_job_and_desired_state_route_validation() {
    let hub = storage_hub();
    let app = hub_router(hub, &ServerConfig::default());
    register_job_node(&app, "node-a").await;
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs",
        "operator",
        serde_json::json!({"spec": {"garbage": true}, "desired_state": "stopped"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_job_spec");

    let mut invalid = job_spec("created-job", 1);
    invalid["sources"] = serde_json::json!([]);
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs",
        "operator",
        serde_json::json!({"spec": invalid, "desired_state": "stopped"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_job_spec");

    let mut unknown_component = job_spec("created-job", 1);
    unknown_component["sources"][0]["input_type"] = serde_json::json!("not-a-real-input");
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs",
        "operator",
        serde_json::json!({"spec": unknown_component, "desired_state": "stopped"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_job_plan");

    let (status, body) = post_json(
        &app,
        "/api/v1/jobs",
        "operator",
        serde_json::json!({"spec": job_spec("created-job", 1), "desired_state": "paused"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_job_state");

    create_job(&app, "created-job").await;
    // Desired-state route validation.
    let (status, body) = put_json(
        &app,
        "/api/v1/jobs/created-job/desired-state",
        "operator",
        serde_json::json!({"state": "bogus"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_job_state");
    let (status, body) = put_json(
        &app,
        "/api/v1/jobs/missing/desired-state",
        "operator",
        serde_json::json!({"state": "running"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let (status, body) = put_json(
        &app,
        "/api/v1/jobs/created-job/desired-state",
        "operator",
        serde_json::json!({"state": "stopped"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["desired_state"], "stopped");

    let viewer_app = hub_router(
        hub::Hub::new(scoped_operator_hub_config()),
        &ServerConfig::default(),
    );
    let (status, _) = post_json(
        &viewer_app,
        "/api/v1/jobs",
        "viewer-secret",
        serde_json::json!({"spec": job_spec("created-job", 2), "desired_state": "stopped"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = put_json(
        &viewer_app,
        "/api/v1/jobs/created-job/desired-state",
        "viewer-secret",
        serde_json::json!({"state": "running"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// The stopped-mode upgrade: guard rails first, then the happy path that
/// swaps in a new version pinned to a completed savepoint.
#[tokio::test]
async fn hub_job_upgrade_stopped_mode_guards_and_commit() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(
        storage_hub_config(),
        storage::StorageActor::start(store.clone(), 8),
    );
    let app = hub_router(hub, &ServerConfig::default());
    create_job(&app, "upgraded-job").await;

    let upgrade = |spec: serde_json::Value,
                   expected_generation: u64,
                   mode: Option<&str>,
                   savepoint_id: Option<&str>| {
        let mut body =
            serde_json::json!({"spec": spec, "expected_generation": expected_generation});
        if let Some(mode) = mode {
            body["mode"] = serde_json::json!(mode);
        }
        if let Some(id) = savepoint_id {
            body["savepoint_id"] = serde_json::json!(id);
        }
        body
    };

    // Stale generation fence.
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/upgraded-job/upgrades",
        "operator",
        upgrade(job_spec("upgraded-job", 2), 999, None, None),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "generation_conflict");
    assert_eq!(body["details"]["current"], 1);

    // Unknown mode.
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/upgraded-job/upgrades",
        "operator",
        upgrade(job_spec("upgraded-job", 2), 1, Some("blue-green"), None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_upgrade_mode");

    // Spec shape problems.
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/upgraded-job/upgrades",
        "operator",
        upgrade(serde_json::json!({"no": "spec"}), 1, None, None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_job_spec");
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/upgraded-job/upgrades",
        "operator",
        upgrade(job_spec("other-job", 2), 1, None, None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_job_spec");
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/upgraded-job/upgrades",
        "operator",
        upgrade(job_spec("upgraded-job", 1), 1, None, None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_job_version");

    let mut unpluckable = job_spec("upgraded-job", 2);
    unpluckable["sources"] = serde_json::json!([]);
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/upgraded-job/upgrades",
        "operator",
        upgrade(unpluckable, 1, None, None),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "invalid_job_plan");

    let mut unknown_component = job_spec("upgraded-job", 2);
    unknown_component["sources"][0]["input_type"] = serde_json::json!("not-a-real-input");
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/upgraded-job/upgrades",
        "operator",
        upgrade(unknown_component, 1, None, None),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "invalid_job_plan");

    // No completed savepoint yet.
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/upgraded-job/upgrades",
        "operator",
        upgrade(job_spec("upgraded-job", 2), 1, None, Some("sp-missing")),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "savepoint_not_ready");

    // Trigger a savepoint; still pending, so still not ready.
    request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/upgraded-job/savepoints",
        Some("operator"),
    )
    .await;
    let (_, checkpoints) =
        get_json(&app, "/api/v1/jobs/upgraded-job/checkpoints", "operator").await;
    let savepoint_id = checkpoints[0]["checkpoint_id"].as_str().unwrap().to_owned();
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/upgraded-job/upgrades",
        "operator",
        upgrade(job_spec("upgraded-job", 2), 1, None, Some(&savepoint_id)),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "savepoint_not_ready");

    // Complete the artifact; a state-format mismatch is rejected.
    store
        .with_connection(|connection| {
            connection.execute("UPDATE cp_job_checkpoints SET status = 'completed'", [])?;
            Ok(())
        })
        .unwrap();
    let mut new_format = job_spec("upgraded-job", 2);
    new_format["state"] = serde_json::json!({"backend": "embedded_kv", "durability": "ephemeral", "format_version": 2});
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/upgraded-job/upgrades",
        "operator",
        upgrade(new_format, 1, None, Some(&savepoint_id)),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "state_format_incompatible");

    // Happy path: version bump pinned to the completed savepoint.
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/upgraded-job/upgrades",
        "operator",
        upgrade(job_spec("upgraded-job", 2), 1, None, Some(&savepoint_id)),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["state"], "pending_recovery");
    assert_eq!(body["savepoint_id"], savepoint_id.as_str());
    assert_eq!(body["job"]["version"], 2);

    // Version rollback: explicit target, missing target, and exhaustion.
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/upgraded-job/upgrades/restore-v99/rollback",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "no_previous_job_version");
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/upgraded-job/upgrades/restore-v1/rollback",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["version"], 1);
    // v1 is now current: no version below it remains.
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/upgraded-job/upgrades/opaque-upgrade-id/rollback",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "no_previous_job_version");
    // Rolling back an unknown Job is a 404.
    let (status, _) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/missing/upgrades/restore-v1/rollback",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Viewer credentials cannot upgrade or roll back.
    let viewer_app = hub_router(
        hub::Hub::new(scoped_operator_hub_config()),
        &ServerConfig::default(),
    );
    let generation = current_generation(&app, "upgraded-job").await;
    let (status, _) = post_json(
        &viewer_app,
        "/api/v1/jobs/upgraded-job/upgrades",
        "viewer-secret",
        upgrade(
            job_spec("upgraded-job", 2),
            generation,
            None,
            Some(&savepoint_id),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// Rollback refuses a previous version whose state format is
/// incompatible with the artifact the Job would actually restore.
#[tokio::test]
async fn hub_job_rollback_rejects_state_format_mismatch() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(
        storage_hub_config(),
        storage::StorageActor::start(store.clone(), 8),
    );
    let app = hub_router(hub.clone(), &ServerConfig::default());
    create_job(&app, "format-job").await;
    request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/format-job/savepoints",
        Some("operator"),
    )
    .await;
    // Artifact written under format 2, previous version spec is format 1.
    store
        .with_connection(|connection| {
            connection.execute(
                "UPDATE cp_job_checkpoints SET status = 'completed', format_version = 2",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    let (_, checkpoints) = get_json(&app, "/api/v1/jobs/format-job/checkpoints", "operator").await;
    let checkpoint_id = checkpoints[0]["checkpoint_id"].as_str().unwrap().to_owned();
    let mut version_two = job_spec("format-job", 2);
    version_two["state"] = serde_json::json!({"backend": "embedded_kv", "durability": "ephemeral", "format_version": 2});
    hub.upsert_job(storage::JobRecord {
        job_id: "format-job".into(),
        version: 2,
        spec_json: version_two.to_string(),
        desired_state: "stopped".into(),
        observed_state: "stopped".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: Vec::new(),
        checkpoint_id: Some(checkpoint_id),
        last_error: None,
        updated_at_ms: hub::now_ms_for_metrics(),
    })
    .await
    .unwrap();
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/format-job/upgrades/restore-v1/rollback",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "state_format_incompatible");
}

/// The atomic upgrade orchestration: exclusivity, status reads, and the
/// pause/resume/cancel action surface.
#[tokio::test]
async fn hub_job_atomic_upgrade_orchestration_lifecycle() {
    let hub = storage_hub();
    let app = hub_router(hub.clone(), &ServerConfig::default());
    register_job_node(&app, "node-a").await;
    create_job(&app, "atomic-job").await;

    // Atomic mode requires a running Job.
    let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("atomic-job", 2), "expected_generation": 1, "mode": "atomic"}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "job_must_be_running");

    // The stopped mode requires a stopped Job (this one will be running).
    let (status, _) = put_json(
        &app,
        "/api/v1/jobs/atomic-job/desired-state",
        "operator",
        serde_json::json!({"state": "running"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let generation = current_generation(&app, "atomic-job").await;
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/atomic-job/upgrades",
        "operator",
        serde_json::json!({"spec": job_spec("atomic-job", 2), "expected_generation": generation}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "job_must_be_stopped");

    // Atomic mode with an unparseable spec.
    let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": {"garbage": true}, "expected_generation": generation, "mode": "atomic"}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_job_spec");
    // Wrong identity and non-monotonic versions.
    let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("other-job", 2), "expected_generation": generation, "mode": "atomic"}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("atomic-job", 1), "expected_generation": generation, "mode": "atomic"}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    // Successful atomic upgrade creation.
    let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("atomic-job", 2), "expected_generation": generation, "mode": "atomic", "verify_timeout_ms": 5000}),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let upgrade_id = body["upgrade_id"].as_str().unwrap().to_owned();

    // Exclusivity: a second orchestration cannot own the Job, and job
    // mutations are fenced while the upgrade is active.
    let (status, body) = post_json(
            &app,
            "/api/v1/jobs/atomic-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("atomic-job", 3), "expected_generation": current_generation(&app, "atomic-job").await, "mode": "atomic"}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "orchestration_in_progress");
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/atomic-job/actions/stop",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "orchestration_in_progress");

    // Status reads: known id, foreign Job, unknown id.
    let (status, record) = get_json(
        &app,
        &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}"),
        "operator",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(record["upgrade_id"], upgrade_id.as_str());
    let (status, _) = get_json(
        &app,
        "/api/v1/jobs/atomic-job/upgrades/unknown-upgrade",
        "operator",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = get_json(
        &app,
        &format!("/api/v1/jobs/someone-else/upgrades/{upgrade_id}"),
        "operator",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // The detail view exposes the active upgrade without its bulk.
    let (status, detail) = get_json(&app, "/api/v1/jobs/atomic-job/detail", "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["active_upgrade"]["upgrade_id"], upgrade_id.as_str());
    assert!(detail["active_upgrade"].get("target_spec_json").is_none());

    // Actions: validation, 404s, pause/resume flows.
    let (status, body) = post_json(
        &app,
        &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}/actions"),
        "operator",
        serde_json::json!({"action": "bogus"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_job_upgrade_action");
    let (status, _) = post_json(
        &app,
        "/api/v1/jobs/atomic-job/upgrades/unknown-upgrade/actions",
        "operator",
        serde_json::json!({"action": "pause"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = post_json(
        &app,
        &format!("/api/v1/jobs/someone-else/upgrades/{upgrade_id}/actions"),
        "operator",
        serde_json::json!({"action": "pause"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, record) = post_json(
        &app,
        &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}/actions"),
        "operator",
        serde_json::json!({"action": "pause"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{record}");
    assert_eq!(record["phase"], "paused");
    // Pausing twice is a phase conflict surfaced as a retryable 409.
    let (status, body) = post_json(
        &app,
        &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}/actions"),
        "operator",
        serde_json::json!({"action": "pause"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "job_upgrade_action_rejected");
    let (status, record) = post_json(
        &app,
        &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}/actions"),
        "operator",
        serde_json::json!({"action": "resume"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{record}");
    assert_ne!(record["phase"], "paused");
    let (status, record) = post_json(
        &app,
        &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}/actions"),
        "operator",
        serde_json::json!({"action": "cancel"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{record}");
    assert_eq!(record["phase"], "cancelled");
    // A terminal orchestration no longer fences job mutations.
    assert!(reject_if_job_upgrade_active(&hub, "atomic-job")
        .await
        .is_ok());

    // Viewer credentials are authorized for reads (the upgrade is simply
    // absent from this separate Hub) but cannot drive actions.
    let viewer_store = storage::ControlPlaneStore::in_memory().unwrap();
    let viewer_app = hub_router(
        hub::Hub::with_storage(
            scoped_operator_hub_config(),
            storage::StorageActor::start(viewer_store, 8),
        ),
        &ServerConfig::default(),
    );
    let (status, _) = get_json(
        &viewer_app,
        &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}"),
        "viewer-secret",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = post_json(
        &viewer_app,
        &format!("/api/v1/jobs/atomic-job/upgrades/{upgrade_id}/actions"),
        "viewer-secret",
        serde_json::json!({"action": "pause"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn reject_if_job_upgrade_active_fences_while_orchestration_owns_the_job() {
    let hub = storage_hub();
    let app = hub_router(hub.clone(), &ServerConfig::default());
    register_job_node(&app, "node-a").await;
    create_job(&app, "fenced-job").await;
    assert!(reject_if_job_upgrade_active(&hub, "fenced-job")
        .await
        .is_ok());
    put_json(
        &app,
        "/api/v1/jobs/fenced-job/desired-state",
        "operator",
        serde_json::json!({"state": "running"}),
    )
    .await;
    let generation = current_generation(&app, "fenced-job").await;
    let (status, _) = post_json(
            &app,
            "/api/v1/jobs/fenced-job/upgrades",
            "operator",
            serde_json::json!({"spec": job_spec("fenced-job", 2), "expected_generation": generation, "mode": "atomic"}),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let response = reject_if_job_upgrade_active(&hub, "fenced-job")
        .await
        .unwrap_err();
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn rollout_routes_manage_staged_configuration_rollouts() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(
        storage_hub_config(),
        storage::StorageActor::start(store.clone(), 8),
    );
    let app = hub_router(hub.clone(), &ServerConfig::default());
    register_hub_node(&app, "node-a").await;

    // Validation failures.
    let (status, body) = post_json(
        &app,
        "/api/v1/rollouts",
        "operator",
        serde_json::json!({"config_version": "v1", "node_ids": [], "batch_size": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_rollout");
    let (status, body) = post_json(
            &app,
            "/api/v1/rollouts",
            "operator",
            serde_json::json!({"config_version": "missing-version", "node_ids": ["node-a"], "batch_size": 1}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_rollout");

    // Seed a durable config version, then create/list/read/act.
    hub.create_rollout_with_content(
        "cfg-seed".into(),
        "{}".into(),
        vec!["node-a".into()],
        1,
        None,
        None,
    )
    .await
    .unwrap();
    let (status, rollout) = post_json(
            &app,
            "/api/v1/rollouts",
            "operator",
            serde_json::json!({"config_version": "cfg-seed", "node_ids": ["node-a"], "batch_size": 1, "correlation_id": null}),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{rollout}");
    assert_eq!(rollout["state"], "pending");
    let rollout_id = rollout["rollout_id"].as_str().unwrap().to_owned();

    let (status, rollouts) = get_json(&app, "/api/v1/rollouts", "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert!(rollouts
        .as_array()
        .unwrap()
        .iter()
        .any(|item| { item["rollout_id"].as_str() == Some(rollout_id.as_str()) }));

    let (status, fetched) =
        get_json(&app, &format!("/api/v1/rollouts/{rollout_id}"), "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["rollout"]["rollout_id"], rollout_id.as_str());
    assert_eq!(fetched["targets"].as_array().unwrap().len(), 1);
    let (status, _) = get_json(&app, "/api/v1/rollouts/missing", "operator").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Lifecycle actions.
    let (status, paused) = post_json(
        &app,
        &format!("/api/v1/rollouts/{rollout_id}/actions"),
        "operator",
        serde_json::json!({"action": "pause"}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{paused}");
    assert_eq!(paused["state"], "paused");
    let (status, resumed) = post_json(
        &app,
        &format!("/api/v1/rollouts/{rollout_id}/actions"),
        "operator",
        serde_json::json!({"action": "resume"}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{resumed}");
    assert_eq!(resumed["state"], "applying");
    let (status, body) = post_json(
        &app,
        &format!("/api/v1/rollouts/{rollout_id}/actions"),
        "operator",
        serde_json::json!({"action": "teleport"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "invalid_rollout_action");
    let (status, _) = post_json(
        &app,
        "/api/v1/rollouts/missing/actions",
        "operator",
        serde_json::json!({"action": "pause"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // Viewer credentials cannot manage rollouts.
    let viewer_app = hub_router(
        hub::Hub::new(scoped_operator_hub_config()),
        &ServerConfig::default(),
    );
    let (status, _) = post_json(
        &viewer_app,
        "/api/v1/rollouts",
        "viewer-secret",
        serde_json::json!({"config_version": "cfg-seed", "node_ids": ["node-a"], "batch_size": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn hub_operations_and_events_routes_support_filters() {
    let hub = storage_hub();
    let app = hub_router(hub.clone(), &ServerConfig::default());
    register_hub_node(&app, "node-a").await;

    // Enqueue a cancellable command through the read-only channel.
    let (status, operation) = post_json(
        &app,
        "/api/v1/nodes/node-a/configuration/validate",
        "operator",
        serde_json::json!({"format": "yaml", "content": "streams: []\n"}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let operation_id = operation["id"].as_str().unwrap().to_owned();

    // An intent-backed operation feeds the filters.
    put_json(
        &app,
        "/api/v1/nodes/node-a/streams/orders/desired-state",
        "operator",
        serde_json::json!({"state": "running"}),
    )
    .await;

    let (status, page) = get_json(
        &app,
        "/api/v1/operations?resource_id=orders&operation=reconcile",
        "operator",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(page["total"].as_u64().unwrap() >= 1, "{page}");
    let (status, page) = get_json(
        &app,
        "/api/v1/operations?node_id=node-a&page=1&page_size=2",
        "operator",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(page["total"].as_u64().unwrap() >= 1);
    let (status, body) = get_json(&app, "/api/v1/operations/missing", "operator").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], "operation_not_found");

    // Cancel the queued command; cancelling twice is idempotent.
    let (status, cancelled) = request_without_body(
        &app,
        "DELETE",
        &format!("/api/v1/operations/{operation_id}"),
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{cancelled}");
    let (status, _) = request_without_body(
        &app,
        "DELETE",
        &format!("/api/v1/operations/{operation_id}"),
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = request_without_body(
        &app,
        "DELETE",
        "/api/v1/operations/missing",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Event filters narrow by type, stream, and correlation id.
    let (status, all) = get_json(&app, "/api/v1/events?node_id=node-a", "operator").await;
    assert_eq!(status, StatusCode::OK);
    let items = all["items"].as_array().unwrap();
    assert!(!items.is_empty(), "events: {all}");
    let event_type = items[0]["event_type"].as_str().unwrap().to_owned();
    let (status, filtered) = get_json(
        &app,
        &format!("/api/v1/events?event_type={event_type}&stream_id=orders"),
        "operator",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(filtered["total"].as_u64().unwrap() <= all["total"].as_u64().unwrap());
    let (status, none) =
        get_json(&app, "/api/v1/events?correlation_id=never-used", "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(none["total"], 0);

    // Viewer cannot cancel.
    let viewer_app = hub_router(
        hub::Hub::new(scoped_operator_hub_config()),
        &ServerConfig::default(),
    );
    let (status, _) = request_without_body(
        &viewer_app,
        "DELETE",
        &format!("/api/v1/operations/{operation_id}"),
        Some("viewer-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Agent endpoints reject missing session credentials.
    let (status, _) =
        request_without_body(&app, "GET", "/api/v1/agent/commands?node_id=node-a", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = post_json(
        &app,
        &format!("/api/v1/agent/commands/{operation_id}/result?node_id=node-a"),
        "",
        serde_json::json!({
            "command_id": "cmd", "operation_id": operation_id, "state": "succeeded", "progress": 100
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn sse_stream_replays_the_gap_marker_and_delivers_live_events() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_events (node_id, event_type, outcome, occurred_at_ms) VALUES ('node-a', 'rollout_changed', 'accepted', 10)",
                    [],
                )?;
                connection.execute(
                    "INSERT INTO cp_events (node_id, event_type, outcome, occurred_at_ms) VALUES ('node-a', 'node_registered', 'accepted', 11)",
                    [],
                )?;
                // Leave a gap so the replay starts with a resync marker.
                connection.execute("DELETE FROM cp_events WHERE event_id = 1", [])?;
                Ok(())
            })
            .unwrap();
    let hub = hub::Hub::with_storage(storage_hub_config(), storage::StorageActor::start(store, 8));
    let app = hub_router(hub.clone(), &ServerConfig::default());
    // Agent-reported events are the live broadcast source; register the
    // node so a report can drive one after the replay drains.
    let session = register_hub_node(&app, "node-a").await;
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/events/stream")
                .header("authorization", "Bearer operator")
                .header("last-event-id", "0")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let chunk = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let text = String::from_utf8(chunk.to_vec()).unwrap();
    assert!(text.contains("event: resync"), "{text}");

    // After the replay drains, live broadcasts continue the stream.
    let live = std::sync::Arc::new(tokio::sync::Notify::new());
    let live_task = live.clone();
    let driver_app = app.clone();
    let session_token = session.session_token.clone();
    tokio::spawn(async move {
        // Wait until the SSE consumer is polled before publishing.
        live_task.notified().await;
        let response = driver_app
            .oneshot(
                axum::http::Request::post("/api/v1/agent/report")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "node_id": "node-a",
                            "session_token": session_token,
                            "version": "test-node",
                            "state": "online",
                            "events": [{
                                "occurred_at_ms": 12,
                                "event_type": "stream_started",
                                "stream_id": "orders",
                                "outcome": "accepted",
                                "message": null,
                                "operation_id": null,
                                "correlation_id": "sse-live",
                                "actor": null
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    });
    let mut saw_replayed = false;
    let mut saw_live = false;
    for _ in 0..16 {
        if !saw_replayed {
            live.notify_one();
        }
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(2), body.next());
        match chunk.await {
            Ok(Some(Ok(chunk))) => {
                let text = String::from_utf8(chunk.to_vec()).unwrap();
                if text.contains("id: 2") {
                    saw_replayed = true;
                }
                if text.contains("stream_started") {
                    saw_live = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(saw_replayed, "the durable replay must arrive");
    assert!(saw_live, "a live event must follow the replay");
}

#[tokio::test]
async fn hub_metrics_and_operational_status_cover_fleet_and_rollout_state() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(storage_hub_config(), storage::StorageActor::start(store, 8));
    let app = hub_router(hub.clone(), &ServerConfig::default());
    let session = register_hub_node(&app, "node-a").await;
    let mut report = agent_report_body(&session);
    report["capabilities"] = serde_json::json!(["job_runtime", "metrics"]);
    report["metrics"] = serde_json::json!({"input_batches": 5.0});
    report["jobs"] = serde_json::json!({
        "metrics-job": {
            "chains": {},
            "checkpoint_duration_ms": 3,
            "checkpoint_failures": 0,
            "watermark_lag_ms": 1,
            "late_events": 0
        }
    });
    report_node_raw(&app, report).await;
    put_json(
        &app,
        "/api/v1/nodes/node-a/streams/orders/desired-state",
        "operator",
        serde_json::json!({"state": "running"}),
    )
    .await;
    hub.create_rollout_with_content(
        "cfg-metrics".into(),
        "{}".into(),
        vec!["node-a".into()],
        1,
        None,
        None,
    )
    .await
    .unwrap();

    // Prometheus text exposition.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/metrics")
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    for family in [
        "arkflow_control_plane_ready",
        "arkflow_reconciliation_runs_total",
        "arkflow_intents_state",
        "arkflow_nodes_state",
        "arkflow_rollouts_state",
        "arkflow_node_compatibility",
        "arkflow_node_capability",
        "arkflow_node_metric",
        "arkflow_job_",
    ] {
        assert!(text.contains(family), "missing family {family} in:\n{text}");
    }

    // The operational status aggregate.
    let (status, body) = get_json(&app, "/api/v1/operations/status", "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["rollouts"].is_object());
    assert_eq!(body["rollouts"]["total"], 1);
    assert_eq!(body["compatibility"]["nodes"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn hub_maintenance_routes_manage_node_lifecycle_state() {
    let hub = storage_hub();
    let app = hub_router(hub, &ServerConfig::default());
    register_hub_node(&app, "node-a").await;

    let (status, node) =
        request_without_body(&app, "POST", "/api/v1/nodes/node-a/drain", Some("operator")).await;
    assert_eq!(status, StatusCode::OK, "{node}");
    assert_eq!(node["maintenance_state"], "draining");
    let (status, node) = request_without_body(
        &app,
        "POST",
        "/api/v1/nodes/node-a/maintenance",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{node}");
    assert_eq!(node["maintenance_state"], "maintenance");
    let (status, node) = request_without_body(
        &app,
        "DELETE",
        "/api/v1/nodes/node-a/maintenance",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{node}");
    assert_eq!(node["maintenance_state"], "active");
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/nodes/missing/drain",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

    let viewer_app = hub_router(
        hub::Hub::new(scoped_operator_hub_config()),
        &ServerConfig::default(),
    );
    let (status, _) = request_without_body(
        &viewer_app,
        "POST",
        "/api/v1/nodes/node-a/drain",
        Some("viewer-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn agent_routes_reject_bad_credentials_and_accept_observations() {
    let hub = storage_hub();
    let app = hub_router(hub, &ServerConfig::default());
    let session = register_hub_node(&app, "node-a").await;

    // Wrong shared registration token.
    let (status, _) = post_json(
        &app,
        "/api/v1/agent/register",
        "",
        serde_json::json!({"node_id": "intruder", "node_token": "wrong"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Stale session on heartbeat/report.
    let (status, _) = post_json(
        &app,
        "/api/v1/agent/heartbeat",
        "",
        serde_json::json!({"node_id": "node-a", "session_token": "stale", "state": "online"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = post_json(
        &app,
        "/api/v1/agent/report",
        "",
        serde_json::json!({
            "node_id": "node-a", "session_token": "stale",
            "version": "v", "state": "online"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Job observations require a live session; unknown Jobs report 204.
    let (status, _) = post_json(
        &app,
        "/api/v1/agent/job-observations",
        "",
        serde_json::json!({
            "node_id": "node-a", "session_token": "stale",
            "job_id": "any", "generation": 1, "state": "running"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = post_json(
        &app,
        "/api/v1/agent/job-observations",
        &session.session_token,
        serde_json::json!({
            "node_id": "node-a", "session_token": session.session_token,
            "job_id": "unknown-job", "generation": 1, "state": "running"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn hub_health_and_readiness_report_leadership_and_recovery() {
    // Without durable storage readiness reports the storage gap.
    let app = hub_router(console_hub(), &ServerConfig::default());
    let (status, body) = get_json(&app, "/readiness", "operator").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["reason"], "storage_unavailable");
    let (status, health) = get_json(&app, "/health", "operator").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(health["status"], "degraded");

    // With storage: startup recovery gates readiness until restored.
    let hub = storage_hub();
    let app = hub_router(hub.clone(), &ServerConfig::default());
    let (status, body) = get_json(&app, "/readiness", "operator").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["reason"], "startup_recovery");
    let (status, _) = get_json(&app, "/health", "operator").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    hub.recover_persisted_state().await.unwrap();
    let (status, body) = get_json(&app, "/readiness", "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ready");
    let (status, body) = get_json(&app, "/health", "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ready"], true);

    // An HA standby that has not won the lease reports standby.
    let standby = hub::Hub::new(hub::HubConfig {
        operator_token: Some("operator".into()),
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 1_000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
    })
    .with_ha(hub::HubHaConfig {
        enabled: true,
        lease_ttl_ms: 1_000,
        ..hub::HubHaConfig::default()
    });
    standby.enter_election().await;
    let app = hub_router(standby, &ServerConfig::default());
    let (status, body) = get_json(&app, "/readiness", "operator").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["reason"], "standby");
    assert_eq!(body["ha"]["role"], "standby");
    // Non-allowlisted routes are fenced off for a standby.
    let (status, body) = get_json(&app, "/api/v1/nodes", "operator").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "hub_standby");
}

/// hub-ha stage 3: a standby's 503 points Agents at the elected leader
/// when the shared lease row carries its advertised address, and stays
/// byte-compatible with the pre-hint body when it does not.
#[tokio::test]
async fn standby_503_carries_leader_hint_from_the_shared_lease_row() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // A leader elsewhere holds the lease and advertises its API base URL.
    let advertised = storage::ControlPlaneStore::in_memory().unwrap();
    let actor = storage::StorageActor::start(advertised, 8);
    assert!(matches!(
        actor
            .try_acquire_hub_lease(
                "hub-leader",
                Some("http://hub-leader:8080".into()),
                60_000,
                now
            )
            .await
            .unwrap(),
        storage::HubLeaseAcquire::Acquired { .. }
    ));
    let standby = hub::Hub::with_storage(storage_hub_config(), actor).with_ha(hub::HubHaConfig {
        enabled: true,
        lease_ttl_ms: 1_000,
        ..hub::HubHaConfig::default()
    });
    standby.enter_election().await;
    let app = hub_router(standby, &ServerConfig::default());
    let (status, body) = get_json(&app, "/api/v1/nodes", "operator").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "hub_standby");
    assert_eq!(body["details"]["leader_url"], "http://hub-leader:8080");

    // A lease row without an advertisement keeps the body identical to
    // the pre-hint standby response: no `leader_url`, no `details`.
    let quiet = storage::ControlPlaneStore::in_memory().unwrap();
    let actor = storage::StorageActor::start(quiet, 8);
    assert!(matches!(
        actor
            .try_acquire_hub_lease("quiet-leader", None, 60_000, now)
            .await
            .unwrap(),
        storage::HubLeaseAcquire::Acquired { .. }
    ));
    let standby = hub::Hub::with_storage(storage_hub_config(), actor).with_ha(hub::HubHaConfig {
        enabled: true,
        lease_ttl_ms: 1_000,
        ..hub::HubHaConfig::default()
    });
    standby.enter_election().await;
    let app = hub_router(standby, &ServerConfig::default());
    let (status, body) = get_json(&app, "/api/v1/nodes", "operator").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "hub_standby");
    assert!(body["details"].is_null(), "no hint without advertisement");
}

#[tokio::test]
async fn hub_targeted_command_routes_cover_storage_and_dispatch_paths() {
    let hub = storage_hub();
    let app = hub_router(hub, &ServerConfig::default());
    let viewer_app = hub_router(
        hub::Hub::new(scoped_operator_hub_config()),
        &ServerConfig::default(),
    );

    // Validation and authorization.
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/nodes/node-a/streams/orders/pause",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_operation");
    let (status, _) = request_without_body(
        &viewer_app,
        "POST",
        "/api/v1/nodes/node-a/streams/orders/start",
        Some("viewer-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Durable intent writes: start, stale generation fence, restart.
    let (status, intent) = request_without_body(
        &app,
        "POST",
        "/api/v1/nodes/node-a/streams/orders/start",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{intent}");
    assert_eq!(intent["generation"], 1);
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/nodes/node-a/streams/orders/stop",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/nodes/node-a/streams/orders/start",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    // Restart without an explicit action id derives one, and a stale
    // If-Match fence fails with 412.
    let (status, restart) = request_without_body(
        &app,
        "POST",
        "/api/v1/nodes/node-a/streams/orders/restart",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{restart}");
    assert!(restart["action_id"].as_str().is_some());
    let stale = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/restart")
                .header("authorization", "Bearer operator")
                .header("if-match", "\"generation-0\"")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
    let stale = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/stop")
                .header("authorization", "Bearer operator")
                .header("if-match", "\"generation-0\"")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);

    // Idempotency-key reuse with a different desired state conflicts.
    let reused = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/start")
                .header("authorization", "Bearer operator")
                .header("idempotency-key", "targeted-1")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reused.status(), StatusCode::ACCEPTED);
    let reused = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/stop")
                .header("authorization", "Bearer operator")
                .header("idempotency-key", "targeted-1")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reused.status(), StatusCode::CONFLICT);

    // The canonical restart action route with a body: an invalid header
    // value is a validation error, a valid one forwards.
    let (status, body) = post_json(
        &app,
        "/api/v1/nodes/node-a/streams/orders/actions/restart",
        "operator",
        serde_json::json!({"action_id": "bad\nvalue"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "validation_failed");
    let (status, _) = post_json(
        &app,
        "/api/v1/nodes/node-a/streams/orders/actions/restart",
        "operator",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // The volatile (no-storage) dispatch fallback.
    let volatile = console_hub();
    let volatile_app = hub_router(volatile, &ServerConfig::default());
    let (status, body) = request_without_body(
        &volatile_app,
        "POST",
        "/api/v1/nodes/missing/streams/orders/start",
        Some("operator-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "node_unavailable");
    register_hub_node(&volatile_app, "node-a").await;
    let (status, _) = request_without_body(
        &volatile_app,
        "POST",
        "/api/v1/nodes/node-a/streams/orders/start",
        Some("operator-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
}

#[tokio::test]
async fn hub_configuration_routes_cover_not_found_and_command_fallback() {
    // A registered node that has not reported configuration: 404s.
    let hub = storage_hub();
    let app = hub_router(hub, &ServerConfig::default());
    let viewer_app = hub_router(
        hub::Hub::new(scoped_operator_hub_config()),
        &ServerConfig::default(),
    );
    let (status, body) = get_json(&app, "/api/v1/nodes/missing/configuration", "operator").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], "node_configuration_unavailable");
    let (status, _) = get_json(
        &app,
        "/api/v1/nodes/missing/configuration/versions",
        "operator",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Viewer credentials cannot validate or apply configuration.
    let (status, _) = post_json(
        &viewer_app,
        "/api/v1/nodes/node-a/configuration/validate",
        "viewer-secret",
        serde_json::json!({"format": "yaml", "content": "streams: []\n"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = post_json(
        &viewer_app,
        "/api/v1/nodes/node-a/configuration/apply",
        "viewer-secret",
        serde_json::json!({"format": "json", "content": "{\"streams\":[]}"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Durable rollback with an unknown config version is a 400.
    register_hub_node(&app, "node-a").await;
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/nodes/node-a/configuration/rollback/no-such-version",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_configuration");

    // The volatile Hub falls back to direct command dispatch.
    let volatile_app = hub_router(console_hub(), &ServerConfig::default());
    register_hub_node(&volatile_app, "node-a").await;
    let (status, operation) = post_json(
        &volatile_app,
        "/api/v1/nodes/node-a/configuration/apply",
        "operator-secret",
        serde_json::json!({"format": "json", "content": "{\"streams\":[]}"}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{operation}");
    assert_eq!(operation["operation"], "apply_configuration");
    let (status, body) = post_json(
        &volatile_app,
        "/api/v1/nodes/missing/configuration/apply",
        "operator-secret",
        serde_json::json!({"format": "json", "content": "{\"streams\":[]}"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "node_unavailable");
    // Reported configuration and versions become readable.
    let session = register_hub_node(&volatile_app, "node-a").await;
    let mut report = agent_report_body(&session);
    report["configuration"] = serde_json::json!({"streams": []});
    report["config_versions"] =
        serde_json::json!([{"id": "v1", "created_at_ms": 1, "format": "json"}]);
    report_node_raw(&volatile_app, report).await;
    let (status, _) = get_json(
        &volatile_app,
        "/api/v1/nodes/node-a/configuration",
        "operator-secret",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, versions) = get_json(
        &volatile_app,
        "/api/v1/nodes/node-a/configuration/versions",
        "operator-secret",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(versions.as_array().unwrap().len(), 1);
}

// ------------------------------------------------------------------
// OIDC login flow: mock provider + hub routes.
// ------------------------------------------------------------------

const OIDC_TEST_EC_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgIGq3kKWaBjOJOQ4J\nc7lugLGZpGy7bpORlkEWnXiQbkqhRANCAATYhN6zoBUzCrLoVXj2FpxVsgFOauvf\nXSKSDtR4WPh2LJ4TJ8a9cXRDPs87HJj6NC2F1iTBHNBgzdE853A+WQnZ\n-----END PRIVATE KEY-----";
const OIDC_TEST_KID: &str = "coverage-oidc-key";
const OIDC_TEST_JWK_X: &str = "2ITes6AVMwqy6FV49hacVbIBTmrr310ikg7UeFj4diw";
const OIDC_TEST_JWK_Y: &str = "nhMnxr1xdEM-zzscmPo0LYXWJMEc0GDN0TzncD5ZCdk";

fn mint_test_id_token(issuer: &str, nonce: &str) -> String {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(OIDC_TEST_KID.into());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let claims = serde_json::json!({
        "sub": "oidc-tester",
        "roles": ["operator"],
        "iss": issuer,
        "aud": "arkflow-audience",
        "exp": now + 600,
        "nonce": nonce,
    });
    encode(
        &header,
        &claims,
        &EncodingKey::from_ec_pem(OIDC_TEST_EC_KEY_PEM.as_bytes()).unwrap(),
    )
    .unwrap()
}

/// A tiny loopback OIDC provider: discovery, JWKS, and a token endpoint
/// that mints an id_token whose nonce rides inside the authorization code
/// (`nonce-<nonce>`), so the login round-trip can complete offline.
async fn spawn_mock_oidc_provider() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                loop {
                    match stream.read(&mut byte).await {
                        Ok(0) => break,
                        Ok(_) => {
                            head.push(byte[0]);
                            if head.ends_with(b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => return,
                    }
                }
                let request = String::from_utf8_lossy(&head).into_owned();
                let mut lines = request.split("\r\n");
                let request_line = lines.next().unwrap_or_default().to_owned();
                let mut content_length = 0usize;
                for line in lines {
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        content_length = value.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; content_length];
                if content_length > 0 {
                    let _ = stream.read_exact(&mut body).await;
                }
                let path = request_line
                    .split(' ')
                    .nth(1)
                    .unwrap_or_default()
                    .to_owned();
                let issuer = format!("http://127.0.0.1:{port}");
                let (status, payload) = match path.as_str() {
                    "/.well-known/openid-configuration" => (
                        "200 OK",
                        serde_json::json!({
                            "authorization_endpoint": format!("{issuer}/authorize"),
                            "token_endpoint": format!("{issuer}/token"),
                        })
                        .to_string(),
                    ),
                    "/jwks" => (
                        "200 OK",
                        serde_json::json!({
                            "keys": [{
                                "kty": "EC",
                                "crv": "P-256",
                                "kid": OIDC_TEST_KID,
                                "x": OIDC_TEST_JWK_X,
                                "y": OIDC_TEST_JWK_Y,
                            }]
                        })
                        .to_string(),
                    ),
                    "/token" => {
                        let form = String::from_utf8_lossy(&body).into_owned();
                        let code = form
                            .split('&')
                            .find_map(|pair| pair.strip_prefix("code="))
                            .unwrap_or_default();
                        let nonce = code.strip_prefix("nonce-").unwrap_or("fixed-nonce");
                        (
                            "200 OK",
                            serde_json::json!({
                                "id_token": mint_test_id_token(&issuer, nonce)
                            })
                            .to_string(),
                        )
                    }
                    _ => ("404 Not Found", "{}".into()),
                };
                let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                        payload.len()
                    );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.flush().await;
            });
        }
    });
    port
}

/// Read one cookie out of a response's Set-Cookie header.
fn response_cookie(response: &Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(header::SET_COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .next()?
        .strip_prefix(&format!("{name}="))
        .map(str::to_owned)
}

async fn oidc_login_hub() -> hub::Hub {
    let port = spawn_mock_oidc_provider().await;
    let issuer = format!("http://127.0.0.1:{port}");
    let federation = crate::oidc::OidcFederation::from_settings(crate::oidc::OidcSettings {
        issuer,
        audience: "arkflow-audience".into(),
        jwks_url: Some(format!("http://127.0.0.1:{port}/jwks")),
        role_claim: None,
        scopes_claim: None,
        client_id: Some("console".into()),
        client_secret: Some("console-secret".into()),
        redirect_uri: Some("http://127.0.0.1:1/console/callback".into()),
        jwks_refresh_interval: None,
    })
    .await
    .expect("federation builds");
    hub::Hub::new(hub::HubConfig {
        operator_token: Some("operator".into()),
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: default_session_ttl_ms(),
    })
    .with_oidc(federation)
}

#[tokio::test]
async fn oidc_status_login_callback_and_logout_round_trip() {
    let hub = oidc_login_hub().await;
    let app = hub_router(hub, &ServerConfig::default());

    // Status before login: enabled but not authenticated.
    let (status, body) = get_json(&app, "/api/v1/auth/oidc/status", "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["login_enabled"], true);
    assert_eq!(body["authenticated"], false);

    // Login starts the authorization-code flow with PKCE state.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/auth/oidc/login")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    let location = response
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(location.contains("/authorize?"), "{location}");
    assert!(location.contains("code_challenge_method=S256"));
    let state_cookie = response_cookie(&response, "arkflow_oidc_state").expect("state cookie");
    let mut parts = state_cookie.split('.');
    let state = parts.next().unwrap().to_owned();
    let _verifier = parts.next().unwrap().to_owned();
    let nonce = parts.next().unwrap().to_owned();

    let callback = |query: &str, cookie: Option<&str>| {
        let mut builder = axum::http::Request::get(format!("/api/v1/auth/oidc/callback?{query}"));
        if let Some(cookie) = cookie {
            builder = builder.header(header::COOKIE, format!("arkflow_oidc_state={cookie}"));
        }
        app.clone()
            .oneshot(builder.body(axum::body::Body::empty()).unwrap())
    };

    // Missing state cookie, malformed cookie, mismatched state, missing
    // code: every failure answers 401 without leaking detail.
    let response = callback(&format!("state={state}&code=nonce-{nonce}"), None)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = callback(&format!("state={state}&code=nonce-{nonce}"), Some("short"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = callback(
        &format!("state=tampered-state&code=nonce-{nonce}"),
        Some(&state_cookie),
    )
    .await
    .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "wrong state must fail"
    );
    let response = callback(&format!("state={state}"), Some(&state_cookie))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // A mismatched nonce (code carries a different nonce than the cookie)
    // is rejected after the token exchange.
    let response = callback(
        &format!("state={state}&code=nonce-someone-elses-nonce"),
        Some(&state_cookie),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // The happy path: exchange validates, the session cookie is issued.
    let response = callback(
        &format!("state={state}&code=nonce-{nonce}"),
        Some(&state_cookie),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/");
    let session_cookie = response_cookie(&response, "arkflow_session").expect("session cookie");

    // Status reports the authenticated principal from the cookie.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/auth/oidc/status")
                .header(header::COOKIE, format!("arkflow_session={session_cookie}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["authenticated"], true);
    assert_eq!(body["principal"]["id"], "oidc-tester");
    assert_eq!(body["principal"]["roles"][0], "operator");

    // Logout with the session bearer credential invalidates the session.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/auth/oidc/logout")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer session:{session_cookie}"),
                )
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key(header::SET_COOKIE));
    let response = app
        .oneshot(
            axum::http::Request::get("/api/v1/auth/oidc/status")
                .header(header::COOKIE, format!("arkflow_session={session_cookie}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["authenticated"], false);
}

#[tokio::test]
async fn oidc_handlers_without_federation_answer_not_found() {
    let hub = console_hub();
    let response = hub_oidc_login(State(hub.clone())).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = hub_oidc_callback(
        State(hub),
        Query(std::collections::HashMap::new()),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    // The status route still works and reports the disabled flow.
    let app = hub_router(console_hub(), &ServerConfig::default());
    let (status, body) = get_json(&app, "/api/v1/auth/oidc/status", "operator").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["login_enabled"], false);
}

// ------------------------------------------------------------------
// Direct helper coverage.
// ------------------------------------------------------------------

#[test]
fn credential_helpers_read_cookies_and_etags() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::COOKIE,
        HeaderValue::from_static("a=1; arkflow_session=xyz; b=2"),
    );
    assert_eq!(
        cookie_value(&headers, "arkflow_session").as_deref(),
        Some("xyz")
    );
    assert_eq!(cookie_value(&headers, "missing"), None);
    // The browser session cookie surfaces through the bearer channel.
    assert_eq!(bearer(&headers).as_deref(), Some("session:xyz"));
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer op-token"),
    );
    assert_eq!(bearer(&headers).as_deref(), Some("op-token"));
    assert_eq!(bearer(&HeaderMap::new()), None);

    assert_eq!(parse_generation_etag("\"generation-7\""), Some(7));
    assert_eq!(parse_generation_etag("generation-42"), Some(42));
    assert_eq!(parse_generation_etag("generation-x"), None);
    assert_eq!(parse_generation_etag("bogus"), None);

    assert_eq!(prometheus_label("node:with spaces!"), "nodewithspaces");
    assert_eq!(
        prometheus_label(&"x".repeat(100)).len(),
        64,
        "labels are truncated to 64 characters"
    );
}

#[tokio::test]
async fn hub_problem_maps_error_kinds_to_status_and_code() {
    let cases = [
        (
            hub::HubError::Unauthorized,
            StatusCode::UNAUTHORIZED,
            "agent_request_rejected",
        ),
        (
            hub::HubError::NodeUnavailable,
            StatusCode::CONFLICT,
            "agent_request_rejected",
        ),
        (
            hub::HubError::NotFound,
            StatusCode::NOT_FOUND,
            "agent_request_rejected",
        ),
        (
            hub::HubError::OrchestrationInProgress,
            StatusCode::CONFLICT,
            "orchestration_in_progress",
        ),
        (
            hub::HubError::OrchestrationPhaseConflict,
            StatusCode::CONFLICT,
            "orchestration_conflict",
        ),
        (
            hub::HubError::StorageUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "storage_unavailable",
        ),
        (
            hub::HubError::Storage("boom".into()),
            StatusCode::SERVICE_UNAVAILABLE,
            "storage_unavailable",
        ),
        (
            hub::HubError::GenerationConflict {
                expected: 1,
                current: 2,
            },
            StatusCode::CONFLICT,
            "generation_conflict",
        ),
        (
            hub::HubError::Invalid("bad input".into()),
            StatusCode::BAD_REQUEST,
            "agent_request_rejected",
        ),
    ];
    for (error, status, code) in cases {
        let display = error.to_string();
        let response = hub_problem(error);
        assert_eq!(response.status(), status);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], code, "for {display}");
    }
}

#[tokio::test]
async fn require_operator_action_rejects_and_audits_missing_credentials() {
    let hub = storage_hub();
    let response = require_operator_action(
        &hub,
        &HeaderMap::new(),
        OperatorAction::Read,
        "job",
        Some("job-a".into()),
    )
    .await
    .unwrap_err();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["code"], "unauthorized");
    // The denial is visible in the audit trail.
    let records = hub.audit(None).await.unwrap();
    assert!(
        records.iter().any(|record| record.outcome == "rejected"
            && record.failure_code.as_deref() == Some("unauthorized")
            && record.resource_id.as_deref() == Some("job-a")),
        "audit records: {records:?}"
    );
}

// ------------------------------------------------------------------
// Local (ControlPlane) router coverage.
// ------------------------------------------------------------------

async fn local_engine_with_stream() -> (ControlPlane, tokio_util::sync::CancellationToken) {
    arkflow_plugin::initialize().unwrap();
    let stream: arkflow_core::stream::StreamConfig = serde_json::from_value(serde_json::json!({
        "id": "orders",
        "input": {
            "type": "generate",
            "context": "{\"value\":1}",
            "interval": "1s",
            "batch_size": 1
        },
        "pipeline": {"thread_num": 1, "processors": []},
        "output": {"type": "drop"}
    }))
    .unwrap();
    let engine = Engine::new(EngineConfig {
        streams: vec![stream],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: NodeConfig {
            control_api: ControlApiConfig {
                api_token: Some("local-token".into()),
                ..Default::default()
            },
            ..NodeConfig::default()
        },
    });
    let control_plane = engine.control_plane();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let token = cancellation.clone();
    tokio::spawn(async move {
        let _ = engine.run_with_cancellation(token).await;
    });
    (control_plane, cancellation)
}

#[tokio::test]
async fn local_resource_routes_cover_lifecycle_operations_and_events() {
    let (control_plane, cancellation) = local_engine_with_stream().await;
    control_plane.health().set_ready(true);
    control_plane.health().set_running(true);
    let app = router(control_plane.clone(), &ServerConfig::default());

    // Wait for the stream runtime to register.
    let mut registered = false;
    for _ in 0..200 {
        let (status, _) = get_json(&app, "/api/v1/streams/orders", "local-token").await;
        if status == StatusCode::OK {
            registered = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(registered, "the stream runtime must register");

    // Node resource and lifecycle actions.
    let (status, node) = get_json(&app, "/api/v1/node", "local-token").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(node["id"], "local-node");
    let (status, _) = request_without_body(
        &app,
        "POST",
        "/api/v1/streams/orders/start",
        Some("local-token"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/streams/missing/start",
        Some("local-token"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], "stream_not_found");
    let unauthorized = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/streams/orders/start")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    // Operations listing with every filter, plus 404 and cancel.
    let (status, page) = get_json(
        &app,
        "/api/v1/operations?resource_id=orders&operation=start&state=succeeded&correlation_id=nope",
        "local-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["total"], 0);
    let (status, page) = get_json(&app, "/api/v1/operations", "local-token").await;
    assert_eq!(status, StatusCode::OK);
    assert!(page["total"].as_u64().unwrap() >= 1);
    let operation_id = page["items"][0]["id"].as_str().unwrap().to_owned();
    let (status, _) = get_json(&app, "/api/v1/operations/missing", "local-token").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = request_without_body(
        &app,
        "DELETE",
        &format!("/api/v1/operations/{operation_id}"),
        Some("local-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = request_without_body(
        &app,
        "DELETE",
        "/api/v1/operations/missing",
        Some("local-token"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let unauthorized = app
        .clone()
        .oneshot(
            axum::http::Request::delete("/api/v1/operations/missing")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    // Events narrow by type/stream and sort newest-first.
    let (status, all) = get_json(&app, "/api/v1/events", "local-token").await;
    assert_eq!(status, StatusCode::OK);
    let items = all["items"].as_array().unwrap();
    assert!(!items.is_empty());
    let event_type = items[0]["event_type"].as_str().unwrap().to_owned();
    let (status, filtered) = get_json(
        &app,
        &format!("/api/v1/events?event_type={event_type}&stream_id=orders"),
        "local-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(filtered["total"].as_u64().unwrap() >= 1);

    // Draft lifecycle: empty draft answers 204.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/configuration/draft")
                .header("authorization", "Bearer local-token")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    cancellation.cancel();
}

#[tokio::test]
async fn local_configuration_routes_apply_diff_and_rollback() {
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: NodeConfig {
            control_api: ControlApiConfig {
                api_token: Some("local-token".into()),
                ..Default::default()
            },
            ..NodeConfig::default()
        },
    });
    let app = router(engine.control_plane(), &ServerConfig::default());
    let candidate = serde_json::json!({"format": "json", "content": "{\"streams\":[]}"});
    let mut created_versions = Vec::new();

    // Apply creates versions; the second apply descends from the first.
    let (status, applied) = post_json(
        &app,
        "/api/v1/configuration/apply",
        "local-token",
        candidate.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{applied}");
    let first = applied["version"]["id"].as_str().unwrap().to_owned();
    created_versions.push(first.clone());
    let (status, applied) = post_json(
        &app,
        "/api/v1/configuration/apply",
        "local-token",
        candidate,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{applied}");
    let second = applied["version"]["id"].as_str().unwrap().to_owned();
    created_versions.push(second.clone());

    // Invalid content is a 422.
    let (status, _) = post_json(
        &app,
        "/api/v1/configuration/apply",
        "local-token",
        serde_json::json!({"format": "json", "content": "not-json"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // Diff: success, missing `to`, missing `from`.
    let (status, diff) = get_json(
        &app,
        &format!("/api/v1/configuration/diff?from={first}&to={second}"),
        "local-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{diff}");
    assert_eq!(diff["changed"], false);
    let (status, _) = get_json(
        &app,
        &format!("/api/v1/configuration/diff?from={first}&to=missing"),
        "local-token",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Rollback to a known version succeeds; an unknown id is a 422.
    let (status, rolled) = request_without_body(
        &app,
        "POST",
        &format!("/api/v1/configuration/rollback/{first}"),
        Some("local-token"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{rolled}");
    if let Some(id) = rolled["version"]["id"].as_str() {
        created_versions.push(id.to_owned());
    }
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/configuration/rollback/does-not-exist",
        Some("local-token"),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    // Authorization gates every configuration route.
    let unauthorized = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/configuration")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    // Component catalogue: unknown kind and unknown name.
    let (status, _) = get_json(&app, "/api/v1/components/bogus/name", "local-token").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = get_json(&app, "/api/v1/components/input/nope", "local-token").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Clean the version files this test created (other tests running
    // in parallel may own the remaining ones).
    for id in &created_versions {
        let path = format!(".arkflow/config-history/{id}.json");
        assert!(
            std::fs::remove_file(&path).is_ok(),
            "test must clean up its own version file {path}"
        );
    }
    let _ = std::fs::remove_dir(".arkflow/config-history");
    let _ = std::fs::remove_dir(".arkflow");
}

// ------------------------------------------------------------------
// Coverage additions: serve lifecycles, repository failure arms,
// command conflicts, SSE filter/lag/close paths, OIDC failure modes.
// ------------------------------------------------------------------

/// `serve` answers requests and resolves Ok after graceful shutdown.
#[tokio::test]
async fn serve_answers_and_resolves_after_graceful_shutdown() {
    let engine = Engine::new(EngineConfig {
        streams: vec![],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: NodeConfig::default(),
    });
    let port = free_port();
    let config = ServerConfig {
        address: format!("127.0.0.1:{port}"),
        ..ServerConfig::default()
    };
    let cancellation = tokio_util::sync::CancellationToken::new();
    let shutdown = cancellation.clone();
    let mut task = tokio::spawn(async move {
        serve(engine.control_plane(), config, shutdown)
            .await
            .expect("serve resolves Ok after the graceful shutdown");
    });
    let client = reqwest::Client::new();
    let mut served = false;
    for _ in 0..100 {
        if let Ok(response) = client
            .get(format!("http://127.0.0.1:{port}/health"))
            .send()
            .await
        {
            let _ = response.status();
            served = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(served, "the local server must answer /health");
    cancellation.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(10), &mut task)
        .await
        .expect("the serve task resolves after cancellation")
        .expect("the serve task did not panic");
}

/// A disabled Hub server is a no-op, and an enabled Hub with storage
/// restores persisted operations before serving and releases leadership
/// on graceful shutdown (both plain and TLS listeners).
#[tokio::test]
async fn serve_hub_disabled_is_a_no_op() {
    let hub = storage_hub();
    let config = ServerConfig {
        enabled: false,
        ..ServerConfig::default()
    };
    let outcome = serve_hub(hub, config, tokio_util::sync::CancellationToken::new()).await;
    assert!(outcome.is_ok(), "{outcome:?}");
}

#[tokio::test]
async fn serve_hub_with_storage_restores_operations_and_shuts_down_cleanly() {
    let port = free_port();
    let hub = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            insecure_local: true,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 10_000,
            session_ttl_ms: default_session_ttl_ms(),
        },
        storage::StorageActor::start(storage::ControlPlaneStore::in_memory().unwrap(), 8),
    );
    let config = ServerConfig {
        address: format!("127.0.0.1:{port}"),
        insecure_local: true,
        ..ServerConfig::default()
    };
    let cancellation = tokio_util::sync::CancellationToken::new();
    let shutdown = cancellation.clone();
    let mut task = tokio::spawn(async move {
        serve_hub(hub, config, shutdown)
            .await
            .expect("the hub serves and shuts down cleanly");
    });
    let client = reqwest::Client::new();
    let mut served = false;
    for _ in 0..150 {
        if let Ok(response) = client
            .get(format!("http://127.0.0.1:{port}/health"))
            .send()
            .await
        {
            let _ = response.status();
            served = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(served, "the hub must answer /health");
    cancellation.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(15), &mut task)
        .await
        .expect("serve_hub resolves after cancellation")
        .expect("the serve task did not panic");
}

#[tokio::test]
async fn serve_hub_over_tls_releases_leadership_on_shutdown() {
    let (cert_path, key_path) = hub_tls_material();
    let port = free_port();
    let hub = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: Some("operator".into()),
            node_token: Some("node-secret".into()),
            insecure_local: true,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 10_000,
            session_ttl_ms: default_session_ttl_ms(),
        },
        storage::StorageActor::start(storage::ControlPlaneStore::in_memory().unwrap(), 8),
    );
    let config = ServerConfig {
        address: format!("127.0.0.1:{port}"),
        insecure_local: true,
        tls_cert: Some(cert_path.to_string_lossy().into_owned()),
        tls_key: Some(key_path.to_string_lossy().into_owned()),
        ..ServerConfig::default()
    };
    let cancellation = tokio_util::sync::CancellationToken::new();
    let shutdown = cancellation.clone();
    let mut task = tokio::spawn(async move {
        serve_hub(hub, config, shutdown)
            .await
            .expect("the TLS hub serves and shuts down cleanly");
    });
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let mut served = false;
    for _ in 0..150 {
        if let Ok(response) = client
            .get(format!("https://127.0.0.1:{port}/liveness"))
            .send()
            .await
        {
            let _ = response.status();
            served = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(served, "the TLS hub must answer /liveness");
    cancellation.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(15), &mut task)
        .await
        .expect("the TLS serve_hub resolves after cancellation")
        .expect("the TLS serve task did not panic");
    let _ = std::fs::remove_dir_all(cert_path.parent().unwrap());
}

/// Every hub route that reads through the durable store surfaces a
/// repository failure as a 503 with the storage_unavailable code.
#[tokio::test]
async fn hub_routes_surface_repository_failures() {
    let _ = arkflow_plugin::initialize();
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(
        storage_hub_config(),
        storage::StorageActor::start(store.clone(), 8),
    );
    let app = hub_router(hub.clone(), &ServerConfig::default());
    create_job(&app, "failing-job").await;
    register_hub_node(&app, "node-a").await;

    // A stopped-mode upgrade whose savepoint lookup fails: only the
    // checkpoint table is broken at this stage so the job read succeeds.
    request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/failing-job/savepoints",
        Some("operator"),
    )
    .await;
    let job_version = 1;
    store
        .with_connection(|connection| {
            connection.execute("DROP TABLE cp_job_checkpoints", [])?;
            Ok(())
        })
        .unwrap();
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/failing-job/upgrades",
        "operator",
        serde_json::json!({
            "spec": job_spec("failing-job", 2),
            "expected_generation": 1,
            "savepoint_id": "checkpoint-1",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    // The detail view and recovery-artifact trigger surface the same
    // failure while the Job record itself is still readable, and an
    // upgrade against an unknown Job keeps its 404.
    let (status, body) = get_json(&app, "/api/v1/jobs/failing-job/detail", "operator").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/failing-job/checkpoints",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/ghost-job/upgrades",
        "operator",
        serde_json::json!({
            "spec": job_spec("ghost-job", 2),
            "expected_generation": 1,
            "savepoint_id": "checkpoint-1",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], "job_not_found");

    // Break the remaining durable tables: every read fails closed.
    store
        .with_connection(|connection| {
            for table in [
                "cp_jobs",
                "cp_job_versions",
                "cp_job_upgrades",
                "cp_rollouts",
                "cp_audit_events",
                "cp_stream_desired",
                "cp_config_versions",
            ] {
                connection.execute(&format!("DROP TABLE {table}"), [])?;
            }
            Ok(())
        })
        .unwrap();
    for (method, path) in [
        ("GET", "/api/v1/jobs"),
        ("GET", "/api/v1/jobs/failing-job"),
        ("GET", "/api/v1/jobs/failing-job/plan"),
        ("GET", "/api/v1/jobs/failing-job/detail"),
        ("GET", "/api/v1/jobs/failing-job/versions"),
        ("GET", "/api/v1/jobs/failing-job/checkpoints"),
        ("GET", "/api/v1/audit"),
        ("GET", "/api/v1/rollouts"),
        ("GET", "/api/v1/rollouts/missing"),
        ("GET", "/api/v1/nodes/node-a/streams/orders"),
    ] {
        let (status, body) = request_without_body(&app, method, path, Some("operator")).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{method} {path} must surface the repository failure: {body}"
        );
    }
    for (method, path, body_value) in [
        (
            "POST",
            "/api/v1/jobs/failing-job/actions/start",
            serde_json::Value::Null,
        ),
        (
            "POST",
            "/api/v1/jobs/failing-job/upgrades",
            serde_json::json!({
                "spec": job_spec("failing-job", 2),
                "expected_generation": 1,
                "savepoint_id": "checkpoint-1",
            }),
        ),
        (
            "POST",
            "/api/v1/jobs/failing-job/upgrades/restore-v1/rollback",
            serde_json::Value::Null,
        ),
        (
            "PUT",
            "/api/v1/jobs/failing-job/desired-state",
            serde_json::json!({"state": "running"}),
        ),
        (
            "POST",
            "/api/v1/jobs/failing-job/checkpoints",
            serde_json::Value::Null,
        ),
        (
            "POST",
            "/api/v1/rollouts",
            serde_json::json!({
                "config_version": "cfg-x",
                "node_ids": ["node-a"],
                "batch_size": 1
            }),
        ),
        (
            "POST",
            "/api/v1/rollouts/r1/actions",
            serde_json::json!({"action": "pause"}),
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(method)
                    .uri(path)
                    .header("authorization", "Bearer operator")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body_value.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{method} {path} must surface the repository failure"
        );
    }
    // A body-less restart request surfaces the same repository failure.
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/nodes/node-a/streams/orders/actions/restart",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    // The plain GET stream resource keeps its distinct error body.
    let _ = job_version;
}

/// Generation fences, idempotency reuse and desired-state validation on
/// the node-scoped stream routes.
#[tokio::test]
async fn hub_targeted_command_conflicts_and_desired_state_validation() {
    let _ = arkflow_plugin::initialize();
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(storage_hub_config(), storage::StorageActor::start(store, 8));
    let app = hub_router(hub, &ServerConfig::default());

    // A desired state outside the running/stopped vocabulary is rejected.
    let (status, body) = put_json(
        &app,
        "/api/v1/nodes/node-a/streams/orders/desired-state",
        "operator",
        serde_json::json!({"state": "paused"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "validation_failed");

    // An if-match generation etag that can never match is a conflict.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/nodes/node-a/streams/orders/actions/restart")
                .header("authorization", "Bearer operator")
                .header("if-match", "generation-999")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

    // The same idempotency key cannot carry a different mutation: the
    // first desired state pins the key, a divergent state conflicts.
    let desired = |state: &str, key: &str| {
        app.clone().oneshot(
            axum::http::Request::put("/api/v1/nodes/node-a/streams/orders/desired-state")
                .header("authorization", "Bearer operator")
                .header("content-type", "application/json")
                .header("idempotency-key", key)
                .body(axum::body::Body::from(
                    serde_json::json!({"state": state}).to_string(),
                ))
                .unwrap(),
        )
    };
    let response = desired("running", "reused-desired-key").await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let response = desired("stopped", "reused-desired-key").await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::CONFLICT,
        "the same key with a different state must conflict"
    );

    // Creating a Job whose rebalance policy contradicts its pinned
    // placement surfaces the store's rejection.
    let mut auto_rebalance = job_spec("rebalance-job", 1);
    auto_rebalance["rebalance"] = serde_json::json!({"mode": "auto"});
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs",
        "operator",
        serde_json::json!({
            "spec": auto_rebalance,
            "desired_state": "stopped",
            "node_ids": ["node-a"],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}

/// On a Hub without durable storage the enqueue paths surface capability
/// and capacity rejections, and the upgrade/rollout routes answer with
/// the storage-unavailable family.
#[tokio::test]
async fn no_storage_hub_reports_enqueue_and_orchestration_limits() {
    let hub = hub::Hub::new(hub::HubConfig {
        operator_token: Some("operator".into()),
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: default_session_ttl_ms(),
    });
    let app = hub_router(hub.clone(), &ServerConfig::default());
    // Register an online node WITHOUT the lifecycle/configuration
    // capabilities so the enqueue capability gate rejects.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/agent/register")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({
                        "node_id": "node-caps",
                        "node_token": "",
                        "protocol_version": "v1",
                        "capabilities": ["job_runtime"],
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Apply configuration requires the configuration capability.
    let (status, body) = post_json(
        &app,
        "/api/v1/nodes/node-caps/configuration/apply",
        "operator",
        serde_json::json!({"format": "yaml", "content": "streams: []\n"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "command_rejected");

    // A targeted lifecycle command requires stream_lifecycle.
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/nodes/node-caps/streams/orders/start",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "command_rejected");

    // Fill the node's command queue: further read-only commands are
    // rejected with the capacity error.
    for index in 0..140 {
        let _ = hub
            .enqueue(
                "node-caps".into(),
                AgentOperation::parse("validate_configuration"),
                format!("cap-{index}"),
                None,
            )
            .await;
    }
    let (status, body) = post_json(
        &app,
        "/api/v1/nodes/node-caps/configuration/validate",
        "operator",
        serde_json::json!({"format": "yaml", "content": "streams: []\n"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "command_rejected");

    // Upgrade orchestration and rollouts require durable storage.
    let (status, body) = get_json(&app, "/api/v1/operations/status", "operator").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let (status, body) =
        get_json(&app, "/api/v1/jobs/some-job/upgrades/upgrade-1", "operator").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/some-job/upgrades/upgrade-1/actions",
        "operator",
        serde_json::json!({"action": "pause"}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let (status, body) = post_json(
        &app,
        "/api/v1/rollouts",
        "operator",
        serde_json::json!({"config_version": "cfg-9", "node_ids": ["node-caps"], "batch_size": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let (status, body) = post_json(
        &app,
        "/api/v1/rollouts/r9/actions",
        "operator",
        serde_json::json!({"action": "pause"}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
}

/// A rollback to a previous version surfaces each corruption class:
/// unparseable JSON, a spec that no longer validates, and a spec whose
/// components no longer build.
#[tokio::test]
async fn job_rollback_reports_corrupt_previous_versions() {
    let _ = arkflow_plugin::initialize();
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(
        storage_hub_config(),
        storage::StorageActor::start(store.clone(), 8),
    );
    let app = hub_router(hub, &ServerConfig::default());
    create_job(&app, "rollback-job").await;
    // Publish version 2 so a previous version exists to roll back to.
    let (status, _) = post_json(
        &app,
        "/api/v1/jobs",
        "operator",
        serde_json::json!({"spec": job_spec("rollback-job", 2), "desired_state": "stopped"}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let rollback = "/api/v1/jobs/rollback-job/upgrades/restore-v1/rollback";

    // Missing credentials never reach the store.
    let (status, _) = request_without_body(&app, "POST", rollback, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // (a) The previous version's spec is no longer parseable.
    store
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE cp_job_versions SET spec_json = 'not json' WHERE job_id = 'rollback-job' AND version = 1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
    let (status, body) = request_without_body(&app, "POST", rollback, Some("operator")).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["code"], "invalid_persisted_job");

    // (b) The previous spec parses but no longer validates.
    let mut sinkless = job_spec("rollback-job", 1);
    sinkless["sinks"] = serde_json::json!([]);
    store
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE cp_job_versions SET spec_json = ?1 WHERE job_id = 'rollback-job' AND version = 1",
                    [serde_json::to_string(&sinkless).unwrap()],
                )?;
                Ok(())
            })
            .unwrap();
    let (status, body) = request_without_body(&app, "POST", rollback, Some("operator")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "invalid_job_plan");

    // (c) The spec builds on paper but its components no longer exist.
    let mut unknown_input = job_spec("rollback-job", 1);
    unknown_input["sources"][0]["input_type"] = serde_json::json!("not-a-real-input");
    store
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE cp_job_versions SET spec_json = ?1 WHERE job_id = 'rollback-job' AND version = 1",
                    [serde_json::to_string(&unknown_input).unwrap()],
                )?;
                Ok(())
            })
            .unwrap();
    let (status, body) = request_without_body(&app, "POST", rollback, Some("operator")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "invalid_job_plan");

    // (d) A recovery pointer set on the Job makes the rollback verify the
    // artifact's state format; a broken checkpoint table surfaces that
    // read as a repository failure.
    store
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE cp_job_versions SET spec_json = ?1 WHERE job_id = 'rollback-job' AND version = 1",
                    [serde_json::to_string(&job_spec("rollback-job", 1)).unwrap()],
                )?;
                connection.execute(
                    "UPDATE cp_jobs SET checkpoint_id = 'cp-9' WHERE job_id = 'rollback-job'",
                    [],
                )?;
                connection.execute("DROP TABLE cp_job_checkpoints", [])?;
                Ok(())
            })
            .unwrap();
    let (status, body) = request_without_body(&app, "POST", rollback, Some("operator")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
}

/// A stopped-mode upgrade honours an explicit node pinning, a Job whose
/// persisted spec no longer parses fails the desired-state reconciliation
/// that follows the update, and an orchestration row that predates the
/// process fences a second atomic upgrade.
#[tokio::test]
async fn job_upgrade_pinning_corrupt_spec_reconciliation_and_orchestration_fence() {
    let _ = arkflow_plugin::initialize();
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(
        storage_hub_config(),
        storage::StorageActor::start(store.clone(), 8),
    );
    let app = hub_router(hub.clone(), &ServerConfig::default());
    register_job_node(&app, "node-a").await;
    create_job(&app, "pin-job").await;

    // Complete a savepoint, then upgrade with an explicit node pinning.
    request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/pin-job/savepoints",
        Some("operator"),
    )
    .await;
    store
        .with_connection(|connection| {
            connection.execute("UPDATE cp_job_checkpoints SET status = 'completed'", [])?;
            Ok(())
        })
        .unwrap();
    let (_, checkpoints) = get_json(&app, "/api/v1/jobs/pin-job/checkpoints", "operator").await;
    let savepoint_id = checkpoints[0]["checkpoint_id"].as_str().unwrap().to_owned();
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/pin-job/upgrades",
        "operator",
        serde_json::json!({
            "spec": job_spec("pin-job", 2),
            "expected_generation": 1,
            "savepoint_id": savepoint_id,
            "node_ids": ["node-a"],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["job"]["node_ids"], serde_json::json!(["node-a"]));

    // A corrupt persisted spec fails the reconciliation that follows a
    // desired-state update (both the route and the action funnel).
    store
        .with_connection(|connection| {
            connection.execute(
                "UPDATE cp_jobs SET spec_json = 'not json' WHERE job_id = 'pin-job'",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    let (status, body) = put_json(
        &app,
        "/api/v1/jobs/pin-job/desired-state",
        "operator",
        serde_json::json!({"state": "running"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, body) = request_without_body(
        &app,
        "POST",
        "/api/v1/jobs/pin-job/actions/start",
        Some("operator"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    // A durable orchestration row that predates this process fences a
    // fresh atomic upgrade even though the in-memory cache is empty.
    create_job(&app, "fenced-job").await;
    let (status, body) = put_json(
        &app,
        "/api/v1/jobs/fenced-job/desired-state",
        "operator",
        serde_json::json!({"state": "running"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let generation = current_generation(&app, "fenced-job").await;
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_job_upgrades (upgrade_id, job_id, from_version, to_version, phase, target_spec_json, phase_deadline_at_ms, savepoint_retries, verify_timeout_ms, created_at_ms, updated_at_ms) VALUES ('upgrade-stale', 'fenced-job', 1, 2, 'saving_savepoint', '{}', 0, 0, 0, 0, 0)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/fenced-job/upgrades",
        "operator",
        serde_json::json!({
            "spec": job_spec("fenced-job", 2),
            "expected_generation": generation,
            "mode": "atomic",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "orchestration_in_progress");

    // An action against an unknown upgrade id resolves as a 404.
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/fenced-job/upgrades/upgrade-missing/actions",
        "operator",
        serde_json::json!({"action": "pause"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], "job_upgrade_not_found");
    let _ = hub;
}

/// Scoped viewer credentials pass route authentication but cannot
/// perform Operate/Configure/ManageRollouts actions.
#[tokio::test]
async fn viewer_credentials_cannot_mutate_jobs_or_rollouts() {
    let _ = arkflow_plugin::initialize();
    let hub = hub::Hub::new(scoped_operator_hub_config());
    hub.upsert_job(crate::storage::JobRecord {
        job_id: "scoped-mutate".into(),
        version: 1,
        spec_json: serde_json::to_string(&job_spec("scoped-mutate", 1)).unwrap(),
        desired_state: "stopped".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: Vec::new(),
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let app = hub_router(hub, &ServerConfig::default());

    // Job validation is a Configure action.
    let (status, body) = post_json(
        &app,
        "/api/v1/jobs/validate",
        "viewer-secret",
        serde_json::json!({"spec": job_spec("scoped-mutate", 1)}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    // Recovery artifacts and rollback are Operate actions.
    for (method, path) in [
        ("POST", "/api/v1/jobs/scoped-mutate/savepoints"),
        (
            "POST",
            "/api/v1/jobs/scoped-mutate/upgrades/restore-v1/rollback",
        ),
    ] {
        let (status, body) = request_without_body(&app, method, path, Some("viewer-secret")).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {body}");
    }

    // Rollout management is its own action level.
    let (status, body) = post_json(
        &app,
        "/api/v1/rollouts/rx/actions",
        "viewer-secret",
        serde_json::json!({"action": "pause"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}

/// The local resource routes: stop/restart lifecycles, conflict mapping,
/// operation reads, correlation filters and the unauthorized arms of
/// every configuration route.
#[tokio::test]
async fn local_lifecycle_and_configuration_routes_cover_conflict_and_unauthorized_arms() {
    // The stream below uses the `generate` input; register the plugin
    // components explicitly so this test does not depend on another
    // test having initialized the registry first (initialize is
    // idempotent through its OnceLock).
    arkflow_plugin::initialize().unwrap();
    let engine = Engine::new(EngineConfig {
        streams: vec![generate_local_stream("local-orders")],
        jobs: Vec::new(),
        logging: LoggingConfig::default(),
        node: NodeConfig {
            control_api: ControlApiConfig {
                api_token: Some("local-token".into()),
                ..Default::default()
            },
            ..NodeConfig::default()
        },
    });
    let cp = engine.control_plane();
    cp.runtime_manager()
        .replace_config(&EngineConfig {
            streams: vec![generate_local_stream("local-orders")],
            jobs: Vec::new(),
            logging: LoggingConfig::default(),
            node: NodeConfig::default(),
        })
        .await
        .expect("the stream registers");
    let app = router(cp, &ServerConfig::default());

    // Stop and restart are first-class lifecycle routes.
    for action in ["stop", "restart"] {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post(format!("/api/v1/streams/local-orders/{action}"))
                    .header("authorization", "Bearer local-token")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED, "{action}");
    }

    // A start on the already-running stream settles through the async
    // lifecycle operation (the handler's only sync error is the
    // unknown-stream 404).
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/streams/local-orders/start")
                .header("authorization", "Bearer local-token")
                .header("x-correlation-id", "corr-local-1")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    // The operation created above is readable by id.
    let (_, operations) = get_json(&app, "/api/v1/operations", "local-token").await;
    let operation_id = operations["items"][0]["id"]
        .as_str()
        .expect("at least one operation exists")
        .to_owned();
    let (status, _) = get_json(
        &app,
        &format!("/api/v1/operations/{operation_id}"),
        "local-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Cancelling the in-flight start records an event synchronously, so
    // the event filters have something to filter over.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::delete(format!("/api/v1/operations/{operation_id}"))
                .header("authorization", "Bearer local-token")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let (status, events) = get_json(&app, "/api/v1/events", "local-token").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        events["items"]
            .as_array()
            .is_some_and(|items| !items.is_empty()),
        "a cancelled operation records an event: {events}"
    );
    // The correlation filter clause runs against the recorded events.
    let (status, _) = get_json(
        &app,
        "/api/v1/events?correlation_id=corr-local-1",
        "local-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Every configuration route answers 401 without the token.
    for (method, path) in [
        ("GET", "/api/v1/configuration/draft"),
        ("PUT", "/api/v1/configuration/draft"),
        ("GET", "/api/v1/configuration/diff?from=a&to=b"),
        ("GET", "/api/v1/configuration/versions"),
        ("POST", "/api/v1/configuration/apply"),
        ("POST", "/api/v1/configuration/rollback/v1"),
    ] {
        let builder = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json");
        let response = app
            .clone()
            .oneshot(
                builder
                    .body(axum::body::Body::from(
                        serde_json::json!({"format": "yaml", "content": "streams: []\n"})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {path} must require the token"
        );
    }
    let _ = engine.runtime_manager().stop_all().await;
}

fn generate_local_stream(id: &str) -> arkflow_core::stream::StreamConfig {
    arkflow_core::stream::StreamConfig {
        id: Some(id.to_string()),
        input: arkflow_core::input::InputConfig {
            input_type: "generate".into(),
            name: None,
            codec: None,
            config: Some(serde_json::json!({
                "context": "local-routes",
                "interval": "50ms",
                "batch_size": 1
            })),
        },
        pipeline: arkflow_core::pipeline::PipelineConfig {
            thread_num: 1,
            processors: Vec::new(),
        },
        output: arkflow_core::output::OutputConfig {
            output_type: "drop".into(),
            name: None,
            codec: None,
            config: None,
        },
        error_output: None,
        buffer: None,
        durability: None,
        state: None,
        temporary: None,
    }
}

/// The SSE stream filters live broadcasts, signals resync after a lag,
/// honors the correlation filter, and ends once the Hub is gone.
#[tokio::test]
async fn sse_filters_live_events_lags_to_resync_and_ends_on_close() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(storage_hub_config(), storage::StorageActor::start(store, 8));
    let app = hub_router(hub.clone(), &ServerConfig::default());
    let session = register_hub_node(&app, "node-a").await;

    // Non-matching live broadcasts are skipped until a matching one
    // arrives (the filter's continue arm).
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get(
                "/api/v1/events/stream?node_id=node-a&correlation_id=corr-match",
            )
            .header("authorization", "Bearer operator")
            .body(axum::body::Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    // A second node's broadcast does not match the filter and must be
    // skipped, the matching one (with a correlation id, so the filter's
    // correlation clause also runs) must arrive.
    let node_b_session = register_hub_node(&app, "node-b").await;
    let driver_app = app.clone();
    let session_a = session.session_token.clone();
    let session_b = node_b_session.session_token.clone();
    let driver = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let report = |session_token: &str, node_id: &str, event_type: &str, correlation: &str| {
            serde_json::json!({
                "node_id": node_id,
                "session_token": session_token,
                "version": "test-node",
                "state": "online",
                "events": [{
                    "occurred_at_ms": 100,
                    "event_type": event_type,
                    "stream_id": "orders",
                    "outcome": "accepted",
                    "message": null,
                    "operation_id": null,
                    "correlation_id": correlation,
                    "actor": null
                }]
            })
        };
        // node-b's event does not match the node_id filter.
        let _ = driver_app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/agent/report")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        report(&session_b, "node-b", "foreign_event", "corr-x").to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        // node-a's matching event carries the filtered correlation id.
        let _ = driver_app
            .oneshot(
                axum::http::Request::post("/api/v1/agent/report")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        report(&session_a, "node-a", "matching_event", "corr-match").to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
    });
    let mut saw_matching = false;
    for _ in 0..32 {
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(2), body.next()).await;
        match chunk {
            Ok(Some(Ok(chunk))) => {
                let text = String::from_utf8(chunk.to_vec()).unwrap();
                if text.contains("matching_event") {
                    saw_matching = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(saw_matching, "the filtered live event must arrive");
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), driver).await;

    // Drop every Hub clone: the broadcast sender closes and the stream
    // ends instead of hanging forever.
    drop(app);
    drop(hub);
    let ended = tokio::time::timeout(std::time::Duration::from_secs(5), body.next()).await;
    assert!(
        matches!(&ended, Ok(None)),
        "the stream must end once the Hub is gone: {ended:?}"
    );

    // A lagging receiver resyncs instead of replaying a gap silently.
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(
        storage_hub_config(),
        storage::StorageActor::start(store.clone(), 8),
    );
    let app = hub_router(hub, &ServerConfig::default());
    let session = register_hub_node(&app, "node-a").await;
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/events/stream?correlation_id=corr-burst")
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    // Broadcast far more events than the channel capacity WITHOUT
    // polling the stream, then read: the receiver lagged.
    let mut events = Vec::new();
    for index in 0..300 {
        events.push(serde_json::json!({
            "occurred_at_ms": 200 + index,
            "event_type": "burst_event",
            "stream_id": "orders",
            "outcome": "accepted",
            "message": null,
            "operation_id": null,
            "correlation_id": "corr-burst",
            "actor": null
        }));
    }
    let response = app
        .oneshot(
            axum::http::Request::post("/api/v1/agent/report")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({
                        "node_id": "node-a",
                        "session_token": session.session_token,
                        "version": "test-node",
                        "state": "online",
                        "events": events
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let mut saw_resync = false;
    for _ in 0..64 {
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(2), body.next()).await;
        match chunk {
            Ok(Some(Ok(chunk))) => {
                let text = String::from_utf8(chunk.to_vec()).unwrap();
                if text.contains("event: resync") {
                    saw_resync = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(saw_resync, "a lagging receiver must resync");
}

/// The agent command-result route maps unknown session credentials onto
/// the hub problem response.
#[tokio::test]
async fn agent_command_result_with_unknown_session_maps_to_hub_problem() {
    let hub = storage_hub();
    let app = hub_router(hub, &ServerConfig::default());
    let response = app
        .oneshot(
            axum::http::Request::post("/api/v1/agent/commands/cmd-unknown/result?node_id=node-a")
                .header("authorization", "Bearer not-a-session")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({
                        "command_id": "cmd-unknown",
                        "operation_id": "op-1",
                        "state": "succeeded",
                        "progress": 100,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Attempt states from the durable store render as Prometheus series.
#[tokio::test]
async fn hub_metrics_renders_attempt_state_series() {
    let store = storage::ControlPlaneStore::in_memory().unwrap();
    let hub = hub::Hub::with_storage(
        storage_hub_config(),
        storage::StorageActor::start(store.clone(), 8),
    );
    let app = hub_router(hub, &ServerConfig::default());
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_intents (intent_id, node_id, stream_id, generation, intent_type, state, convergence_state, created_at_ms, updated_at_ms) VALUES ('intent-1', 'node-a', 'orders', 1, 'start', 'pending', 'pending', 1, 1)",
                    [],
                )?;
                connection.execute(
                    "INSERT INTO cp_attempts (attempt_id, intent_id, command_id, node_id, stream_id, generation, operation, state, created_at_ms) VALUES ('att-1', 'intent-1', 'cmd-1', 'node-a', 'orders', 1, 'start', 'queued', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
    let response = app
        .oneshot(
            axum::http::Request::get("/api/v1/metrics")
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(
        text.contains("arkflow_attempts_state{state=\"queued\"} 1"),
        "attempt states must render: {text}"
    );
}

/// A mock OIDC provider whose token endpoint can fail or mint
/// wrong-issuer tokens, plus a secure (https) redirect URI.
async fn spawn_failing_mock_oidc_provider() -> (u16, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let issuer = format!("http://127.0.0.1:{port}");
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                loop {
                    match stream.read(&mut byte).await {
                        Ok(0) => break,
                        Ok(_) => {
                            head.push(byte[0]);
                            if head.ends_with(b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => return,
                    }
                }
                let request = String::from_utf8_lossy(&head).into_owned();
                let mut lines = request.split("\r\n");
                let request_line = lines.next().unwrap_or_default().to_owned();
                let mut content_length = 0usize;
                for line in lines {
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        content_length = value.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; content_length];
                if content_length > 0 {
                    let _ = stream.read_exact(&mut body).await;
                }
                let path = request_line
                    .split(' ')
                    .nth(1)
                    .unwrap_or_default()
                    .to_owned();
                let issuer = format!("http://127.0.0.1:{port}");
                let (status, payload) = match path.as_str() {
                    "/.well-known/openid-configuration" => (
                        "200 OK",
                        serde_json::json!({
                            "authorization_endpoint": format!("{issuer}/authorize"),
                            "token_endpoint": format!("{issuer}/token"),
                        })
                        .to_string(),
                    ),
                    "/jwks" => (
                        "200 OK",
                        serde_json::json!({
                            "keys": [{
                                "kty": "EC",
                                "crv": "P-256",
                                "kid": OIDC_TEST_KID,
                                "x": OIDC_TEST_JWK_X,
                                "y": OIDC_TEST_JWK_Y,
                            }]
                        })
                        .to_string(),
                    ),
                    "/token" => {
                        let form = String::from_utf8_lossy(&body).into_owned();
                        let code = form
                            .split('&')
                            .find_map(|pair| pair.strip_prefix("code="))
                            .unwrap_or_default()
                            .to_owned();
                        if code == "reject-me" {
                            ("500 Internal Server Error", String::new())
                        } else {
                            let (issuer_used, nonce) = if let Some(nonce) =
                                code.strip_prefix("wrong-issuer-nonce-")
                            {
                                ("http://127.0.0.1:1".to_owned(), nonce.to_owned())
                            } else {
                                let nonce = code.strip_prefix("nonce-").unwrap_or("fixed-nonce");
                                (issuer.clone(), nonce.to_owned())
                            };
                            (
                                "200 OK",
                                serde_json::json!({
                                    "id_token": mint_test_id_token(&issuer_used, &nonce)
                                })
                                .to_string(),
                            )
                        }
                    }
                    _ => ("404 Not Found", "{}".into()),
                };
                let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                        payload.len()
                    );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.flush().await;
            });
        }
    });
    (port, issuer)
}

/// OIDC login over a secure redirect issues Secure cookies, and the
/// callback rejects a missing state parameter, a failing token exchange
/// and a token from the wrong issuer.
#[tokio::test]
async fn oidc_secure_login_and_callback_failure_modes() {
    let (port, issuer) = spawn_failing_mock_oidc_provider().await;
    let federation = crate::oidc::OidcFederation::from_settings(crate::oidc::OidcSettings {
        issuer,
        audience: "arkflow-audience".into(),
        jwks_url: Some(format!("http://127.0.0.1:{port}/jwks")),
        role_claim: None,
        scopes_claim: None,
        client_id: Some("console".into()),
        client_secret: Some("console-secret".into()),
        redirect_uri: Some("https://127.0.0.1:1/console/callback".into()),
        jwks_refresh_interval: None,
    })
    .await
    .expect("federation builds");
    let hub = hub::Hub::new(hub::HubConfig {
        operator_token: Some("operator".into()),
        node_token: None,
        insecure_local: true,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: default_session_ttl_ms(),
    })
    .with_oidc(federation);
    let app = hub_router(hub, &ServerConfig::default());

    // The login cookie carries the Secure attribute for https redirects.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/api/v1/auth/oidc/login")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    let cookie = response_cookie(&response, "arkflow_oidc_state").expect("state cookie");
    assert!(
        response
            .headers()
            .get(header::SET_COOKIE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("; Secure")),
        "an https redirect must set the Secure attribute"
    );
    let mut parts = cookie.split('.');
    let state = parts.next().unwrap().to_owned();
    let nonce = parts.next_back().unwrap().to_owned();

    let callback = |query: &str| {
        app.clone().oneshot(
            axum::http::Request::get(format!("/api/v1/auth/oidc/callback?{query}"))
                .header(header::COOKIE, format!("arkflow_oidc_state={cookie}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
    };

    // Missing state parameter with a valid cookie.
    let response = callback(&format!("code=nonce-{nonce}")).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    // The token exchange itself fails.
    let response = callback(&format!("state={state}&code=reject-me"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    // The nonce matches but the token comes from the wrong issuer.
    let response = callback(&format!("state={state}&code=wrong-issuer-nonce-{nonce}"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    // The happy path still issues a Secure session cookie.
    let response = callback(&format!("state={state}&code=nonce-{nonce}"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert!(
        response
            .headers()
            .get(header::SET_COOKIE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("; Secure")),
        "the session cookie must carry the Secure attribute"
    );
}

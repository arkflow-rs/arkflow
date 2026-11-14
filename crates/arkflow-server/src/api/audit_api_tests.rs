//! Audit access and query regressions: authorization precedes reads, and
//! filters are applied before pagination over the retained history.
use super::tests::{get_json, request_without_body};
use super::{hub, hub_router, ServerConfig};
use crate::storage::{AuditRecord, ControlPlaneStore, StorageActor};
use axum::http::StatusCode;

async fn audit_router(credential: &str, label: &str) -> (hub::Hub, axum::Router) {
    let store = ControlPlaneStore::contract(label).await;
    let hub = hub::Hub::with_storage(
        hub::HubConfig {
            operator_token: Some(credential.into()),
            node_token: Some("node-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 100,
            session_ttl_ms: hub::default_session_ttl_ms(),
            approval_required: false,
            approval_ttl_ms: hub::default_approval_ttl_ms(),
        },
        StorageActor::start(store, 8),
    );
    let app = hub_router(hub.clone(), &ServerConfig::default());
    (hub, app)
}

async fn seed_audit(hub: &hub::Hub, actor: Option<&str>, action: &str, resource: &str, time: u64) {
    hub.record_audit_event(AuditRecord {
        event_id: 0,
        actor: actor.map(str::to_owned),
        action: action.into(),
        resource_type: "job".into(),
        resource_id: Some(resource.into()),
        node_id: None,
        stream_id: None,
        correlation_id: None,
        outcome: "accepted".into(),
        failure_code: None,
        message: None,
        occurred_at_ms: time,
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn audit_reads_enforce_role_and_authentication() {
    for (role, expected) in [
        ("admin", StatusCode::OK),
        ("viewer", StatusCode::OK),
        ("operator", StatusCode::FORBIDDEN),
    ] {
        let credential = format!("reader|{role}|reader-secret");
        let (hub, app) = audit_router(&credential, &format!("audit_role_{role}")).await;
        seed_audit(&hub, Some("alice"), "job.start", "orders", 100).await;
        let (status, page) = get_json(&app, "/api/v1/audit", "reader-secret").await;
        assert_eq!(status, expected, "{role}: {page}");
        if expected == StatusCode::OK {
            assert_eq!(page["items"][0]["resource_id"], "orders");
        } else {
            assert!(page.get("items").is_none(), "{page}");
            let records = hub.audit(None).await.unwrap();
            let denial = records
                .iter()
                .find(|record| record.action == "operator.readaudit")
                .unwrap();
            assert_eq!(denial.actor.as_deref(), Some("reader"));
            assert_eq!(denial.failure_code.as_deref(), Some("forbidden"));
        }
        let (status, body) = request_without_body(&app, "GET", "/api/v1/audit", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    }
}

#[tokio::test]
async fn audit_scopes_cannot_be_widened_by_filters() {
    let (hub, app) = audit_router("reader|viewer|reader-secret|audit=orders", "audit_scopes").await;
    seed_audit(&hub, Some("alice"), "job.start", "orders", 100).await;
    seed_audit(&hub, Some("alice"), "job.start", "payments", 100).await;
    let (status, page) = get_json(&app, "/api/v1/audit?resource_id=orders", "reader-secret").await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["total"], 1);
    assert_eq!(page["items"][0]["resource_id"], "orders");
    for path in [
        "/api/v1/audit",
        "/api/v1/audit?actor=alice",
        "/api/v1/audit?resource_id=payments",
    ] {
        let (status, body) = get_json(&app, path, "reader-secret").await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}: {body}");
        assert!(body.get("items").is_none());
    }
    let (_, app) = audit_router(
        "reader|viewer|reader-secret|job=orders",
        "audit_wrong_scope",
    )
    .await;
    let (status, body) = get_json(&app, "/api/v1/audit?resource_id=orders", "reader-secret").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}

#[tokio::test]
async fn audit_filters_precede_pagination_and_include_time_endpoints() {
    let (hub, app) = audit_router("root|admin|admin-secret", "audit_filters").await;
    seed_audit(&hub, Some("alice"), "job.start", "orders", 100).await;
    seed_audit(&hub, Some("alice"), "job.start", "orders", 200).await;
    seed_audit(&hub, Some("alice"), "job.start", "orders", 300).await;
    seed_audit(&hub, Some("bob"), "job.start", "orders", 150).await;
    seed_audit(&hub, Some("alice"), "job.stop", "orders", 150).await;
    seed_audit(&hub, Some("alice"), "job.start", "payments", 150).await;
    seed_audit(&hub, None, "job.start", "orders", 150).await;
    // Newer non-matches push the matches beyond both the first page and
    // the storage result bound, so SQL must filter before applying LIMIT.
    for time in 1_000..2_055 {
        seed_audit(&hub, Some("bob"), "job.start", "orders", time).await;
    }
    let query = "/api/v1/audit?resource_id=orders&actor=alice&action=job.start&from_ms=100&to_ms=200&page_size=1";
    let (status, first) = get_json(&app, query, "admin-secret").await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["total"], 2);
    assert_eq!(first["items"].as_array().unwrap().len(), 1);
    assert_eq!(first["items"][0]["occurred_at_ms"], 200);
    let (status, second) = get_json(&app, &format!("{query}&page=2"), "admin-secret").await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second["total"], 2);
    assert_eq!(second["items"][0]["occurred_at_ms"], 100);
    let (_, third) = get_json(&app, &format!("{query}&page=3"), "admin-secret").await;
    assert!(third["items"].as_array().unwrap().is_empty());
    assert_eq!(third["total"], 2);

    for (filter, expected) in [
        ("actor=alice&action=job.start&from_ms=200", 2),
        ("actor=alice&action=job.start&to_ms=100", 1),
        ("actor=alice&action=job.start&from_ms=200&to_ms=200", 1),
        ("actor=unknown", 0),
        ("action=job.unknown", 0),
    ] {
        let path = format!("/api/v1/audit?resource_id=orders&{filter}");
        let (status, page) = get_json(&app, &path, "admin-secret").await;
        assert_eq!(status, StatusCode::OK, "{path}: {page}");
        assert_eq!(page["total"], expected, "{path}: {page}");
    }
}

#[tokio::test]
async fn audit_invalid_time_range_returns_stable_problem() {
    let (_, app) = audit_router("root|admin|admin-secret", "audit_invalid_range").await;
    let (status, body) =
        get_json(&app, "/api/v1/audit?from_ms=201&to_ms=200", "admin-secret").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_audit_query");
}

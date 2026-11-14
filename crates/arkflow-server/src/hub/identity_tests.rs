//! Managed identity tests: credential tokens, revocation, directory
//! invariants, and authentication resolution.

use super::identity::{
    hash_secret, parse_managed_token, validate_user_id, MAX_CREDENTIALS_PER_USER,
};
use super::{default_session_ttl_ms, Hub, HubConfig};
use crate::storage::{ControlPlaneStore, StorageActor, UserRecord};

fn config(with_static: bool) -> HubConfig {
    HubConfig {
        operator_token: with_static.then(|| "break-glass".to_string()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
        approval_required: false,
        approval_ttl_ms: crate::hub::default_approval_ttl_ms(),
    }
}

async fn identity_fixture(name: &str, with_static: bool) -> Hub {
    let store = ControlPlaneStore::contract(name).await;
    Hub::with_storage(config(with_static), StorageActor::start(store, 8))
}

fn user(user_id: &str, role: &str) -> UserRecord {
    UserRecord {
        user_id: user_id.into(),
        display_name: user_id.into(),
        role: role.into(),
        scopes_json: "[]".into(),
        disabled: false,
        oidc_subject: None,
        tokens_valid_after_ms: 0,
        created_at_ms: 0,
        updated_at_ms: 0,
    }
}

#[test]
fn credential_token_format_and_hash_are_stable() {
    assert_eq!(
        parse_managed_token("ark_abc_secret"),
        Some(("abc", "secret"))
    );
    assert_eq!(
        parse_managed_token("ark_abc_secret_with_underscore"),
        Some(("abc", "secret_with_underscore"))
    );
    assert_eq!(parse_managed_token("ark__secret"), None);
    assert_eq!(parse_managed_token("ark_abc_"), None);
    assert_eq!(parse_managed_token("Bearer abc"), None);
    let hash = hash_secret("cred", "secret");
    assert_eq!(hash, hash_secret("cred", "secret"), "hash is deterministic");
    assert_ne!(hash, hash_secret("cred", "other"));
    assert_ne!(hash, hash_secret("other", "secret"));
    assert!(validate_user_id("alice").is_ok());
    assert!(validate_user_id("alice.smith-1").is_ok());
    assert!(validate_user_id("").is_err());
    assert!(validate_user_id("Alice").is_err());
    assert!(validate_user_id("alice smith").is_err());
}

#[tokio::test]
async fn managed_credential_authenticates_and_revocation_is_immediate() {
    let hub = identity_fixture("managed_credential_authenticates", false).await;
    let created = hub
        .create_user(user("alice", "operator"), "admin")
        .await
        .unwrap();
    let (credential, token) = hub
        .issue_credential("alice", "laptop", None, "admin")
        .await
        .unwrap();
    assert_eq!(credential.user_id, "alice");
    assert!(credential.secret_hash.len() == 64);

    let principal = hub.operator_principal(Some(&token)).await.unwrap();
    assert_eq!(principal.id, "alice");
    assert_eq!(
        principal.roles,
        vec![crate::api_contract::OperatorRole::Operator]
    );
    assert!(principal.can(crate::api_contract::OperatorAction::Operate));
    assert!(!principal.can(crate::api_contract::OperatorAction::ReadAudit));
    let _ = created;

    // A tampered secret is rejected (same response shape as unknown).
    let tampered = token.replace("_", "_0");
    assert!(hub.operator_principal(Some(&tampered)).await.is_none());
    assert!(hub
        .operator_principal(Some("ark_unknown_secret"))
        .await
        .is_none());

    let metrics = hub.command_metrics().render();
    assert!(
        metrics.contains("arkflow_operator_auth_total{source=\"managed\",outcome=\"ok\"}"),
        "metrics missing managed ok: {metrics}"
    );

    // Revocation takes effect on the next request.
    assert!(hub
        .revoke_credential(&credential.credential_id, "admin")
        .await
        .unwrap());
    assert!(hub.operator_principal(Some(&token)).await.is_none());
    let metrics = hub.command_metrics().render();
    assert!(
        metrics.contains("arkflow_operator_auth_total{source=\"managed\",outcome=\"denied\"}"),
        "metrics missing managed denied: {metrics}"
    );
}

#[tokio::test]
async fn rotation_replaces_the_previous_credential() {
    let hub = identity_fixture("rotation_replaces_the_previous_credential", false).await;
    hub.create_user(user("alice", "viewer"), "admin")
        .await
        .unwrap();
    let (first, token) = hub
        .issue_credential("alice", "laptop", None, "admin")
        .await
        .unwrap();
    let (second, rotated) = hub
        .rotate_credential(&first.credential_id, "admin")
        .await
        .unwrap();
    assert_ne!(first.credential_id, second.credential_id);
    assert!(hub.operator_principal(Some(&token)).await.is_none());
    let principal = hub.operator_principal(Some(&rotated)).await.unwrap();
    assert_eq!(principal.id, "alice");
    assert_eq!(
        principal.roles,
        vec![crate::api_contract::OperatorRole::Viewer]
    );
    // Rotating a revoked credential is rejected.
    assert!(hub
        .rotate_credential(&first.credential_id, "admin")
        .await
        .is_err());
}

#[tokio::test]
async fn expired_disabled_and_deleted_users_cannot_authenticate() {
    let hub = identity_fixture("expired_disabled_deleted_users", false).await;
    hub.create_user(user("alice", "operator"), "admin")
        .await
        .unwrap();
    let (_, expired) = hub
        .issue_credential("alice", "old", Some(1), "admin")
        .await
        .unwrap();
    assert!(hub.operator_principal(Some(&expired)).await.is_none());

    let (_, token) = hub
        .issue_credential("alice", "live", None, "admin")
        .await
        .unwrap();
    let mut record = hub.user("alice").await.unwrap().unwrap();
    record.disabled = true;
    hub.save_user(record, "admin").await.unwrap();
    assert!(hub.operator_principal(Some(&token)).await.is_none());
    let mut record = hub.user("alice").await.unwrap().unwrap();
    record.disabled = false;
    hub.save_user(record, "admin").await.unwrap();
    assert!(hub.operator_principal(Some(&token)).await.is_some());
    assert!(hub.delete_user("alice", "admin").await.unwrap());
    assert!(hub.operator_principal(Some(&token)).await.is_none());
}

#[tokio::test]
async fn directory_invariants_are_enforced() {
    let hub = identity_fixture("directory_invariants_are_enforced", false).await;
    // Without a static credential the last enabled admin is protected.
    hub.create_user(user("admin1", "admin"), "admin")
        .await
        .unwrap();
    assert!(matches!(
        hub.delete_user("admin1", "admin").await,
        Err(super::error::HubError::LastAdminRequired)
    ));
    let mut admin1 = hub.user("admin1").await.unwrap().unwrap();
    admin1.disabled = true;
    assert!(matches!(
        hub.save_user(admin1, "admin").await,
        Err(super::error::HubError::LastAdminRequired)
    ));

    // With a second admin the first can be demoted.
    hub.create_user(user("admin2", "admin"), "admin")
        .await
        .unwrap();
    let mut admin1 = hub.user("admin1").await.unwrap().unwrap();
    admin1.role = "viewer".into();
    hub.save_user(admin1, "admin").await.unwrap();
    assert_eq!(hub.user("admin1").await.unwrap().unwrap().role, "viewer");

    // Duplicate ids and OIDC subjects are rejected.
    assert!(hub
        .create_user(user("admin1", "viewer"), "admin")
        .await
        .is_err());
    let mut bound = user("bound", "viewer");
    bound.oidc_subject = Some("sub-1".into());
    hub.create_user(bound, "admin").await.unwrap();
    let mut clash = user("clash", "viewer");
    clash.oidc_subject = Some("sub-1".into());
    assert!(matches!(
        hub.create_user(clash, "admin").await,
        Err(super::error::HubError::OidcSubjectConflict)
    ));

    // Per-user credential bound.
    for index in 0..MAX_CREDENTIALS_PER_USER {
        hub.issue_credential("bound", &format!("label-{index}"), None, "admin")
            .await
            .unwrap();
    }
    assert!(matches!(
        hub.issue_credential("bound", "one-too-many", None, "admin")
            .await,
        Err(super::error::HubError::CredentialLimit)
    ));
}

#[tokio::test]
async fn managed_identity_requires_durable_storage() {
    let volatile = Hub::new(config(false));
    assert!(matches!(
        volatile.list_users().await,
        Err(super::error::HubError::IdentityStoreUnavailable)
    ));
    // A managed-looking token never authenticates on a volatile Hub and
    // never falls through to the OIDC path.
    assert!(volatile
        .operator_principal(Some("ark_abc_secret"))
        .await
        .is_none());
}

#[tokio::test]
async fn revoke_tokens_clears_sessions_and_raises_the_threshold() {
    let hub = identity_fixture("revoke_tokens_clears_sessions", false).await;
    hub.create_user(user("alice", "operator"), "admin")
        .await
        .unwrap();
    let (_, token) = hub
        .issue_credential("alice", "laptop", None, "admin")
        .await
        .unwrap();
    let sessions = hub.revoke_user_tokens("alice", "admin").await.unwrap();
    assert_eq!(sessions, 0);
    let record = hub.user("alice").await.unwrap().unwrap();
    assert!(record.tokens_valid_after_ms > 0);
    // Opaque credentials are revoked individually, not by the token epoch.
    assert!(hub.operator_principal(Some(&token)).await.is_some());
}

#[tokio::test]
async fn durable_sessions_survive_restart_and_are_revocable() {
    let store = ControlPlaneStore::contract("durable_sessions_survive_restart").await;
    let storage = StorageActor::start(store, 8);
    let hub = Hub::with_storage(config(false), storage.clone());
    hub.create_user(user("alice", "operator"), "admin")
        .await
        .unwrap();
    let principal = crate::api_contract::OperatorPrincipal {
        id: "alice".into(),
        roles: vec![crate::api_contract::OperatorRole::Operator],
        scopes: Vec::new(),
    };
    let session_id = hub.create_session(principal.clone()).await.unwrap();
    // The same session resolves through a Hub that never saw it in memory:
    // sessions are durable, not per-process.
    let restarted = Hub::with_storage(config(false), storage.clone());
    let resolved = restarted.resolve_session(&session_id).await.unwrap();
    assert_eq!(resolved.id, "alice");

    // Revocation deletes the row; the old cookie is immediately invalid.
    restarted
        .revoke_user_tokens("alice", "admin")
        .await
        .unwrap();
    assert!(restarted.resolve_session(&session_id).await.is_none());

    // Disabling the bound user invalidates its remaining sessions.
    let session_id = restarted.create_session(principal).await.unwrap();
    let mut record = restarted.user("alice").await.unwrap().unwrap();
    record.disabled = true;
    restarted.save_user(record, "admin").await.unwrap();
    assert!(restarted.resolve_session(&session_id).await.is_none());

    // Logout deletes the row.
    let mut record = restarted.user("alice").await.unwrap().unwrap();
    record.disabled = false;
    restarted.save_user(record, "admin").await.unwrap();
    let principal = crate::api_contract::OperatorPrincipal {
        id: "alice".into(),
        roles: vec![crate::api_contract::OperatorRole::Operator],
        scopes: Vec::new(),
    };
    let session_id = restarted.create_session(principal).await.unwrap();
    assert!(restarted.resolve_session(&session_id).await.is_some());
    restarted.remove_session(&session_id).await;
    assert!(restarted.resolve_session(&session_id).await.is_none());
}

#[tokio::test]
async fn federated_subjects_follow_the_directory_and_revocation_threshold() {
    let hub = identity_fixture("federated_subjects_follow_directory", true).await;
    let mut bound = user("alice", "admin");
    bound.oidc_subject = Some("sub-alice".into());
    hub.create_user(bound, "admin").await.unwrap();
    let claims_principal = |id: &str| crate::api_contract::OperatorPrincipal {
        id: id.into(),
        roles: vec![crate::api_contract::OperatorRole::Viewer],
        scopes: Vec::new(),
    };

    // Bound subject: directory role/scopes win over claims.
    let now = crate::hub::now_ms_for_metrics();
    let principal = hub
        .apply_federated_directory(claims_principal("sub-alice"), Some(now), false)
        .await
        .unwrap();
    assert_eq!(principal.id, "alice");
    assert_eq!(
        principal.roles,
        vec![crate::api_contract::OperatorRole::Admin]
    );

    // Missing iat on a bound subject fails closed.
    assert!(hub
        .apply_federated_directory(claims_principal("sub-alice"), None, false)
        .await
        .is_none());

    // Revocation threshold rejects tokens issued before it (60s skew).
    hub.revoke_user_tokens("alice", "admin").await.unwrap();
    let stale_iat = now.saturating_sub(120_000);
    assert!(hub
        .apply_federated_directory(claims_principal("sub-alice"), Some(stale_iat), false)
        .await
        .is_none());
    assert!(hub
        .apply_federated_directory(claims_principal("sub-alice"), Some(now + 1), false)
        .await
        .is_some());

    // Disabled bound users fail closed.
    let mut record = hub.user("alice").await.unwrap().unwrap();
    record.disabled = true;
    hub.save_user(record, "admin").await.unwrap();
    assert!(hub
        .apply_federated_directory(claims_principal("sub-alice"), Some(now + 1), false)
        .await
        .is_none());

    // Unbound subjects keep claim mapping unless managed-users-only is set.
    assert!(hub
        .apply_federated_directory(claims_principal("stranger"), None, false)
        .await
        .is_some());
    assert!(hub
        .apply_federated_directory(claims_principal("stranger"), None, true)
        .await
        .is_none());
}

#[tokio::test]
async fn unparsable_scopes_fail_closed() {
    let store = ControlPlaneStore::contract("unparsable_scopes_fail_closed").await;
    let storage = StorageActor::start(store, 8);
    let hub = Hub::with_storage(config(false), storage.clone());

    // Written directly to storage: a corrupt `scopes_json` row comes from
    // schema drift, not the validated API path. An empty scope list means
    // "unrestricted", so a parse error must deny instead of widen.
    let mut alice = user("alice", "operator");
    alice.scopes_json = "not-json".into();
    storage.upsert_user(alice).await.unwrap();
    let (_, token) = hub
        .issue_credential("alice", "laptop", None, "admin")
        .await
        .unwrap();
    assert!(hub.operator_principal(Some(&token)).await.is_none());

    let mut bob = user("bob", "admin");
    bob.oidc_subject = Some("sub-bob".into());
    bob.scopes_json = "not-json".into();
    storage.upsert_user(bob).await.unwrap();
    let claims = crate::api_contract::OperatorPrincipal {
        id: "sub-bob".into(),
        roles: vec![crate::api_contract::OperatorRole::Viewer],
        scopes: Vec::new(),
    };
    assert!(hub
        .apply_federated_directory(claims, Some(crate::hub::now_ms_for_metrics()), false)
        .await
        .is_none());
}

#[tokio::test]
async fn volatile_hub_managed_users_only_denies_unbound_subjects() {
    let volatile = Hub::new(config(false));
    let stranger = crate::api_contract::OperatorPrincipal {
        id: "stranger".into(),
        roles: vec![crate::api_contract::OperatorRole::Viewer],
        scopes: Vec::new(),
    };
    // Without storage the directory cannot be consulted; a deployment that
    // requires managed users must not admit unbound subjects there either.
    assert!(volatile
        .apply_federated_directory(stranger.clone(), None, true)
        .await
        .is_none());
    assert!(volatile
        .apply_federated_directory(stranger, None, false)
        .await
        .is_some());
}

#[tokio::test]
async fn authorization_changes_revoke_durable_sessions() {
    let hub = identity_fixture("authorization_changes_revoke_sessions", false).await;
    hub.create_user(user("alice", "operator"), "admin")
        .await
        .unwrap();
    let principal = crate::api_contract::OperatorPrincipal {
        id: "alice".into(),
        roles: vec![crate::api_contract::OperatorRole::Operator],
        scopes: Vec::new(),
    };
    let session_id = hub.create_session(principal).await.unwrap();
    assert!(hub.resolve_session(&session_id).await.is_some());

    // An update that does not touch authorization keeps the session.
    let record = hub.user("alice").await.unwrap().unwrap();
    hub.save_user(record, "admin").await.unwrap();
    assert!(hub.resolve_session(&session_id).await.is_some());

    // A role change revokes it: the stored principal still carries the old
    // role until the next login.
    let mut record = hub.user("alice").await.unwrap().unwrap();
    record.role = "viewer".into();
    hub.save_user(record, "admin").await.unwrap();
    assert!(hub.resolve_session(&session_id).await.is_none());

    // A scope-only change revokes too.
    let session_id = hub
        .create_session(crate::api_contract::OperatorPrincipal {
            id: "alice".into(),
            roles: vec![crate::api_contract::OperatorRole::Viewer],
            scopes: Vec::new(),
        })
        .await
        .unwrap();
    assert!(hub.resolve_session(&session_id).await.is_some());
    let mut record = hub.user("alice").await.unwrap().unwrap();
    record.scopes_json = r#"[{"resource_type":"job","resource_id":"orders"}]"#.into();
    hub.save_user(record, "admin").await.unwrap();
    assert!(hub.resolve_session(&session_id).await.is_none());
}

async fn identity_http_request(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    payload: Option<serde_json::Value>,
) -> (axum::http::StatusCode, serde_json::Value) {
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use tower::ServiceExt;

    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"));
    let request = match payload {
        Some(payload) => request
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string())),
        None => request.body(Body::empty()),
    };
    let response = app.clone().oneshot(request.unwrap()).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

/// Two independent PostgreSQL stores must observe the same revocation and
/// reject identity writes stamped with the previous leader's fencing epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_identity_revocation_survives_failover_and_fences_stale_writes() {
    use super::{HubError, HubHaConfig, Leadership};
    use axum::http::StatusCode;
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let Ok(base) = std::env::var("ARKFLOW_TEST_POSTGRES_URL") else {
        eprintln!("skipping: ARKFLOW_TEST_POSTGRES_URL not set");
        return;
    };
    let mut url =
        url::Url::parse(&base).expect("ARKFLOW_TEST_POSTGRES_URL must be a PostgreSQL URL");
    assert!(matches!(url.scheme(), "postgres" | "postgresql"));
    // The identifier contains only a fixed prefix, pid, and hexadecimal RNG
    // output. No public-schema fallback: every pool uses only this schema.
    let schema = format!(
        "ct_identity_ha_{}_{:016x}",
        std::process::id(),
        rand::random::<u64>()
    );
    url.query_pairs_mut()
        .append_pair("options", &format!("-c search_path={schema}"));
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&base)
        .await
        .expect("connect to real PostgreSQL");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create isolated identity HA schema");
    eprintln!("running PostgreSQL identity HA assertions in schema {schema}");

    let mut pools = Vec::with_capacity(2);
    // Keep teardown outside the caught assertions so a failed assertion (or
    // scenario timeout) still closes both pools and drops the test schema.
    let outcome = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(30), async {
        let backend_a = crate::storage::postgres::PostgresBackend::open(url.as_str())
            .await
            .unwrap();
        pools.push(backend_a.pool().clone());
        let backend_b = crate::storage::postgres::PostgresBackend::open(url.as_str())
            .await
            .unwrap();
        pools.push(backend_b.pool().clone());
        for pool in &pools {
            let actual_schema: String = sqlx::query_scalar("SELECT current_schema()")
                .fetch_one(pool)
                .await
                .unwrap();
            assert_eq!(actual_schema, schema);
        }
        let storage_a = StorageActor::start(ControlPlaneStore::Postgres(backend_a), 8);
        let storage_b = StorageActor::start(ControlPlaneStore::Postgres(backend_b), 8);
        let claim_a = storage_a.leadership_epoch();
        let claim_b = storage_b.leadership_epoch();
        assert!(!std::sync::Arc::ptr_eq(&claim_a, &claim_b));
        let ha = |holder: &str| HubHaConfig {
            enabled: true,
            holder_id: Some(holder.into()),
            lease_ttl_ms: 60_000,
            ..HubHaConfig::default()
        };
        let hub_a = Hub::with_storage(config(true), storage_a.clone()).with_ha(ha("identity-ha-a"));
        let hub_b = Hub::with_storage(config(true), storage_b.clone()).with_ha(ha("identity-ha-b"));
        hub_a.enter_election().await;
        hub_b.enter_election().await;
        let old_epoch = hub_a.run_election_tick().await.epoch().unwrap();
        assert_eq!(old_epoch, 1);
        assert!(matches!(
            hub_b.run_election_tick().await,
            Leadership::Standby { .. }
        ));
        assert_eq!(claim_a.load(Ordering::Acquire), old_epoch);
        assert_eq!(claim_b.load(Ordering::Acquire), 0);
        let lease = storage_b.hub_lease_snapshot().await.unwrap().unwrap();
        assert_eq!(lease.holder, "identity-ha-a");
        assert_eq!(lease.epoch, old_epoch);

        let app_a = crate::hub_router(hub_a.clone(), &crate::ServerConfig::default());
        let app_b = crate::hub_router(hub_b.clone(), &crate::ServerConfig::default());
        let (status, standby) =
            identity_http_request(&app_b, "GET", "/api/v1/system", "break-glass", None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(standby["code"], "hub_standby");
        let (status, _) = identity_http_request(
            &app_a,
            "POST",
            "/api/v1/users",
            "break-glass",
            Some(serde_json::json!({"user_id": "alice", "role": "viewer"})),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, issued) = identity_http_request(
            &app_a,
            "POST",
            "/api/v1/users/alice/credentials",
            "break-glass",
            Some(serde_json::json!({"label": "before-failover"})),
        )
        .await;
        // Do not include an issuance response in assertion diagnostics: it
        // contains the one-time plaintext credential.
        assert_eq!(status, StatusCode::CREATED);
        let token = issued["token"].as_str().unwrap();
        let credential_id = issued["credential"]["credential_id"].as_str().unwrap();
        let (status, identity) =
            identity_http_request(&app_a, "GET", "/api/v1/system", token, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(identity["principal"]["id"], "alice");
        assert_eq!(
            identity["principal"]["roles"],
            serde_json::json!(["viewer"])
        );
        let (status, _) = identity_http_request(
            &app_a,
            "DELETE",
            &format!("/api/v1/credentials/{credential_id}"),
            "break-glass",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = identity_http_request(&app_a, "GET", "/api/v1/system", token, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        hub_a.release_leadership().await;
        assert!(matches!(
            hub_a.leadership().await,
            Leadership::Standby { .. }
        ));
        assert_eq!(claim_a.load(Ordering::Acquire), 0);
        let lease = storage_b.hub_lease_snapshot().await.unwrap().unwrap();
        assert_eq!(lease.epoch, old_epoch);
        assert!(lease.expires_at_ms <= super::now_ms());
        let new_epoch = hub_b.run_election_tick().await.epoch().unwrap();
        assert_eq!(new_epoch, old_epoch + 1);
        assert_eq!(claim_b.load(Ordering::Acquire), new_epoch);
        let lease = storage_a.hub_lease_snapshot().await.unwrap().unwrap();
        assert_eq!(lease.holder, "identity-ha-b");
        assert_eq!(lease.epoch, new_epoch);
        let (status, _) =
            identity_http_request(&app_b, "GET", "/api/v1/system", "break-glass", None).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "the new leader must serve HTTP requests"
        );
        let (status, _) = identity_http_request(&app_b, "GET", "/api/v1/system", token, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let users_before = hub_b.list_users().await.unwrap();
        assert_eq!(users_before.len(), 1);
        let credentials_before = hub_b.list_credentials("alice").await.unwrap();
        assert_eq!(credentials_before.len(), 1);
        assert_eq!(credentials_before[0].credential_id, credential_id);
        assert!(credentials_before[0].revoked_at_ms.is_some());
        let mut stale_user = users_before[0].clone();
        stale_user.display_name = "stale leader update".into();
        stale_user.role = "admin".into();
        // Retain the old sender epoch to model work stamped before demotion.
        // Election, lease release/acquisition, and the write fence stay real.
        claim_a.store(old_epoch, Ordering::Release);
        let stale_update = match hub_a.save_user(stale_user, "old-leader").await {
            Ok(_) => panic!("a stale leader must not update a user"),
            Err(error) => error,
        };
        let stale_issuance = match hub_a
            .issue_credential("alice", "stale-leader", None, "old-leader")
            .await
        {
            Ok(_) => panic!("a stale leader must not issue a credential"),
            Err(error) => error,
        };
        for error in [stale_update, stale_issuance] {
            assert!(
                matches!(&error, HubError::StaleLeader { claimed, current }
                    if *claimed == old_epoch && *current == new_epoch),
                "expected the PostgreSQL write fence to reject the old epoch: {error:?}"
            );
        }
        claim_a.store(0, Ordering::Release);
        assert_eq!(claim_b.load(Ordering::Acquire), new_epoch);
        assert_eq!(hub_b.list_users().await.unwrap(), users_before);
        assert!(
            hub_b.list_credentials("alice").await.unwrap() == credentials_before,
            "stale writes changed the new leader's credential directory"
        );

        // A write using the actual new leader's epoch succeeds; authentication
        // is healthy while the old credential remains revoked.
        let (status, fresh) = identity_http_request(
            &app_b,
            "POST",
            "/api/v1/users/alice/credentials",
            "break-glass",
            Some(serde_json::json!({"label": "after-failover"})),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, identity) = identity_http_request(
            &app_b,
            "GET",
            "/api/v1/system",
            fresh["token"].as_str().unwrap(),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(identity["principal"]["id"], "alice");
        let (status, _) = identity_http_request(&app_b, "GET", "/api/v1/system", token, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        hub_b.release_leadership().await;
    }))
    .catch_unwind()
    .await;

    let cleanup = tokio::time::timeout(Duration::from_secs(10), async {
        for pool in pools {
            pool.close().await;
        }
        let dropped = sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin)
            .await;
        admin.close().await;
        dropped
    })
    .await;
    if !matches!(&cleanup, Ok(Ok(_))) {
        eprintln!("identity HA cleanup failed; remove isolated schema {schema}");
    }
    match outcome {
        Ok(result) => result.expect("PostgreSQL identity HA scenario timed out"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("PostgreSQL identity HA cleanup timed out")
        .expect("drop isolated identity HA schema");
    eprintln!("PostgreSQL identity HA assertions completed; removed schema {schema}");
}

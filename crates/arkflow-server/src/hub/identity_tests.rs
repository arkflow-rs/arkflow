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

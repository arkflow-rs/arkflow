//! Per-node credential identity, bootstrap lock-out, break-glass,
//! cross-node impersonation defense, immediate session invalidation, and
//! HTTP API / audit trail coverage (PA-P1-07).

use super::{
    default_approval_ttl_ms, default_session_ttl_ms, nodes::MAX_CREDENTIALS_PER_NODE, Hub,
    HubConfig, HubError, RegisterRequest, MAX_NODES, SUPPORTED_PROTOCOL_VERSION,
};
use crate::api::{hub_router, ServerConfig};
use crate::hub::{AgentAuth, CommandResult, HeartbeatRequest, HubOperationState};
use crate::storage::{ControlPlaneStore, DesiredMutation, StorageActor};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt;

fn hub_config() -> HubConfig {
    HubConfig {
        operator_token: Some("ops|operator|op-secret".into()),
        node_token: Some("shared-bootstrap-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 50,
        session_ttl_ms: default_session_ttl_ms(),
        approval_required: false,
        approval_ttl_ms: default_approval_ttl_ms(),
    }
}

async fn durable_hub(label: &str) -> Hub {
    let storage = StorageActor::start(ControlPlaneStore::contract(label).await, 8);
    Hub::with_storage(hub_config(), storage)
}

async fn http_json(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"));
    let request_body = if let Some(json_body) = body {
        builder = builder.header("content-type", "application/json");
        Body::from(json_body.to_string())
    } else {
        Body::empty()
    };
    let response = app
        .clone()
        .oneshot(builder.body(request_body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

#[tokio::test]
async fn shared_bootstrap_enrolls_per_node_credential_once_and_locks_out_shared_token() {
    let hub = durable_hub("node_bootstrap_once").await;

    // Initial bootstrap with shared node_token mints a per-node credential.
    let first = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec!["job_runtime".into()],
            boot_id: Some("boot-1".into()),
        })
        .await
        .unwrap();
    let issued = first
        .node_credential
        .expect("bootstrap registration must return per-node credential once");
    assert!(issued.starts_with("arkn_"));

    // Subsequent registration with shared node_token MUST be rejected once enrolled.
    let err = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot-2".into()),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, HubError::Unauthorized));

    // Re-registering with the issued per-node credential succeeds and does not re-mint.
    let second = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: issued.clone(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot-2".into()),
        })
        .await
        .unwrap();
    assert!(second.node_credential.is_none());

    // Another node CANNOT use node-a's credential (strict node_id binding).
    let spoof = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-b".into(),
            node_token: issued,
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(spoof, HubError::Unauthorized));

    let audit = hub.audit(Some("node-b")).await.unwrap();
    assert!(audit.iter().any(|record| {
        record.action == "agent.register"
            && record.outcome == "rejected"
            && record.failure_code.as_deref() == Some("node_identity_mismatch")
    }));

    // Explicit break-glass flag permits emergency re-registration via shared node_token.
    let hub = hub.with_allow_shared_node_token_break_glass(true);
    let break_glass = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot-3".into()),
        })
        .await
        .unwrap();
    assert!(break_glass.node_credential.is_none());
}

#[tokio::test]
async fn bootstrap_disabled_blocks_shared_token_but_allows_preprovisioned_credential() {
    let hub = durable_hub("node_bootstrap_disabled")
        .await
        .with_node_bootstrap_disabled(true);

    let err = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-edge".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, HubError::Unauthorized));

    let (_rec, token) = hub
        .create_node_credential("node-edge", "preprovisioned", None, "ops")
        .await
        .unwrap();
    let ok = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-edge".into(),
            node_token: token,
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap();
    assert!(ok.node_credential.is_none());
}

#[tokio::test]
async fn rotate_and_revoke_immediately_invalidate_active_agent_session_and_enforce_limit() {
    let hub = durable_hub("node_rotate_revoke_invalidation").await;
    let (cred1, token1) = hub
        .create_node_credential("node-1", "primary", None, "ops")
        .await
        .unwrap();

    let session1 = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-1".into(),
            node_token: token1.clone(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot-1".into()),
        })
        .await
        .unwrap();

    // Active session succeeds before rotation.
    hub.heartbeat(HeartbeatRequest {
        auth: AgentAuth {
            node_id: "node-1".into(),
            session_token: session1.session_token.clone(),
        },
        state: "online".into(),
        protocol_version: Some("v1".into()),
        software_version: None,
        capabilities: vec![],
        rollout_id: None,
    })
    .await
    .unwrap();

    // Rotate invalidates the old credential AND immediately expires the active session.
    let (cred2, token2) = hub
        .rotate_node_credential(&cred1.credential_id, "ops")
        .await
        .unwrap();
    let hb_err = hub
        .heartbeat(HeartbeatRequest {
            auth: AgentAuth {
                node_id: "node-1".into(),
                session_token: session1.session_token,
            },
            state: "online".into(),
            protocol_version: Some("v1".into()),
            software_version: None,
            capabilities: vec![],
            rollout_id: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(hb_err, HubError::Unauthorized));

    // Old credential cannot register; new credential registers cleanly.
    assert!(matches!(
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "node-1".into(),
            node_token: token1,
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await,
        Err(HubError::Unauthorized)
    ));
    let session2 = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-1".into(),
            node_token: token2.clone(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: Some("boot-2".into()),
        })
        .await
        .unwrap();

    // Revoking cred2 invalidates session2 immediately too.
    assert!(hub
        .revoke_node_credential(&cred2.credential_id, "ops")
        .await
        .unwrap());
    assert!(matches!(
        hub.commands(AgentAuth {
            node_id: "node-1".into(),
            session_token: session2.session_token,
        })
        .await,
        Err(HubError::Unauthorized)
    ));

    // Enforce MAX_CREDENTIALS_PER_NODE cap.
    for index in 0..MAX_CREDENTIALS_PER_NODE {
        hub.create_node_credential("node-cap", &format!("k-{index}"), None, "ops")
            .await
            .unwrap();
    }
    let cap_err = hub
        .create_node_credential("node-cap", "overflow", None, "ops")
        .await
        .unwrap_err();
    assert!(matches!(cap_err, HubError::CredentialLimit));
}

#[tokio::test]
async fn node_credential_http_routes_enforce_rbac_and_command_result_audits_without_secrets() {
    let storage = StorageActor::start(ControlPlaneStore::contract("node_cred_api_audit").await, 8);
    let hub = Hub::with_storage(
        HubConfig {
            operator_token: Some("ops-a|operator|ops-a-secret|node=node-1".into()),
            node_token: Some("shared-bootstrap-secret".into()),
            insecure_local: false,
            lease_ttl_ms: 10_000,
            poll_interval_ms: 50,
            session_ttl_ms: default_session_ttl_ms(),
            approval_required: false,
            approval_ttl_ms: default_approval_ttl_ms(),
        },
        storage,
    );
    let app = hub_router(hub.clone(), &ServerConfig::default());

    // Scoped operator can issue & list node-1 credentials, forbidden for node-2.
    let (status, created) = http_json(
        &app,
        "POST",
        "/api/v1/nodes/node-1/credentials",
        "ops-a-secret",
        Some(json!({ "label": "prod-key" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let cred_id = created["credential"]["credential_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let token = created["token"].as_str().unwrap().to_owned();
    assert!(token.starts_with(&format!("arkn_{cred_id}_")));

    let (status, _) = http_json(
        &app,
        "POST",
        "/api/v1/nodes/node-2/credentials",
        "ops-a-secret",
        Some(json!({ "label": "denied" })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, listed) = http_json(
        &app,
        "GET",
        "/api/v1/nodes/node-1/credentials",
        "ops-a-secret",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    let listed_str = listed.to_string();
    assert!(!listed_str.contains(&token));
    assert!(!listed_str.contains("secret_hash"));

    // Register node-1 with token, dispatch a command, and confirm terminal command_result audit.
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-1".into(),
            node_token: token.clone(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec!["stream_lifecycle".into()],
            boot_id: Some("boot-1".into()),
        })
        .await
        .unwrap();
    hub.set_desired_state(DesiredMutation {
        node_id: "node-1".into(),
        stream_id: "orders".into(),
        desired_state: "running".into(),
        expected_generation: Some(0),
        ..Default::default()
    })
    .await
    .unwrap();
    hub.reconcile_once("dispatch").await.unwrap();
    let auth = AgentAuth {
        node_id: "node-1".into(),
        session_token: session.session_token,
    };
    let cmd = hub.commands(auth.clone()).await.unwrap().pop().unwrap();
    hub.command_result(
        auth,
        CommandResult {
            command_id: cmd.id,
            operation_id: cmd.operation_id,
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: cmd.correlation_id,
            generation: cmd.generation,
            observed_generation: Some(cmd.generation),
            action_id: None,
            failure_class: None,
            config_version_id: None,
            rollout_id: None,
            observed_checkpoint_id: None,
            checkpoint_manifest_uri: None,
            result: None,
        },
    )
    .await
    .unwrap();

    // Rotate & revoke over HTTP routes.
    let (status, rotated) = http_json(
        &app,
        "POST",
        &format!("/api/v1/node-credentials/{cred_id}/rotate"),
        "ops-a-secret",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rotated}");
    let new_cred_id = rotated["credential"]["credential_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let new_token = rotated["token"].as_str().unwrap().to_owned();

    let (status, _) = http_json(
        &app,
        "DELETE",
        &format!("/api/v1/node-credentials/{new_cred_id}"),
        "ops-a-secret",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let audits = hub.audit(None).await.unwrap();
    assert!(audits
        .iter()
        .any(|row| row.action == "agent.register" && row.actor.as_deref() == Some("node:node-1")));
    assert!(audits.iter().any(|row| {
        row.action == "agent.command_result"
            && row.actor.as_deref() == Some("node:node-1")
            && row.outcome == "succeeded"
    }));
    let serialized_audits = serde_json::to_string(&audits).unwrap();
    assert!(!serialized_audits.contains(&token));
    assert!(!serialized_audits.contains(&new_token));
}

#[tokio::test]
async fn split_job_start_distributes_filtered_cached_edge_secrets_and_evicts_on_stop() {
    let hub = durable_hub("split_per_edge_secrets").await;

    let mut auths = std::collections::BTreeMap::new();
    for (index, node_id) in ["node-a", "node-b", "node-c"].into_iter().enumerate() {
        let session = hub
            .register(RegisterRequest {
                data_address: Some(format!("127.0.0.1:{}", 19100 + index)),
                node_id: node_id.into(),
                node_token: "shared-bootstrap-secret".into(),
                protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
                capabilities: vec![
                    "job_runtime".into(),
                    "state_backend".into(),
                    "network_shuffle".into(),
                ],
                boot_id: Some(format!("boot-{node_id}")),
            })
            .await
            .unwrap();
        let auth = AgentAuth {
            node_id: node_id.into(),
            session_token: session.session_token,
        };
        auths.insert(node_id.to_owned(), (auth, session.node_credential.unwrap()));
    }

    let spec = serde_json::json!({
        "id": "edge-secret-job",
        "version": 1,
        "placement": "split",
        "parallelism": 1,
        "operators": [
            {"id": "source", "kind": "source"},
            {"id": "map", "kind": "map"},
            {"id": "sink", "kind": "sink"}
        ],
        "edges": [
            {"id": "e1", "from": "source", "to": "map"},
            {"id": "e2", "from": "map", "to": "sink"}
        ],
        "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
        "sinks": [{"operator_id": "sink", "output_type": "drop"}]
    });
    hub.upsert_job(crate::storage::JobRecord {
        job_id: "edge-secret-job".into(),
        version: 1,
        spec_json: spec.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec!["node-a".into(), "node-b".into(), "node-c".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
        pause_phase: None,
        pause_artifact_id: None,
        pause_prev_checkpoint_id: None,
        pause_retry_count: 0,
        paused_at_ms: None,
        paused_by: None,
    })
    .await
    .unwrap();

    // Verify per-node command payloads contain ONLY edges touching that node.
    let mut per_node_secrets: std::collections::BTreeMap<String, Vec<(String, String, String)>> =
        std::collections::BTreeMap::new();
    let mut node_a_start = None;
    for node_id in ["node-a", "node-b", "node-c"] {
        let auth = &auths[node_id].0;
        let cmds = hub.commands(auth.clone()).await.unwrap();
        let start = cmds
            .iter()
            .find(|cmd| cmd.operation.as_str() == "job_start")
            .expect("job_start command");
        if node_id == "node-a" {
            node_a_start = Some((
                start.id.clone(),
                start.operation_id.clone(),
                start.correlation_id.clone(),
                start.generation,
            ));
        }
        let payload = start.payload.as_ref().unwrap();
        let entries = payload["edge_secrets"]
            .as_array()
            .expect("edge_secrets array on split job_start");
        let parsed: Vec<(String, String, String)> = entries
            .iter()
            .map(|e| {
                (
                    e["src"].as_str().unwrap().to_owned(),
                    e["dst"].as_str().unwrap().to_owned(),
                    e["secret"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        for (src, dst, secret) in &parsed {
            assert!(
                src == node_id || dst == node_id,
                "{node_id} received unrelated edge {src}->{dst}"
            );
            assert!(!secret.is_empty());
        }
        per_node_secrets.insert(node_id.to_owned(), parsed);
    }

    // Confirm distinct secrets per edge and agreement between endpoints.
    let secrets_a = &per_node_secrets["node-a"];
    let secrets_b = &per_node_secrets["node-b"];
    let secrets_c = &per_node_secrets["node-c"];
    assert_eq!(secrets_a.len(), 1);
    assert_eq!(secrets_b.len(), 2);
    assert_eq!(secrets_c.len(), 1);
    assert_eq!(
        secrets_a[0].2,
        secrets_b
            .iter()
            .find(|e| e.0 == secrets_a[0].0 && e.1 == secrets_a[0].1)
            .unwrap()
            .2
    );
    assert_eq!(
        secrets_c[0].2,
        secrets_b
            .iter()
            .find(|e| e.0 == secrets_c[0].0 && e.1 == secrets_c[0].1)
            .unwrap()
            .2
    );
    assert_ne!(secrets_a[0].2, secrets_c[0].2);

    // Simulate node-a restart/retry on same generation: Hub must serve the cached secret.
    let cached_secret_a_before = secrets_a[0].2.clone();
    let (_, op_id, _, _) = node_a_start.unwrap();
    if let Some(op) = hub.operations.write().await.get_mut(&op_id) {
        op.expires_at_ms = Some(0);
    }
    assert_eq!(hub.expire_stale_job_operations().await.unwrap(), 1);
    let restarted_a = hub
        .register(RegisterRequest {
            data_address: Some("127.0.0.1:19100".into()),
            node_id: "node-a".into(),
            node_token: auths["node-a"].1.clone(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![
                "job_runtime".into(),
                "state_backend".into(),
                "network_shuffle".into(),
            ],
            boot_id: Some("boot-node-a-rebooted".into()),
        })
        .await
        .unwrap();
    hub.reconcile_jobs().await.unwrap();
    let cmds_restarted = hub
        .commands(AgentAuth {
            node_id: "node-a".into(),
            session_token: restarted_a.session_token,
        })
        .await
        .unwrap();
    let restart_start = cmds_restarted
        .iter()
        .find(|cmd| cmd.operation.as_str() == "job_start")
        .unwrap();
    let restart_secret = restart_start.payload.as_ref().unwrap()["edge_secrets"][0]["secret"]
        .as_str()
        .unwrap();
    assert_eq!(restart_secret, cached_secret_a_before);

    // Stopping the job evicts cached secrets for that job.
    hub.update_job_desired_state("edge-secret-job", "stopped", 1)
        .await
        .unwrap();
    hub.reconcile_jobs().await.unwrap();
    assert!(!hub
        .job_edge_secrets
        .read()
        .await
        .keys()
        .any(|(job_id, _)| job_id == "edge-secret-job"));
}

#[tokio::test]
async fn revoking_last_credential_does_not_reopen_shared_bootstrap() {
    let hub = durable_hub("revoke_no_reenroll").await;
    let first = hub
        .register(RegisterRequest {
            node_id: "node-revoked".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            data_address: None,
            boot_id: Some("boot-1".into()),
        })
        .await
        .unwrap();
    let creds = hub.list_node_credentials("node-revoked").await.unwrap();
    assert_eq!(creds.len(), 1);

    assert!(hub
        .revoke_node_credential(&creds[0].credential_id, "admin")
        .await
        .unwrap());

    // Even with zero active credentials remaining, shared node_token cannot re-enroll
    // this node_id unless explicit break-glass is enabled.
    let err = hub
        .register(RegisterRequest {
            node_id: "node-revoked".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            data_address: None,
            boot_id: Some("boot-2".into()),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, HubError::Unauthorized), "{err:?}");

    // The minted credential itself is also rejected.
    let err_cred = hub
        .register(RegisterRequest {
            node_id: "node-revoked".into(),
            node_token: first.node_credential.unwrap(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            data_address: None,
            boot_id: Some("boot-2".into()),
        })
        .await
        .unwrap_err();
    assert!(matches!(err_cred, HubError::Unauthorized), "{err_cred:?}");
}

#[tokio::test]
async fn same_boot_unconfirmed_bootstrap_retry_recovers_until_authenticated_call() {
    let hub = durable_hub("same_boot_unconfirmed_retry").await;
    let first = hub
        .register(RegisterRequest {
            node_id: "node-flaky".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            data_address: None,
            boot_id: Some("boot-same".into()),
        })
        .await
        .unwrap();
    let first_cred = first.node_credential.unwrap();

    // Retry with shared node_token and the SAME boot_id before making any authenticated call:
    // recovers by rotating the single unconfirmed bootstrap credential.
    let retried = hub
        .register(RegisterRequest {
            node_id: "node-flaky".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            data_address: None,
            boot_id: Some("boot-same".into()),
        })
        .await
        .unwrap();
    let second_cred = retried.node_credential.unwrap();
    assert_ne!(second_cred, first_cred);

    // Confirm receipt by sending an authenticated heartbeat.
    hub.heartbeat(HeartbeatRequest {
        auth: AgentAuth {
            node_id: "node-flaky".into(),
            session_token: retried.session_token,
        },
        state: "online".into(),
        protocol_version: Some(SUPPORTED_PROTOCOL_VERSION.into()),
        software_version: None,
        capabilities: vec![],
        rollout_id: None,
    })
    .await
    .unwrap();

    // Once confirmed, even the same boot_id can no longer re-enroll via shared node_token.
    let err = hub
        .register(RegisterRequest {
            node_id: "node-flaky".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            data_address: None,
            boot_id: Some("boot-same".into()),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, HubError::Unauthorized), "{err:?}");

    // The replacement credential still authenticates cleanly.
    hub.register(RegisterRequest {
        node_id: "node-flaky".into(),
        node_token: second_cred,
        protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
        capabilities: vec![],
        data_address: None,
        boot_id: Some("boot-same".into()),
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn full_node_registry_rejects_before_minting_bootstrap_credential() {
    let hub = durable_hub("capacity_before_mint").await;
    for i in 0..MAX_NODES {
        hub.register(RegisterRequest {
            node_id: format!("node-cap-{i}"),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            data_address: None,
            boot_id: Some(format!("boot-{i}")),
        })
        .await
        .unwrap();
    }

    let err = hub
        .register(RegisterRequest {
            node_id: "node-overflow".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            data_address: None,
            boot_id: Some("boot-overflow".into()),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, HubError::Capacity), "{err:?}");
    assert!(
        hub.list_node_credentials("node-overflow")
            .await
            .unwrap()
            .is_empty(),
        "capacity rejection must happen before minting a bootstrap credential"
    );
}

#[tokio::test]
async fn edge_secrets_persist_across_hub_restart_and_refresh_upstream_on_downstream_replacement() {
    let store = ControlPlaneStore::contract("edge_secret_restart_and_replace").await;
    let storage = StorageActor::start(store, 8);
    let hub = Hub::with_storage(hub_config(), storage.clone());

    let reg_a = hub
        .register(RegisterRequest {
            data_address: Some("127.0.0.1:19200".into()),
            node_id: "node-a".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![
                "job_runtime".into(),
                "state_backend".into(),
                "network_shuffle".into(),
            ],
            boot_id: Some("boot-a".into()),
        })
        .await
        .unwrap();
    let reg_b = hub
        .register(RegisterRequest {
            data_address: Some("127.0.0.1:19201".into()),
            node_id: "node-b".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![
                "job_runtime".into(),
                "state_backend".into(),
                "network_shuffle".into(),
            ],
            boot_id: Some("boot-b".into()),
        })
        .await
        .unwrap();

    let spec = serde_json::json!({
        "id": "persist-replace-job",
        "version": 1,
        "placement": "split",
        "parallelism": 1,
        "operators": [
            {"id": "source", "kind": "source"},
            {"id": "sink", "kind": "sink"}
        ],
        "edges": [
            {"id": "e1", "from": "source", "to": "sink"}
        ],
        "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
        "sinks": [{"operator_id": "sink", "output_type": "drop"}]
    });
    // Pin node-a/node-b initially so post-restart single-node registration waits
    // cleanly for both peers instead of collapsing to single-node colocated mode.
    hub.upsert_job(crate::storage::JobRecord {
        job_id: "persist-replace-job".into(),
        version: 1,
        spec_json: spec.to_string(),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec!["node-a".into(), "node-b".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
        pause_phase: None,
        pause_artifact_id: None,
        pause_prev_checkpoint_id: None,
        pause_retry_count: 0,
        paused_at_ms: None,
        paused_by: None,
    })
    .await
    .unwrap();

    let cmds_a = hub
        .commands(AgentAuth {
            node_id: "node-a".into(),
            session_token: reg_a.session_token.clone(),
        })
        .await
        .unwrap();
    let start_a = cmds_a
        .iter()
        .find(|c| c.operation.as_str() == "job_start")
        .unwrap();
    let secret_ab = start_a.payload.as_ref().unwrap()["edge_secrets"][0]["secret"]
        .as_str()
        .unwrap()
        .to_owned();
    let cmds_b = hub
        .commands(AgentAuth {
            node_id: "node-b".into(),
            session_token: reg_b.session_token.clone(),
        })
        .await
        .unwrap();
    let start_b = cmds_b
        .iter()
        .find(|c| c.operation.as_str() == "job_start")
        .unwrap();
    for (auth, start) in [
        (
            AgentAuth {
                node_id: "node-a".into(),
                session_token: reg_a.session_token.clone(),
            },
            start_a,
        ),
        (
            AgentAuth {
                node_id: "node-b".into(),
                session_token: reg_b.session_token.clone(),
            },
            start_b,
        ),
    ] {
        hub.command_result(
            auth,
            CommandResult {
                command_id: start.id.clone(),
                operation_id: start.operation_id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: start.correlation_id.clone(),
                generation: start.generation,
                observed_generation: Some(1),
                action_id: None,
                failure_class: None,
                config_version_id: None,
                rollout_id: None,
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
                result: None,
            },
        )
        .await
        .unwrap();
    }

    // Simulate Hub restart from the same storage: recovered Hub reloads cp_job_edge_secrets.
    let restarted_hub = Hub::with_storage(hub_config(), storage);
    restarted_hub.recover_persisted_state().await.unwrap();
    let persisted_edge_secret = restarted_hub
        .job_edge_secrets
        .read()
        .await
        .get(&("persist-replace-job".into(), 1))
        .and_then(|edges| {
            edges
                .get(&("node-a".into(), "node-b".into()))
                .or_else(|| edges.get(&("node-b".into(), "node-a".into())))
        })
        .cloned();
    assert_eq!(persisted_edge_secret.as_deref(), Some(secret_ab.as_str()));

    // Register node-a and node-b on the restarted Hub: once both are online, the
    // Hub re-serves the EXACT persisted `secret_ab` from `cp_job_edge_secrets`.
    let reg_a2 = restarted_hub
        .register(RegisterRequest {
            data_address: Some("127.0.0.1:19200".into()),
            node_id: "node-a".into(),
            node_token: reg_a.node_credential.unwrap(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![
                "job_runtime".into(),
                "state_backend".into(),
                "network_shuffle".into(),
            ],
            boot_id: Some("boot-a".into()),
        })
        .await
        .unwrap();
    let reg_b2 = restarted_hub
        .register(RegisterRequest {
            data_address: Some("127.0.0.1:19201".into()),
            node_id: "node-b".into(),
            node_token: reg_b.node_credential.unwrap(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![
                "job_runtime".into(),
                "state_backend".into(),
                "network_shuffle".into(),
            ],
            boot_id: Some("boot-b".into()),
        })
        .await
        .unwrap();
    let restarted_cmds_a = restarted_hub
        .commands(AgentAuth {
            node_id: "node-a".into(),
            session_token: reg_a2.session_token.clone(),
        })
        .await
        .unwrap();
    let restarted_start_a = restarted_cmds_a
        .iter()
        .rfind(|c| c.operation.as_str() == "job_start")
        .unwrap();
    assert_eq!(
        restarted_start_a.payload.as_ref().unwrap()["edge_secrets"][0]["secret"],
        secret_ab
    );
    let restarted_cmds_b = restarted_hub
        .commands(AgentAuth {
            node_id: "node-b".into(),
            session_token: reg_b2.session_token.clone(),
        })
        .await
        .unwrap();
    let restarted_start_b = restarted_cmds_b
        .iter()
        .rfind(|c| c.operation.as_str() == "job_start")
        .unwrap();
    assert_eq!(
        restarted_start_b.payload.as_ref().unwrap()["edge_secrets"][0]["secret"],
        secret_ab
    );

    // Next, verify unpinned incremental replacement refreshes upstream node-a
    // with replacement node-c's data port and fresh per-edge secret.
    restarted_hub
        .upsert_job(crate::storage::JobRecord {
            job_id: "replace-unpinned-job".into(),
            version: 1,
            spec_json: serde_json::json!({
                "id": "replace-unpinned-job",
                "version": 1,
                "placement": "split",
                "parallelism": 1,
                "operators": [
                    {"id": "source", "kind": "source"},
                    {"id": "sink", "kind": "sink"}
                ],
                "edges": [
                    {"id": "e1", "from": "source", "to": "sink"}
                ],
                "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
                "sinks": [{"operator_id": "sink", "output_type": "drop"}]
            })
            .to_string(),
            desired_state: "running".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
            pause_phase: None,
            pause_artifact_id: None,
            pause_prev_checkpoint_id: None,
            pause_retry_count: 0,
            paused_at_ms: None,
            paused_by: None,
        })
        .await
        .unwrap();
    for (nid, tok) in [
        ("node-a", &reg_a2.session_token),
        ("node-b", &reg_b2.session_token),
    ] {
        let cmds = restarted_hub
            .commands(AgentAuth {
                node_id: nid.into(),
                session_token: tok.clone(),
            })
            .await
            .unwrap();
        let st = cmds
            .iter()
            .rfind(|c| {
                c.operation.as_str() == "job_start" && c.resource_id == "replace-unpinned-job"
            })
            .unwrap();
        restarted_hub
            .command_result(
                AgentAuth {
                    node_id: nid.into(),
                    session_token: tok.clone(),
                },
                CommandResult {
                    command_id: st.id.clone(),
                    operation_id: st.operation_id.clone(),
                    state: HubOperationState::Succeeded,
                    progress: 100,
                    error: None,
                    correlation_id: st.correlation_id.clone(),
                    generation: st.generation,
                    observed_generation: Some(1),
                    action_id: None,
                    failure_class: None,
                    config_version_id: None,
                    rollout_id: None,
                    observed_checkpoint_id: None,
                    checkpoint_manifest_uri: None,
                    result: None,
                },
            )
            .await
            .unwrap();
    }

    let _reg_c = restarted_hub
        .register(RegisterRequest {
            data_address: Some("127.0.0.1:19202".into()),
            node_id: "node-c".into(),
            node_token: "shared-bootstrap-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![
                "job_runtime".into(),
                "state_backend".into(),
                "network_shuffle".into(),
            ],
            boot_id: Some("boot-c".into()),
        })
        .await
        .unwrap();
    restarted_hub
        .nodes
        .write()
        .await
        .get_mut("node-b")
        .unwrap()
        .resource
        .maintenance_state = arkflow_core::control::NodeMaintenanceState::Maintenance;
    restarted_hub.reconcile_jobs().await.unwrap();

    let refreshed_cmds_a = restarted_hub
        .commands(AgentAuth {
            node_id: "node-a".into(),
            session_token: reg_a2.session_token,
        })
        .await
        .unwrap();
    let refreshed_start = refreshed_cmds_a
        .iter()
        .rfind(|c| c.operation.as_str() == "job_start" && c.resource_id == "replace-unpinned-job")
        .expect(
            "upstream node-a must be re-dispatched when downstream node-b is replaced by node-c",
        );
    let payload = refreshed_start.payload.as_ref().unwrap();
    assert_eq!(payload["node_data_ports"]["node-c"], "127.0.0.1:19202");
    let secrets = payload["edge_secrets"].as_array().unwrap();
    assert!(
        secrets
            .iter()
            .any(|s| s["dst"] == "node-c" || s["src"] == "node-c"),
        "refreshed job_start must carry edge secret for node-c: {payload}"
    );
}

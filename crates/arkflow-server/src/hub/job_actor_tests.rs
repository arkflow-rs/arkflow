//! Job request attribution through dispatch, durable recovery, and retries.

use crate::api::{hub_router, ServerConfig};
use crate::hub::{
    default_approval_ttl_ms, default_session_ttl_ms, persist_operation, AgentAuth, AgentOperation,
    CommandResult, Hub, HubConfig, HubError, HubOperation, HubOperationState, RegisterRequest,
};
use crate::storage::{ControlPlaneStore, JobCheckpointRecord, JobRecord, StorageActor, UserRecord};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use serde_json::{json, Value};
use tower::ServiceExt;

fn config() -> HubConfig {
    HubConfig {
        operator_token: Some("root|admin|bootstrap-secret".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 60_000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
        approval_required: false,
        approval_ttl_ms: default_approval_ttl_ms(),
    }
}

async fn register(hub: &Hub, capabilities: &[&str]) -> AgentAuth {
    let registration = hub
        .register(RegisterRequest {
            node_id: "compute-1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: capabilities.iter().map(|value| (*value).into()).collect(),
            data_address: None,
            boot_id: Some("actor-test-boot".into()),
        })
        .await
        .unwrap();
    AgentAuth {
        node_id: registration.node_id,
        session_token: registration.session_token,
    }
}

async fn fixture(label: &str, capabilities: &[&str]) -> (Hub, StorageActor, Router, AgentAuth) {
    let storage = StorageActor::start(ControlPlaneStore::contract(label).await, 8);
    let hub = Hub::with_storage(config(), storage.clone());
    let auth = register(&hub, capabilities).await;
    let app = hub_router(hub.clone(), &ServerConfig::default());
    (hub, storage, app, auth)
}

async fn credential(hub: &Hub, id: &str) -> String {
    hub.create_user(
        UserRecord {
            user_id: id.into(),
            display_name: id.into(),
            role: "operator".into(),
            scopes_json: "[]".into(),
            disabled: false,
            oidc_subject: None,
            tokens_valid_after_ms: 0,
            created_at_ms: 0,
            updated_at_ms: 0,
        },
        "root",
    )
    .await
    .unwrap();
    hub.issue_credential(id, "actor-test", None, "root")
        .await
        .unwrap()
        .1
}

fn spec() -> Value {
    json!({
        "id": "orders",
        "version": 1,
        "operators": [{"id": "source", "kind": "source"}, {"id": "sink", "kind": "sink"}],
        "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
        "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
        "sinks": [{"operator_id": "sink", "output_type": "drop"}],
        "connection_string": "job-config-secret"
    })
}

fn job(desired_state: &str) -> JobRecord {
    JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: spec().to_string(),
        desired_state: desired_state.into(),
        observed_state: "stopped".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec!["compute-1".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
        pause_phase: None,
        pause_artifact_id: None,
        pause_prev_checkpoint_id: None,
        pause_retry_count: 0,
        paused_at_ms: None,
        paused_by: None,
    }
}

fn resumable_spec() -> Value {
    let mut spec = spec();
    spec["recovery"] = json!("latest_checkpoint");
    spec
}

fn paused_job(job_id: &str) -> JobRecord {
    let mut job = job("paused");
    job.job_id = job_id.into();
    job.spec_json = resumable_spec().to_string();
    job.observed_state = "stopped".into();
    job.convergence = "converged".into();
    job.pause_phase = Some(crate::hub::job_pause::PAUSE_PAUSED.into());
    job.pause_artifact_id = Some("checkpoint-paused".into());
    job.checkpoint_id = Some("checkpoint-paused".into());
    job.paused_by = Some("pauser".into());
    job
}

async fn request(
    app: &Router,
    method: &str,
    path: &str,
    token: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(body.map_or_else(Body::empty, |value| Body::from(value.to_string())))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn managed_job_requests_attribute_dispatch_without_credentials() {
    let (hub, storage, app, auth) = fixture(
        "managed_job_request_actor",
        &["job_runtime", "state_backend"],
    )
    .await;
    let alice = credential(&hub, "alice").await;
    let bob = credential(&hub, "bob").await;
    let (status, body) = request(
        &app,
        "POST",
        "/api/v1/jobs",
        &alice,
        Some(json!({"spec": spec(), "node_ids": ["compute-1"], "desired_state": "running"})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let (status, body) =
        request(&app, "POST", "/api/v1/jobs/orders/actions/stop", &bob, None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = request(
        &app,
        "PUT",
        "/api/v1/jobs/orders/desired-state",
        &alice,
        Some(json!({"state": "running"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for (path, token) in [("checkpoints", &bob), ("savepoints", &alice)] {
        let (status, body) = request(
            &app,
            "POST",
            &format!("/api/v1/jobs/orders/{path}"),
            token,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    }
    let operations = hub.operations(None).await;
    assert_eq!(operations.len(), 5);
    for operation in &operations {
        let expected = match operation.operation {
            AgentOperation::JobStop | AgentOperation::JobCheckpoint => "bob",
            _ => "alice",
        };
        assert_eq!(operation.actor.as_deref(), Some(expected), "{operation:?}");
    }
    let audits = hub.audit(Some("orders")).await.unwrap();
    for (action, expected, count) in [
        ("job.start", "alice", 2),
        ("job.stop", "bob", 1),
        ("job.checkpoint", "bob", 1),
        ("job.savepoint", "alice", 1),
    ] {
        let records: Vec<_> = audits
            .iter()
            .filter(|record| record.action == action)
            .collect();
        assert_eq!(records.len(), count, "{action}: {records:?}");
        assert!(records.iter().all(
            |record| record.actor.as_deref() == Some(expected) && record.outcome == "accepted"
        ));
    }
    let persisted = storage.list_operations(None::<String>).await.unwrap();
    assert!(persisted.iter().all(|record| {
        let operation: HubOperation = serde_json::from_str(&record.operation_json).unwrap();
        operation
            .actor
            .as_deref()
            .is_some_and(|actor| actor == "alice" || actor == "bob")
    }));
    let commands = hub.commands(auth).await.unwrap();
    let rendered = format!(
        "{} {} {} {}",
        serde_json::to_string(&operations).unwrap(),
        persisted
            .iter()
            .map(|record| record.operation_json.as_str())
            .collect::<Vec<_>>()
            .join(" "),
        serde_json::to_string(&commands).unwrap(),
        serde_json::to_string(&audits).unwrap()
    );
    for token in [&alice, &bob] {
        assert!(!rendered.contains(token));
        assert!(!rendered.contains(token.splitn(3, '_').nth(2).unwrap()));
    }
    assert!(commands.iter().all(|command| {
        command
            .payload
            .as_ref()
            .is_none_or(|payload| payload.get("actor").is_none())
    }));
    assert!(!serde_json::to_string(&audits)
        .unwrap()
        .contains("job-config-secret"));
    assert!(!serde_json::to_string(&operations)
        .unwrap()
        .contains("job-config-secret"));
}

#[tokio::test]
async fn rejected_job_dispatch_keeps_the_authenticated_actor() {
    let (hub, _, app, _) = fixture("rejected_job_request_actor", &["stream_lifecycle"]).await;
    hub.upsert_job(job("stopped")).await.unwrap();
    let alice = credential(&hub, "alice").await;
    for path in ["actions/start", "checkpoints"] {
        let (status, _) = request(
            &app,
            "POST",
            &format!("/api/v1/jobs/orders/{path}"),
            &alice,
            None,
        )
        .await;
        assert!(!status.is_success());
    }
    let records: Vec<_> = hub
        .audit(Some("orders"))
        .await
        .unwrap()
        .into_iter()
        .filter(|record| record.outcome == "rejected")
        .collect();
    assert_eq!(records.len(), 2);
    assert!(records
        .iter()
        .all(|record| record.actor.as_deref() == Some("alice")
            && record.failure_code.as_deref() == Some("incompatible_capability")));
    assert!(records.iter().any(|record| record.action == "job.start"));
    assert!(records
        .iter()
        .any(|record| record.action == "job.checkpoint"));
    let rendered = serde_json::to_string(&records).unwrap();
    assert!(!rendered.contains(&alice));
    assert!(!rendered.contains(alice.splitn(3, '_').nth(2).unwrap()));
}

#[tokio::test]
async fn background_job_dispatch_and_rejection_use_system() {
    let (hub, _, _, _) = fixture("background_job_actor", &["job_runtime", "state_backend"]).await;
    hub.upsert_job(job("running")).await.unwrap();
    let operation = hub.operations(None).await.pop().unwrap();
    assert_eq!(operation.actor.as_deref(), Some("system"));
    assert!(matches!(
        hub.enqueue(
            "missing".into(),
            AgentOperation::JobStop,
            "orders".into(),
            None
        )
        .await,
        Err(HubError::NodeUnavailable)
    ));
    let audits = hub.audit(Some("orders")).await.unwrap();
    assert_eq!(audits.len(), 2);
    assert!(audits
        .iter()
        .all(|record| record.actor.as_deref() == Some("system")));
    assert!(audits.iter().any(|record| record.outcome == "accepted"));
    assert!(audits
        .iter()
        .any(|record| record.failure_code.as_deref() == Some("node_unavailable")));
}

#[tokio::test]
async fn job_actor_survives_json_recovery_and_reconcile_retry() {
    let (hub, storage, app, _) =
        fixture("recovered_job_actor", &["job_runtime", "state_backend"]).await;
    hub.upsert_job_as(job("running"), "alice").await.unwrap();
    let original = hub.operations(None).await.pop().unwrap();
    let persisted = storage
        .get_operation(original.id.clone())
        .await
        .unwrap()
        .unwrap();
    let persisted: HubOperation = serde_json::from_str(&persisted.operation_json).unwrap();
    assert_eq!(persisted.actor.as_deref(), Some("alice"));
    drop(app);
    drop(hub);

    let restored = Hub::with_storage(config(), storage.clone());
    assert_eq!(restored.restore_persisted_operations().await.unwrap(), 1);
    let recovered = restored.operation(&original.id).await.unwrap();
    assert_eq!(recovered.actor.as_deref(), Some("alice"));
    assert_eq!(recovered.state, HubOperationState::TimedOut);
    restored.recover_persisted_state().await.unwrap();
    register(&restored, &["job_runtime", "state_backend"]).await;
    let first_retry = restored
        .operations(None)
        .await
        .into_iter()
        .find(|operation| operation.id != original.id)
        .unwrap();
    assert_eq!(first_retry.actor.as_deref(), Some("alice"));
    restored
        .operations
        .write()
        .await
        .get_mut(&first_retry.id)
        .unwrap()
        .expires_at_ms = Some(0);
    assert_eq!(restored.expire_stale_job_operations().await.unwrap(), 1);
    assert_eq!(restored.reconcile_jobs().await.unwrap(), 1);
    let retry = restored
        .operations(None)
        .await
        .into_iter()
        .find(|operation| operation.state == HubOperationState::Queued)
        .unwrap();
    assert_eq!(retry.actor.as_deref(), Some("alice"));
    assert_eq!(retry.retry_count, 1);
    let durable = storage.get_operation(retry.id).await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<HubOperation>(&durable.operation_json)
            .unwrap()
            .actor
            .as_deref(),
        Some("alice")
    );
    assert!(restored
        .audit(Some("orders"))
        .await
        .unwrap()
        .iter()
        .filter(|record| record.action == "job.start")
        .all(|record| record.actor.as_deref() == Some("alice")));
}

#[tokio::test]
async fn historical_job_json_restores_without_fabricating_a_user() {
    let storage = StorageActor::start(ControlPlaneStore::contract("historical_job_actor").await, 8);
    let historical: HubOperation = serde_json::from_value(json!({
        "id": "legacy-start", "command_id": "legacy-command", "node_id": "compute-1",
        "operation": "job_start", "resource_id": "orders", "generation": 7,
        "state": "queued", "progress": 0, "created_at_ms": 1
    }))
    .unwrap();
    assert!(historical.actor.is_none());
    assert!(serde_json::to_value(&historical)
        .unwrap()
        .get("actor")
        .is_none());
    persist_operation(&storage, &historical).await.unwrap();
    let hub = Hub::with_storage(config(), storage);
    assert_eq!(hub.restore_persisted_operations().await.unwrap(), 1);
    assert!(hub.operation("legacy-start").await.unwrap().actor.is_none());
    register(&hub, &["job_runtime", "state_backend"]).await;
    let retry = hub
        .enqueue_with_metadata_as(
            "compute-1".into(),
            AgentOperation::JobStart,
            "orders".into(),
            None,
            None,
            7,
            None,
            None,
            None,
            None,
            None,
            "later-user",
        )
        .await
        .unwrap();
    assert_eq!(retry.actor.as_deref(), Some("system"));
}

#[tokio::test]
async fn deferred_dispatch_after_offline_targets_keeps_the_requesting_actor() {
    let (hub, _, _, _) = fixture("deferred_job_actor", &["job_runtime", "state_backend"]).await;
    hub.upsert_job(job("stopped")).await.unwrap();
    // The only target is offline: the accepted mutation cannot dispatch yet.
    hub.nodes
        .write()
        .await
        .get_mut("compute-1")
        .unwrap()
        .resource
        .lease_expires_at_ms = 0;
    let desired = hub
        .update_job_desired_state_as("orders", "running", 1, "alice")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(desired.desired_state, "running");
    assert!(!hub
        .operations(None)
        .await
        .iter()
        .any(|operation| operation.operation == AgentOperation::JobStart));

    // The target returns; background reconciliation must not fall back to
    // `system` when the Job records an attributed request.
    hub.nodes
        .write()
        .await
        .get_mut("compute-1")
        .unwrap()
        .resource
        .lease_expires_at_ms = u64::MAX / 2;
    hub.reconcile_jobs().await.unwrap();
    let start = hub
        .operations(None)
        .await
        .into_iter()
        .find(|operation| operation.operation == AgentOperation::JobStart)
        .unwrap();
    assert_eq!(start.actor.as_deref(), Some("alice"));
    assert!(hub
        .audit(Some("orders"))
        .await
        .unwrap()
        .iter()
        .any(|record| {
            record.action == "job.start"
                && record.outcome == "accepted"
                && record.actor.as_deref() == Some("alice")
        }));
}

#[tokio::test]
async fn volatile_hub_deferred_dispatch_keeps_the_requesting_actor() {
    let hub = Hub::new(config());
    register(&hub, &["job_runtime", "state_backend"]).await;
    hub.upsert_job(job("stopped")).await.unwrap();
    hub.nodes
        .write()
        .await
        .get_mut("compute-1")
        .unwrap()
        .resource
        .lease_expires_at_ms = 0;
    hub.update_job_desired_state_as("orders", "running", 1, "alice")
        .await
        .unwrap();
    assert!(!hub
        .operations(None)
        .await
        .iter()
        .any(|operation| operation.operation == AgentOperation::JobStart));

    hub.nodes
        .write()
        .await
        .get_mut("compute-1")
        .unwrap()
        .resource
        .lease_expires_at_ms = u64::MAX / 2;
    hub.reconcile_jobs().await.unwrap();
    let start = hub
        .operations(None)
        .await
        .into_iter()
        .find(|operation| operation.operation == AgentOperation::JobStart)
        .unwrap();
    assert_eq!(start.actor.as_deref(), Some("alice"));
}

#[tokio::test]
async fn deferred_resume_and_stop_keep_the_requesting_actor() {
    let (hub, storage, _, _) =
        fixture("deferred_pause_actor", &["job_runtime", "state_backend"]).await;
    hub.upsert_job(paused_job("orders")).await.unwrap();
    hub.upsert_job(paused_job("payments")).await.unwrap();
    // The resume start needs a compatible completed artifact to select.
    let artifact = JobCheckpointRecord {
        job_id: "orders".into(),
        job_version: 1,
        checkpoint_id: "checkpoint-paused".into(),
        kind: "checkpoint".into(),
        status: "completed".into(),
        manifest_uri: Some("file:///tmp/arkflow-actor-test/manifest.json".into()),
        format_version: 1,
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    storage
        .upsert_job_checkpoint(artifact.clone())
        .await
        .unwrap();
    hub.job_checkpoints
        .write()
        .await
        .entry("orders".into())
        .or_default()
        .push(artifact);
    hub.nodes
        .write()
        .await
        .get_mut("compute-1")
        .unwrap()
        .resource
        .lease_expires_at_ms = 0;
    let resumed = hub.resume_job("orders", "bob").await.unwrap();
    hub.stop_paused_job("payments", "carol").await.unwrap();
    assert_eq!(
        storage.job_requested_by("orders").await.unwrap().as_deref(),
        Some("bob")
    );
    assert_eq!(
        storage
            .job_requested_by("payments")
            .await
            .unwrap()
            .as_deref(),
        Some("carol")
    );

    hub.nodes
        .write()
        .await
        .get_mut("compute-1")
        .unwrap()
        .resource
        .lease_expires_at_ms = u64::MAX / 2;
    hub.reconcile_jobs().await.unwrap();
    let stop = hub
        .operations(None)
        .await
        .into_iter()
        .find(|operation| {
            operation.operation == AgentOperation::JobStop && operation.resource_id == "payments"
        })
        .unwrap();
    assert_eq!(stop.actor.as_deref(), Some("carol"));
    let start = hub
        .operations(None)
        .await
        .into_iter()
        .find(|operation| {
            operation.operation == AgentOperation::JobStart && operation.resource_id == "orders"
        })
        .unwrap();
    assert_eq!(start.actor.as_deref(), Some("bob"));

    hub.observe_job("orders", resumed.generation, "running", None, None)
        .await
        .unwrap();
    hub.reconcile_job_pauses().await.unwrap();
    let audits = hub.audit(Some("orders")).await.unwrap();
    let resume_audits = audits
        .iter()
        .filter(|record| record.action == "job.resume" && record.outcome == "accepted")
        .collect::<Vec<_>>();
    assert!(resume_audits.len() >= 2, "{resume_audits:?}");
    assert!(resume_audits
        .iter()
        .all(|record| record.actor.as_deref() == Some("bob")));
}

#[tokio::test]
async fn checkpoint_poll_retry_and_commit_preserve_actor_per_artifact() {
    let (hub, _, _, auth) =
        fixture("checkpoint_retry_actor", &["job_runtime", "state_backend"]).await;
    hub.upsert_job(job("stopped")).await.unwrap();
    let record = JobCheckpointRecord {
        job_id: "orders".into(),
        job_version: 1,
        checkpoint_id: "checkpoint-bob".into(),
        kind: "checkpoint".into(),
        status: "pending".into(),
        manifest_uri: None,
        format_version: 1,
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    hub.record_job_checkpoint_as(record.clone(), "bob")
        .await
        .unwrap();
    let original = hub.operations(None).await.pop().unwrap();
    hub.nodes
        .write()
        .await
        .get_mut("compute-1")
        .unwrap()
        .commands
        .front_mut()
        .unwrap()
        .expires_at_ms = 0;
    assert!(hub.commands(auth.clone()).await.unwrap().is_empty());
    let retry = hub.commands(auth.clone()).await.unwrap().pop().unwrap();
    assert_ne!(retry.operation_id, original.id);
    assert_eq!(
        hub.operation(&retry.operation_id)
            .await
            .unwrap()
            .actor
            .as_deref(),
        Some("bob")
    );
    hub.command_result(
        auth,
        CommandResult {
            command_id: retry.id,
            operation_id: retry.operation_id,
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: None,
            generation: retry.generation,
            observed_generation: None,
            action_id: None,
            failure_class: None,
            config_version_id: None,
            rollout_id: None,
            observed_checkpoint_id: Some("checkpoint-bob".into()),
            checkpoint_manifest_uri: None,
            result: None,
        },
    )
    .await
    .unwrap();
    let commit = hub
        .operations(None)
        .await
        .into_iter()
        .find(|operation| operation.operation == AgentOperation::JobCheckpointCommit)
        .unwrap();
    assert_eq!(commit.actor.as_deref(), Some("bob"));
    let mut another = record;
    another.checkpoint_id = "checkpoint-alice".into();
    hub.record_job_checkpoint_as(another, "alice")
        .await
        .unwrap();
    let another = hub
        .operations(None)
        .await
        .into_iter()
        .find(|operation| operation.checkpoint_id.as_deref() == Some("checkpoint-alice"))
        .unwrap();
    assert_eq!(another.actor.as_deref(), Some("alice"));
}

use super::*;
use arkflow_core::control::{ConvergenceState, StreamMetricsSnapshot, StreamState};

#[test]
fn recovery_selection_requires_matching_job_and_state_versions() {
    let spec: arkflow_core::job::JobSpec = serde_json::from_value(serde_json::json!({
        "id": "orders",
        "version": 2,
        "operators": [],
        "sources": [],
        "sinks": [],
        "state": {"backend": "embedded_kv", "format_version": 3}
    }))
    .unwrap();
    assert_eq!(job_state_format_version(&spec), 3);
    let compatible = JobCheckpointRecord {
        job_id: "orders".into(),
        job_version: 2,
        checkpoint_id: "checkpoint-current".into(),
        kind: "checkpoint".into(),
        status: "completed".into(),
        manifest_uri: None,
        format_version: 3,
        created_at_ms: 2,
        updated_at_ms: 2,
    };
    assert!(recovery_record_is_compatible(&spec, &compatible));
    // A savepoint written by an OLDER Job version with the same state
    // format is a compatible upgrade target (the shared evaluator's
    // version-direction rule); a NEWER artifact has no downgrade path.
    let mut upgrade = compatible.clone();
    upgrade.job_version = 1;
    assert!(
        recovery_record_is_compatible(&spec, &upgrade),
        "an equal-format older artifact restores into the newer version"
    );
    let mut downgrade = compatible.clone();
    downgrade.job_version = 3;
    assert!(
        !recovery_record_is_compatible(&spec, &downgrade),
        "downgrades have no compatibility path"
    );
    let mut old_format = compatible;
    old_format.format_version = 2;
    assert!(!recovery_record_is_compatible(&spec, &old_format));
}

#[tokio::test]
async fn checkpoint_completion_after_hub_restart_preserves_metadata() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub1 = Hub::with_storage(config(), storage.clone());
    let hub2 = Hub::with_storage(config(), storage);
    let spec_json = serde_json::json!({
            "id": "orders",
            "version": 2,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}],
            "state": {"backend": "embedded_kv", "format_version": 3}
        })
        .to_string();
    hub1.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 2,
        spec_json,
        desired_state: "stopped".into(),
        observed_state: "stopped".into(),
        convergence: "converged".into(),
        generation: 4,
        node_ids: Vec::new(),
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    hub1.record_job_checkpoint(JobCheckpointRecord {
        job_id: "orders".into(),
        job_version: 2,
        checkpoint_id: "checkpoint-4".into(),
        kind: "checkpoint".into(),
        status: "pending".into(),
        manifest_uri: None,
        format_version: 3,
        created_at_ms: 1,
        updated_at_ms: 1,
    })
    .await
    .unwrap();
    hub2.complete_job_checkpoint(
        "orders",
        "checkpoint-4",
        "completed",
        Some("s3://bucket/checkpoint-4/manifest.json".into()),
    )
    .await
    .unwrap();
    let records = hub2.job_checkpoints("orders").await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].job_version, 2);
    assert_eq!(records[0].format_version, 3);
    assert_eq!(records[0].status, "completed");
}

// ---------- review P1 regressions (repair-control-plane-review-defects) ----------

/// Session credentials authenticate every agent request; they must be
/// independent high-entropy values, not a sequential counter an attacker
/// can enumerate.
#[tokio::test]
async fn session_tokens_are_random_and_unique() {
    let hub = Hub::new(config());
    let mut tokens = Vec::new();
    for node_id in ["node-a", "node-b", "node-c"] {
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["stream_lifecycle".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        assert!(
            session.session_token.len() >= 32,
            "session token must carry real entropy"
        );
        assert!(
            !session.session_token.starts_with("node-session-"),
            "session token must not be a sequential counter"
        );
        tokens.push(session.session_token);
    }
    let unique: std::collections::BTreeSet<_> = tokens.iter().collect();
    assert_eq!(
        unique.len(),
        tokens.len(),
        "every session token must be unique"
    );
    // Re-registration issues an independent token, not the next counter
    // value.
    let re_registered = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["stream_lifecycle".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    assert!(!tokens.contains(&re_registered.session_token));
}

/// A terminal-failure operation must not wedge its intent: the retry
/// attempt enqueued by the reconciler replaces the failed record and a
/// fresh command reaches the node.
#[tokio::test]
async fn terminal_failure_intent_reenqueues_a_fresh_command_on_retry() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = StorageActor::start(store, 8);
    let hub = Hub::with_storage(config(), storage.clone());
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["stream_lifecycle".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    let intent = hub
        .set_desired_state(DesiredMutation {
            node_id: "node-a".into(),
            stream_id: "orders".into(),
            desired_state: "running".into(),
            expected_generation: Some(0),
            ..Default::default()
        })
        .await
        .unwrap();
    let dispatched = hub.reconcile_once("dispatch").await.unwrap().unwrap();
    assert_eq!(dispatched.id, intent.intent_id);
    let auth = AgentAuth {
        node_id: "node-a".into(),
        session_token: session.session_token.clone(),
    };
    let polled = hub.commands(auth.clone()).await.unwrap();
    assert_eq!(polled.len(), 1);
    let command_id = polled[0].id.clone();
    // The agent reports a transient execution failure: the intent must
    // move to `retrying` with a due retry row.
    hub.command_result(
        auth.clone(),
        CommandResult {
            command_id,
            operation_id: dispatched.id.clone(),
            state: HubOperationState::Failed,
            progress: 0,
            error: Some("agent worker crashed".into()),
            correlation_id: None,
            generation: dispatched.generation,
            observed_generation: None,
            action_id: None,
            failure_class: Some("temporary_execution".into()),
            config_version_id: None,
            rollout_id: None,
            observed_checkpoint_id: None,
            checkpoint_manifest_uri: None,
            result: None,
        },
    )
    .await
    .unwrap();
    // Wait past the 1s retry backoff, then reconcile: the retry attempt
    // must enqueue a fresh command instead of returning the terminal
    // record without queueing anything.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    // The node's short test lease expired during the backoff; the agent
    // reconnects before the reconciler retries.
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["stream_lifecycle".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    let retried = hub.reconcile_once("retry").await.unwrap();
    assert!(retried.is_some(), "retry attempt must be enqueued");
    let auth = AgentAuth {
        node_id: "node-a".into(),
        session_token: session.session_token,
    };
    let commands = hub.commands(auth).await.unwrap();
    assert!(
        !commands.is_empty(),
        "a retried intent must produce a fresh command"
    );
}

/// Restart recovery: persisted operations come back into the in-memory
/// map. The restart wiped the assignment-fingerprint memory, so the first
/// reconcile re-dispatches the succeeded start ONCE as an assignment
/// confirmation (the Agent no-ops a matching assignment); once that
/// confirmation succeeds, later ticks skip again.
#[tokio::test]
async fn restart_restores_persisted_operations_and_skips_satisfied_starts() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let hub1 = Hub::with_storage(config(), StorageActor::start(store.clone(), 8));
    let session = hub1
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: Some("boot-a".into()),
        })
        .await
        .unwrap();
    hub1.upsert_job(JobRecord {
        job_id: "job-1".into(),
        version: 1,
        spec_json: job_spec_json("job-1"),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec!["node-a".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    hub1.reconcile_jobs().await.unwrap();
    let auth = AgentAuth {
        node_id: "node-a".into(),
        session_token: session.session_token.clone(),
    };
    let commands = hub1.commands(auth.clone()).await.unwrap();
    let start_command = commands
        .iter()
        .find(|command| command.operation == "job_start")
        .expect("job_start must be dispatched for a desired-running job")
        .clone();
    hub1.command_result(
        auth,
        CommandResult {
            command_id: start_command.id.clone(),
            operation_id: start_command.operation_id.clone(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: start_command.correlation_id.clone(),
            generation: start_command.generation,
            observed_generation: Some(start_command.generation),
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

    // "Restart": same durable store, fresh in-memory state. hub1 also
    // persisted its own bookkeeping rows (checkpoint triggers), so the
    // restore brings back more than the start operation — assert on the
    // semantics, not on an exact count.
    let hub2 = Hub::with_storage(config(), StorageActor::start(store.clone(), 8));
    let restored = hub2.restore_persisted_operations().await.unwrap();
    assert!(restored >= 1, "at least the succeeded start is restored");
    assert!(hub2.operations(None).await.iter().any(|operation| {
        operation.operation == "job_start" && operation.state == HubOperationState::Succeeded
    }));

    let session2 = hub2
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: Some("boot-a".into()),
        })
        .await
        .unwrap();
    hub2.reconcile_jobs().await.unwrap();
    let auth2 = AgentAuth {
        node_id: "node-a".into(),
        session_token: session2.session_token.clone(),
    };
    let commands2 = hub2.commands(auth2.clone()).await.unwrap();
    let confirmation = commands2
        .iter()
        .find(|command| command.operation == "job_start")
        .expect("one assignment-confirmation start is re-dispatched after the restart");
    // The confirmation completes (a real Agent no-ops a matching assignment).
    hub2.command_result(
        auth2.clone(),
        CommandResult {
            command_id: confirmation.id.clone(),
            operation_id: confirmation.operation_id.clone(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: confirmation.correlation_id.clone(),
            generation: confirmation.generation,
            observed_generation: Some(confirmation.generation),
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
    hub2.reconcile_jobs().await.unwrap();
    let commands3 = hub2.commands(auth2).await.unwrap();
    assert!(
        !commands3
            .iter()
            .any(|command| command.operation == "job_start"),
        "after the one-shot confirmation the dispatch skip holds again"
    );
}

#[tokio::test]
async fn durable_replacement_without_checkpoint_fails_closed_before_dispatch() {
    let hub = Hub::new(config());
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: Some("boot-a".into()),
        })
        .await
        .unwrap();
    hub.upsert_job(JobRecord {
        job_id: "durable-orders".into(),
        version: 1,
        spec_json: durable_job_spec_json("durable-orders"),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec!["node-a".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let auth = AgentAuth {
        node_id: "node-a".into(),
        session_token: session.session_token,
    };
    let start = hub
        .commands(auth.clone())
        .await
        .unwrap()
        .into_iter()
        .find(|command| command.operation == "job_start")
        .expect("initial durable deployment is allowed to start empty");
    assert_eq!(
        start
            .payload
            .as_ref()
            .and_then(|payload| payload.get("recovery_required"))
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
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
            observed_generation: Some(start.generation),
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

    let mut replacement = hub.job("durable-orders").await.unwrap().unwrap();
    replacement.version = 2;
    let error = hub
        .upsert_job(replacement)
        .await
        .expect_err("a replacement without a completed checkpoint must fail closed");
    assert!(error.to_string().contains("requires recovery"));
    assert!(hub.nodes.read().await.get("node-a").is_some_and(|node| {
        !node
            .commands
            .iter()
            .any(|command| command.operation == "job_start" && command.generation == 2)
    }));
}

fn job_spec_json(id: &str) -> String {
    serde_json::json!({
            "id": id,
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        })
        .to_string()
}

#[tokio::test]
async fn current_generation_recovery_required_failure_overrides_running_observation() {
    let hub = Hub::new(config());
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec!["job_runtime".into(), "state_backend".into()],
        boot_id: Some("boot-a".into()),
    })
    .await
    .unwrap();
    let job = JobRecord {
        job_id: "durable-orders-restarted".into(),
        version: 1,
        spec_json: durable_job_spec_json("durable-orders-restarted"),
        desired_state: "running".into(),
        observed_state: "running".into(),
        convergence: "reconciling".into(),
        generation: 1,
        node_ids: vec!["node-a".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    };
    hub.operations.write().await.insert(
        "recovery-required-start".into(),
        HubOperation {
            id: "recovery-required-start".into(),
            intent_id: None,
            command_id: "command-recovery-required".into(),
            node_id: "node-a".into(),
            operation: "job_start".into(),
            resource_id: job.job_id.clone(),
            checkpoint_id: None,
            generation: job.generation,
            attempt_id: None,
            config_version_id: None,
            state: HubOperationState::NodeUnavailable,
            progress: 100,
            created_at_ms: 1,
            expires_at_ms: None,
            dispatched_at_ms: None,
            acknowledged_at_ms: None,
            finished_at_ms: Some(2),
            correlation_id: None,
            error: Some("previous successful start invalidated by Agent reboot".into()),
            failure_class: Some("recovery_required".into()),
            intent_state: None,
            convergence_state: None,
            retry_count: 0,
            next_retry_at_ms: None,
            superseded_by_intent_id: None,
            superseded_generation: None,
            observed_generation: None,
            observed_state: None,
            result: None,
        },
    );

    let error = hub
        .reconcile_job(&job)
        .await
        .expect_err("a rebooted durable Job must not start from empty state");
    assert!(error.to_string().contains("requires recovery"), "{error}");
    assert!(hub.nodes.read().await.get("node-a").is_some_and(|node| {
        node.commands
            .iter()
            .all(|command| command.operation != "job_start")
    }));
}

fn durable_job_spec_json(id: &str) -> String {
    let mut spec = serde_json::from_str::<serde_json::Value>(&job_spec_json(id)).unwrap();
    spec["operators"] = serde_json::json!([
        {"id": "source", "kind": "source"},
        {
            "id": "aggregate",
            "kind": "aggregate",
            "stateful": true,
            "key_field": "key"
        },
        {"id": "sink", "kind": "sink"}
    ]);
    spec["edges"] = serde_json::json!([
        {"id": "source-aggregate", "from": "source", "to": "aggregate"},
        {"id": "aggregate-sink", "from": "aggregate", "to": "sink"}
    ]);
    spec["state"] = serde_json::json!({
        "backend": "embedded_kv",
        "durability": "durable",
        "format_version": 1
    });
    spec["checkpoint"] = serde_json::json!({
        "interval_ms": 1000,
        "retention": 2,
        "object_store_uri": "file:///tmp/arkflow-hub-recovery-test"
    });
    spec.to_string()
}

/// A stopped Job whose stop command already succeeded must not receive a
/// fresh stop command (and a fresh persistent operation row) on every
/// reconcile tick: that churn loop grew the durable operation store
/// without bound.
#[tokio::test]
async fn a_stopped_job_is_not_recommanded_once_its_stop_succeeds() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = StorageActor::start(store, 8);
    let hub = Hub::with_storage(config(), storage);
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: job_spec_json("orders"),
        desired_state: "stopped".into(),
        observed_state: "stopped".into(),
        convergence: "converged".into(),
        generation: 1,
        node_ids: vec!["node-a".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let auth = AgentAuth {
        node_id: "node-a".into(),
        session_token: session.session_token.clone(),
    };
    let job = hub.jobs().await.unwrap().remove(0);
    let first = hub.reconcile_job(&job).await.unwrap();
    assert_eq!(first, 1, "the first tick dispatches the stop");
    let polled = hub.commands(auth.clone()).await.unwrap();
    assert_eq!(polled.len(), 1);
    hub.command_result(
        auth.clone(),
        CommandResult {
            command_id: polled[0].id.clone(),
            operation_id: polled[0].operation_id.clone(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: None,
            generation: job.generation,
            observed_generation: None,
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
    // The node's command queue is drained; later ticks must NOT enqueue
    // another stop for the settled (job, generation, node).
    for _ in 0..3 {
        let dispatched = hub.reconcile_job(&job).await.unwrap();
        assert_eq!(
            dispatched, 0,
            "a settled stop must not be re-dispatched by later ticks"
        );
    }
    let operations = hub.operations.read().await;
    let stops = operations
        .values()
        .filter(|record| {
            record.resource_id == "orders"
                && record.operation == "job_stop"
                && record.node_id == "node-a"
        })
        .count();
    assert_eq!(
        stops, 1,
        "exactly one stop operation record must exist for the settled generation"
    );
}

/// One Job whose dispatch fails (here: its node's command queue is at
/// capacity) must not stall the reconcile tick for every other Job —
/// the failure is recorded and only that Job is skipped.
#[tokio::test]
async fn one_jobs_dispatch_failure_does_not_stall_the_scan() {
    let hub = Hub::new(config());
    for node_id in ["node-a", "node-b"] {
        hub.register(RegisterRequest {
            data_address: None,
            node_id: node_id.into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![
                "stream_lifecycle".into(),
                "job_runtime".into(),
                "state_backend".into(),
            ],
            boot_id: None,
        })
        .await
        .unwrap();
    }
    let job = |job_id: &str, node_id: &str| JobRecord {
        job_id: job_id.into(),
        version: 1,
        spec_json: job_spec_json(job_id),
        desired_state: "stopped".into(),
        observed_state: "stopped".into(),
        convergence: "converged".into(),
        generation: 1,
        node_ids: vec![node_id.into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    };
    hub.upsert_job(job("job-blocked", "node-a")).await.unwrap();
    hub.upsert_job(job("job-healthy", "node-b")).await.unwrap();
    // Fill node-a's command queue to the dispatch bound.
    for index in 0..MAX_COMMANDS_PER_NODE {
        hub.enqueue_with_metadata(
            "node-a".into(),
            "start".into(),
            format!("fill-{index}"),
            None,
            None,
            1,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    }
    // Arm both Jobs for dispatch without going through upsert's eager
    // reconcile (the blocked one would fail the upsert itself).
    {
        let mut jobs = hub.jobs.write().await;
        for job_id in ["job-blocked", "job-healthy"] {
            if let Some(record) = jobs.get_mut(job_id) {
                record.desired_state = "running".into();
            }
        }
    }
    // The scan must succeed overall: the blocked Job is skipped, the
    // healthy Job still dispatches its start.
    let dispatched = hub.reconcile_jobs().await.unwrap();
    assert!(dispatched >= 1, "the healthy Job must still dispatch");
    let operations = hub.operations.read().await;
    assert!(
        operations
            .values()
            .any(|record| record.resource_id == "job-healthy" && record.operation == "job_start"),
        "the healthy Job's start must be enqueued"
    );
    assert!(
        !operations
            .values()
            .any(|record| record.resource_id == "job-blocked" && record.operation == "job_start"),
        "the blocked Job's start must be skipped, not enqueued"
    );
}

/// Operation and checkpoint records must be reclaimed by a bounded
/// retention: pending/failed checkpoint attempt rows and old terminal
/// operation rows used to accumulate forever.
#[tokio::test]
async fn stale_operation_and_checkpoint_records_are_reclaimed() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = StorageActor::start(store, 8);
    let hub = Hub::with_storage(config(), storage);
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: job_spec_json("orders"),
        desired_state: "running".into(),
        observed_state: "running".into(),
        convergence: "in_sync".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    hub.record_job_checkpoint(JobCheckpointRecord {
        job_id: "orders".into(),
        job_version: 1,
        checkpoint_id: "checkpoint-stale-pending".into(),
        kind: "checkpoint".into(),
        status: "pending".into(),
        manifest_uri: None,
        format_version: 1,
        created_at_ms: 1,
        updated_at_ms: 1,
    })
    .await
    .unwrap();
    hub.record_job_checkpoint(JobCheckpointRecord {
        job_id: "orders".into(),
        job_version: 1,
        checkpoint_id: "checkpoint-stale-failed".into(),
        kind: "checkpoint".into(),
        status: "failed".into(),
        manifest_uri: None,
        format_version: 1,
        created_at_ms: 1,
        updated_at_ms: 1,
    })
    .await
    .unwrap();
    let stale_op = HubOperation {
        id: "op-stale".into(),
        intent_id: None,
        command_id: "command-stale".into(),
        node_id: "node-a".into(),
        operation: "job_stop".into(),
        resource_id: "orders".into(),
        checkpoint_id: None,
        generation: 1,
        expires_at_ms: None,
        attempt_id: None,
        config_version_id: None,
        state: HubOperationState::Succeeded,
        progress: 100,
        created_at_ms: 1,
        dispatched_at_ms: None,
        acknowledged_at_ms: None,
        finished_at_ms: Some(1),
        correlation_id: None,
        error: None,
        failure_class: None,
        intent_state: None,
        convergence_state: None,
        retry_count: 0,
        next_retry_at_ms: None,
        superseded_by_intent_id: None,
        superseded_generation: None,
        observed_generation: None,
        observed_state: None,
        result: None,
    };
    hub.operations
        .write()
        .await
        .insert(stale_op.id.clone(), stale_op.clone());
    hub.prune_stale_checkpoint_records().await.unwrap();
    hub.prune_operation_history().await.unwrap();
    let records = hub.job_checkpoints("orders").await.unwrap();
    assert!(
        records.is_empty(),
        "stale pending/failed checkpoint records must be reclaimed"
    );
    assert!(
        !hub.operations.read().await.contains_key("op-stale"),
        "old terminal operation records must be reclaimed"
    );
}

/// The observation write is a compare-and-set on the generation the
/// caller read: a concurrent desired-state bump must not be rolled back
/// by a stale report.
#[tokio::test]
async fn stale_job_observation_cannot_rollback_generation() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = StorageActor::start(store, 8);
    let spec_json = serde_json::json!({
            "id": "orders",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        })
        .to_string();
    storage
        .upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json,
            desired_state: "running".into(),
            observed_state: "starting".into(),
            convergence: "reconciling".into(),
            generation: 1,
            node_ids: vec![],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
    let applied = storage
        .update_job_observation("orders", "running", "converged", 1, 1, None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(applied.generation, 1);
    // A concurrent desired-state change bumps the generation.
    let bumped = storage
        .update_job_desired_state("orders", "stopped", 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bumped.generation, 2);
    // The stale observation (still expecting generation 1) must be
    // rejected instead of writing generation 1 back.
    let conflict = storage
        .update_job_observation("orders", "running", "converged", 1, 1, None, None)
        .await;
    assert!(matches!(
        conflict,
        Err(StorageError::GenerationConflict {
            expected: 1,
            current: 2
        })
    ));
    let current = storage.get_job("orders").await.unwrap().unwrap();
    assert_eq!(current.generation, 2);
    assert_eq!(current.desired_state, "stopped");
    // A fresh report at the current generation still applies.
    let fresh = storage
        .update_job_observation("orders", "stopped", "converged", 2, 2, None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fresh.generation, 2);
    assert_eq!(fresh.observed_state, "stopped");
}

/// Re-placement after a node blip must fence the abandoned node: its
/// current-generation Succeeded start is marked Superseded and the node
/// receives a stop when it reappears, instead of being deduped back into
/// the target set and double-running the Job.
#[tokio::test]
async fn replaced_placement_supersedes_abandoned_start_and_stops_it() {
    let hub = Hub::new(config());
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec!["job_runtime".into(), "state_backend".into()],
        boot_id: None,
    })
    .await
    .unwrap();
    let job = hub
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: serde_json::json!({
                    "id": "orders",
                    "version": 1,
                    "operators": [
                        {"id": "source", "kind": "source"},
                        {"id": "sink", "kind": "sink"}
                    ],
                    "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
                    "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
                    "sinks": [{"operator_id": "sink", "output_type": "drop"}]
                })
                .to_string(),
                desired_state: "running".into(),
                observed_state: "stopped".into(),
                convergence: "reconciling".into(),
                generation: 1,
                node_ids: vec![],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 0,
            })
            .await
            .unwrap();
    // Auto-place on the only online node and let its start succeed.
    hub.reconcile_job(&job).await.unwrap();
    let start_op_id = {
        let operations = hub.operations.read().await;
        operations
            .values()
            .find(|operation| {
                operation.resource_id == "orders"
                    && operation.operation == "job_start"
                    && operation.node_id == "node-a"
                    && operation.generation == 1
            })
            .map(|operation| operation.id.clone())
            .expect("job_start dispatched to node-a")
    };
    {
        let mut operations = hub.operations.write().await;
        let operation = operations.get_mut(&start_op_id).unwrap();
        operation.state = HubOperationState::Succeeded;
    }
    // node-a loses its lease (partition); node-b joins. The reconciler
    // must move the Job to node-b and fence node-a's stale claim.
    hub.nodes
        .write()
        .await
        .get_mut("node-a")
        .unwrap()
        .resource
        .lease_expires_at_ms = now_ms();
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-b".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec!["job_runtime".into(), "state_backend".into()],
        boot_id: None,
    })
    .await
    .unwrap();
    hub.reconcile_job(&job).await.unwrap();
    {
        let operations = hub.operations.read().await;
        let abandoned = operations
            .values()
            .find(|operation| operation.id == start_op_id)
            .unwrap();
        assert_eq!(
            abandoned.state,
            HubOperationState::Superseded,
            "the abandoned placement must be fenced"
        );
        assert!(operations.values().any(|operation| {
            operation.node_id == "node-b"
                && operation.operation == "job_start"
                && operation.generation == 1
                && operation.state == HubOperationState::Queued
        }));
    }
    // node-b's start succeeds; node-a reappears. The reconciler must NOT
    // dedupe node-a back into the target set: it receives a stop.
    {
        let mut operations = hub.operations.write().await;
        for operation in operations.values_mut() {
            if operation.node_id == "node-b"
                && operation.operation == "job_start"
                && operation.generation == 1
            {
                operation.state = HubOperationState::Succeeded;
            }
        }
    }
    hub.nodes
        .write()
        .await
        .get_mut("node-a")
        .unwrap()
        .resource
        .lease_expires_at_ms = now_ms() + config().lease_ttl_ms;
    hub.reconcile_job(&job).await.unwrap();
    let nodes = hub.nodes.read().await;
    let node_a = nodes.get("node-a").unwrap();
    assert!(
        node_a
            .commands
            .iter()
            .any(|command| command.operation == "job_stop"),
        "the abandoned node must receive a stop command"
    );
    assert_eq!(
        node_a
            .commands
            .iter()
            .filter(|command| command.operation == "job_start")
            .count(),
        1,
        "only the original start remains queued; the abandoned node must not be re-targeted"
    );
    assert!(
        !nodes
            .get("node-b")
            .unwrap()
            .commands
            .iter()
            .any(|command| command.operation == "job_stop"),
        "the live placement must keep running"
    );
}

fn config() -> HubConfig {
    HubConfig {
        operator_token: Some("operator".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 1000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
    }
}

#[tokio::test]
async fn secure_hub_fails_closed_for_missing_credentials_without_mutation() {
    let hub = Hub::new(HubConfig {
        operator_token: None,
        node_token: None,
        insecure_local: false,
        lease_ttl_ms: 1_000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
    });
    assert!(!hub.operator_authorized(None).await);
    let result = hub
        .register(RegisterRequest {
            node_id: "unauthorized-node".into(),
            node_token: String::new(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: None,
            data_address: None,
        })
        .await;
    assert!(matches!(result, Err(HubError::Unauthorized)));
    assert!(hub.nodes().await.is_empty());
}

#[test]
fn node_metrics_accept_only_finite_whitelisted_values() {
    let metrics = sanitize_metrics(BTreeMap::from([
        ("input_messages".into(), 4.0),
        ("arbitrary_label".into(), 99.0),
        ("output_errors".into(), f64::NAN),
        ("restarts".into(), -1.0),
    ]));
    assert_eq!(metrics.get("input_messages"), Some(&4.0));
    assert!(!metrics.contains_key("arbitrary_label"));
    assert!(!metrics.contains_key("output_errors"));
    assert!(!metrics.contains_key("restarts"));
}

#[test]
fn resource_gauges_pass_the_whitelist_into_the_node_view() {
    let metrics = sanitize_metrics(BTreeMap::from([
        ("node_cpu_usage_percent".into(), 37.5),
        ("node_memory_used_bytes".into(), 1_000.0),
        ("node_memory_total_bytes".into(), 8_000.0),
        ("node_memory_available_bytes".into(), 7_000.0),
        ("node_not_a_real_gauge".into(), 1.0),
    ]));
    assert_eq!(
        metrics,
        BTreeMap::from([
            ("node_cpu_usage_percent".to_string(), 37.5),
            ("node_memory_used_bytes".to_string(), 1_000.0),
            ("node_memory_total_bytes".to_string(), 8_000.0),
            ("node_memory_available_bytes".to_string(), 7_000.0),
        ])
    );
}

async fn register_and_report_resources(hub: &Hub, report_seq: u64) {
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap();
    hub.report(NodeReport {
        auth: AgentAuth {
            node_id: "n1".into(),
            session_token: session.session_token,
        },
        version: "test".into(),
        state: "online".into(),
        capabilities: vec![],
        streams: vec![],
        operations: vec![],
        events: vec![],
        metrics: BTreeMap::from([
            ("node_cpu_usage_percent".to_string(), 12.5),
            ("node_memory_total_bytes".to_string(), 16_000.0),
        ]),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: None,
        boot_id: None,
        report_seq,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn reported_resource_gauges_surface_in_the_node_metrics_view() {
    let hub = Hub::new(config());
    register_and_report_resources(&hub, 1).await;
    let view = hub
        .metrics_by_node(Some("n1"))
        .await
        .into_iter()
        .next()
        .expect("node view");
    assert_eq!(view.metrics.get("node_cpu_usage_percent"), Some(&12.5));
    assert_eq!(view.metrics.get("node_memory_total_bytes"), Some(&16_000.0));
}

#[tokio::test]
async fn resource_gauges_are_ephemeral_across_a_hub_restart() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub1 = Hub::with_storage(config(), storage.clone());
    register_and_report_resources(&hub1, 1).await;
    assert!(hub1
        .metrics_by_node(Some("n1"))
        .await
        .into_iter()
        .next()
        .expect("node view")
        .metrics
        .contains_key("node_cpu_usage_percent"));
    // After a restart the registry starts empty and the reconnecting
    // node re-registers with an empty gauge set: gauges only reappear
    // with the node's next report. No durable gauge history exists.
    let hub2 = Hub::with_storage(config(), storage);
    assert!(hub2.metrics_by_node(Some("n1")).await.is_empty());
    let session = hub2
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: SUPPORTED_PROTOCOL_VERSION.into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap();
    let view = hub2
        .metrics_by_node(Some("n1"))
        .await
        .into_iter()
        .next()
        .expect("re-registered node view");
    assert!(!view.metrics.keys().any(|key| key.starts_with("node_")));
    register_and_report_resources(&hub2, 2).await;
    let view = hub2
        .metrics_by_node(Some("n1"))
        .await
        .into_iter()
        .next()
        .expect("reported node view");
    assert!(view.metrics.contains_key("node_cpu_usage_percent"));
    drop(session);
}

#[test]
fn capability_allowlist_is_bounded_and_label_safe() {
    let long = "x".repeat(65);
    let capabilities =
        sanitize_capabilities(vec!["configuration".into(), "unsafe label".into(), long]);
    assert_eq!(capabilities, vec!["configuration"]);
}

#[tokio::test]
async fn compatibility_token_can_be_scoped_to_a_role() {
    let hub = Hub::new(HubConfig {
        operator_token: Some("readonly|viewer|viewer-secret".into()),
        ..config()
    });
    assert!(hub.operator_authorized(Some("viewer-secret")).await);
    assert!(
        hub.operator_can(Some("viewer-secret"), OperatorAction::Read)
            .await
    );
    assert!(
        !hub.operator_can(Some("viewer-secret"), OperatorAction::Operate)
            .await
    );
    assert!(!hub.operator_authorized(Some("operator")).await);
}

#[tokio::test]
async fn operator_credential_can_limit_resource_scope() {
    let hub = Hub::new(HubConfig {
        operator_token: Some("ops|operator|operator-secret|node=node-a,rollout=".into()),
        ..config()
    });
    assert!(
        hub.operator_can_scope(
            Some("operator-secret"),
            OperatorAction::Operate,
            "node",
            Some("node-a")
        )
        .await
    );
    assert!(
        !hub.operator_can_scope(
            Some("operator-secret"),
            OperatorAction::Operate,
            "node",
            Some("node-b")
        )
        .await
    );
    assert!(
        hub.operator_can_scope(
            Some("operator-secret"),
            OperatorAction::ManageRollouts,
            "rollout",
            Some("rollout-1")
        )
        .await
    );
}

#[test]
fn agent_wire_contract_round_trips_reconciliation_fields() {
    let command = AgentCommand {
        id: "cmd-1".into(),
        operation_id: "intent-1".into(),
        node_id: "node-a".into(),
        operation: "restart".into(),
        resource_id: "orders".into(),
        expires_at_ms: 123,
        generation: 7,
        action_id: Some("restart-7".into()),
        config_version_id: Some("cfg-7".into()),
        attempt_id: Some("attempt-7".into()),
        correlation_id: Some("corr-7".into()),
        payload: None,
        required_capabilities: vec!["stream_lifecycle".into()],
        rollout_id: None,
    };
    let encoded = serde_json::to_vec(&command).unwrap();
    let decoded: AgentCommand = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded.generation, 7);
    assert_eq!(decoded.action_id.as_deref(), Some("restart-7"));
    assert_eq!(decoded.config_version_id.as_deref(), Some("cfg-7"));
    assert_eq!(decoded.attempt_id.as_deref(), Some("attempt-7"));
    assert_eq!(decoded.expires_at_ms, 123);

    let report = NodeReport {
        auth: AgentAuth {
            node_id: "node-a".into(),
            session_token: "session".into(),
        },
        version: "test".into(),
        state: "online".into(),
        capabilities: vec![],
        streams: vec![],
        operations: vec![],
        events: vec![],
        metrics: BTreeMap::new(),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: Some("cfg-7".into()),
        boot_id: Some("boot-7".into()),
        report_seq: 9,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    };
    let decoded: NodeReport =
        serde_json::from_value(serde_json::to_value(report).unwrap()).unwrap();
    assert_eq!(decoded.boot_id.as_deref(), Some("boot-7"));
    assert_eq!(decoded.report_seq, 9);
    assert_eq!(decoded.configuration_version.as_deref(), Some("cfg-7"));
}

fn stopped_report(stream_id: &str, generation: Option<u64>) -> StreamStatus {
    StreamStatus {
        id: stream_id.into(),
        state: StreamState::Stopped,
        desired_state: None,
        desired_generation: 0,
        desired_config_version: None,
        observed_generation: generation,
        observed_config_version: None,
        convergence: ConvergenceState::Unknown,
        intent_id: None,
        attempt_id: None,
        last_completed_action_id: None,
        retry_count: 0,
        next_retry_at_ms: None,
        transition_started_at_ms: None,
        active_operation_id: None,
        node_id: Some("node-a".into()),
        started_at_ms: None,
        last_error: None,
        metrics: StreamMetricsSnapshot::default(),
    }
}

#[tokio::test]
async fn persisted_intent_survives_hub_restart_before_dispatch() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = StorageActor::start(store, 8);
    let hub1 = Hub::with_storage(config(), storage.clone());
    let intent = hub1
        .set_desired_state(DesiredMutation {
            node_id: "node-a".into(),
            stream_id: "orders".into(),
            desired_state: "running".into(),
            expected_generation: Some(0),
            ..Default::default()
        })
        .await
        .unwrap();
    drop(hub1);

    let hub2 = Hub::with_storage(config(), storage);
    hub2.register(RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec!["stream_lifecycle".into()],
        boot_id: None,
    })
    .await
    .unwrap();
    let operation = hub2.reconcile_once("after-restart").await.unwrap();
    assert_eq!(operation.as_ref().map(|value| value.generation), Some(1));
    assert_eq!(
        operation.as_ref().map(|value| value.id.as_str()),
        Some(intent.intent_id.as_str())
    );
    assert!(hub2
        .operations(None)
        .await
        .iter()
        .any(|value| value.intent_id.as_deref() == Some(intent.intent_id.as_str())));
}

#[tokio::test]
async fn rollout_actions_are_durable_and_audited() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-current', 'digest', '{}', 'json', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
    let storage = StorageActor::start(store, 8);
    let hub = Hub::with_storage(config(), storage);
    let rollout = hub
        .create_rollout(
            "cfg-current".into(),
            vec!["node-a".into(), "node-b".into()],
            1,
            Some("operator".into()),
            Some("corr-1".into()),
        )
        .await
        .unwrap();

    let paused = hub
        .act_rollout(
            &rollout.rollout_id,
            "pause",
            None,
            Some("operator".into()),
            Some("corr-2".into()),
        )
        .await
        .unwrap();
    assert_eq!(paused.state, "paused");
    let resumed = hub
        .act_rollout(
            &rollout.rollout_id,
            "resume",
            None,
            Some("operator".into()),
            Some("corr-3".into()),
        )
        .await
        .unwrap();
    assert_eq!(resumed.state, "applying");
    let cancelled = hub
        .act_rollout(
            &rollout.rollout_id,
            "cancel",
            None,
            Some("operator".into()),
            Some("corr-4".into()),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.state, "cancelled");
    let persisted = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
    assert_eq!(persisted.0.state, "cancelled");
    assert_eq!(
        persisted
            .1
            .iter()
            .filter(|target| target.state == "cancelled")
            .count(),
        2
    );
    assert_eq!(hub.audit(Some(&rollout.rollout_id)).await.unwrap().len(), 4);
}

#[tokio::test]
async fn rollout_dispatch_keeps_reference_when_secret_missing() {
    // Reconcile itself does not resolve secrets: the dispatch-time
    // pre-resolution happens later (at command delivery), and the
    // version store keeps the verbatim reference.
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let assertion_store = store.clone();
    let candidate = serde_json::json!({
        "format": "yaml",
        "content": "health_check:\n  api_token: ${secret:never_set_x}\n"
    })
    .to_string();
    std::env::remove_var("ARKFLOW_SECRET_never_set_x");
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-missing', 'digest', ?1, 'json', 1)",
                    [&candidate],
                )?;
                Ok(())
            })
            .unwrap();
    let hub = Hub::with_storage(config(), StorageActor::start(store, 8));
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec!["configuration".into()],
        boot_id: None,
    })
    .await
    .unwrap();
    let rollout = hub
        .create_rollout("cfg-missing".into(), vec!["node-a".into()], 1, None, None)
        .await
        .unwrap();
    hub.reconcile_rollouts().await.unwrap();
    let (_, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
    assert_eq!(targets[0].state, "applying");
    let content =
        crate::storage::StorageBackend::get_config_version_content(&assertion_store, "cfg-missing")
            .await
            .unwrap()
            .expect("version content");
    assert!(
        content.contains("${secret:never_set_x}"),
        "stored version must keep the reference: {content}"
    );
}

#[tokio::test]
async fn rollout_dispatch_preresolves_secret_references() {
    std::env::set_var("ARKFLOW_SECRET_db_pass", "s3cret");
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let assertion_store = store.clone();
    let candidate = serde_json::json!({
        "format": "yaml",
        "content": "health_check:\n  api_token: ${secret:db_pass}\n  host: ${env:HUB_HOST}\n"
    })
    .to_string();
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-secret', 'digest', ?1, 'json', 1)",
                    [&candidate],
                )?;
                Ok(())
            })
            .unwrap();
    let hub = Hub::with_storage(config(), StorageActor::start(store, 8));
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec!["configuration".into()],
        boot_id: None,
    })
    .await
    .unwrap();
    let rollout = hub
        .create_rollout("cfg-secret".into(), vec!["node-a".into()], 1, None, None)
        .await
        .unwrap();
    hub.reconcile_rollouts().await.unwrap();
    let (_, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
    assert_eq!(targets[0].state, "applying");

    std::env::remove_var("ARKFLOW_SECRET_db_pass");
    // The dispatched intent payload carries the resolved value and no
    // secret reference; env refs stay node-local.
    let payload: Option<String> = assertion_store
            .with_connection(|connection| {
                let mut statement = connection
                    .prepare("SELECT payload_json FROM cp_intents WHERE intent_type = 'apply_configuration' ORDER BY created_at_ms DESC LIMIT 1")?;
                let value: Option<String> = statement.query_row([], |row| row.get(0))?;
                Ok(value)
            })
            .unwrap();
    let payload = payload.expect("apply_configuration intent payload");
    // Storage keeps the verbatim reference: no plaintext at rest, and
    // later delivery attempts re-resolve against fresh environment
    // values. env refs stay node-local.
    assert!(payload.contains("${secret:db_pass}"), "{payload}");
    assert!(!payload.contains("s3cret"), "{payload}");
    assert!(payload.contains("${env:HUB_HOST}"), "{payload}");
}

/// A dispatch-time pre-resolution failure is permanent, so it must land
/// in the attempt/intent failure machinery (intent blocked, rollout
/// target failed, outbox row consumed) instead of returning an error
/// that leaves the lease to expire and re-claim the identical payload
/// forever.
#[tokio::test]
async fn reconcile_once_pre_resolution_failure_blocks_the_intent() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let assertion_store = store.clone();
    let candidate = serde_json::json!({
        "format": "yaml",
        "content": "health_check:\n  api_token: ${secret:never_set_y}\n"
    })
    .to_string();
    std::env::remove_var("ARKFLOW_SECRET_never_set_y");
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-block', 'digest', ?1, 'json', 1)",
                    [&candidate],
                )?;
                Ok(())
            })
            .unwrap();
    let hub = Hub::with_storage(config(), StorageActor::start(store, 8));
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec!["configuration".into()],
        boot_id: None,
    })
    .await
    .unwrap();
    let rollout = hub
        .create_rollout("cfg-block".into(), vec!["node-a".into()], 1, None, None)
        .await
        .unwrap();
    hub.reconcile_rollouts().await.unwrap();
    let (_, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
    assert_eq!(targets[0].state, "applying");

    // Dispatch consumes the outbox row, fails the attempt, and returns
    // no command — no half-resolved configuration reaches the Agent.
    let dispatched = hub.reconcile_once("worker-1").await.unwrap();
    assert!(dispatched.is_none(), "no command may be dispatched");

    let (intent_state, failure_class) = assertion_store
            .with_connection(|connection| {
                let mut statement = connection
                    .prepare("SELECT state, COALESCE(last_failure_class, '') FROM cp_intents WHERE intent_type = 'apply_configuration'")?;
                let row = statement.query_row([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?;
                Ok(row)
            })
            .unwrap();
    assert_eq!(intent_state, "blocked");
    assert_eq!(failure_class, "invalid_config");

    let (attempt_state, attempt_class) = assertion_store
        .with_connection(|connection| {
            let mut statement =
                connection.prepare("SELECT state, COALESCE(failure_class, '') FROM cp_attempts")?;
            let row = statement.query_row([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            Ok(row)
        })
        .unwrap();
    assert_eq!(attempt_state, "failed");
    assert_eq!(attempt_class, "invalid_config");

    let unprocessed: i64 = assertion_store
        .with_connection(|connection| {
            connection.query_row(
                "SELECT COUNT(*) FROM cp_outbox WHERE processed_at_ms IS NULL",
                [],
                |row| row.get(0),
            )
        })
        .unwrap();
    assert_eq!(unprocessed, 0, "the outbox row must be consumed");

    // The blocked intent rolls the rollout target forward to failed, so
    // the rollout no longer reports applying forever.
    hub.reconcile_rollouts().await.unwrap();
    let (_, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
    assert_eq!(targets[0].state, "failed");
}

#[tokio::test]
async fn rollout_reconciler_dispatches_only_the_current_batch() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-batch', 'digest', '{}', 'json', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
    let hub = Hub::with_storage(config(), StorageActor::start(store, 8));
    for node_id in ["node-a", "node-b"] {
        hub.register(RegisterRequest {
            data_address: None,
            node_id: node_id.into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["configuration".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    }
    let rollout = hub
        .create_rollout(
            "cfg-batch".into(),
            vec!["node-a".into(), "node-b".into()],
            1,
            Some("operator".into()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(hub.reconcile_rollouts().await.unwrap(), 2);
    let (_, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
    assert_eq!(targets[0].state, "applying");
    assert_eq!(targets[1].state, "pending");
    assert_eq!(
        hub.rollout(&rollout.rollout_id)
            .await
            .unwrap()
            .unwrap()
            .0
            .current_batch,
        0
    );
}

#[tokio::test]
async fn rollout_converges_only_after_target_configuration_is_observed() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-health', 'digest', '{}', 'json', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
    let hub = Hub::with_storage(config(), StorageActor::start(store, 8));
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["configuration".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    let rollout = hub
        .create_rollout(
            "cfg-health".into(),
            vec!["node-a".into()],
            1,
            Some("operator".into()),
            None,
        )
        .await
        .unwrap();
    hub.reconcile_rollouts().await.unwrap();
    assert_eq!(
        hub.rollout(&rollout.rollout_id)
            .await
            .unwrap()
            .unwrap()
            .0
            .state,
        "applying"
    );
    hub.report(NodeReport {
        auth: AgentAuth {
            node_id: "node-a".into(),
            session_token: session.session_token.clone(),
        },
        version: "agent-1".into(),
        state: "online".into(),
        capabilities: vec!["configuration".into()],
        streams: vec![],
        operations: vec![],
        events: vec![],
        metrics: BTreeMap::new(),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: Some("cfg-health".into()),
        boot_id: Some(session.session_token.clone()),
        report_seq: 1,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    })
    .await
    .unwrap();
    hub.reconcile_rollouts().await.unwrap();
    let (rollout, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
    assert_eq!(targets[0].state, "succeeded");
    assert_eq!(rollout.state, "converged");
}

#[tokio::test]
async fn rollout_state_machine_covers_gates_drain_restart_rollback_and_cancel() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    store
            .with_connection(|connection| {
                for version in ["cfg-state-a", "cfg-state-b"] {
                    connection.execute(
                        "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES (?1, 'digest', '{}', 'json', 1)",
                        [version],
                    )?;
                }
                Ok(())
            })
            .unwrap();
    let storage = StorageActor::start(store, 8);
    let hub = Hub::with_storage(config(), storage.clone());
    let node_a = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["configuration".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-b".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec!["configuration".into()],
        boot_id: None,
    })
    .await
    .unwrap();
    hub.set_node_maintenance(
        "node-b",
        NodeMaintenanceState::Draining,
        Some("operator".into()),
        Some("drain-state".into()),
    )
    .await
    .unwrap();

    let rollout = hub
        .create_rollout(
            "cfg-state-a".into(),
            vec!["node-a".into(), "node-b".into()],
            1,
            Some("operator".into()),
            Some("state-machine".into()),
        )
        .await
        .unwrap();
    hub.reconcile_rollouts().await.unwrap();
    let (_, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
    assert_eq!(targets[0].state, "applying");
    assert_eq!(targets[1].state, "pending");

    let paused = hub
        .act_rollout(
            &rollout.rollout_id,
            "pause",
            None,
            Some("operator".into()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(paused.state, "paused");
    let resumed = hub
        .act_rollout(
            &rollout.rollout_id,
            "resume",
            None,
            Some("operator".into()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resumed.state, "applying");

    hub.report(NodeReport {
        auth: AgentAuth {
            node_id: "node-a".into(),
            session_token: node_a.session_token.clone(),
        },
        version: "agent-state".into(),
        state: "online".into(),
        capabilities: vec!["configuration".into()],
        streams: vec![],
        operations: vec![],
        events: vec![],
        metrics: BTreeMap::new(),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: Some("cfg-state-a".into()),
        boot_id: Some(node_a.session_token.clone()),
        report_seq: 1,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    })
    .await
    .unwrap();
    hub.reconcile_rollouts().await.unwrap();
    hub.set_node_maintenance(
        "node-b",
        NodeMaintenanceState::Active,
        Some("operator".into()),
        Some("resume-state".into()),
    )
    .await
    .unwrap();
    hub.reconcile_rollouts().await.unwrap();
    let (_, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
    assert_eq!(targets[0].state, "succeeded");
    assert_eq!(targets[1].state, "applying");

    let rollback = hub
        .act_rollout(
            &rollout.rollout_id,
            "rollback",
            Some("cfg-state-b".into()),
            Some("operator".into()),
            Some("rollback-state".into()),
        )
        .await
        .unwrap();
    assert_eq!(rollback.total_targets, 2);
    assert_eq!(
        hub.rollout(&rollout.rollout_id)
            .await
            .unwrap()
            .unwrap()
            .0
            .state,
        "rolled_back"
    );

    let failed = hub
        .create_rollout(
            "cfg-state-a".into(),
            vec!["node-a".into()],
            1,
            Some("operator".into()),
            Some("permanent-failure".into()),
        )
        .await
        .unwrap();
    storage
        .update_rollout_target(RolloutTargetUpdate {
            rollout_id: failed.rollout_id.clone(),
            node_id: "node-a".into(),
            state: "failed".into(),
            attempt_id: None,
            error: Some("permanent_execution".into()),
            observed_config_version: None,
            updated_at_ms: now_ms(),
        })
        .await
        .unwrap();
    hub.reconcile_rollouts().await.unwrap();
    assert_eq!(
        hub.rollout(&failed.rollout_id)
            .await
            .unwrap()
            .unwrap()
            .0
            .state,
        "paused"
    );

    drop(hub);
    let recovered = Hub::with_storage(config(), storage);
    recovered.recover_persisted_state().await.unwrap();
    assert_eq!(
        recovered
            .rollout(&rollback.rollout_id)
            .await
            .unwrap()
            .unwrap()
            .0
            .state,
        "applying"
    );
    let cancelled = recovered
        .act_rollout(
            &rollback.rollout_id,
            "cancel",
            None,
            Some("operator".into()),
            Some("cancel-state".into()),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.state, "cancelled");
}

#[tokio::test]
async fn multiple_agent_rollout_smoke_completes_through_commands_and_reports() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-e2e', 'digest', '{}', 'json', 1)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
    let hub = Hub::with_storage(config(), StorageActor::start(store, 8));
    let mut sessions = Vec::new();
    for node_id in ["agent-a", "agent-b"] {
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec!["configuration".into()],
                boot_id: None,
            })
            .await
            .unwrap();
        sessions.push((node_id.to_owned(), session.session_token));
    }
    let rollout = hub
        .create_rollout(
            "cfg-e2e".into(),
            vec!["agent-a".into(), "agent-b".into()],
            2,
            Some("operator".into()),
            Some("e2e-rollout".into()),
        )
        .await
        .unwrap();
    assert_eq!(hub.reconcile_rollouts().await.unwrap(), 3);

    for (node_id, session_token) in sessions {
        let worker_id = format!("e2e-reconcile-{node_id}");
        let operation = hub.reconcile_once(&worker_id).await.unwrap().unwrap();
        let auth = AgentAuth {
            node_id: node_id.clone(),
            session_token,
        };
        let commands = hub.commands(auth.clone()).await.unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].rollout_id.as_deref(),
            Some(rollout.rollout_id.as_str())
        );
        hub.command_result(
            auth.clone(),
            CommandResult {
                command_id: commands[0].id.clone(),
                operation_id: operation.id,
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: commands[0].correlation_id.clone(),
                generation: commands[0].generation,
                observed_generation: None,
                action_id: commands[0].action_id.clone(),
                failure_class: None,
                config_version_id: Some("cfg-e2e".into()),
                rollout_id: commands[0].rollout_id.clone(),
                observed_checkpoint_id: None,
                checkpoint_manifest_uri: None,
                result: None,
            },
        )
        .await
        .unwrap();
        hub.report(NodeReport {
            auth: auth.clone(),
            version: "agent-e2e".into(),
            state: "online".into(),
            capabilities: vec!["configuration".into()],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::from([("streams_total".into(), 0.0)]),
            jobs: BTreeMap::new(),
            configuration: None,
            configuration_version: Some("cfg-e2e".into()),
            boot_id: Some(auth.session_token.clone()),
            report_seq: 1,
            config_versions: Vec::new(),
            job_tasks: BTreeMap::new(),
        })
        .await
        .unwrap();
    }
    hub.reconcile_rollouts().await.unwrap();
    let (rollout, targets) = hub.rollout(&rollout.rollout_id).await.unwrap().unwrap();
    assert_eq!(rollout.state, "converged");
    assert!(targets.iter().all(|target| target.state == "succeeded"));
}

#[tokio::test]
async fn dispatched_attempt_waits_for_fresh_report_after_hub_restart() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = StorageActor::start(store, 8);
    let hub1 = Hub::with_storage(config(), storage.clone());
    let session1 = hub1
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["stream_lifecycle".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    let intent = hub1
        .set_desired_state(DesiredMutation {
            node_id: "node-a".into(),
            stream_id: "orders".into(),
            desired_state: "running".into(),
            expected_generation: Some(0),
            ..Default::default()
        })
        .await
        .unwrap();
    hub1.reconcile_once("dispatch").await.unwrap().unwrap();
    hub1.commands(AgentAuth {
        node_id: "node-a".into(),
        session_token: session1.session_token,
    })
    .await
    .unwrap();
    storage
        .expire_attempts(now_ms() + config().lease_ttl_ms + 1)
        .await
        .unwrap();
    drop(hub1);

    let hub2 = Hub::with_storage(config(), storage);
    let session2 = hub2
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["stream_lifecycle".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    hub2.recover_persisted_state().await.unwrap();
    assert!(hub2
        .reconcile_once("without-report")
        .await
        .unwrap()
        .is_none());
    hub2.report(NodeReport {
        auth: AgentAuth {
            node_id: "node-a".into(),
            session_token: session2.session_token.clone(),
        },
        version: "test".into(),
        state: "online".into(),
        capabilities: vec!["stream_lifecycle".into()],
        streams: vec![stopped_report("orders", Some(0))],
        operations: vec![],
        events: vec![],
        metrics: BTreeMap::new(),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: None,
        boot_id: Some(session2.session_token.clone()),
        report_seq: 1,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    })
    .await
    .unwrap();
    let operation = hub2.reconcile_once("after-report").await.unwrap();
    assert_eq!(
        operation.as_ref().map(|value| value.id.as_str()),
        Some(intent.intent_id.as_str())
    );
}

#[tokio::test]
async fn registers_reports_and_dispatches_targeted_commands() {
    let hub = Hub::new(config());
    assert!(matches!(
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "bad".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await,
        Err(HubError::Unauthorized)
    ));
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["stream_lifecycle".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    hub.report(NodeReport {
        auth: AgentAuth {
            node_id: "n1".into(),
            session_token: session.session_token.clone(),
        },
        version: "test".into(),
        state: "online".into(),
        capabilities: vec!["stream_lifecycle".into()],
        streams: vec![],
        operations: vec![],
        events: vec![],
        metrics: BTreeMap::new(),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: None,
        boot_id: None,
        report_seq: 0,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    })
    .await
    .unwrap();
    let first = hub
        .enqueue(
            "n1".into(),
            "start".into(),
            "orders".into(),
            Some("corr".into()),
        )
        .await
        .unwrap();
    let second = hub
        .enqueue(
            "n1".into(),
            "start".into(),
            "orders".into(),
            Some("corr".into()),
        )
        .await
        .unwrap();
    assert_eq!(first.id, second.id);
    let commands = hub
        .commands(AgentAuth {
            node_id: "n1".into(),
            session_token: session.session_token.clone(),
        })
        .await
        .unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].operation_id, first.id);
    let result = hub
        .command_result(
            AgentAuth {
                node_id: "n1".into(),
                session_token: session.session_token,
            },
            CommandResult {
                command_id: commands[0].id.clone(),
                operation_id: first.id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: Some("corr".into()),
                generation: first.generation,
                observed_generation: None,
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
    assert_eq!(result.state, HubOperationState::Succeeded);
}

#[tokio::test]
async fn expired_lease_is_not_commandable() {
    let hub = Hub::new(HubConfig {
        lease_ttl_ms: 1,
        ..config()
    });
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    hub.mark_stale().await;
    assert!(matches!(
        hub.enqueue("n1".into(), "start".into(), "orders".into(), None)
            .await,
        Err(HubError::NodeUnavailable)
    ));
    assert_eq!(hub.nodes().await[0].state, NodeConnectionState::Stale);
    assert!(!session.session_token.is_empty());
}

fn job_snapshot(rows: u64) -> arkflow_core::executor::metrics::KernelMetricsSnapshot {
    let mut chains = BTreeMap::new();
    chains.insert(
        "src".to_string(),
        arkflow_core::executor::metrics::ChainMetricsSnapshot {
            rows,
            ..Default::default()
        },
    );
    arkflow_core::executor::metrics::KernelMetricsSnapshot {
        chains,
        ..Default::default()
    }
}

/// An Agent that predates the per-Job reporting field (empty map after
/// serde defaults) must not produce any data-plane series.
#[tokio::test]
async fn report_without_job_snapshots_exports_no_data_plane_series() {
    let hub = Hub::new(config());
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap();
    hub.report(NodeReport {
        auth: AgentAuth {
            node_id: "n1".into(),
            session_token: session.session_token.clone(),
        },
        version: "test".into(),
        state: "online".into(),
        capabilities: vec![],
        streams: vec![],
        operations: vec![],
        events: vec![],
        metrics: BTreeMap::new(),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: None,
        boot_id: None,
        report_seq: 1,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    })
    .await
    .unwrap();
    assert!(hub.job_metrics().await.is_empty());
}

/// Two reporting Agents produce the same series vocabulary distinguished
/// by the `node` label, per (node, job) granularity.
#[tokio::test]
async fn reported_job_metrics_carry_node_and_job_labels() {
    let hub = Hub::new(config());
    let mut sessions = BTreeMap::new();
    for node_id in ["node-a", "node-b"] {
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: None,
            })
            .await
            .unwrap();
        sessions.insert(node_id.to_string(), session.session_token.clone());
        hub.report(NodeReport {
            auth: AgentAuth {
                node_id: node_id.into(),
                session_token: session.session_token.clone(),
            },
            version: "test".into(),
            state: "online".into(),
            capabilities: vec![],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            jobs: BTreeMap::from([(
                "job-1".into(),
                job_snapshot(if node_id == "node-a" { 7 } else { 9 }),
            )]),
            configuration: None,
            configuration_version: None,
            boot_id: None,
            report_seq: 1,
            config_versions: Vec::new(),
            job_tasks: BTreeMap::new(),
        })
        .await
        .unwrap();
    }
    let exported = hub.job_metrics().await;
    assert_eq!(exported.len(), 2);
    for (node_id, jobs) in &exported {
        let snapshot = jobs.get("job-1").expect("job-1 snapshot stored");
        let expected_rows = if node_id == "node-a" { 7 } else { 9 };
        assert_eq!(snapshot.chains["src"].rows, expected_rows);
        let text = crate::metrics::encode_families(crate::metrics::kernel_job_families(
            "job-1",
            snapshot,
            &[("node", node_id.clone())],
        ));
        assert!(
            text.contains(&format!("node=\"{node_id}\"")),
            "series must carry the node label"
        );
    }
}

/// An Agent whose lease expires stops being exported while a live peer's
/// series remain.
#[tokio::test]
async fn expired_lease_stops_data_plane_export() {
    let hub = Hub::new(config());
    let mut sessions = BTreeMap::new();
    for node_id in ["n1", "n2"] {
        let session = hub
            .register(RegisterRequest {
                data_address: None,
                node_id: node_id.into(),
                node_token: "node-secret".into(),
                protocol_version: "v1".into(),
                capabilities: vec![],
                boot_id: None,
            })
            .await
            .unwrap();
        sessions.insert(node_id.to_string(), session.session_token.clone());
        hub.report(NodeReport {
            auth: AgentAuth {
                node_id: node_id.into(),
                session_token: session.session_token.clone(),
            },
            version: "test".into(),
            state: "online".into(),
            capabilities: vec![],
            streams: vec![],
            operations: vec![],
            events: vec![],
            metrics: BTreeMap::new(),
            jobs: BTreeMap::from([(format!("{node_id}-job"), job_snapshot(1))]),
            configuration: None,
            configuration_version: None,
            boot_id: None,
            report_seq: 1,
            config_versions: Vec::new(),
            job_tasks: BTreeMap::new(),
        })
        .await
        .unwrap();
    }
    assert_eq!(hub.job_metrics().await.len(), 2);

    // n1's lease lapses deterministically; a tiny TTL would race the
    // wall clock across the registration awaits above.
    hub.nodes
        .write()
        .await
        .get_mut("n1")
        .unwrap()
        .resource
        .lease_expires_at_ms = now_ms();
    hub.heartbeat(HeartbeatRequest {
        auth: AgentAuth {
            node_id: "n2".into(),
            session_token: sessions["n2"].clone(),
        },
        state: "online".into(),
        protocol_version: Some("v1".into()),
        software_version: None,
        capabilities: vec![],
        rollout_id: None,
    })
    .await
    .unwrap();

    let exported = hub.job_metrics().await;
    assert_eq!(exported.len(), 1, "only the live node keeps exporting");
    assert_eq!(exported[0].0, "n2");
    assert!(exported[0].1.contains_key("n2-job"));
}

#[tokio::test]
async fn unsupported_capability_is_rejected_before_dispatch() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub = Hub::with_storage(config(), storage.clone());
    assert!(matches!(
        hub.register(RegisterRequest {
            data_address: None,
            node_id: "incompatible".into(),
            node_token: "node-secret".into(),
            protocol_version: "v0".into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await,
        Err(HubError::Invalid(message)) if message.contains("protocol")
    ));
    let protocol_audit = hub.audit(Some("incompatible")).await.unwrap();
    assert_eq!(
        protocol_audit[0].failure_code.as_deref(),
        Some("incompatible_protocol")
    );
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "n1".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec!["configuration".into()],
        boot_id: None,
    })
    .await
    .unwrap();
    assert!(matches!(
        hub.enqueue("n1".into(), "start".into(), "orders".into(), None)
            .await,
        Err(HubError::Invalid(message)) if message.contains("capability")
    ));
    let audit = hub.audit(Some("orders")).await.unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(
        audit[0].failure_code.as_deref(),
        Some("incompatible_capability")
    );
    assert_eq!(audit[0].outcome, "rejected");
}

#[tokio::test]
async fn ignores_replayed_reports_from_the_same_boot() {
    let hub = Hub::new(config());
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap();
    let stream = |state| {
        serde_json::from_value(serde_json::json!({
            "id": "orders",
            "state": state,
            "metrics": {
                "input_batches": 0,
                "input_messages": 0,
                "processing_errors": 0,
                "output_batches": 0,
                "output_messages": 0,
                "input_errors": 0,
                "input_reconnects": 0,
                "output_errors": 0,
                "restarts": 0
            }
        }))
        .unwrap()
    };
    let report = |report_seq, state| NodeReport {
        auth: AgentAuth {
            node_id: "n1".into(),
            session_token: session.session_token.clone(),
        },
        version: "test".into(),
        state: "online".into(),
        capabilities: vec![],
        streams: vec![stream(state)],
        operations: vec![],
        events: vec![],
        metrics: BTreeMap::new(),
        jobs: BTreeMap::new(),
        configuration: None,
        configuration_version: None,
        boot_id: Some(session.session_token.clone()),
        report_seq,
        config_versions: Vec::new(),
        job_tasks: BTreeMap::new(),
    };
    hub.report(report(2, "running")).await.unwrap();
    hub.report(report(1, "stopped")).await.unwrap();
    let streams = hub.streams(Some("n1")).await;
    assert_eq!(
        streams[0].1.state,
        arkflow_core::control::StreamState::Running
    );
}

#[tokio::test]
async fn reconciler_dispatches_persisted_intent_with_generation() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let hub = Hub::with_storage(config(), crate::storage::StorageActor::start(store, 8));
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap();
    let intent = hub
        .set_desired_state(crate::storage::DesiredMutation {
            node_id: "n1".into(),
            stream_id: "orders".into(),
            desired_state: "running".into(),
            config_version_id: None,
            action_id: None,
            expected_generation: Some(0),
            actor: Some("operator".into()),
            correlation_id: None,
            idempotency_key: None,
            intent_type: None,
            payload_json: None,
        })
        .await
        .unwrap();
    assert_eq!(intent.generation, 1);
    let operation = hub
        .reconcile_once("test-reconciler")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.operation, "start");
    let commands = hub
        .commands(AgentAuth {
            node_id: "n1".into(),
            session_token: session.session_token.clone(),
        })
        .await
        .unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].generation, 1);
    assert!(commands[0].attempt_id.is_some());

    hub.set_desired_state(crate::storage::DesiredMutation {
        node_id: "n1".into(),
        stream_id: "orders".into(),
        desired_state: "running".into(),
        config_version_id: None,
        action_id: Some("restart-action-1".into()),
        expected_generation: Some(1),
        actor: Some("operator".into()),
        correlation_id: None,
        idempotency_key: Some("restart-1".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let restart = hub
        .reconcile_once("test-reconciler")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restart.operation, "restart");
    let commands = hub
        .commands(AgentAuth {
            node_id: "n1".into(),
            session_token: session.session_token,
        })
        .await
        .unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].action_id.as_deref(), Some("restart-action-1"));
}

#[tokio::test]
async fn reconnect_replaces_session_but_preserves_node_resources() {
    let hub = Hub::new(config());
    let first = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["first".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    let second = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["second".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    assert_ne!(first.session_token, second.session_token);
    assert!(matches!(
        hub.heartbeat(HeartbeatRequest {
            auth: AgentAuth {
                node_id: "n1".into(),
                session_token: first.session_token
            },
            state: "online".into(),
            protocol_version: None,
            software_version: None,
            capabilities: vec![],
            rollout_id: None,
        })
        .await,
        Err(HubError::Unauthorized)
    ));
    hub.heartbeat(HeartbeatRequest {
        auth: AgentAuth {
            node_id: "n1".into(),
            session_token: second.session_token,
        },
        state: "online".into(),
        protocol_version: None,
        software_version: None,
        capabilities: vec![],
        rollout_id: None,
    })
    .await
    .unwrap();
    assert_eq!(hub.nodes().await.len(), 1);
}

#[tokio::test]
async fn command_queues_are_bounded_and_isolated_per_node() {
    let hub = Hub::new(config());
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "n1".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec![],
        boot_id: None,
    })
    .await
    .unwrap();
    let n2_session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "n2".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: None,
        })
        .await
        .unwrap()
        .session_token;
    for index in 0..128 {
        hub.enqueue("n1".into(), "start".into(), format!("stream-{index}"), None)
            .await
            .unwrap();
    }
    assert!(matches!(
        hub.enqueue("n1".into(), "start".into(), "overflow".into(), None)
            .await,
        Err(HubError::Capacity)
    ));
    let n2 = hub
        .commands(AgentAuth {
            node_id: "n2".into(),
            session_token: n2_session,
        })
        .await
        .unwrap();
    assert!(n2.is_empty());
}

#[tokio::test]
async fn job_observation_rejects_stale_generation() {
    let hub = Hub::new(config());
    let spec_json = serde_json::json!({
        "id": "orders",
        "version": 1,
        "operators": [
            {"id": "source", "kind": "source"},
            {"id": "sink", "kind": "sink"}
        ],
        "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
        "sources": [{
            "operator_id": "source",
            "input_type": "memory",
            "time": {"mode": "processing_time"}
        }],
        "sinks": [{"operator_id": "sink", "output_type": "drop"}]
    })
    .to_string();
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json,
        desired_state: "running".into(),
        observed_state: "starting".into(),
        convergence: "reconciling".into(),
        generation: 3,
        node_ids: vec!["n1".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let stale = hub
        .observe_job("orders", 2, "stopped", None, Some("stale"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stale.generation, 3);
    assert_eq!(stale.observed_state, "starting");
    let future = hub
        .observe_job("orders", 4, "running", Some("forged"), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(future.generation, 3);
    assert_eq!(future.checkpoint_id, None);
    let converged = hub
        .observe_job("orders", 3, "running", Some("cp-1"), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(converged.convergence, "converged");
    assert_eq!(converged.checkpoint_id.as_deref(), Some("cp-1"));
}

#[tokio::test]
async fn concurrent_job_generation_updates_use_compare_and_swap() {
    let hub = Hub::new(config());
    let spec_json = serde_json::json!({
            "id": "orders",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        }).to_string();
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json,
        desired_state: "stopped".into(),
        observed_state: "stopped".into(),
        convergence: "converged".into(),
        generation: 3,
        node_ids: Vec::new(),
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let (first, second) = tokio::join!(
        hub.update_job_desired_state("orders", "running", 3),
        hub.update_job_desired_state("orders", "stopped", 3),
    );
    assert!(matches!(
        (first, second),
        (Ok(Some(_)), Err(HubError::GenerationConflict { .. }))
            | (Err(HubError::GenerationConflict { .. }), Ok(Some(_)))
    ));
    let current = hub.job("orders").await.unwrap().unwrap();
    assert_eq!(current.generation, 4);
    assert_eq!(current.convergence, "reconciling");
}

#[tokio::test]
async fn replacing_a_job_preserves_generation_fencing() {
    let hub = Hub::new(config());
    let original = hub
        .upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: "{}".into(),
            desired_state: "stopped".into(),
            observed_state: "stopped".into(),
            convergence: "converged".into(),
            generation: 6,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 1,
        })
        .await
        .unwrap();
    assert_eq!(original.generation, 6);

    let replacement = hub
        .upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 2,
            spec_json: "{}".into(),
            desired_state: "stopped".into(),
            observed_state: "validated".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 2,
        })
        .await
        .unwrap();
    assert_eq!(replacement.generation, 7);
    assert_eq!(hub.job("orders").await.unwrap(), Some(replacement));
}

#[tokio::test]
async fn running_job_is_dispatched_to_compatible_agent() {
    let hub = Hub::new(config());
    let registration = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "compute-1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    let spec_json = serde_json::json!({
            "id": "orders",
            "version": 1,
            "operators": [
                {"id": "source", "kind": "source"},
                {"id": "sink", "kind": "sink"}
            ],
            "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
            "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
            "sinks": [{"operator_id": "sink", "output_type": "drop"}]
        })
        .to_string();

    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json,
        desired_state: "running".into(),
        observed_state: "starting".into(),
        convergence: "reconciling".into(),
        generation: 7,
        node_ids: Vec::new(),
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();

    let commands = hub
        .commands(AgentAuth {
            node_id: "compute-1".into(),
            session_token: registration.session_token.clone(),
        })
        .await
        .unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].operation, "job_start");
    assert_eq!(commands[0].resource_id, "orders");
    assert_eq!(commands[0].generation, 7);
    assert_eq!(
        commands[0].required_capabilities,
        vec!["job_runtime", "state_backend"]
    );
    assert_eq!(
        commands[0]
            .payload
            .as_ref()
            .and_then(|payload| payload.get("assignments"))
            .and_then(serde_json::Value::as_array)
            .map(Vec::len),
        Some(2)
    );

    let start_command = commands[0].clone();
    hub.command_result(
        AgentAuth {
            node_id: "compute-1".into(),
            session_token: registration.session_token.clone(),
        },
        CommandResult {
            command_id: start_command.id.clone(),
            operation_id: start_command.operation_id.clone(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: start_command.correlation_id.clone(),
            generation: start_command.generation,
            observed_generation: Some(start_command.generation),
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
    let job = hub.job("orders").await.unwrap().unwrap();
    hub.reconcile_job(&job).await.unwrap();
    let commands = hub
        .commands(AgentAuth {
            node_id: "compute-1".into(),
            session_token: registration.session_token.clone(),
        })
        .await
        .unwrap();
    assert!(!commands
        .iter()
        .any(|command| command.operation == "job_start"));

    hub.record_job_checkpoint(JobCheckpointRecord {
        job_id: "orders".into(),
        job_version: 1,
        checkpoint_id: "checkpoint-7".into(),
        kind: "checkpoint".into(),
        status: "pending".into(),
        manifest_uri: None,
        format_version: 1,
        created_at_ms: 0,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    let commands = hub
        .commands(AgentAuth {
            node_id: "compute-1".into(),
            session_token: registration.session_token.clone(),
        })
        .await
        .unwrap();
    assert!(commands.iter().any(|command| {
        command.operation == "job_checkpoint"
            && command
                .payload
                .as_ref()
                .and_then(|payload| payload.get("checkpoint_id"))
                .and_then(serde_json::Value::as_str)
                == Some("checkpoint-7")
    }));
    let checkpoint_command = commands
        .iter()
        .find(|command| command.operation == "job_checkpoint")
        .unwrap();
    assert_eq!(
        hub.operation(&checkpoint_command.operation_id)
            .await
            .unwrap()
            .checkpoint_id
            .as_deref(),
        Some("checkpoint-7")
    );
    hub.command_result(
        AgentAuth {
            node_id: "compute-1".into(),
            session_token: registration.session_token.clone(),
        },
        CommandResult {
            command_id: checkpoint_command.id.clone(),
            operation_id: checkpoint_command.operation_id.clone(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: checkpoint_command.correlation_id.clone(),
            generation: checkpoint_command.generation,
            observed_generation: Some(checkpoint_command.generation),
            action_id: None,
            failure_class: None,
            config_version_id: None,
            rollout_id: None,
            observed_checkpoint_id: Some("checkpoint-7".into()),
            checkpoint_manifest_uri: Some("/tmp/checkpoint-7/manifest.json".into()),
            result: None,
        },
    )
    .await
    .unwrap();

    let commands = hub
        .commands(AgentAuth {
            node_id: "compute-1".into(),
            session_token: registration.session_token.clone(),
        })
        .await
        .unwrap();
    let commit_command = commands
        .iter()
        .find(|command| command.operation == "job_checkpoint_commit")
        .expect("checkpoint commit command");
    assert_eq!(
        commit_command
            .payload
            .as_ref()
            .and_then(|payload| payload.get("checkpoint_id"))
            .and_then(serde_json::Value::as_str),
        Some("checkpoint-7")
    );
    assert_eq!(
        commit_command
            .payload
            .as_ref()
            .and_then(|payload| payload.get("manifest_nodes"))
            .and_then(serde_json::Value::as_array)
            .map(|nodes| nodes.len()),
        Some(1)
    );
    hub.command_result(
        AgentAuth {
            node_id: "compute-1".into(),
            session_token: registration.session_token,
        },
        CommandResult {
            command_id: commit_command.id.clone(),
            operation_id: commit_command.operation_id.clone(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: commit_command.correlation_id.clone(),
            generation: commit_command.generation,
            observed_generation: Some(commit_command.generation),
            action_id: None,
            failure_class: None,
            config_version_id: None,
            rollout_id: None,
            observed_checkpoint_id: Some("checkpoint-7".into()),
            checkpoint_manifest_uri: Some("/tmp/final/checkpoint-7/manifest.json".into()),
            result: None,
        },
    )
    .await
    .unwrap();
    let records = hub.job_checkpoints("orders").await.unwrap();
    assert_eq!(records[0].status, "completed");
    assert_eq!(
        records[0].manifest_uri.as_deref(),
        Some("/tmp/final/checkpoint-7/manifest.json")
    );

    // A new process has an empty local JobRuntime. Its new boot identity
    // must invalidate the old successful start and trigger reconciliation
    // instead of treating the absent local Job as already running.
    let restarted = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "compute-1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: Some("boot-after-process-restart".into()),
        })
        .await
        .unwrap();
    let restart_commands = hub
        .commands(AgentAuth {
            node_id: "compute-1".into(),
            session_token: restarted.session_token,
        })
        .await
        .unwrap();
    assert!(restart_commands
        .iter()
        .any(|command| command.operation == "job_start"));
}

/// Verification 2026-09-11 (harden-unified-streaming-runtime re-audit,
/// WARNING 1): the Job-level observed state aggregates every planned
/// assignment — one peer's success while another is still pending leaves
/// the Job converging, and a retryable peer degradation never overwrites
/// the healthy peer's observation as failed.
#[tokio::test]
async fn job_observed_state_waits_for_every_assignment_and_ignores_retryable_peer_degradation() {
    let storage = StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
    let hub = Hub::with_storage(config(), storage);
    let node_a = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "compute-1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    let node_b = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "compute-2".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    // Two components so each node receives one start assignment.
    let job = hub
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: serde_json::json!({
                    "id": "orders",
                    "version": 1,
                    "max_parallelism": 1,
                    "parallelism": 1,
                    "operators": [
                        {"id": "source-a", "kind": "source"},
                        {"id": "sink-a", "kind": "sink"},
                        {"id": "source-b", "kind": "source"},
                        {"id": "sink-b", "kind": "sink"}
                    ],
                    "edges": [
                        {"id": "edge-a", "from": "source-a", "to": "sink-a"},
                        {"id": "edge-b", "from": "source-b", "to": "sink-b"}
                    ],
                    "sources": [
                        {"operator_id": "source-a", "input_type": "memory", "time": {"mode": "processing_time"}},
                        {"operator_id": "source-b", "input_type": "memory", "time": {"mode": "processing_time"}}
                    ],
                    "sinks": [
                        {"operator_id": "sink-a", "output_type": "drop"},
                        {"operator_id": "sink-b", "output_type": "drop"}
                    ]
                })
                .to_string(),
                desired_state: "running".into(),
                observed_state: "starting".into(),
                convergence: "reconciling".into(),
                generation: 1,
                node_ids: vec!["compute-1".into(), "compute-2".into()],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 0,
            })
            .await
            .unwrap();

    // Peer A succeeds while peer B is still pending: the Job must stay
    // non-terminal until every planned assignment reports success.
    let command_a = hub
        .commands(AgentAuth {
            node_id: "compute-1".into(),
            session_token: node_a.session_token.clone(),
        })
        .await
        .unwrap()
        .into_iter()
        .find(|command| command.operation == "job_start")
        .expect("compute-1 receives a start assignment");
    hub.command_result(
        AgentAuth {
            node_id: "compute-1".into(),
            session_token: node_a.session_token.clone(),
        },
        CommandResult {
            command_id: command_a.id.clone(),
            operation_id: command_a.operation_id.clone(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: command_a.correlation_id,
            generation: command_a.generation,
            observed_generation: Some(job.generation),
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
    let observed = hub.job("orders").await.unwrap().unwrap();
    assert_eq!(
        observed.observed_state, "starting",
        "one peer's success must not report a fully running Job"
    );
    assert_eq!(observed.convergence, "reconciling");

    // Peer B reports a retryable degradation (a TimedOut state, as the
    // Agent produces for an expired lease). The aggregation is driven
    // purely by operation states — failure_class is informational — and
    // a retryable state must keep the healthy peer's observation
    // neutral instead of overwriting it as failed.
    let command_b = hub
        .commands(AgentAuth {
            node_id: "compute-2".into(),
            session_token: node_b.session_token.clone(),
        })
        .await
        .unwrap()
        .into_iter()
        .find(|command| command.operation == "job_start")
        .expect("compute-2 receives a start assignment");
    hub.command_result(
        AgentAuth {
            node_id: "compute-2".into(),
            session_token: node_b.session_token.clone(),
        },
        CommandResult {
            command_id: command_b.id.clone(),
            operation_id: command_b.operation_id.clone(),
            state: HubOperationState::TimedOut,
            progress: 0,
            error: Some("command lease expired".into()),
            correlation_id: command_b.correlation_id,
            generation: command_b.generation,
            observed_generation: Some(job.generation),
            action_id: None,
            failure_class: Some("temporary_execution".into()),
            config_version_id: None,
            rollout_id: None,
            observed_checkpoint_id: None,
            checkpoint_manifest_uri: None,
            result: None,
        },
    )
    .await
    .unwrap();
    let observed = hub.job("orders").await.unwrap().unwrap();
    assert_eq!(
        observed.observed_state, "starting",
        "a retryable peer degradation must stay observed-neutral"
    );
    assert_eq!(observed.convergence, "reconciling");

    // The retry is re-enqueued by reconciliation; once it succeeds too,
    // the complete assignment set is successful and the Job reports
    // running (the terminal half of the aggregation contract).
    assert_eq!(hub.reconcile_jobs().await.unwrap(), 1);
    let retry = hub
        .commands(AgentAuth {
            node_id: "compute-2".into(),
            session_token: node_b.session_token.clone(),
        })
        .await
        .unwrap()
        .into_iter()
        .find(|command| command.operation == "job_start")
        .expect("the timed-out peer receives a replacement start command");
    hub.command_result(
        AgentAuth {
            node_id: "compute-2".into(),
            session_token: node_b.session_token.clone(),
        },
        CommandResult {
            command_id: retry.id.clone(),
            operation_id: retry.operation_id.clone(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: retry.correlation_id,
            generation: retry.generation,
            observed_generation: Some(job.generation),
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
    let observed = hub.job("orders").await.unwrap().unwrap();
    assert_eq!(
        observed.observed_state, "running",
        "every planned assignment succeeded, so the Job reports running"
    );
    assert_eq!(observed.convergence, "converged");
}

/// Verification (repair-kernel-review-findings task 2.3): the terminal
/// failure half of the aggregation — once the complete assignment set is
/// evaluated and any assignment reports a permanent execution failure,
/// the Job reports failed even though a peer succeeded.
#[tokio::test]
async fn job_observed_state_reports_failed_when_a_peer_permanently_fails() {
    let storage = StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
    let hub = Hub::with_storage(config(), storage);
    let node_a = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "compute-1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    let node_b = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "compute-2".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    let job = hub
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: serde_json::json!({
                    "id": "orders",
                    "version": 1,
                    "max_parallelism": 1,
                    "parallelism": 1,
                    "operators": [
                        {"id": "source-a", "kind": "source"},
                        {"id": "sink-a", "kind": "sink"},
                        {"id": "source-b", "kind": "source"},
                        {"id": "sink-b", "kind": "sink"}
                    ],
                    "edges": [
                        {"id": "edge-a", "from": "source-a", "to": "sink-a"},
                        {"id": "edge-b", "from": "source-b", "to": "sink-b"}
                    ],
                    "sources": [
                        {"operator_id": "source-a", "input_type": "memory", "time": {"mode": "processing_time"}},
                        {"operator_id": "source-b", "input_type": "memory", "time": {"mode": "processing_time"}}
                    ],
                    "sinks": [
                        {"operator_id": "sink-a", "output_type": "drop"},
                        {"operator_id": "sink-b", "output_type": "drop"}
                    ]
                })
                .to_string(),
                desired_state: "running".into(),
                observed_state: "starting".into(),
                convergence: "reconciling".into(),
                generation: 1,
                node_ids: vec!["compute-1".into(), "compute-2".into()],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 0,
            })
            .await
            .unwrap();

    for (node_id, token, state, error) in [
        (
            "compute-1",
            node_a.session_token.clone(),
            HubOperationState::Succeeded,
            None,
        ),
        (
            "compute-2",
            node_b.session_token.clone(),
            HubOperationState::Failed,
            Some("runner process exited".into()),
        ),
    ] {
        let command = hub
            .commands(AgentAuth {
                node_id: node_id.into(),
                session_token: token.clone(),
            })
            .await
            .unwrap()
            .into_iter()
            .find(|command| command.operation == "job_start")
            .expect("each peer receives a start assignment");
        hub.command_result(
            AgentAuth {
                node_id: node_id.into(),
                session_token: token,
            },
            CommandResult {
                command_id: command.id.clone(),
                operation_id: command.operation_id.clone(),
                state,
                progress: 100,
                error,
                correlation_id: command.correlation_id,
                generation: command.generation,
                observed_generation: Some(job.generation),
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

    let observed = hub.job("orders").await.unwrap().unwrap();
    assert_eq!(
        observed.observed_state, "failed",
        "a complete set with a permanent failure aggregates to failed"
    );
}

#[tokio::test]
async fn periodic_job_reconciliation_retries_a_failed_runtime() {
    let storage = StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
    let hub = Hub::with_storage(config(), storage);
    let registration = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "compute-1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    let job = hub
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: serde_json::json!({
                    "id": "orders",
                    "version": 1,
                    "operators": [{"id": "source", "kind": "source"}, {"id": "sink", "kind": "sink"}],
                    "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
                    "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
                    "sinks": [{"operator_id": "sink", "output_type": "drop"}]
                })
                .to_string(),
                desired_state: "running".into(),
                observed_state: "starting".into(),
                convergence: "reconciling".into(),
                generation: 1,
                node_ids: vec!["compute-1".into()],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 0,
            })
            .await
            .unwrap();
    let first = hub
        .commands(AgentAuth {
            node_id: "compute-1".into(),
            session_token: registration.session_token.clone(),
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    hub.command_result(
        AgentAuth {
            node_id: "compute-1".into(),
            session_token: registration.session_token.clone(),
        },
        CommandResult {
            command_id: first.id.clone(),
            operation_id: first.operation_id.clone(),
            state: HubOperationState::Failed,
            progress: 100,
            error: Some("runner failed".into()),
            correlation_id: first.correlation_id,
            generation: job.generation,
            observed_generation: Some(job.generation),
            action_id: None,
            failure_class: Some("runtime".into()),
            config_version_id: None,
            rollout_id: None,
            observed_checkpoint_id: None,
            checkpoint_manifest_uri: None,
            result: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(
        hub.job("orders").await.unwrap().unwrap().observed_state,
        "failed"
    );
    assert_eq!(hub.reconcile_jobs().await.unwrap(), 1);
    let commands = hub
        .commands(AgentAuth {
            node_id: "compute-1".into(),
            session_token: registration.session_token,
        })
        .await
        .unwrap();
    assert!(commands.iter().any(|command| {
        command.operation == "job_start"
            && command.generation == job.generation
            && command.id != first.id
    }));
}

#[tokio::test]
async fn periodic_job_reconciliation_stops_persisted_divergence_after_recovery() {
    let storage = StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
    let hub1 = Hub::with_storage(config(), storage.clone());
    hub1.register(RegisterRequest {
        data_address: None,
        node_id: "compute-1".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec!["job_runtime".into(), "state_backend".into()],
        boot_id: None,
    })
    .await
    .unwrap();
    let job = hub1
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: serde_json::json!({
                    "id": "orders",
                    "version": 1,
                    "operators": [{"id": "source", "kind": "source"}, {"id": "sink", "kind": "sink"}],
                    "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
                    "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
                    "sinks": [{"operator_id": "sink", "output_type": "drop"}]
                })
                .to_string(),
                desired_state: "stopped".into(),
                observed_state: "running".into(),
                convergence: "reconciling".into(),
                generation: 3,
                node_ids: vec!["compute-1".into()],
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 0,
            })
            .await
            .unwrap();
    drop(hub1);

    let hub2 = Hub::with_storage(config(), storage);
    hub2.recover_persisted_state().await.unwrap();
    let registration = hub2
        .register(RegisterRequest {
            data_address: None,
            node_id: "compute-1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();

    assert_eq!(hub2.reconcile_jobs().await.unwrap(), 1);
    let commands = hub2
        .commands(AgentAuth {
            node_id: "compute-1".into(),
            session_token: registration.session_token,
        })
        .await
        .unwrap();
    assert!(commands.iter().any(|command| {
        command.operation == "job_stop"
            && command.resource_id == "orders"
            && command.generation == job.generation
    }));
}

// ===== Job operation audit + command metrics + expiry
// (add-hub-job-audit-and-command-metrics) =====

/// A storage-backed Hub with one online node and one Job whose spec
/// carries an injected `connection_string` so tests can prove audit
/// records never echo configuration bodies.
async fn audited_job_hub(secret_marker: &str) -> (Hub, crate::hub::RegisterResponse) {
    let storage = StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
    let hub = Hub::with_storage(config(), storage);
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "compute-1".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec!["job_runtime".into(), "state_backend".into()],
            boot_id: None,
        })
        .await
        .unwrap();
    let spec = serde_json::json!({
        "id": "orders",
        "version": 1,
        "operators": [
            {"id": "source", "kind": "source"},
            {"id": "sink", "kind": "sink"}
        ],
        "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
        "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
        "sinks": [{"operator_id": "sink", "output_type": "drop"}],
        "state": {"backend": "embedded_kv", "format_version": 3},
        "connection_string": format!("password={secret_marker}")
    });
    hub.upsert_job(JobRecord {
        job_id: "orders".into(),
        version: 1,
        spec_json: spec.to_string(),
        desired_state: "running".into(),
        observed_state: "stopped".into(),
        convergence: "reconciling".into(),
        generation: 1,
        node_ids: vec!["compute-1".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    (hub, session)
}

#[tokio::test]
async fn job_start_is_audited_without_configuration_bodies() {
    let (hub, _session) = audited_job_hub("super-secret-42").await;
    let audits = hub.audit(Some("orders")).await.unwrap();
    let starts: Vec<&crate::storage::AuditRecord> = audits
        .iter()
        .filter(|record| record.action == "job.start")
        .collect();
    assert_eq!(
        starts.len(),
        1,
        "the accepted start is audited exactly once"
    );
    assert_eq!(starts[0].resource_type, "job");
    assert_eq!(starts[0].outcome, "accepted");
    assert_eq!(starts[0].node_id.as_deref(), Some("compute-1"));
    // The audit trail identifies the operation without echoing the
    // configuration: the injected secret must not surface anywhere in
    // the record, and neither must the spec field that carried it.
    let rendered = format!(
        "{} {}",
        starts[0].message.as_deref().unwrap_or_default(),
        serde_json::to_string(starts[0]).unwrap()
    );
    assert!(!rendered.contains("super-secret-42"));
    assert!(!rendered.contains("connection_string"));
}

#[tokio::test]
async fn job_stop_rejection_for_unknown_node_is_audited() {
    let (hub, _session) = audited_job_hub("irrelevant").await;
    let error = hub
        .enqueue("ghost".into(), "job_stop".into(), "orders".into(), None)
        .await
        .unwrap_err();
    assert!(matches!(error, HubError::NodeUnavailable));
    let audits = hub.audit(Some("orders")).await.unwrap();
    assert!(audits.iter().any(|record| {
        record.action == "job.stop"
            && record.outcome == "rejected"
            && record.failure_code.as_deref() == Some("node_unavailable")
    }));
}

#[tokio::test]
async fn command_metrics_track_enqueues_latency_and_rejections() {
    let (hub, session) = audited_job_hub("irrelevant").await;
    // The start dispatch inside the setup counted one enqueued job_start.
    assert!(hub
        .command_metrics()
        .render()
        .contains("arkflow_command_total{command=\"job_start\",outcome=\"enqueued\"} 1"));
    // Acknowledgement records the enqueue→ack latency into the buckets.
    let command = hub
        .commands(AgentAuth {
            node_id: "compute-1".into(),
            session_token: session.session_token.clone(),
        })
        .await
        .unwrap()
        .into_iter()
        .find(|command| command.operation == "job_start")
        .expect("compute-1 receives the start command");
    hub.command_result(
        AgentAuth {
            node_id: "compute-1".into(),
            session_token: session.session_token.clone(),
        },
        CommandResult {
            command_id: command.id.clone(),
            operation_id: command.operation_id.clone(),
            state: HubOperationState::Acknowledged,
            progress: 10,
            error: None,
            correlation_id: command.correlation_id.clone(),
            generation: command.generation,
            observed_generation: None,
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
    let rendered = hub.command_metrics().render();
    assert!(
        rendered.contains("arkflow_command_duration_bucket{command=\"job_start\",le=\"+Inf\"} 1")
    );
    assert!(rendered.contains("arkflow_command_duration_count{command=\"job_start\"} 1"));
    assert!(rendered
        .contains("arkflow_command_total{command=\"job_start\",outcome=\"acknowledged\"} 1"));
    // A dispatch to an unknown node counts the fixed outcome class.
    let _ = hub
        .enqueue("ghost".into(), "job_stop".into(), "orders".into(), None)
        .await;
    assert!(hub
        .command_metrics()
        .render()
        .contains("arkflow_command_total{command=\"job_stop\",outcome=\"node_unavailable\"} 1"));
    // Dynamic operation names collapse into the `other` label so the
    // series space stays bounded by the fixed enumeration.
    let _ = hub
        .enqueue(
            "compute-1".into(),
            "exotic-operation".into(),
            "orders".into(),
            None,
        )
        .await
        .unwrap();
    assert!(hub
        .command_metrics()
        .render()
        .contains("arkflow_command_total{command=\"other\",outcome=\"enqueued\"} 1"));
}

#[tokio::test]
async fn expired_job_operations_retry_then_reach_the_terminal_cap() {
    let (hub, session) = audited_job_hub("irrelevant").await;
    let rewind_expiry = |operations: &mut BTreeMap<String, HubOperation>, id: &str| {
        if let Some(record) = operations.get_mut(id) {
            record.expires_at_ms = Some(1);
        }
    };
    let mut current_id = {
        let commands = hub
            .commands(AgentAuth {
                node_id: "compute-1".into(),
                session_token: session.session_token.clone(),
            })
            .await
            .unwrap();
        let start = commands
            .iter()
            .find(|command| command.operation == "job_start")
            .expect("the start command is queued");
        let operations = hub.operations.read().await;
        let queued = operations
            .values()
            .find(|record| record.command_id == start.id)
            .expect("the queued operation exists")
            .id
            .clone();
        drop(operations);
        {
            let mut operations = hub.operations.write().await;
            rewind_expiry(&mut operations, &queued);
        }
        queued
    };
    // Two expiry sweeps retry the operation (TimedOut, retry_count 1
    // then 2); each re-enqueue inherits the accumulated count.
    for expected_retry in [1, 2] {
        assert_eq!(hub.expire_stale_job_operations().await.unwrap(), 1);
        let operations = hub.operations.read().await;
        let expired = operations.get(&current_id).unwrap();
        assert_eq!(expired.state, HubOperationState::TimedOut);
        assert_eq!(expired.retry_count, expected_retry);
        assert_eq!(expired.failure_class.as_deref(), Some("expired"));
        drop(operations);
        // Re-enqueue at the SAME generation the expired start used (the
        // reconciler always passes job.generation) so the retry count
        // inheritance lookup matches.
        let replacement = hub
            .enqueue_with_metadata(
                "compute-1".into(),
                "job_start".into(),
                "orders".into(),
                None,
                None,
                1,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(replacement.retry_count, expected_retry);
        // The retry is reconciler mechanics, not a new mutation: the
        // audit trail must still hold exactly one accepted job.start.
        let starts = hub
            .audit(Some("orders"))
            .await
            .unwrap()
            .iter()
            .filter(|record| record.action == "job.start" && record.outcome == "accepted")
            .count();
        assert_eq!(
            starts, 1,
            "reconciler re-dispatches must not add audit rows"
        );
        current_id = replacement.id.clone();
        let mut operations = hub.operations.write().await;
        rewind_expiry(&mut operations, &current_id);
    }
    // The third expiry exhausts the budget: terminal failed/expired.
    assert_eq!(hub.expire_stale_job_operations().await.unwrap(), 1);
    {
        let operations = hub.operations.read().await;
        let failed = operations.get(&current_id).unwrap();
        assert_eq!(failed.state, HubOperationState::Failed);
        assert_eq!(failed.retry_count, 3);
        assert_eq!(failed.failure_class.as_deref(), Some("expired"));
    }
    // And the exhausted budget refuses further re-enqueue attempts.
    let error = hub
        .enqueue_with_metadata(
            "compute-1".into(),
            "job_start".into(),
            "orders".into(),
            None,
            None,
            1,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, HubError::Invalid(_)));
}

#[tokio::test]
async fn periodic_checkpoint_scheduling_writes_no_audit_rows() {
    // The periodic scheduler funnels through the same dispatch path as
    // operator triggers; only the latter is a mutation and may appear in
    // the audit trail.
    let storage = StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
    let hub = Hub::with_storage(config(), storage);
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "compute-1".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec!["job_runtime".into(), "state_backend".into()],
        boot_id: None,
    })
    .await
    .unwrap();
    hub.upsert_job(JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: serde_json::json!({
                "id": "orders",
                "version": 1,
                "operators": [
                    {"id": "source", "kind": "source"},
                    {"id": "sink", "kind": "sink"}
                ],
                "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
                "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
                "sinks": [{"operator_id": "sink", "output_type": "drop"}],
                "state": {"backend": "embedded_kv", "format_version": 3},
                "checkpoint": {"interval_ms": 1, "object_store_uri": "file:///tmp/checkpoints"}
            })
            .to_string(),
            desired_state: "running".into(),
            observed_state: "running".into(),
            convergence: "converged".into(),
            generation: 1,
            node_ids: vec!["compute-1".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 0,
        })
        .await
        .unwrap();
    // One scheduling round dispatches the auto checkpoint; the second is
    // deduplicated by the pending record's created_at gate.
    let scheduled = hub.schedule_periodic_checkpoints().await.unwrap();
    assert_eq!(scheduled, 1, "the auto checkpoint was scheduled");
    let audits = hub.audit(Some("orders")).await.unwrap();
    assert!(
        !audits
            .iter()
            .any(|record| record.action == "job.checkpoint"),
        "scheduler mechanics must not appear in the audit trail"
    );
    // The dispatch itself is still metriced like any command.
    assert!(hub
        .command_metrics()
        .render()
        .contains("command=\"job_checkpoint\""));
}

#[tokio::test]
async fn audit_history_prunes_old_records_but_keeps_recent() {
    let storage = StorageActor::start(crate::storage::ControlPlaneStore::in_memory().unwrap(), 8);
    let hub = Hub::with_storage(config(), storage);
    let now = now_ms() as i64;
    let day_ms = 24 * 60 * 60 * 1000;
    for (action, occurred_at_ms) in [
        ("job.start", now - 31 * day_ms),
        ("job.stop", now - 60 * 60 * 1000),
    ] {
        hub.record_audit_event(crate::storage::AuditRecord {
            event_id: 0,
            actor: Some("operator".into()),
            action: action.into(),
            resource_type: "job".into(),
            resource_id: Some("orders".into()),
            node_id: None,
            stream_id: None,
            correlation_id: None,
            outcome: "accepted".into(),
            failure_code: None,
            message: None,
            occurred_at_ms: occurred_at_ms as u64,
        })
        .await
        .unwrap();
    }
    hub.prune_audit_history().await.unwrap();
    let remaining = hub.audit(None).await.unwrap();
    assert_eq!(remaining.len(), 1, "only the recent record survives");
    assert_eq!(remaining[0].action, "job.stop");
}

// ---- HA lease election (hub-ha stage 2) ----
use axum::http::StatusCode;
use tower::ServiceExt;

fn ha_config(holder: &str, ttl_ms: u64) -> HubHaConfig {
    HubHaConfig {
        enabled: true,
        lease_ttl_ms: ttl_ms,
        holder_id: Some(holder.into()),
    }
}

fn ha_job_record(job_id: &str) -> JobRecord {
    let spec_json = serde_json::json!({
        "id": job_id,
        "version": 1,
        "operators": [
            {"id": "source", "kind": "source"},
            {"id": "sink", "kind": "sink"}
        ],
        "edges": [{"id": "source-sink", "from": "source", "to": "sink"}],
        "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
        "sinks": [{"operator_id": "sink", "output_type": "drop"}],
        "state": {"backend": "embedded_kv", "format_version": 3}
    })
    .to_string();
    JobRecord {
        job_id: job_id.into(),
        version: 1,
        spec_json,
        desired_state: "stopped".into(),
        observed_state: "draft".into(),
        convergence: "unknown".into(),
        generation: 1,
        node_ids: vec![],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 1,
    }
}

#[tokio::test]
async fn standby_gates_routes_and_writes_nothing() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub =
        Hub::with_storage(config(), storage.clone()).with_ha(ha_config("standby-hub", 60_000));
    hub.enter_election().await;
    assert!(matches!(hub.leadership().await, Leadership::Standby { .. }));
    assert!(!hub.is_leader().await);
    let app = crate::hub_router(hub.clone(), &crate::ServerConfig::default());

    // Liveness stays 200 through the gate; /health passes the gate too (its
    // own unrecovered-status semantics may still answer 503, but never with
    // the standby gate payload); readiness reports the standby role.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/liveness")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let health: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        health.get("code").is_none_or(|code| code != "hub_standby"),
        "/health must pass the standby gate"
    );
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::get("/readiness")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let readiness: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(readiness["reason"], "standby");
    assert_eq!(readiness["ha"]["role"], "standby");

    // Operator mutations are rejected before any handler runs.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/jobs")
                .header("authorization", "Bearer operator")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"spec":{}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(error["code"], "hub_standby");

    // Agent registration is refused: no node session may exist on a standby.
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
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        hub.nodes().await.is_empty(),
        "standby created a node record"
    );

    // Reads are refused too, and nothing was persisted.
    let response = app
        .oneshot(
            axum::http::Request::get("/api/v1/jobs")
                .header("authorization", "Bearer operator")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        storage.list_jobs().await.unwrap().is_empty(),
        "the rejected POST must not have persisted a job"
    );
}

#[tokio::test]
async fn disabled_ha_keeps_the_single_instance_surface() {
    let hub = Hub::new(config());
    hub.enter_election().await;
    assert_eq!(hub.leadership().await, Leadership::Disabled);
    assert!(hub.is_leader().await, "disabled HA gates nothing");
    let app = crate::hub_router(hub.clone(), &crate::ServerConfig::default());
    let response = app
        .oneshot(
            axum::http::Request::get("/readiness")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // No storage: readiness stays unavailable for its existing reason, but
    // the role is reported and the standby gate does not fire.
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn lease_failover_promotes_standby_and_recovers_durable_state() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub_a = Hub::with_storage(config(), storage.clone()).with_ha(ha_config("hub-a", 60_000));
    let hub_b = Hub::with_storage(config(), storage.clone()).with_ha(ha_config("hub-b", 60_000));
    hub_a.enter_election().await;
    hub_b.enter_election().await;

    // First tick wins the empty lease; the second hub stays standby.
    assert!(matches!(
        hub_a.run_election_tick().await,
        Leadership::Leader { epoch: 1, .. }
    ));
    assert!(matches!(
        hub_b.run_election_tick().await,
        Leadership::Standby { .. }
    ));

    // The leader persists state a standby must recover on takeover.
    hub_a.upsert_job(ha_job_record("ha-job")).await.unwrap();

    // Graceful shutdown releases the lease immediately; the standby takes
    // over on its next probe with a bumped epoch.
    hub_a.release_leadership().await;
    assert!(matches!(
        hub_a.leadership().await,
        Leadership::Standby { .. }
    ));
    assert!(matches!(
        hub_b.run_election_tick().await,
        Leadership::Leader { epoch: 2, .. }
    ));
    let jobs = hub_b.jobs().await.unwrap();
    assert!(
        jobs.iter().any(|job| job.job_id == "ha-job"),
        "the promoted hub must serve the recovered durable state"
    );

    // The demoted hub no longer renews: it observes the loss and stays down.
    assert!(matches!(
        hub_a.run_election_tick().await,
        Leadership::Standby { .. }
    ));

    // The promoted hub is ready over HTTP with the leader role.
    let app = crate::hub_router(hub_b, &crate::ServerConfig::default());
    let response = app
        .oneshot(
            axum::http::Request::get("/readiness")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let readiness: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(readiness["ha"]["role"], "leader");
    assert_eq!(readiness["ha"]["epoch"], 2);
}

#[tokio::test]
async fn expired_lease_is_taken_over_without_release() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub_a = Hub::with_storage(config(), storage.clone()).with_ha(ha_config("hub-a", 1_000));
    let hub_b = Hub::with_storage(config(), storage.clone()).with_ha(ha_config("hub-b", 60_000));
    hub_a.enter_election().await;
    hub_b.enter_election().await;
    assert!(matches!(
        hub_a.run_election_tick().await,
        Leadership::Leader { epoch: 1, .. }
    ));
    // The leader dies without releasing: after the TTL lapses the standby
    // takes over on its first probe (bounded by TTL + one probe).
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    assert!(matches!(
        hub_b.run_election_tick().await,
        Leadership::Leader { epoch: 2, .. }
    ));
}

#[tokio::test]
async fn promotion_replaces_stale_memory_from_the_previous_term() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub_a = Hub::with_storage(config(), storage.clone()).with_ha(ha_config("hub-a", 60_000));
    let hub_b = Hub::with_storage(config(), storage.clone()).with_ha(ha_config("hub-b", 60_000));
    hub_a.enter_election().await;
    hub_b.enter_election().await;
    assert!(matches!(
        hub_a.run_election_tick().await,
        Leadership::Leader { .. }
    ));
    hub_a
        .upsert_job(ha_job_record("durable-job"))
        .await
        .unwrap();
    hub_a.release_leadership().await;
    assert!(matches!(
        hub_b.run_election_tick().await,
        Leadership::Leader { .. }
    ));

    // While B leads, it advances durable state; A keeps a ghost entry in
    // memory from its previous term.
    hub_b
        .update_job("durable-job", Some("running"), None)
        .await
        .unwrap();
    hub_a
        .jobs
        .write()
        .await
        .insert("ghost-job".into(), ha_job_record("ghost-job"));

    // B hands back the lease; A re-promotes and must serve durable truth.
    hub_b.release_leadership().await;
    assert!(matches!(
        hub_a.run_election_tick().await,
        Leadership::Leader { .. }
    ));
    let jobs = hub_a.jobs().await.unwrap();
    assert!(
        !jobs.iter().any(|job| job.job_id == "ghost-job"),
        "stale in-memory entries from the previous term must be dropped"
    );
    let durable = jobs
        .iter()
        .find(|job| job.job_id == "durable-job")
        .expect("durable job recovered");
    assert_eq!(durable.desired_state, "running");
    // The node registry is rebuilt from scratch after promotion.
    assert!(hub_a.nodes().await.is_empty());
}

#[tokio::test]
async fn serve_hub_elects_leadership_and_flips_readiness() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let storage = crate::storage::StorageActor::start(store, 8);
    let hub = Hub::with_storage(config(), storage).with_ha(ha_config("serve-hub", 1_000));
    // Reserve an ephemeral port, then hand it to serve_hub.
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let cancellation = tokio_util::sync::CancellationToken::new();
    let server_config = crate::ServerConfig {
        address: format!("127.0.0.1:{port}"),
        insecure_local: true,
        ..crate::ServerConfig::default()
    };
    let shutdown = cancellation.clone();
    let hub_handle = hub.clone();
    tokio::spawn(async move {
        let _ = crate::serve_hub(hub_handle, server_config, shutdown).await;
    });
    // The election loop probes at ttl/3 (>= 1s): readiness must flip from
    // standby-503 to leader-200 within a bounded window.
    let client = reqwest::Client::new();
    let mut leader_ready = false;
    for _ in 0..50 {
        if let Ok(response) = client
            .get(format!("http://127.0.0.1:{port}/readiness"))
            .send()
            .await
        {
            if response.status().as_u16() == 200 {
                let body: serde_json::Value = response.json().await.unwrap();
                assert_eq!(body["ha"]["role"], "leader");
                assert!(body["ha"]["epoch"].as_u64().unwrap_or(0) >= 1);
                leader_ready = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(
        leader_ready,
        "serve_hub must promote the standby via the election loop"
    );
    assert!(matches!(hub.leadership().await, Leadership::Leader { .. }));
    assert!(hub.leadership_transitions() >= 1);
    let events = hub.events(None).await;
    assert!(
        events
            .iter()
            .any(|event| event.event.event_type == "hub.leadership"),
        "the promotion must appear in the event stream"
    );
    // Graceful shutdown releases the lease: another holder can immediately
    // take over with the next epoch.
    cancellation.cancel();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(matches!(hub.leadership().await, Leadership::Standby { .. }));
}

// ---------------------------------------------------------------------
// Atomic job-upgrade orchestration
// ---------------------------------------------------------------------

/// Deep-validatable durable spec: identical to `durable_job_spec_json` but
/// the aggregate operator carries a registered processor type so the
/// orchestration's deep-validation guard passes.
fn durable_atomic_spec_json(id: &str, version: u64) -> String {
    let mut spec = serde_json::from_str::<serde_json::Value>(&durable_job_spec_json(id)).unwrap();
    spec["version"] = serde_json::json!(version);
    spec["operators"][1]["config"] =
        serde_json::json!({"type": "batch", "count": 100, "timeout_ms": 1000});
    spec["checkpoint"]["object_store_uri"] = serde_json::json!(format!(
        "file:///tmp/arkflow-hub-upgrade-{}-{}",
        id,
        std::process::id()
    ));
    spec.to_string()
}

fn durable_spec_v2(id: &str) -> arkflow_core::job::JobSpec {
    serde_json::from_str(&durable_atomic_spec_json(id, 2)).unwrap()
}

async fn succeed_command(hub: &Hub, auth: &AgentAuth, operation: &str) -> Option<AgentCommand> {
    let commands = hub.commands(auth.clone()).await.unwrap();
    let command = commands
        .into_iter()
        .find(|command| command.operation == operation)?
        .clone();
    hub.command_result(
        auth.clone(),
        CommandResult {
            command_id: command.id.clone(),
            operation_id: command.operation_id.clone(),
            state: HubOperationState::Succeeded,
            progress: 100,
            error: None,
            correlation_id: command.correlation_id.clone(),
            generation: command.generation,
            observed_generation: Some(command.generation),
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
    Some(command)
}

async fn succeed_all_starts(hub: &Hub, auth: &AgentAuth) -> usize {
    let commands = hub.commands(auth.clone()).await.unwrap();
    let mut completed = 0;
    for command in commands
        .iter()
        .filter(|command| command.operation == "job_start")
    {
        hub.command_result(
            auth.clone(),
            CommandResult {
                command_id: command.id.clone(),
                operation_id: command.operation_id.clone(),
                state: HubOperationState::Succeeded,
                progress: 100,
                error: None,
                correlation_id: command.correlation_id.clone(),
                generation: command.generation,
                observed_generation: Some(command.generation),
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
        completed += 1;
    }
    completed
}

/// Register one node, create a durable running Job at version 1, and drive
/// its start to a succeeded, observed-running state.
async fn atomic_upgrade_fixture(job_id: &str) -> (Hub, AgentAuth) {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let hub = Hub::with_storage(config(), StorageActor::start(store, 8));
    let session = hub
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: Some("boot-a".into()),
        })
        .await
        .unwrap();
    let auth = AgentAuth {
        node_id: "node-a".into(),
        session_token: session.session_token.clone(),
    };
    hub.upsert_job(JobRecord {
        job_id: job_id.into(),
        version: 1,
        spec_json: durable_atomic_spec_json(job_id, 1),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec!["node-a".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    hub.reconcile_jobs().await.unwrap();
    succeed_command(&hub, &auth, "job_start").await;
    (hub, auth)
}

/// Complete a dispatched savepoint the way the real agent round would:
/// write a full artifact (per-task snapshots + sealed manifest) into the
/// Job's object store, then flip the record to completed through the
/// ordinary checkpoint-recording path (which moves the recovery pointer
/// while the Job version still matches — the property the commit relies
/// on).
async fn complete_savepoint(hub: &Hub, job_id: &str, savepoint_id: &str) {
    use arkflow_core::checkpoint::{
        CheckpointManifest, CheckpointRepository, FileCheckpointStore, RecoveryArtifactKind,
        TaskAttemptSnapshot,
    };
    let job = hub.job(job_id).await.unwrap().unwrap();
    let spec: arkflow_core::job::JobSpec = serde_json::from_str(&job.spec_json).unwrap();
    let plan = arkflow_core::job::JobPlan::compile(spec.clone()).unwrap();
    let root = std::path::PathBuf::from(
        spec.checkpoint
            .as_ref()
            .unwrap()
            .object_store_uri
            .trim_start_matches("file://"),
    );
    std::fs::create_dir_all(&root).unwrap();
    let repository = CheckpointRepository::new(FileCheckpointStore::new(&root).unwrap());
    let mut snapshots = Vec::new();
    let mut attempts = Vec::new();
    for task in &plan.tasks {
        let namespace = arkflow_core::job::effective_state_namespace(
            &plan.spec.id,
            plan.spec.state.as_ref(),
            &task.operator_id,
            &task.id,
        );
        let snapshot = arkflow_core::state::StateSnapshot::new(
            spec.state
                .as_ref()
                .map(|state| state.format_version)
                .unwrap_or(1),
            vec![arkflow_core::state::StateEntry {
                namespace,
                key: b"utf8:seed".to_vec(),
                value: b"v".to_vec(),
                expires_at_ms: None,
            }],
        );
        let mut reference = repository
            .write_state_snapshot(savepoint_id, &snapshot)
            .unwrap();
        reference.task_id = task.id.clone();
        snapshots.push(reference);
        attempts.push(TaskAttemptSnapshot {
            task_id: task.id.clone(),
            attempt_id: format!("{}:node-a:{}", task.id, job.generation),
            node_id: "node-a".into(),
        });
    }
    let mut manifest = CheckpointManifest {
        checkpoint_id: savepoint_id.into(),
        job_id: plan.spec.id.clone(),
        job_version: spec.version,
        generation: job.generation,
        task_attempts: attempts,
        source_positions: Vec::new(),
        watermarks_ms: Default::default(),
        watermark_partitions: Default::default(),
        in_flight_barrier: arkflow_core::checkpoint::CheckpointBarrier {
            checkpoint_id: savepoint_id.into(),
            generation: job.generation,
            trace_context: None,
        },
        state_snapshots: snapshots,
        format_version: spec
            .state
            .as_ref()
            .map(|state| state.format_version)
            .unwrap_or(1),
        checksum: 0,
    };
    manifest.seal();
    repository
        .write_manifest(
            &manifest,
            RecoveryArtifactKind::Savepoint,
            arkflow_core::checkpoint::recovery_manifest_key(
                RecoveryArtifactKind::Savepoint,
                savepoint_id,
            ),
        )
        .unwrap();

    let mut record = hub
        .job_checkpoints(job_id)
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.checkpoint_id == savepoint_id)
        .expect("savepoint record exists");
    record.status = "completed".into();
    record.updated_at_ms = crate::hub::now_ms_for_metrics();
    hub.record_job_checkpoint(record).await.unwrap();
}

async fn expire_phase_deadline(hub: &Hub, upgrade_id: &str) {
    let storage = hub.storage.as_ref().unwrap();
    let mut record = storage
        .get_job_upgrade(upgrade_id.to_owned())
        .await
        .unwrap()
        .unwrap();
    record.phase_deadline_at_ms = 1;
    storage.upsert_job_upgrade(record).await.unwrap();
}

#[tokio::test]
async fn atomic_upgrade_walks_savepoint_commit_and_verification_to_success() {
    let (hub, auth) = atomic_upgrade_fixture("job-atomic-walk").await;
    let mut spec = durable_spec_v2("job-atomic-walk");
    let record = hub
        .create_job_upgrade("job-atomic-walk", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    assert_eq!(record.phase, "saving_savepoint");
    // Fence: while the savepoint phase owns the Job, reconcile is a no-op.
    let job = hub.job("job-atomic-walk").await.unwrap().unwrap();
    assert_eq!(hub.reconcile_job(&job).await.unwrap(), 0);

    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    let savepoint_id = record.savepoint_id.expect("savepoint dispatched");
    let job = hub.job("job-atomic-walk").await.unwrap().unwrap();
    assert_eq!(job.checkpoint_id.as_deref(), Some(savepoint_id.as_str()));

    complete_savepoint(&hub, "job-atomic-walk", &savepoint_id).await;
    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    assert_eq!(record.phase, "verifying");
    let job = hub.job("job-atomic-walk").await.unwrap().unwrap();
    assert_eq!(job.version, 2);
    assert_eq!(job.desired_state, "running");
    // The pointer survives the commit: the fenced write never copies the
    // caller's read over the checkpoint path's pointer.
    assert_eq!(job.checkpoint_id.as_deref(), Some(savepoint_id.as_str()));
    assert!(hub
        .job_versions("job-atomic-walk")
        .await
        .unwrap()
        .iter()
        .any(|version| version.version == 2));

    // Verification falls through the fence: normal reconciliation starts the
    // new generation; its success observation completes the orchestration.
    hub.reconcile_jobs().await.unwrap();
    succeed_command(&hub, &auth, "job_start").await;
    let job = hub.job("job-atomic-walk").await.unwrap().unwrap();
    assert_eq!((job.version, job.observed_state.as_str()), (2, "running"));
    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    assert_eq!(record.phase, "succeeded");
}

#[tokio::test]
async fn atomic_upgrade_savepoint_failures_abort_without_touching_the_running_job() {
    let (hub, _auth) = atomic_upgrade_fixture("job-atomic-fail").await;
    let mut spec = durable_spec_v2("job-atomic-fail");
    let record = hub
        .create_job_upgrade("job-atomic-fail", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    let before = hub.job("job-atomic-fail").await.unwrap().unwrap();
    // Each failed round consumes two ticks: one observes the failure and
    // clears the in-flight reference, the next dispatches a fresh round.
    let mut failures = 0;
    for _ in 0..12 {
        hub.reconcile_job_upgrades().await.unwrap();
        let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
        if record.phase == "aborted" {
            break;
        }
        if let Some(savepoint_id) = record.savepoint_id.clone() {
            let mut checkpoint = hub
                .job_checkpoints("job-atomic-fail")
                .await
                .unwrap()
                .into_iter()
                .find(|checkpoint| checkpoint.checkpoint_id == savepoint_id)
                .unwrap();
            checkpoint.status = "failed".into();
            hub.record_job_checkpoint(checkpoint).await.unwrap();
            failures += 1;
        }
    }
    assert_eq!(failures, 3, "exactly the retry bound of rounds failed");
    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    assert_eq!(record.phase, "aborted");
    let after = hub.job("job-atomic-fail").await.unwrap().unwrap();
    assert_eq!(after.version, before.version);
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.desired_state, "running");
    assert_eq!(after.observed_state, "running");
}

#[tokio::test]
async fn commit_conflict_is_interpreted_against_observable_state() {
    // (a) An already-applied commit (crash between write and phase update)
    // advances to verification without a second write.
    let (hub, _auth) = atomic_upgrade_fixture("job-atomic-cas").await;
    let mut spec = durable_spec_v2("job-atomic-cas");
    let record = hub
        .create_job_upgrade("job-atomic-cas", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    let savepoint_id = record.savepoint_id.unwrap();
    complete_savepoint(&hub, "job-atomic-cas", &savepoint_id).await;
    // Pre-apply exactly the write the crashed orchestrator had made.
    let current = hub.job("job-atomic-cas").await.unwrap().unwrap();
    hub.update_job_with_expected_generation(
        JobRecord {
            version: 2,
            spec_json: durable_atomic_spec_json("job-atomic-cas", 2),
            desired_state: "running".into(),
            observed_state: "stopped".into(),
            convergence: "pending_recovery".into(),
            generation: current.generation,
            ..current.clone()
        },
        current.generation,
    )
    .await
    .unwrap();
    let committed = hub.job("job-atomic-cas").await.unwrap().unwrap();
    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    assert_eq!(record.phase, "verifying");
    let after = hub.job("job-atomic-cas").await.unwrap().unwrap();
    assert_eq!(after.generation, committed.generation, "no second write");

    // (b) An unrelated concurrent change aborts and preserves the newer Job
    // state: someone else upgraded the Job to a version outside this
    // orchestration's range while its savepoint round was completing.
    let (hub, _auth) = atomic_upgrade_fixture("job-atomic-cas-b").await;
    let mut spec = durable_spec_v2("job-atomic-cas-b");
    let record = hub
        .create_job_upgrade("job-atomic-cas-b", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    let savepoint_id = record.savepoint_id.unwrap();
    complete_savepoint(&hub, "job-atomic-cas-b", &savepoint_id).await;
    let current = hub.job("job-atomic-cas-b").await.unwrap().unwrap();
    let mut external = durable_spec_v2("job-atomic-cas-b");
    external.version = arkflow_core::job::JobVersion(3);
    hub.update_job_with_expected_generation(
        JobRecord {
            version: 3,
            spec_json: serde_json::to_string(&external).unwrap(),
            ..current.clone()
        },
        current.generation,
    )
    .await
    .unwrap();
    let raced = hub.job("job-atomic-cas-b").await.unwrap().unwrap();
    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    assert_eq!(record.phase, "aborted");
    let after = hub.job("job-atomic-cas-b").await.unwrap().unwrap();
    assert_eq!(after.generation, raced.generation, "newer state preserved");
    assert_eq!(after.version, raced.version);
}

#[tokio::test]
async fn verification_deadline_rolls_back_and_rollback_failure_is_terminal() {
    // Success path: deadline in verification triggers a rollback that
    // restores the previous version from the same savepoint.
    let (hub, auth) = atomic_upgrade_fixture("job-atomic-rb").await;
    let mut spec = durable_spec_v2("job-atomic-rb");
    let record = hub
        .create_job_upgrade("job-atomic-rb", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    let savepoint_id = record.savepoint_id.unwrap();
    complete_savepoint(&hub, "job-atomic-rb", &savepoint_id).await;
    hub.reconcile_job_upgrades().await.unwrap();
    // Verification never converges (no start is completed): expire it.
    hub.reconcile_jobs().await.unwrap();
    expire_phase_deadline(&hub, &record.upgrade_id).await;
    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    assert_eq!(record.phase, "rolling_back");
    // The rollback write applies; the restored generation's start succeeds.
    // A stale verification-phase start may still be queued: complete every
    // job_start (stale generations are dropped by the result fence) so the
    // newest one's observation lands.
    hub.reconcile_job_upgrades().await.unwrap();
    hub.reconcile_jobs().await.unwrap();
    for _ in 0..3 {
        if succeed_all_starts(&hub, &auth).await == 0 {
            break;
        }
    }
    let job = hub.job("job-atomic-rb").await.unwrap().unwrap();
    assert_eq!((job.version, job.observed_state.as_str()), (1, "running"));
    assert_eq!(job.checkpoint_id.as_deref(), Some(savepoint_id.as_str()));
    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    assert_eq!(record.phase, "rolled_back");

    // Failure path: the rollback restore applies but never converges.
    let (hub, _auth) = atomic_upgrade_fixture("job-atomic-rbf").await;
    let mut spec = durable_spec_v2("job-atomic-rbf");
    let record = hub
        .create_job_upgrade("job-atomic-rbf", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    let savepoint_id = record.savepoint_id.unwrap();
    complete_savepoint(&hub, "job-atomic-rbf", &savepoint_id).await;
    hub.reconcile_job_upgrades().await.unwrap();
    hub.reconcile_jobs().await.unwrap();
    expire_phase_deadline(&hub, &record.upgrade_id).await;
    hub.reconcile_job_upgrades().await.unwrap();
    hub.reconcile_job_upgrades().await.unwrap();
    // Restore applied; expire the rollback verification.
    expire_phase_deadline(&hub, &record.upgrade_id).await;
    hub.reconcile_job_upgrades().await.unwrap();
    let record = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    assert_eq!(record.phase, "failed");
    let job = hub.job("job-atomic-rbf").await.unwrap().unwrap();
    assert_eq!(job.version, 1);
    assert_eq!(job.desired_state, "running");
    // The savepoint and the pointer stay intact for operator recovery.
    assert_eq!(job.checkpoint_id.as_deref(), Some(savepoint_id.as_str()));
    assert!(hub
        .job_checkpoints("job-atomic-rbf")
        .await
        .unwrap()
        .iter()
        .any(|checkpoint| checkpoint.checkpoint_id == savepoint_id));
}

#[tokio::test]
async fn orchestration_survives_hub_restart_and_keeps_the_fence() {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    let hub1 = Hub::with_storage(config(), StorageActor::start(store.clone(), 8));
    let session = hub1
        .register(RegisterRequest {
            data_address: None,
            node_id: "node-a".into(),
            node_token: "node-secret".into(),
            protocol_version: "v1".into(),
            capabilities: vec![],
            boot_id: Some("boot-a".into()),
        })
        .await
        .unwrap();
    let auth = AgentAuth {
        node_id: "node-a".into(),
        session_token: session.session_token.clone(),
    };
    hub1.upsert_job(JobRecord {
        job_id: "job-atomic-restart".into(),
        version: 1,
        spec_json: durable_atomic_spec_json("job-atomic-restart", 1),
        desired_state: "running".into(),
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec!["node-a".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    })
    .await
    .unwrap();
    hub1.reconcile_jobs().await.unwrap();
    succeed_command(&hub1, &auth, "job_start").await;
    let mut spec = durable_spec_v2("job-atomic-restart");
    let record = hub1
        .create_job_upgrade("job-atomic-restart", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    hub1.reconcile_job_upgrades().await.unwrap();
    let dispatched = hub1
        .job_upgrade(&record.upgrade_id)
        .await
        .unwrap()
        .unwrap()
        .savepoint_id
        .expect("savepoint dispatched before the restart");

    // "Restart": same store, fresh in-memory state.
    let hub2 = Hub::with_storage(config(), StorageActor::start(store, 8));
    hub2.recover_persisted_state().await.unwrap();
    // The fence survives: the reconciler cannot re-place the Job while the
    // savepoint round it references is still pending.
    let job = hub2.job("job-atomic-restart").await.unwrap().unwrap();
    let generation_before = job.generation;
    assert_eq!(hub2.reconcile_job(&job).await.unwrap(), 0);
    // The tick keeps waiting on the SAME savepoint round — no fresh dispatch.
    hub2.reconcile_job_upgrades().await.unwrap();
    let record = hub2.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    assert_eq!(record.savepoint_id.as_deref(), Some(dispatched.as_str()));
    let job = hub2.job("job-atomic-restart").await.unwrap().unwrap();
    assert_eq!(job.generation, generation_before);
    // Completing the round on the new Hub instance resumes the orchestration.
    complete_savepoint(&hub2, "job-atomic-restart", &dispatched).await;
    hub2.reconcile_job_upgrades().await.unwrap();
    let record = hub2.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    assert_eq!(record.phase, "verifying");
}

#[tokio::test]
async fn concurrent_orchestration_and_pause_resume_are_guarded() {
    let (hub, _auth) = atomic_upgrade_fixture("job-atomic-exclusive").await;
    let mut spec = durable_spec_v2("job-atomic-exclusive");
    let record = hub
        .create_job_upgrade("job-atomic-exclusive", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    let second = hub
        .create_job_upgrade(
            "job-atomic-exclusive",
            &mut durable_spec_v2("job-atomic-exclusive"),
            1,
            0,
            None,
            None,
        )
        .await;
    assert!(matches!(
        second,
        Err(crate::hub::HubError::OrchestrationInProgress)
    ));

    // Pause holds the fence; resume re-enters the phase.
    let paused = hub
        .act_job_upgrade(&record.upgrade_id, "pause", None, None)
        .await
        .unwrap();
    assert_eq!(paused.phase, "paused");
    let job = hub.job("job-atomic-exclusive").await.unwrap().unwrap();
    assert_eq!(
        hub.reconcile_job(&job).await.unwrap(),
        0,
        "pause still fences"
    );
    hub.reconcile_job_upgrades().await.unwrap();
    let still = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    assert_eq!(
        still.phase, "paused",
        "a paused orchestration does not tick"
    );
    let resumed = hub
        .act_job_upgrade(&record.upgrade_id, "resume", None, None)
        .await
        .unwrap();
    assert_eq!(resumed.phase, "saving_savepoint");
    assert!(
        hub.act_job_upgrade(&record.upgrade_id, "resume", None, None)
            .await
            .is_err(),
        "only a paused orchestration can resume"
    );

    // Cancel releases the Job and clears the exclusivity.
    let cancelled = hub
        .act_job_upgrade(&record.upgrade_id, "cancel", None, None)
        .await
        .unwrap();
    assert_eq!(cancelled.phase, "cancelled");
    assert!(hub
        .active_job_upgrade_for("job-atomic-exclusive")
        .await
        .is_none());
    let job = hub.job("job-atomic-exclusive").await.unwrap().unwrap();
    // Unfenced again: reconcile proceeds (dispatch is a no-op here because
    // the succeeded start still satisfies the desired state).
    hub.reconcile_job(&job).await.unwrap();
}

// ---------------------------------------------------------------------
// Orchestration verification: retention pin, events/audit, guards, HTTP
// ---------------------------------------------------------------------

/// Write a real completed artifact of either kind, exactly like a finished
/// agent round would leave it in the Job's object store.
async fn complete_artifact(hub: &Hub, job_id: &str, artifact_id: &str, kind: &str) {
    use arkflow_core::checkpoint::{
        CheckpointManifest, CheckpointRepository, FileCheckpointStore, RecoveryArtifactKind,
        TaskAttemptSnapshot,
    };
    let artifact_kind = if kind == "savepoint" {
        RecoveryArtifactKind::Savepoint
    } else {
        RecoveryArtifactKind::Checkpoint
    };
    let job = hub.job(job_id).await.unwrap().unwrap();
    let spec: arkflow_core::job::JobSpec = serde_json::from_str(&job.spec_json).unwrap();
    let plan = arkflow_core::job::JobPlan::compile(spec.clone()).unwrap();
    let root = std::path::PathBuf::from(
        spec.checkpoint
            .as_ref()
            .unwrap()
            .object_store_uri
            .trim_start_matches("file://"),
    );
    std::fs::create_dir_all(&root).unwrap();
    let repository = CheckpointRepository::new(FileCheckpointStore::new(&root).unwrap());
    let mut snapshots = Vec::new();
    let mut attempts = Vec::new();
    for task in &plan.tasks {
        let namespace = arkflow_core::job::effective_state_namespace(
            &plan.spec.id,
            plan.spec.state.as_ref(),
            &task.operator_id,
            &task.id,
        );
        let snapshot = arkflow_core::state::StateSnapshot::new(
            spec.state
                .as_ref()
                .map(|state| state.format_version)
                .unwrap_or(1),
            vec![arkflow_core::state::StateEntry {
                namespace,
                key: b"utf8:seed".to_vec(),
                value: b"v".to_vec(),
                expires_at_ms: None,
            }],
        );
        let mut reference = repository
            .write_state_snapshot(artifact_id, &snapshot)
            .unwrap();
        reference.task_id = task.id.clone();
        snapshots.push(reference);
        attempts.push(TaskAttemptSnapshot {
            task_id: task.id.clone(),
            attempt_id: format!("{}:node-a:{}", task.id, job.generation),
            node_id: "node-a".into(),
        });
    }
    let mut manifest = CheckpointManifest {
        checkpoint_id: artifact_id.into(),
        job_id: plan.spec.id.clone(),
        job_version: spec.version,
        generation: job.generation,
        task_attempts: attempts,
        source_positions: Vec::new(),
        watermarks_ms: Default::default(),
        watermark_partitions: Default::default(),
        in_flight_barrier: arkflow_core::checkpoint::CheckpointBarrier {
            checkpoint_id: artifact_id.into(),
            generation: job.generation,
            trace_context: None,
        },
        state_snapshots: snapshots,
        format_version: spec
            .state
            .as_ref()
            .map(|state| state.format_version)
            .unwrap_or(1),
        checksum: 0,
    };
    manifest.seal();
    repository
        .write_manifest(
            &manifest,
            artifact_kind,
            arkflow_core::checkpoint::recovery_manifest_key(artifact_kind, artifact_id),
        )
        .unwrap();
    // The deletion path removes the per-node intermediate manifests the real
    // agent round writes before aggregation; leave the same layout behind.
    use arkflow_core::checkpoint::CheckpointStore as _;
    FileCheckpointStore::new(&root)
        .unwrap()
        .put(
            &format!("{kind}s/{artifact_id}/manifests/node-a.json"),
            &serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();

    let now = crate::hub::now_ms_for_metrics();
    hub.record_job_checkpoint(crate::storage::JobCheckpointRecord {
        job_id: job_id.into(),
        job_version: job.version,
        checkpoint_id: artifact_id.into(),
        kind: kind.into(),
        status: "completed".into(),
        manifest_uri: None,
        format_version: spec
            .state
            .as_ref()
            .map(|state| state.format_version)
            .unwrap_or(1),
        created_at_ms: now,
        updated_at_ms: now,
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retention_pin_shields_orchestration_referenced_artifacts() {
    let (hub, _auth) = atomic_upgrade_fixture("job-atomic-pin").await;
    // Retention 1: without a pin, completing a newer checkpoint deletes the
    // older one. The orchestration references the older artifact.
    let mut spec_value =
        serde_json::from_str::<serde_json::Value>(&durable_atomic_spec_json("job-atomic-pin", 1))
            .unwrap();
    spec_value["checkpoint"]["retention"] = serde_json::json!(1);
    let job = hub.job("job-atomic-pin").await.unwrap().unwrap();
    hub.update_job_with_expected_generation(
        JobRecord {
            spec_json: spec_value.to_string(),
            ..job.clone()
        },
        job.generation,
    )
    .await
    .unwrap();

    complete_artifact(&hub, "job-atomic-pin", "checkpoint-old", "checkpoint").await;
    // Register a non-terminal orchestration referencing the old artifact.
    let now = crate::hub::now_ms_for_metrics();
    let upgrade = crate::storage::JobUpgradeRecord {
        upgrade_id: "job-upgrade-pin".into(),
        job_id: "job-atomic-pin".into(),
        from_version: 1,
        to_version: 2,
        phase: "verifying".into(),
        savepoint_id: Some("checkpoint-old".into()),
        target_spec_json: durable_atomic_spec_json("job-atomic-pin", 2),
        phase_deadline_at_ms: now + 600_000,
        savepoint_retries: 0,
        verify_timeout_ms: 0,
        actor: None,
        correlation_id: None,
        last_error: None,
        paused_from: None,
        created_at_ms: now,
        updated_at_ms: now,
    };
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job_upgrade(upgrade.clone())
        .await
        .unwrap();
    hub.job_upgrades
        .write()
        .await
        .insert(upgrade.upgrade_id.clone(), upgrade.clone());
    assert_eq!(
        hub.pinned_job_upgrade_savepoints("job-atomic-pin").await,
        vec!["checkpoint-old".to_owned()]
    );

    // A newer completed checkpoint triggers the retention sweep: the pinned
    // artifact survives it.
    complete_artifact(&hub, "job-atomic-pin", "checkpoint-new", "checkpoint").await;
    let ids = hub
        .job_checkpoints("job-atomic-pin")
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.checkpoint_id)
        .collect::<Vec<_>>();
    assert!(
        ids.contains(&"checkpoint-old".to_owned()),
        "pin held: {ids:?}"
    );

    // Terminal orchestration releases the pin; the next sweep reclaims it.
    let mut finished = upgrade.clone();
    finished.phase = "cancelled".into();
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job_upgrade(finished)
        .await
        .unwrap();
    hub.job_upgrades
        .write()
        .await
        .insert("job-upgrade-pin".to_owned(), upgrade_phase_cancelled());
    complete_artifact(&hub, "job-atomic-pin", "checkpoint-newest", "checkpoint").await;
    let ids = hub
        .job_checkpoints("job-atomic-pin")
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.checkpoint_id)
        .collect::<Vec<_>>();
    assert!(
        !ids.contains(&"checkpoint-old".to_owned()),
        "pin released: {ids:?}"
    );
}

fn upgrade_phase_cancelled() -> crate::storage::JobUpgradeRecord {
    let now = crate::hub::now_ms_for_metrics();
    crate::storage::JobUpgradeRecord {
        upgrade_id: "job-upgrade-pin".into(),
        job_id: "job-atomic-pin".into(),
        from_version: 1,
        to_version: 2,
        phase: "cancelled".into(),
        savepoint_id: None,
        target_spec_json: String::new(),
        phase_deadline_at_ms: now,
        savepoint_retries: 0,
        verify_timeout_ms: 0,
        actor: None,
        correlation_id: None,
        last_error: None,
        paused_from: None,
        created_at_ms: now,
        updated_at_ms: now,
    }
}

#[tokio::test]
async fn upgrade_lifecycle_broadcasts_events_and_audits_actions() {
    let (hub, _auth) = atomic_upgrade_fixture("job-atomic-events").await;
    let mut receiver = hub.subscribe();
    let mut spec = durable_spec_v2("job-atomic-events");
    let record = hub
        .create_job_upgrade("job-atomic-events", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    hub.act_job_upgrade(&record.upgrade_id, "cancel", None, None)
        .await
        .unwrap();

    let mut outcomes = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        if event.event.event_type == "job.upgrade" {
            outcomes.push(event.event.outcome.clone());
        }
    }
    assert!(outcomes.contains(&"initiated".to_owned()), "{outcomes:?}");
    assert!(outcomes.contains(&"cancelled".to_owned()), "{outcomes:?}");

    let actions = hub
        .storage
        .as_ref()
        .unwrap()
        .list_audit(Some("job-atomic-events".to_owned()))
        .await
        .unwrap()
        .into_iter()
        .filter(|record| record.action.starts_with("job.upgrade.atomic."))
        .map(|record| record.action)
        .collect::<Vec<_>>();
    assert!(
        actions.contains(&"job.upgrade.atomic.initiate".to_owned()),
        "{actions:?}"
    );
    assert!(
        actions.contains(&"job.upgrade.atomic.cancel".to_owned()),
        "{actions:?}"
    );
}

#[tokio::test]
async fn create_job_upgrade_rejects_stale_versions_and_format_changes() {
    let (hub, _auth) = atomic_upgrade_fixture("job-atomic-guards").await;
    // Same version: rejected.
    let same = serde_json::from_str::<arkflow_core::job::JobSpec>(&durable_atomic_spec_json(
        "job-atomic-guards",
        1,
    ))
    .unwrap();
    assert!(hub
        .create_job_upgrade("job-atomic-guards", &mut same.clone(), 1, 0, None, None)
        .await
        .is_err());
    // State-format change: the savepoint this orchestration would take could
    // never restore into the target version.
    let mut incompatible = serde_json::from_str::<serde_json::Value>(&durable_atomic_spec_json(
        "job-atomic-guards",
        2,
    ))
    .unwrap();
    incompatible["state"]["format_version"] = serde_json::json!(2);
    let mut incompatible: arkflow_core::job::JobSpec =
        serde_json::from_value(incompatible).unwrap();
    let error = hub
        .create_job_upgrade("job-atomic-guards", &mut incompatible, 1, 0, None, None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("incompatible"), "{error:?}");
}

#[tokio::test]
async fn atomic_upgrade_http_contract_202_and_conflicts() {
    use axum::body::Body;
    use tower::ServiceExt;

    let (hub, _auth) = atomic_upgrade_fixture("job-atomic-http").await;
    let job = hub.job("job-atomic-http").await.unwrap().unwrap();
    let router = crate::hub_router(hub, &crate::ServerConfig::default());
    let authorization = "Bearer operator".to_owned();

    let spec =
        serde_json::from_str::<serde_json::Value>(&durable_atomic_spec_json("job-atomic-http", 2))
            .unwrap();
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/api/v1/jobs/job-atomic-http/upgrades")
        .header("authorization", authorization.clone())
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "mode": "atomic",
                "spec": spec,
                "expected_generation": job.generation,
            })
            .to_string(),
        ))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::ACCEPTED);

    // A second upgrade and a desired-state change are both fenced.
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/api/v1/jobs/job-atomic-http/upgrades")
        .header("authorization", authorization.clone())
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "mode": "atomic",
                "spec": spec,
                "expected_generation": job.generation,
            })
            .to_string(),
        ))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);

    let request = axum::http::Request::builder()
        .method("PUT")
        .uri("/api/v1/jobs/job-atomic-http/desired-state")
        .header("authorization", authorization.clone())
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({"state": "stopped"}).to_string(),
        ))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["code"], "orchestration_in_progress");

    // Unknown orchestration ids are 404s, not action-rejected conflicts.
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/api/v1/jobs/job-atomic-http/upgrades/no-such-upgrade/actions")
        .header("authorization", &authorization)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({"action": "pause"}).to_string(),
        ))
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
}

//! Durable Job pause/resume state-machine tests.

use super::error::HubError;
use super::job_pause::{
    MAX_PAUSE_CAPTURE_RETRIES, PAUSE_CAPTURING, PAUSE_PAUSED, PAUSE_RESUMING, PAUSE_STOPPING,
};
use super::{default_session_ttl_ms, Hub, HubConfig};
use crate::storage::{ControlPlaneStore, JobRecord, StorageActor};

fn config() -> HubConfig {
    HubConfig {
        operator_token: Some("operator".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 10,
        session_ttl_ms: default_session_ttl_ms(),
        approval_required: false,
        approval_ttl_ms: crate::hub::default_approval_ttl_ms(),
    }
}

fn stateful_spec(id: &str, version: u64, restoration: &str) -> String {
    serde_json::json!({
        "id": id,
        "version": version,
        "operators": [
            {"id": "source", "kind": "source"},
            {
                "id": "aggregate",
                "kind": "aggregate",
                "stateful": true,
                "key_field": "key",
                "config": {"type": "batch", "count": 100, "timeout_ms": 1000}
            },
            {"id": "sink", "kind": "sink"}
        ],
        "edges": [
            {"id": "source-aggregate", "from": "source", "to": "aggregate"},
            {"id": "aggregate-sink", "from": "aggregate", "to": "sink"}
        ],
        "sources": [{"operator_id": "source", "input_type": "memory", "time": {"mode": "processing_time"}}],
        "sinks": [{"operator_id": "sink", "output_type": "drop"}],
        "state": {"backend": "embedded_kv", "durability": "durable", "format_version": 1},
        "checkpoint": {
            "interval_ms": 1000,
            "retention": 2,
            "object_store_uri": format!("file:///tmp/arkflow-pause-{}-{id}", std::process::id())
        },
        "recovery": restoration
    })
    .to_string()
}

async fn pause_fixture(name: &str) -> (Hub, StorageActor) {
    let store = ControlPlaneStore::contract(name).await;
    let storage = StorageActor::start(store, 8);
    let hub = Hub::with_storage(config(), storage.clone());
    (hub, storage)
}

async fn seed_running_job(storage: &StorageActor, job_id: &str, restoration: &str) {
    storage
        .upsert_job(JobRecord {
            job_id: job_id.into(),
            version: 1,
            spec_json: stateful_spec(job_id, 1, restoration),
            desired_state: "running".into(),
            observed_state: "running".into(),
            convergence: "converged".into(),
            generation: 1,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 1,
            pause_phase: None,
            pause_artifact_id: None,
            pause_prev_checkpoint_id: None,
            pause_retry_count: 0,
            paused_at_ms: None,
            paused_by: None,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn pause_captures_then_stops_and_resume_restores_from_the_capture() {
    let (hub, storage) = pause_fixture("pause_captures_then_stops_and_resume").await;
    seed_running_job(&storage, "orders", "latest_checkpoint").await;

    let begun = hub.pause_job("orders", "admin").await.unwrap();
    assert_eq!(begun.desired_state, "paused");
    assert_eq!(begun.pause_phase.as_deref(), Some(PAUSE_CAPTURING));
    assert_eq!(begun.generation, 1, "pausing must not bump the generation");
    let artifact = begun.pause_artifact_id.clone().unwrap();
    assert_eq!(begun.checkpoint_id.as_deref(), Some(artifact.as_str()));
    let records = hub.job_checkpoints("orders").await.unwrap();
    assert_eq!(
        records
            .iter()
            .find(|record| record.checkpoint_id == artifact)
            .map(|record| record.kind.as_str()),
        Some("checkpoint"),
        "the capture kind follows the Job's latest_checkpoint policy"
    );

    // The capture completing moves the pause into the stop phase.
    hub.complete_job_checkpoint("orders", &artifact, "completed", None)
        .await
        .unwrap();
    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 1);
    let stopping = hub.job("orders").await.unwrap().unwrap();
    assert_eq!(stopping.pause_phase.as_deref(), Some(PAUSE_STOPPING));
    assert_eq!(stopping.generation, 1);

    // The stop converging settles the pause.
    hub.observe_job("orders", 1, "stopped", None, None)
        .await
        .unwrap();
    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 1);
    let paused = hub.job("orders").await.unwrap().unwrap();
    assert_eq!(paused.pause_phase.as_deref(), Some(PAUSE_PAUSED));
    assert_eq!(paused.observed_state, "stopped");
    assert_eq!(paused.convergence, "converged");
    assert_eq!(paused.checkpoint_id.as_deref(), Some(artifact.as_str()));

    // Resume bumps the generation and keeps the pointer on the capture. The
    // resume *write* is exercised here; the recovery start's manifest
    // validation needs a real object-store artifact and is covered by the
    // stateless resume test below plus the placement recovery tests.
    let resume = crate::storage::JobPauseUpdate {
        expected_generation: paused.generation,
        expected_phase: Some(PAUSE_PAUSED.into()),
        desired_state: Some("running".into()),
        phase: Some(PAUSE_RESUMING.into()),
        bump_generation: true,
        ..crate::storage::JobPauseUpdate::default()
    };
    let resumed = hub
        .storage
        .as_ref()
        .unwrap()
        .update_job_pause("orders", resume)
        .await
        .unwrap()
        .unwrap();
    hub.jobs
        .write()
        .await
        .insert(resumed.job_id.clone(), resumed.clone());
    assert_eq!(resumed.desired_state, "running");
    assert_eq!(resumed.pause_phase.as_deref(), Some(PAUSE_RESUMING));
    assert_eq!(resumed.generation, 2);
    assert_eq!(resumed.checkpoint_id.as_deref(), Some(artifact.as_str()));

    // Convergence clears the pause bookkeeping without touching the pointer.
    hub.observe_job("orders", 2, "running", None, None)
        .await
        .unwrap();
    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 1);
    let cleared = hub.job("orders").await.unwrap().unwrap();
    assert_eq!(cleared.pause_phase, None);
    assert_eq!(cleared.pause_artifact_id, None);
    assert_eq!(cleared.paused_by, None);
    assert_eq!(cleared.desired_state, "running");
    assert_eq!(cleared.checkpoint_id.as_deref(), Some(artifact.as_str()));

    // Pause outcomes are exported as a closed-label counter.
    let metrics = hub.command_metrics().render();
    assert!(
        metrics.contains("arkflow_job_pause_total{outcome=\"requested\"}"),
        "metrics missing requested: {metrics}"
    );
    assert!(
        metrics.contains("arkflow_job_pause_total{outcome=\"paused\"}"),
        "metrics missing paused: {metrics}"
    );

    // Audit trail: accepted pause, accepted resume.
    let audit = hub.audit(Some("orders")).await.unwrap();
    assert!(audit
        .iter()
        .any(|record| record.action == "job.pause" && record.outcome == "accepted"));
    assert!(audit
        .iter()
        .any(|record| record.action == "job.resume" && record.outcome == "accepted"));
}

#[tokio::test]
async fn savepoint_policy_records_a_savepoint_capture() {
    let (hub, storage) = pause_fixture("savepoint_policy_records_a_savepoint_capture").await;
    seed_running_job(&storage, "orders", "latest_savepoint").await;
    let begun = hub.pause_job("orders", "admin").await.unwrap();
    let artifact = begun.pause_artifact_id.clone().unwrap();
    let records = hub.job_checkpoints("orders").await.unwrap();
    let record = records
        .iter()
        .find(|record| record.checkpoint_id == artifact)
        .unwrap();
    assert_eq!(record.kind, "savepoint");
    hub.complete_job_checkpoint("orders", &artifact, "completed", None)
        .await
        .unwrap();
    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 1);
    assert_eq!(
        hub.job("orders")
            .await
            .unwrap()
            .unwrap()
            .pause_phase
            .as_deref(),
        Some(PAUSE_STOPPING)
    );
}

#[tokio::test]
async fn failed_capture_aborts_the_pause_and_restores_the_previous_pointer() {
    let (hub, storage) = pause_fixture("failed_capture_aborts_the_pause").await;
    seed_running_job(&storage, "orders", "latest_checkpoint").await;
    // The Job had a recovery pointer before the pause (pointer writes never
    // bump the generation: that belongs to the desired-state path).
    hub.storage
        .as_ref()
        .unwrap()
        .update_job(
            "orders",
            None,
            None,
            None,
            None,
            Some("checkpoint-prev".to_string()),
            None,
        )
        .await
        .unwrap();
    // The seeded checkpoint is a completed checkpoint of this version.
    hub.complete_job_checkpoint("orders", "checkpoint-prev", "completed", None)
        .await
        .unwrap();

    let begun = hub.pause_job("orders", "admin").await.unwrap();
    let artifact = begun.pause_artifact_id.clone().unwrap();
    assert_eq!(
        begun.pause_prev_checkpoint_id.as_deref(),
        Some("checkpoint-prev")
    );
    hub.complete_job_checkpoint("orders", &artifact, "failed", None)
        .await
        .unwrap();
    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 1);
    let aborted = hub.job("orders").await.unwrap().unwrap();
    assert_eq!(aborted.desired_state, "running");
    assert_eq!(aborted.pause_phase, None);
    assert_eq!(
        aborted.checkpoint_id.as_deref(),
        Some("checkpoint-prev"),
        "the failed capture must not stay the recovery pointer"
    );
    assert_eq!(aborted.generation, 1);
    assert!(aborted
        .last_error
        .as_deref()
        .is_some_and(|error| error.starts_with("pause_savepoint")));
}

fn stateless_spec(id: &str) -> String {
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
async fn stateless_pause_stops_directly_and_resume_restores_running() {
    let (hub, storage) = pause_fixture("stateless_pause_stops_directly_and_resume").await;
    storage
        .upsert_job(JobRecord {
            job_id: "plain".into(),
            version: 1,
            spec_json: stateless_spec("plain"),
            desired_state: "running".into(),
            observed_state: "running".into(),
            convergence: "converged".into(),
            generation: 1,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 1,
            pause_phase: None,
            pause_artifact_id: None,
            pause_prev_checkpoint_id: None,
            pause_retry_count: 0,
            paused_at_ms: None,
            paused_by: None,
        })
        .await
        .unwrap();

    let begun = hub.pause_job("plain", "admin").await.unwrap();
    assert_eq!(begun.desired_state, "paused");
    assert_eq!(
        begun.pause_phase.as_deref(),
        Some(PAUSE_STOPPING),
        "a Job without checkpoint configuration stops without a capture"
    );
    assert_eq!(begun.pause_artifact_id, None);
    assert_eq!(begun.generation, 1);

    hub.observe_job("plain", 1, "stopped", None, None)
        .await
        .unwrap();
    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 1);
    let paused = hub.job("plain").await.unwrap().unwrap();
    assert_eq!(paused.pause_phase.as_deref(), Some(PAUSE_PAUSED));
    assert_eq!(paused.convergence, "converged");

    // Resume through the public Hub path: stateless Jobs need no recovery
    // artifact, so reconciliation accepts the start immediately.
    let resumed = hub.resume_job("plain", "admin").await.unwrap();
    assert_eq!(resumed.desired_state, "running");
    assert_eq!(resumed.pause_phase.as_deref(), Some(PAUSE_RESUMING));
    assert_eq!(resumed.generation, 2);

    hub.observe_job("plain", 2, "running", None, None)
        .await
        .unwrap();
    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 1);
    let cleared = hub.job("plain").await.unwrap().unwrap();
    assert_eq!(cleared.pause_phase, None);
    assert_eq!(cleared.desired_state, "running");

    // Stopping a paused Job clears the pause bookkeeping.
    hub.observe_job("plain", 2, "running", None, None)
        .await
        .unwrap();
    let stopped = hub.stop_paused_job("plain", "admin").await;
    // `stop_paused_job` needs a pause phase; after convergence it is a plain
    // desired-state stop. Assert the explicit rejection instead.
    assert!(matches!(
        stopped,
        Err(super::error::HubError::PauseNotPaused)
    ));
}

#[tokio::test]
async fn pause_preconditions_are_explicit() {
    let (hub, storage) = pause_fixture("pause_preconditions_are_explicit").await;
    seed_running_job(&storage, "orders", "latest_checkpoint").await;

    // Already running but not converged.
    hub.storage
        .as_ref()
        .unwrap()
        .update_job(
            "orders",
            None,
            None,
            Some("reconciling".to_string()),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        hub.pause_job("orders", "admin").await,
        Err(super::error::HubError::JobNotRunning)
    ));

    // Recovery policy `fail` cannot resume, so it cannot pause either.
    seed_running_job(&storage, "no-recovery", "fail").await;
    assert!(matches!(
        hub.pause_job("no-recovery", "admin").await,
        Err(super::error::HubError::PauseRequiresRecoveryPolicy)
    ));

    // Restore convergence, then a second pause while one is in flight is
    // rejected.
    hub.storage
        .as_ref()
        .unwrap()
        .update_job(
            "orders",
            None,
            None,
            Some("converged".to_string()),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    hub.pause_job("orders", "admin").await.unwrap();
    assert!(matches!(
        hub.pause_job("orders", "admin").await,
        Err(super::error::HubError::PauseInProgress)
    ));

    // Resume requires the paused phase.
    assert!(matches!(
        hub.resume_job("orders", "admin").await,
        Err(super::error::HubError::PauseInProgress)
    ));
    assert!(matches!(
        hub.resume_job("no-recovery", "admin").await,
        Err(super::error::HubError::PauseNotPaused)
    ));

    // A Hub without durable storage cannot pause (durable semantics).
    let volatile = Hub::new(config());
    seed_running_job(&storage, "volatile", "latest_checkpoint").await;
    assert!(matches!(
        volatile.pause_job("volatile", "admin").await,
        Err(super::error::HubError::PauseRequiresDurableStorage)
    ));
}

#[tokio::test]
async fn capture_retry_budget_is_bounded() {
    let (hub, storage) = pause_fixture("capture_retry_budget_is_bounded").await;
    seed_running_job(&storage, "orders", "latest_checkpoint").await;
    let begun = hub.pause_job("orders", "admin").await.unwrap();
    let first_artifact = begun.pause_artifact_id.clone().unwrap();
    // Simulate a Hub restart that settled the capture command without a
    // completed artifact: no in-flight operation remains. Force the capture
    // past its retry budget through the pause CAS (never the desired-state
    // path, which would bump the generation).
    hub.operations.write().await.clear();
    let current = hub.job("orders").await.unwrap().unwrap();
    let update = crate::storage::JobPauseUpdate {
        expected_generation: current.generation,
        expected_phase: Some(PAUSE_CAPTURING.into()),
        phase: Some(PAUSE_CAPTURING.into()),
        retry_count: Some(MAX_PAUSE_CAPTURE_RETRIES),
        paused_at_ms: Some(0),
        ..crate::storage::JobPauseUpdate::default()
    };
    hub.storage
        .as_ref()
        .unwrap()
        .update_job_pause("orders", update)
        .await
        .unwrap();
    let _ = first_artifact;
    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 1);
    let aborted = hub.job("orders").await.unwrap().unwrap();
    assert_eq!(aborted.pause_phase, None);
    assert_eq!(aborted.desired_state, "running");
    assert!(aborted
        .last_error
        .as_deref()
        .is_some_and(|error| error.starts_with("pause_savepoint")));
}

/// Pause a seeded stateful Job, seal its capture, and advance the machine to
/// the stop phase. Returns the capture artifact id.
async fn pause_to_stopping(hub: &Hub, job_id: &str) -> String {
    let begun = hub.pause_job(job_id, "admin").await.unwrap();
    assert_eq!(begun.pause_phase.as_deref(), Some(PAUSE_CAPTURING));
    let artifact = begun
        .pause_artifact_id
        .clone()
        .expect("a stateful pause captures an artifact");
    hub.complete_job_checkpoint(job_id, &artifact, "completed", None)
        .await
        .unwrap();
    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 1);
    let stopping = hub.job(job_id).await.unwrap().unwrap();
    assert_eq!(stopping.pause_phase.as_deref(), Some(PAUSE_STOPPING));
    artifact
}

#[tokio::test]
async fn hub_restart_reissues_the_capture_without_bumping_the_generation() {
    let (hub, storage) = pause_fixture("hub_restart_reissues_the_capture").await;
    seed_running_job(&storage, "orders", "latest_checkpoint").await;

    let begun = hub.pause_job("orders", "admin").await.unwrap();
    let first_artifact = begun.pause_artifact_id.clone().unwrap();
    assert_eq!(begun.pause_phase.as_deref(), Some(PAUSE_CAPTURING));
    assert_eq!(begun.pause_retry_count, 0);
    let before = hub.job_checkpoints("orders").await.unwrap();
    assert_eq!(
        before.len(),
        1,
        "the pause begins exactly one capture round"
    );
    assert_eq!(before[0].checkpoint_id, first_artifact);
    assert_eq!(before[0].status, "pending");
    assert_eq!(before[0].kind, "checkpoint");

    // A Hub restart loses the in-memory operation table: the in-flight
    // capture command is settled as timed out, so no round is left in flight
    // and the restarted Hub must re-issue one.
    hub.operations.write().await.clear();
    // The fresh artifact id encodes the wall clock; keep it distinct from the
    // pre-restart round's id.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let restarted = Hub::with_storage(config(), storage.clone());
    assert_eq!(
        restarted.jobs().await.unwrap().len(),
        1,
        "the restarted Hub reloads the Job from durable storage"
    );
    let mut updates = restarted.subscribe();

    assert_eq!(restarted.reconcile_job_pauses().await.unwrap(), 1);
    let retried = restarted.job("orders").await.unwrap().unwrap();
    assert_eq!(
        retried.pause_phase.as_deref(),
        Some(PAUSE_CAPTURING),
        "a re-dispatched capture keeps the pause in the capturing phase"
    );
    assert_eq!(retried.pause_retry_count, 1);
    assert_eq!(retried.desired_state, "paused");
    assert_eq!(
        retried.generation, 1,
        "capture retries never bump the generation"
    );
    assert_ne!(
        retried.pause_artifact_id.as_deref(),
        Some(first_artifact.as_str()),
        "the retry records a fresh artifact"
    );
    assert_eq!(
        retried.checkpoint_id, retried.pause_artifact_id,
        "the recovery pointer follows the fresh capture artifact"
    );

    let after = restarted.job_checkpoints("orders").await.unwrap();
    assert_eq!(after.len(), 2, "the retry records a new capture round");
    let stale = after
        .iter()
        .find(|record| record.checkpoint_id == first_artifact)
        .expect("the pre-restart capture round is retained");
    assert_eq!(stale.status, "pending");
    let fresh = after
        .iter()
        .find(|record| Some(&record.checkpoint_id) == retried.pause_artifact_id.as_ref())
        .expect("the retried capture round is recorded");
    assert_eq!(fresh.status, "pending");
    assert_eq!(fresh.kind, "checkpoint");
    assert_eq!(fresh.job_version, 1);

    let retry_event = updates
        .try_recv()
        .expect("the re-dispatched capture is broadcast to observers");
    assert_eq!(retry_event.event.event_type, "job.pause_retrying");
    assert!(
        retry_event
            .event
            .message
            .as_deref()
            .is_some_and(|message| message.contains("1/3")),
        "the retry event reports the bounded budget: {:?}",
        retry_event.event.message
    );

    // Recovery is not failure: the pause was retried, never aborted.
    let audit = restarted.audit(Some("orders")).await.unwrap();
    assert_eq!(
        audit
            .iter()
            .filter(|record| record.action == "job.pause" && record.outcome == "accepted")
            .count(),
        1
    );
    assert!(!audit
        .iter()
        .any(|record| record.action == "job.pause" && record.outcome == "failed"));
    assert!(!restarted
        .command_metrics()
        .render()
        .contains("outcome=\"failed\""));
}

#[tokio::test]
async fn offline_nodes_keep_the_pause_reconciling_until_the_stop_is_observed() {
    let (hub, storage) = pause_fixture("offline_nodes_keep_the_pause_reconciling").await;
    seed_running_job(&storage, "orders", "latest_checkpoint").await;
    let artifact = pause_to_stopping(&hub, "orders").await;

    let stopping = hub.job("orders").await.unwrap().unwrap();
    assert_eq!(stopping.convergence, "reconciling");
    assert_eq!(stopping.desired_state, "paused");
    assert_eq!(stopping.observed_state, "running");
    assert_eq!(stopping.generation, 1);
    assert_eq!(stopping.pause_retry_count, 0);

    // The fleet is offline: no node ever reports the stop. Repeated ticks must
    // keep the pause reconciling instead of claiming convergence on their own.
    for _ in 0..3 {
        assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 0);
        let job = hub.job("orders").await.unwrap().unwrap();
        assert_eq!(job.pause_phase.as_deref(), Some(PAUSE_STOPPING));
        assert_eq!(job.convergence, "reconciling");
        assert_eq!(job.generation, 1);
        assert_eq!(job.desired_state, "paused");
        assert_eq!(job.pause_retry_count, 0, "stop re-dispatch is not a retry");
    }
    let records = hub.job_checkpoints("orders").await.unwrap();
    assert_eq!(records.len(), 1, "no extra capture round is issued");
    assert_eq!(records[0].checkpoint_id, artifact);
    assert_eq!(records[0].status, "completed");
    assert!(!hub
        .command_metrics()
        .render()
        .contains("arkflow_job_pause_total{outcome=\"paused\"}"));

    // The Agent returns and reports the stop at the pause generation; the
    // observation alone does not converge a stopping pause.
    let observed = hub
        .observe_job("orders", 1, "stopped", None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.convergence, "reconciling");

    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 1);
    let paused = hub.job("orders").await.unwrap().unwrap();
    assert_eq!(paused.pause_phase.as_deref(), Some(PAUSE_PAUSED));
    assert_eq!(paused.convergence, "converged");
    assert_eq!(paused.observed_state, "stopped");
    assert_eq!(paused.desired_state, "paused");
    assert_eq!(paused.generation, 1);
    assert_eq!(paused.checkpoint_id.as_deref(), Some(artifact.as_str()));

    // A settled pause is inert on later ticks.
    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 0);
    assert!(hub
        .command_metrics()
        .render()
        .contains("arkflow_job_pause_total{outcome=\"paused\"} 1"));
}

#[tokio::test]
async fn stopping_a_pausing_job_wins_the_race_and_clears_the_pause() {
    let (hub, storage) = pause_fixture("stopping_a_pausing_job_wins_the_race").await;
    seed_running_job(&storage, "orders", "latest_checkpoint").await;
    let begun = hub.pause_job("orders", "admin").await.unwrap();
    let artifact = begun.pause_artifact_id.clone().unwrap();
    assert_eq!(begun.pause_phase.as_deref(), Some(PAUSE_CAPTURING));

    // The operator stops the Job while the capture round is still pending.
    let stopped = hub.stop_paused_job("orders", "admin").await.unwrap();
    assert_eq!(stopped.desired_state, "stopped");
    assert_eq!(stopped.pause_phase, None);
    assert_eq!(stopped.pause_artifact_id, None);
    assert_eq!(stopped.pause_prev_checkpoint_id, None);
    assert_eq!(stopped.pause_retry_count, 0);
    assert_eq!(stopped.paused_by, None);
    assert_eq!(
        stopped.generation, 2,
        "the operator stop fences the pause with a generation bump"
    );
    assert_eq!(stopped.convergence, "reconciling");
    assert_eq!(
        stopped.checkpoint_id, None,
        "stopping during the capture restores the pre-pause pointer (none here)          instead of pinning recovery to an artifact that never completed"
    );

    // The pause machine must not advance a Job it no longer owns.
    for _ in 0..3 {
        assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 0);
        let job = hub.job("orders").await.unwrap().unwrap();
        assert_eq!(job.pause_phase, None);
        assert_eq!(job.desired_state, "stopped");
        assert_eq!(job.generation, 2);
        assert_eq!(job.pause_artifact_id, None);
    }
    // The abandoned capture round is deleted with the pause: a pending record
    // would otherwise pin the recovery pointer to an artifact nothing will
    // ever seal.
    let records = hub.job_checkpoints("orders").await.unwrap();
    assert!(
        records
            .iter()
            .all(|record| record.checkpoint_id != artifact),
        "the abandoned pending capture must be removed: {records:?}"
    );
    assert!(!hub
        .command_metrics()
        .render()
        .contains("arkflow_job_pause_total{outcome=\"paused\"}"));

    // The stopped Job is no longer pausable and no longer resumable: the
    // operator has to start it first.
    assert!(matches!(
        hub.pause_job("orders", "admin").await,
        Err(HubError::JobNotRunning)
    ));
    assert!(matches!(
        hub.resume_job("orders", "admin").await,
        Err(HubError::PauseNotPaused)
    ));

    let audit = hub.audit(Some("orders")).await.unwrap();
    assert_eq!(
        audit
            .iter()
            .filter(|record| record.action == "job.pause" && record.outcome == "accepted")
            .count(),
        1,
        "the winning operator stop leaves only the accepted pause begin"
    );
    assert_eq!(
        audit
            .iter()
            .filter(|record| record.action == "job.stop" && record.outcome == "accepted")
            .count(),
        1
    );
}

#[tokio::test]
async fn pause_and_resume_exclude_each_other_across_phases() {
    let (hub, storage) = pause_fixture("pause_and_resume_exclude_each_other").await;
    seed_running_job(&storage, "orders", "latest_checkpoint").await;

    // Capturing: neither a second pause nor a resume may interleave.
    let begun = hub.pause_job("orders", "admin").await.unwrap();
    let artifact = begun.pause_artifact_id.clone().unwrap();
    assert!(matches!(
        hub.pause_job("orders", "admin").await,
        Err(HubError::PauseInProgress)
    ));
    assert!(matches!(
        hub.resume_job("orders", "admin").await,
        Err(HubError::PauseInProgress)
    ));

    // Stopping: a resume still has to wait for the stop to converge.
    hub.complete_job_checkpoint("orders", &artifact, "completed", None)
        .await
        .unwrap();
    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 1);
    assert_eq!(
        hub.job("orders")
            .await
            .unwrap()
            .unwrap()
            .pause_phase
            .as_deref(),
        Some(PAUSE_STOPPING)
    );
    assert!(matches!(
        hub.resume_job("orders", "admin").await,
        Err(HubError::PauseInProgress)
    ));

    // Paused: the pause is terminal until it is resumed (or stopped).
    hub.observe_job("orders", 1, "stopped", None, None)
        .await
        .unwrap();
    assert_eq!(hub.reconcile_job_pauses().await.unwrap(), 1);
    assert_eq!(
        hub.job("orders")
            .await
            .unwrap()
            .unwrap()
            .pause_phase
            .as_deref(),
        Some(PAUSE_PAUSED)
    );
    assert!(matches!(
        hub.pause_job("orders", "admin").await,
        Err(HubError::JobAlreadyPaused)
    ));

    // Stopping the paused Job ends the pause; a resume may not follow it.
    let stopped = hub.stop_paused_job("orders", "admin").await.unwrap();
    assert_eq!(stopped.desired_state, "stopped");
    assert_eq!(stopped.pause_phase, None);
    assert_eq!(stopped.generation, 2);
    assert!(matches!(
        hub.resume_job("orders", "admin").await,
        Err(HubError::PauseNotPaused)
    ));
    assert!(matches!(
        hub.pause_job("orders", "admin").await,
        Err(HubError::JobNotRunning)
    ));

    // Only accepted mutations reach the audit trail: every rejected
    // interleaving above returned before the write.
    let audit = hub.audit(Some("orders")).await.unwrap();
    assert_eq!(
        audit
            .iter()
            .filter(|record| record.action == "job.pause" && record.outcome == "accepted")
            .count(),
        2,
        "the pause begin plus its convergence settle"
    );
    assert_eq!(
        audit
            .iter()
            .filter(|record| record.action == "job.resume")
            .count(),
        0,
        "rejected resumes are not recorded as accepted"
    );
    assert_eq!(
        audit
            .iter()
            .filter(|record| record.action == "job.stop" && record.outcome == "accepted")
            .count(),
        1
    );

    let metrics = hub.command_metrics().render();
    assert!(metrics.contains("arkflow_job_pause_total{outcome=\"requested\"} 1"));
    assert!(metrics.contains("arkflow_job_pause_total{outcome=\"paused\"} 1"));
    assert!(!metrics.contains("arkflow_job_pause_total{outcome=\"resumed\"}"));
    assert!(!metrics.contains("arkflow_job_pause_total{outcome=\"failed\"}"));
}

#[tokio::test]
async fn two_hubs_share_one_pause_state_machine_without_double_advancing() {
    let (leader, storage) = pause_fixture("two_hubs_share_one_pause_state_machine").await;
    seed_running_job(&storage, "orders", "latest_checkpoint").await;
    let artifact = pause_to_stopping(&leader, "orders").await;

    // A second Hub process reads the same durable row.
    let follower = Hub::with_storage(config(), storage.clone());
    for _ in 0..2 {
        assert_eq!(follower.reconcile_job_pauses().await.unwrap(), 0);
        let job = follower.job("orders").await.unwrap().unwrap();
        assert_eq!(job.pause_phase.as_deref(), Some(PAUSE_STOPPING));
        assert_eq!(job.generation, 1);
        assert_eq!(job.checkpoint_id.as_deref(), Some(artifact.as_str()));
    }
    // The follower's ticks wrote nothing: the stop is not converged yet.
    let audit = follower.audit(Some("orders")).await.unwrap();
    assert_eq!(
        audit
            .iter()
            .filter(|record| record.action == "job.pause" && record.outcome == "accepted")
            .count(),
        1,
        "a redundant reconcile tick must not re-apply a transition"
    );

    // The follower settles the stop; the leader must then treat the pause as
    // already advanced instead of applying it a second time.
    follower
        .observe_job("orders", 1, "stopped", None, None)
        .await
        .unwrap();
    assert_eq!(follower.reconcile_job_pauses().await.unwrap(), 1);
    assert_eq!(
        leader.reconcile_job_pauses().await.unwrap(),
        0,
        "the leader must not re-apply the settle the follower performed"
    );
    let paused = leader.job("orders").await.unwrap().unwrap();
    assert_eq!(paused.pause_phase.as_deref(), Some(PAUSE_PAUSED));
    assert_eq!(paused.convergence, "converged");
    assert_eq!(paused.generation, 1);
    let audit = leader.audit(Some("orders")).await.unwrap();
    assert_eq!(
        audit
            .iter()
            .filter(|record| record.action == "job.pause" && record.outcome == "accepted")
            .count(),
        2,
        "exactly one begin and one convergence settle"
    );
    assert_eq!(
        leader.job_checkpoints("orders").await.unwrap().len(),
        1,
        "convergence never creates another capture round"
    );

    // Resume as the durable fenced write: the public `resume_job` path also
    // dispatches a recovery start, whose manifest validation needs a real
    // object-store artifact (covered by the placement recovery tests). The
    // state-machine convergence below is what must not be double-applied.
    let paused = leader.job("orders").await.unwrap().unwrap();
    let resumed = leader
        .storage
        .as_ref()
        .unwrap()
        .update_job_pause(
            "orders",
            crate::storage::JobPauseUpdate {
                expected_generation: paused.generation,
                expected_phase: Some(PAUSE_PAUSED.into()),
                desired_state: Some("running".into()),
                phase: Some(PAUSE_RESUMING.into()),
                bump_generation: true,
                ..crate::storage::JobPauseUpdate::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.generation, 2);
    assert_eq!(resumed.pause_phase.as_deref(), Some(PAUSE_RESUMING));
    assert_eq!(
        follower.reconcile_job_pauses().await.unwrap(),
        0,
        "a resuming pause waits for the running observation"
    );
    leader
        .observe_job("orders", 2, "running", None, None)
        .await
        .unwrap();
    assert_eq!(leader.reconcile_job_pauses().await.unwrap(), 1);
    assert_eq!(
        follower.reconcile_job_pauses().await.unwrap(),
        0,
        "both Hubs observe the cleared phase instead of re-clearing it"
    );
    let cleared = leader.job("orders").await.unwrap().unwrap();
    assert_eq!(cleared.pause_phase, None);
    assert_eq!(cleared.desired_state, "running");
    assert_eq!(cleared.generation, 2);
    assert_eq!(
        cleared.checkpoint_id.as_deref(),
        Some(artifact.as_str()),
        "resume keeps the pause capture as the recovery pointer"
    );
    let audit = leader.audit(Some("orders")).await.unwrap();
    assert_eq!(
        audit
            .iter()
            .filter(|record| record.action == "job.resume" && record.outcome == "accepted")
            .count(),
        1,
        "the single convergence settle"
    );
}

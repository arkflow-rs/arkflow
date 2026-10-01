//! State-machine coverage for the atomic job-upgrade orchestration
//! (`hub/job_orchestration.rs`): creation guards, action validation, and
//! the abort/failure branches of every reconcile-tick phase.
//!
//! Where the happy paths are driven end-to-end in `tests.rs`, these tests
//! seed durable rows directly (the same authority the tick reads) so each
//! terminal branch is reached deterministically without racing a second
//! Hub against the guarded writes.

use super::*;

fn config() -> HubConfig {
    HubConfig {
        operator_token: Some("operator".into()),
        node_token: Some("node-secret".into()),
        insecure_local: false,
        lease_ttl_ms: 10_000,
        poll_interval_ms: 100,
        session_ttl_ms: default_session_ttl_ms(),
    }
}

/// Deep-validatable durable spec: a stateful aggregate with a registered
/// processor type, an explicit state format, and a checkpoint block.
fn durable_spec_json(id: &str, version: u64) -> String {
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
            "object_store_uri": format!("file:///tmp/arkflow-hub-orchestration-{}-{id}", std::process::id())
        }
    })
    .to_string()
}

fn durable_spec(id: &str, version: u64) -> arkflow_core::job::JobSpec {
    serde_json::from_str(&durable_spec_json(id, version)).unwrap()
}

fn job_row(job_id: &str, version: u64, desired: &str, observed: &str) -> JobRecord {
    JobRecord {
        job_id: job_id.into(),
        version,
        spec_json: durable_spec_json(job_id, version),
        desired_state: desired.into(),
        observed_state: observed.into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: vec!["node-a".into()],
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: 0,
    }
}

/// Storage-backed Hub with one registered node and a running durable Job at
/// version 1. Job rows are written through the storage actor directly so the
/// fixture never dispatches reconciler commands.
async fn orchestration_hub(job_id: &str) -> (Hub, crate::storage::ControlPlaneStore) {
    let store = crate::storage::ControlPlaneStore::contract("job_orchestration_tests").await;
    orchestration_hub_with(store, job_id).await
}

/// Fixture for tests that delete the Job row through the SQLite-only
/// `with_connection` introspection path.
async fn orchestration_hub_sqlite(job_id: &str) -> (Hub, crate::storage::ControlPlaneStore) {
    let store = crate::storage::ControlPlaneStore::in_memory().unwrap();
    orchestration_hub_with(store, job_id).await
}

async fn orchestration_hub_with(
    store: crate::storage::ControlPlaneStore,
    job_id: &str,
) -> (Hub, crate::storage::ControlPlaneStore) {
    let hub = Hub::with_storage(config(), StorageActor::start(store.clone(), 8));
    hub.register(RegisterRequest {
        data_address: None,
        node_id: "node-a".into(),
        node_token: "node-secret".into(),
        protocol_version: "v1".into(),
        capabilities: vec![],
        boot_id: Some("boot-a".into()),
    })
    .await
    .unwrap();
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job(job_row(job_id, 1, "running", "running"))
        .await
        .unwrap();
    (hub, store)
}

fn upgrade_row(job_id: &str, phase: &str) -> crate::storage::JobUpgradeRecord {
    let now = now_ms_for_metrics();
    crate::storage::JobUpgradeRecord {
        upgrade_id: format!("job-upgrade-{job_id}"),
        job_id: job_id.into(),
        from_version: 1,
        to_version: 2,
        phase: phase.into(),
        savepoint_id: None,
        target_spec_json: durable_spec_json(job_id, 2),
        phase_deadline_at_ms: now + 600_000,
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

/// Seed a durable orchestration row exactly as a crash or restart would
/// have left it; the tick re-reads this state authoritatively.
async fn seed(hub: &Hub, record: crate::storage::JobUpgradeRecord) -> String {
    let upgrade_id = record.upgrade_id.clone();
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job_upgrade(record)
        .await
        .unwrap();
    upgrade_id
}

async fn phase_of(hub: &Hub, upgrade_id: &str) -> String {
    hub.job_upgrade(upgrade_id)
        .await
        .unwrap()
        .expect("seeded upgrade row")
        .phase
}

async fn tick(hub: &Hub) {
    hub.reconcile_job_upgrades().await.unwrap();
}

async fn delete_job(store: &crate::storage::ControlPlaneStore, job_id: &str) {
    store
        .with_connection(|connection| {
            connection.execute("DELETE FROM cp_jobs WHERE job_id = ?", [job_id])?;
            Ok(())
        })
        .unwrap();
}

// ---------------------------------------------------------------------
// Creation guards and read paths
// ---------------------------------------------------------------------

#[tokio::test]
async fn create_job_upgrade_rejects_missing_drifted_and_unparseable_jobs() {
    let (hub, _store) = orchestration_hub("guard-job").await;

    // Unknown Job.
    let mut spec = durable_spec("missing-job", 2);
    let error = hub
        .create_job_upgrade("missing-job", &mut spec, 1, 0, None, None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("job not found"), "{error:?}");

    // Stale generation fence.
    let mut spec = durable_spec("guard-job", 2);
    let error = hub
        .create_job_upgrade("guard-job", &mut spec, 999, 0, None, None)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        crate::hub::HubError::GenerationConflict {
            expected: 999,
            current: 1
        }
    ));

    // A stopped Job is not upgradable in the atomic mode.
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job(job_row("stopped-job", 1, "stopped", "stopped"))
        .await
        .unwrap();
    let mut spec = durable_spec("stopped-job", 2);
    let error = hub
        .create_job_upgrade("stopped-job", &mut spec, 1, 0, None, None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("running Job"), "{error:?}");

    // A corrupt persisted spec cannot be format-checked against the target.
    let mut corrupt = job_row("corrupt-job", 1, "running", "running");
    corrupt.spec_json = "{{not json".into();
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job(corrupt)
        .await
        .unwrap();
    let mut spec = durable_spec("corrupt-job", 2);
    let error = hub
        .create_job_upgrade("corrupt-job", &mut spec, 1, 0, None, None)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("invalid persisted Job spec"),
        "{error:?}"
    );
}

#[tokio::test]
async fn job_upgrade_reads_list_history_and_require_storage() {
    let (hub, _store) = orchestration_hub("list-job").await;
    let mut spec = durable_spec("list-job", 2);
    let first = hub
        .create_job_upgrade("list-job", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    hub.act_job_upgrade(&first.upgrade_id, "cancel", None, None)
        .await
        .unwrap();
    let mut spec = durable_spec("list-job", 3);
    let second = hub
        .create_job_upgrade("list-job", &mut spec, 1, 0, None, None)
        .await
        .unwrap();

    let listed = hub.job_upgrades("list-job").await.unwrap();
    assert_eq!(listed.len(), 2, "{listed:?}");
    assert!(listed.iter().any(|record| record.upgrade_id == first.upgrade_id));
    assert!(listed.iter().any(|record| record.upgrade_id == second.upgrade_id));

    // Without durable storage the reads surface unavailability, and a
    // lookup against a foreign Job stays empty.
    let ephemeral = Hub::new(config());
    assert!(ephemeral.job_upgrades("list-job").await.is_err());
    assert!(ephemeral.job_upgrade("list-job").await.is_err());
    assert!(hub.job_upgrades("someone-else").await.unwrap().is_empty());
}

// ---------------------------------------------------------------------
// Operator actions
// ---------------------------------------------------------------------

#[tokio::test]
async fn act_job_upgrade_rejects_unknown_and_terminal_records() {
    let (hub, _store) = orchestration_hub("terminal-job").await;
    let error = hub
        .act_job_upgrade("job-upgrade-missing", "pause", None, None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not found"), "{error:?}");

    let mut spec = durable_spec("terminal-job", 2);
    let record = hub
        .create_job_upgrade("terminal-job", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    hub.act_job_upgrade(&record.upgrade_id, "cancel", None, None)
        .await
        .unwrap();
    let error = hub
        .act_job_upgrade(&record.upgrade_id, "pause", None, None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("terminal"), "{error:?}");
}

#[tokio::test]
async fn rollback_action_is_accepted_only_from_the_verification_phase() {
    let (hub, _store) = orchestration_hub("rollback-action").await;

    // Fresh orchestration (still saving its savepoint): rejected.
    let mut spec = durable_spec("rollback-action", 2);
    let record = hub
        .create_job_upgrade("rollback-action", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    let error = hub
        .act_job_upgrade(&record.upgrade_id, "rollback", None, None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("verified"), "{error:?}");

    // From verification: the operator rollback re-enters the rollback phase.
    let verifying = upgrade_row("rollback-action", "verifying");
    let upgrade_id = seed(&hub, verifying).await;
    let rolled = hub
        .act_job_upgrade(&upgrade_id, "rollback", None, None)
        .await
        .unwrap();
    assert_eq!(rolled.phase, "rolling_back");
    assert_eq!(rolled.paused_from, None);

    // A pause that interrupted verification keeps the action available.
    let verifying = upgrade_row("rollback-action", "verifying");
    let upgrade_id = seed(&hub, verifying).await;
    hub.act_job_upgrade(&upgrade_id, "pause", None, None)
        .await
        .unwrap();
    let rolled = hub
        .act_job_upgrade(&upgrade_id, "rollback", None, None)
        .await
        .unwrap();
    assert_eq!(rolled.phase, "rolling_back");
    assert_eq!(rolled.paused_from, None);
}

// ---------------------------------------------------------------------
// Savepoint phase aborts
// ---------------------------------------------------------------------

#[tokio::test]
async fn savepoint_phase_aborts_before_dispatch_when_retries_are_exhausted() {
    let (hub, _store) = orchestration_hub("retries-job").await;
    let mut record = upgrade_row("retries-job", "saving_savepoint");
    record.savepoint_retries = 3;
    let upgrade_id = seed(&hub, record).await;
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &upgrade_id).await, "aborted");
    // The Job was never touched.
    let job = hub.job("retries-job").await.unwrap().unwrap();
    assert_eq!(job.version, 1);
    assert_eq!(job.desired_state, "running");
}

#[tokio::test]
async fn savepoint_phase_aborts_when_the_job_disappears_or_drifts() {
    // The Job row vanished (deleted out from under the orchestration).
    let (hub, store) = orchestration_hub_sqlite("vanished-job").await;
    let upgrade_id = seed(&hub, upgrade_row("vanished-job", "saving_savepoint")).await;
    delete_job(&store, "vanished-job").await;
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &upgrade_id).await, "aborted");

    // An external writer moved the Job outside the orchestration's range.
    let (hub, _store) = orchestration_hub("drifted-job").await;
    let upgrade_id = seed(&hub, upgrade_row("drifted-job", "saving_savepoint")).await;
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job(job_row("drifted-job", 3, "running", "running"))
        .await
        .unwrap();
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &upgrade_id).await, "aborted");
}

#[tokio::test]
async fn savepoint_round_deadline_aborts_and_leaves_the_running_job_alone() {
    let (hub, _store) = orchestration_hub("deadline-job").await;
    let mut spec = durable_spec("deadline-job", 2);
    let record = hub
        .create_job_upgrade("deadline-job", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    // First tick dispatches the savepoint round.
    tick(&hub).await;
    let dispatched = hub
        .job_upgrade(&record.upgrade_id)
        .await
        .unwrap()
        .unwrap();
    let savepoint_id = dispatched.savepoint_id.clone().expect("round dispatched");

    // The round stays pending past the phase deadline: abort, keep the Job.
    let mut expired = dispatched;
    expired.phase_deadline_at_ms = 1;
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job_upgrade(expired)
        .await
        .unwrap();
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &record.upgrade_id).await, "aborted");
    let job = hub.job("deadline-job").await.unwrap().unwrap();
    assert_eq!((job.version, job.desired_state.as_str()), (1, "running"));
    // The artifact row itself is retained for the operator.
    assert!(hub
        .job_checkpoints("deadline-job")
        .await
        .unwrap()
        .iter()
        .any(|checkpoint| checkpoint.checkpoint_id == savepoint_id));
}

// ---------------------------------------------------------------------
// Commit phase aborts
// ---------------------------------------------------------------------

#[tokio::test]
async fn commit_phase_aborts_on_a_missing_job_an_expired_deadline_or_a_moved_version() {
    // Job disappeared between the savepoint and the commit.
    let (hub, store) = orchestration_hub_sqlite("commit-vanished").await;
    let upgrade_id = seed(&hub, upgrade_row("commit-vanished", "committing_version")).await;
    delete_job(&store, "commit-vanished").await;
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &upgrade_id).await, "aborted");

    // Deadline exceeded before the fenced write landed.
    let (hub, _store) = orchestration_hub("commit-late").await;
    let mut record = upgrade_row("commit-late", "committing_version");
    record.phase_deadline_at_ms = 1;
    let upgrade_id = seed(&hub, record).await;
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &upgrade_id).await, "aborted");
    assert_eq!(hub.job("commit-late").await.unwrap().unwrap().version, 1);

    // The Job moved to a version outside [from, to] before the commit.
    let (hub, _store) = orchestration_hub("commit-moved").await;
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job(job_row("commit-moved", 3, "running", "running"))
        .await
        .unwrap();
    let upgrade_id = seed(&hub, upgrade_row("commit-moved", "committing_version")).await;
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &upgrade_id).await, "aborted");
    assert_eq!(hub.job("commit-moved").await.unwrap().unwrap().version, 3);
}

#[tokio::test]
async fn a_positive_verify_timeout_arms_the_verification_deadline() {
    let (hub, _store) = orchestration_hub("timeout-job").await;
    let mut spec = durable_spec("timeout-job", 2);
    let record = hub
        .create_job_upgrade("timeout-job", &mut spec, 1, 9_000, None, None)
        .await
        .unwrap();
    tick(&hub).await;
    let dispatched = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    let savepoint_id = dispatched.savepoint_id.expect("round dispatched");
    // Complete the round through the ordinary checkpoint-recording path.
    let mut checkpoint = hub
        .job_checkpoints("timeout-job")
        .await
        .unwrap()
        .into_iter()
        .find(|checkpoint| checkpoint.checkpoint_id == savepoint_id)
        .unwrap();
    checkpoint.status = "completed".into();
    hub.record_job_checkpoint(checkpoint).await.unwrap();
    tick(&hub).await;

    let verifying = hub.job_upgrade(&record.upgrade_id).await.unwrap().unwrap();
    assert_eq!(verifying.phase, "verifying");
    let now = now_ms_for_metrics();
    assert!(
        verifying.phase_deadline_at_ms > now + 8_000
            && verifying.phase_deadline_at_ms < now + 10_000,
        "override not applied: {}",
        verifying.phase_deadline_at_ms
    );
    assert_eq!(hub.job("timeout-job").await.unwrap().unwrap().version, 2);
}

// ---------------------------------------------------------------------
// Verification phase failures
// ---------------------------------------------------------------------

#[tokio::test]
async fn verification_fails_when_the_job_disappears_or_moves_off_the_target() {
    let (hub, store) = orchestration_hub_sqlite("verify-vanished").await;
    let upgrade_id = seed(&hub, upgrade_row("verify-vanished", "verifying")).await;
    delete_job(&store, "verify-vanished").await;
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &upgrade_id).await, "failed");

    let (hub, _store) = orchestration_hub("verify-moved").await;
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job(job_row("verify-moved", 3, "running", "running"))
        .await
        .unwrap();
    let upgrade_id = seed(&hub, upgrade_row("verify-moved", "verifying")).await;
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &upgrade_id).await, "failed");
}

// ---------------------------------------------------------------------
// Rollback phase outcomes
// ---------------------------------------------------------------------

#[tokio::test]
async fn rollback_completes_once_the_restored_generation_is_observed() {
    let (hub, _store) = orchestration_hub("rb-wait").await;
    // The restore already applied and is observed running: the rollback
    // settles immediately instead of waiting for its deadline.
    let upgrade_id = seed(&hub, upgrade_row("rb-wait", "rolling_back")).await;
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &upgrade_id).await, "rolled_back");
}

#[tokio::test]
async fn rollback_phase_fails_when_the_job_version_moves_mid_restore() {
    let (hub, _store) = orchestration_hub("rb-moved").await;
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job(job_row("rb-moved", 3, "running", "running"))
        .await
        .unwrap();
    let upgrade_id = seed(&hub, upgrade_row("rb-moved", "rolling_back")).await;
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &upgrade_id).await, "failed");
}

#[tokio::test]
async fn rollback_phase_fails_when_the_previous_version_is_missing_or_unparseable() {
    // No version history row for the previous version.
    let (hub, _store) = orchestration_hub("rb-missing").await;
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job(job_row("rb-missing", 2, "running", "stopped"))
        .await
        .unwrap();
    let mut record = upgrade_row("rb-missing", "rolling_back");
    record.from_version = 99;
    let upgrade_id = seed(&hub, record).await;
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &upgrade_id).await, "failed");

    // A corrupt previous-version spec surfaces as an invalid-state error
    // from the tick instead of a silent half-restore.
    let (hub, _store) = orchestration_hub("rb-corrupt").await;
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job(job_row("rb-corrupt", 2, "running", "stopped"))
        .await
        .unwrap();
    let now = now_ms_for_metrics();
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job_version(crate::storage::JobVersionRecord {
            job_id: "rb-corrupt".into(),
            version: 1,
            spec_json: "{{not json".into(),
            plan_json: "{}".into(),
            created_at_ms: now,
        })
        .await
        .unwrap();
    seed(&hub, upgrade_row("rb-corrupt", "rolling_back")).await;
    let error = hub.reconcile_job_upgrades().await.unwrap_err();
    assert!(
        error.to_string().contains("invalid persisted Job spec"),
        "{error:?}"
    );
}

#[tokio::test]
async fn rollback_phase_fails_when_the_pinned_savepoint_cannot_restore_the_old_version() {
    let (hub, _store) = orchestration_hub("rb-incompatible").await;
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job(job_row("rb-incompatible", 2, "running", "stopped"))
        .await
        .unwrap();
    let now = now_ms_for_metrics();
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job_version(crate::storage::JobVersionRecord {
            job_id: "rb-incompatible".into(),
            version: 1,
            spec_json: durable_spec_json("rb-incompatible", 1),
            plan_json: "{}".into(),
            created_at_ms: now,
        })
        .await
        .unwrap();
    // The orchestration's savepoint was written under format 2 while the
    // previous version's spec declares format 1: no restore path exists.
    hub.storage
        .as_ref()
        .unwrap()
        .upsert_job_checkpoint(crate::storage::JobCheckpointRecord {
            job_id: "rb-incompatible".into(),
            job_version: 2,
            checkpoint_id: "sp-format-two".into(),
            kind: "savepoint".into(),
            status: "completed".into(),
            manifest_uri: None,
            format_version: 2,
            created_at_ms: now,
            updated_at_ms: now,
        })
        .await
        .unwrap();
    let mut record = upgrade_row("rb-incompatible", "rolling_back");
    record.savepoint_id = Some("sp-format-two".into());
    let upgrade_id = seed(&hub, record).await;
    tick(&hub).await;
    assert_eq!(phase_of(&hub, &upgrade_id).await, "failed");
    // Terminal failure keeps the artifact pinned for operator recovery.
    assert!(hub
        .job_checkpoints("rb-incompatible")
        .await
        .unwrap()
        .iter()
        .any(|checkpoint| checkpoint.checkpoint_id == "sp-format-two"));
}

// ---------------------------------------------------------------------
// Retention and boot-recovery helpers
// ---------------------------------------------------------------------

#[tokio::test]
async fn upgrade_history_prunes_only_terminal_rows_beyond_retention() {
    let (hub, _store) = orchestration_hub("prune-job").await;
    let mut cancelled = upgrade_row("prune-job", "cancelled");
    cancelled.updated_at_ms = 1;
    let upgrade_id = seed(&hub, cancelled).await;
    // Age-based sweep with a zero retention floor removes the terminal row.
    let storage = hub.storage.as_ref().unwrap();
    let pruned = storage.prune_job_upgrades(10_000, 0).await.unwrap();
    assert!(pruned >= 1, "nothing pruned");
    assert!(hub.job_upgrade(&upgrade_id).await.unwrap().is_none());

    // A live orchestration row is never retention sweepable.
    let fresh = seed(&hub, upgrade_row("prune-job", "verifying")).await;
    storage.prune_job_upgrades(10_000, 0).await.unwrap();
    assert!(hub.job_upgrade(&fresh).await.unwrap().is_some());

    // Without durable storage both helpers are benign no-ops.
    let ephemeral = Hub::new(config());
    ephemeral.recover_job_upgrade_cache().await.unwrap();
    assert_eq!(ephemeral.prune_job_upgrade_history().await.unwrap(), 0);
}

#[tokio::test]
async fn boot_recovery_repopulates_the_fence_cache_from_durable_rows() {
    let store = crate::storage::ControlPlaneStore::contract("orchestration_boot_recovery").await;
    let hub1 = Hub::with_storage(config(), StorageActor::start(store.clone(), 8));
    let mut spec = durable_spec("boot-job", 2);
    let record = hub1
        .create_job_upgrade("boot-job", &mut spec, 0, 0, None, None)
        .await;
    // No Job row: creation is rejected, but the seeded-row path below still
    // exercises recovery against rows a previous process left behind.
    assert!(record.is_err());
    hub1.storage
        .as_ref()
        .unwrap()
        .upsert_job(job_row("boot-job", 1, "running", "running"))
        .await
        .unwrap();
    let mut spec = durable_spec("boot-job", 2);
    let record = hub1
        .create_job_upgrade("boot-job", &mut spec, 1, 0, None, None)
        .await
        .unwrap();
    assert!(
        hub1.active_job_upgrade_for("boot-job").await.is_some(),
        "creation populated the cache"
    );

    // A fresh process over the same store rebuilds the fence from storage.
    let hub2 = Hub::with_storage(config(), StorageActor::start(store, 8));
    assert!(hub2.active_job_upgrade_for("boot-job").await.is_none());
    hub2.recover_job_upgrade_cache().await.unwrap();
    let recovered = hub2.active_job_upgrade_for("boot-job").await.unwrap();
    assert_eq!(recovered.upgrade_id, record.upgrade_id);
    assert!(hub2.job_upgrade_fences_reconciliation("boot-job").await);
}

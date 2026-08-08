//! Durable control-plane storage primitives.
//!
//! SQLite is accessed synchronously through a Storage Actor boundary. The
//! repository schema is deliberately independent from HTTP and Hub types so
//! the state machine can later move to a server database.

use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone, Default)]
pub struct DesiredMutation {
    pub node_id: String,
    pub stream_id: String,
    pub desired_state: String,
    pub config_version_id: Option<String>,
    pub action_id: Option<String>,
    pub expected_generation: Option<u64>,
    pub actor: Option<String>,
    pub correlation_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub intent_type: Option<String>,
    pub payload_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NodeMutation {
    pub node_id: String,
    pub version: String,
    pub state: String,
    pub capabilities_json: String,
    pub boot_id: Option<String>,
    pub report_seq: Option<u64>,
    pub last_seen_at_ms: u64,
    pub lease_expires_at_ms: u64,
    pub maintenance_state: Option<String>,
    pub maintenance_updated_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct NodeMaintenanceMutation {
    pub node_id: String,
    pub state: String,
    pub actor: Option<String>,
    pub correlation_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct OperationalAggregates {
    pub node_states: Vec<(String, u64)>,
    pub maintenance_states: Vec<(String, u64)>,
    pub intent_states: Vec<(String, u64)>,
    pub convergence_states: Vec<(String, u64)>,
    pub attempt_states: Vec<(String, u64)>,
    pub failure_classes: Vec<(String, u64)>,
    pub outbox_pending: u64,
    pub outbox_claimed: u64,
    pub stale_nodes: u64,
    pub active_attempts: u64,
    pub non_terminal_intents: u64,
    pub oldest_pending_age_seconds: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct IntentRecord {
    pub intent_id: String,
    pub node_id: String,
    pub stream_id: String,
    pub generation: u64,
    pub state: String,
    pub desired_state: String,
    pub config_version_id: Option<String>,
    pub action_id: Option<String>,
    pub convergence_state: String,
    pub retry_count: u32,
    pub next_retry_at_ms: Option<u64>,
    pub failure_class: Option<String>,
    pub superseded_by_intent_id: Option<String>,
    pub superseded_generation: Option<u64>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub observed_generation: Option<u64>,
    pub observed_state: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DesiredRecord {
    pub node_id: String,
    pub stream_id: String,
    pub generation: u64,
    pub desired_state: String,
    pub config_version_id: Option<String>,
    pub action_id: Option<String>,
    pub correlation_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ObservedMutation {
    pub node_id: String,
    pub stream_id: String,
    pub boot_id: Option<String>,
    pub report_seq: u64,
    pub observed_generation: Option<u64>,
    pub observed_state: String,
    pub config_version_id: Option<String>,
    pub action_id: Option<String>,
    pub snapshot_json: String,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AttemptRecord {
    pub attempt_id: String,
    pub intent_id: String,
    pub command_id: String,
    pub state: String,
    pub failure_class: Option<String>,
    pub node_id: String,
    pub stream_id: String,
    pub generation: u64,
    pub operation: String,
    pub action_id: Option<String>,
    pub config_version_id: Option<String>,
    pub payload_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OutboxRecord {
    pub outbox_id: i64,
    pub event_key: String,
    pub event_type: String,
    pub node_id: String,
    pub stream_id: Option<String>,
    pub intent_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StoredEvent {
    pub event_id: i64,
    pub node_id: Option<String>,
    pub stream_id: Option<String>,
    pub intent_id: Option<String>,
    pub attempt_id: Option<String>,
    pub event_type: String,
    pub outcome: String,
    pub failure_class: Option<String>,
    pub message: Option<String>,
    pub generation: Option<u64>,
    pub correlation_id: Option<String>,
    pub occurred_at_ms: u64,
    pub actor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditRecord {
    pub event_id: i64,
    pub actor: Option<String>,
    pub action: String,
    pub resource_type: String,
    pub resource_id: Option<String>,
    pub node_id: Option<String>,
    pub stream_id: Option<String>,
    pub correlation_id: Option<String>,
    pub outcome: String,
    pub failure_code: Option<String>,
    pub message: Option<String>,
    pub occurred_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RolloutRecord {
    pub rollout_id: String,
    pub config_version_id: String,
    pub state: String,
    pub batch_size: u32,
    pub current_batch: u32,
    pub total_targets: u32,
    pub actor: Option<String>,
    pub correlation_id: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RolloutTargetRecord {
    pub rollout_id: String,
    pub node_id: String,
    pub ordinal: u32,
    pub state: String,
    pub attempt_id: Option<String>,
    pub error: Option<String>,
    pub observed_config_version: Option<String>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedOperation {
    pub operation_id: String,
    pub node_id: String,
    pub resource_id: String,
    pub operation: String,
    pub state: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub operation_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecord {
    pub job_id: String,
    pub version: u64,
    pub spec_json: String,
    pub desired_state: String,
    pub observed_state: String,
    pub convergence: String,
    pub generation: u64,
    pub node_ids: Vec<String>,
    pub checkpoint_id: Option<String>,
    pub last_error: Option<String>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobVersionRecord {
    pub job_id: String,
    pub version: u64,
    pub spec_json: String,
    pub plan_json: String,
    pub created_at_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskAssignmentRecord {
    pub job_id: String,
    pub generation: u64,
    pub task_id: String,
    pub node_id: String,
    pub attempt_id: String,
    pub state: String,
    pub updated_at_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCheckpointRecord {
    pub job_id: String,
    /// Job version that produced this artifact. Recovery must never reuse a
    /// checkpoint from a prior deployment of the same logical Job id.
    pub job_version: u64,
    pub checkpoint_id: String,
    pub kind: String,
    pub status: String,
    pub manifest_uri: Option<String>,
    pub format_version: u32,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobObservationRecord {
    pub job_id: String,
    pub node_id: String,
    pub boot_id: Option<String>,
    pub report_seq: u64,
    pub generation: u64,
    pub state: String,
    pub convergence: String,
    pub checkpoint_id: Option<String>,
    pub snapshot_json: String,
    pub observed_at_ms: u64,
}

fn row_to_job(row: &Row<'_>) -> rusqlite::Result<JobRecord> {
    let node_ids_json: String = row.get(7)?;
    let node_ids = serde_json::from_str(&node_ids_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(JobRecord {
        job_id: row.get(0)?,
        version: row.get(1)?,
        spec_json: row.get(2)?,
        desired_state: row.get(3)?,
        observed_state: row.get(4)?,
        convergence: row.get(5)?,
        generation: row.get(6)?,
        node_ids,
        checkpoint_id: row.get(8)?,
        last_error: row.get(9)?,
        updated_at_ms: row.get(10)?,
    })
}

#[derive(Debug, Clone)]
pub struct RolloutTargetUpdate {
    pub rollout_id: String,
    pub node_id: String,
    pub state: String,
    pub attempt_id: Option<String>,
    pub error: Option<String>,
    pub observed_config_version: Option<String>,
    pub updated_at_ms: u64,
}

/// Storage-neutral contract used by Hub/Reconciler code.
///
/// Implementations must keep each method's state transition atomic. Network
/// dispatch is intentionally absent; callers claim an Attempt, commit, then
/// talk to the Agent outside the storage transaction.
pub trait ControlPlaneRepository: Send + Sync {
    fn upsert_node(&self, mutation: NodeMutation) -> Result<(), StorageError>;
    fn set_desired(&self, mutation: DesiredMutation) -> Result<IntentRecord, StorageError>;
    fn record_observed(&self, mutation: ObservedMutation) -> Result<(), StorageError>;
    fn claim_attempt(&self, intent_id: &str) -> Result<Option<AttemptRecord>, StorageError>;
    fn mark_attempt_dispatched(
        &self,
        attempt_id: &str,
        expires_at_ms: u64,
    ) -> Result<(), StorageError>;
    fn expire_attempts(&self, now_ms: u64) -> Result<usize, StorageError>;
    fn wake_node(&self, node_id: &str, now_ms: u64) -> Result<(), StorageError>;
    fn list_events(&self, node_id: Option<&str>) -> Result<Vec<StoredEvent>, StorageError>;
    fn complete_attempt(
        &self,
        attempt_id: &str,
        state: &str,
        failure_class: Option<&str>,
    ) -> Result<(), StorageError>;
    fn claim_outbox(
        &self,
        worker_id: &str,
        now_ms: u64,
    ) -> Result<Option<OutboxRecord>, StorageError>;
    fn mark_outbox_processed(&self, outbox_id: i64, now_ms: u64) -> Result<(), StorageError>;
    fn set_node_maintenance(
        &self,
        mutation: NodeMaintenanceMutation,
        now_ms: u64,
    ) -> Result<bool, StorageError>;
    fn get_node_maintenance(&self, node_id: &str) -> Result<Option<String>, StorageError>;
    fn operational_aggregates(&self, now_ms: u64) -> Result<OperationalAggregates, StorageError>;
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("storage mutex poisoned")]
    Poisoned,
    #[error("storage actor is closed")]
    ActorClosed,
    #[error("desired state generation conflict: expected {expected}, current {current}")]
    GenerationConflict { expected: u64, current: u64 },
    #[error("idempotency key was already used for a different mutation")]
    IdempotencyKeyReused,
}

#[derive(Clone)]
pub struct ControlPlaneStore {
    connection: Arc<Mutex<Connection>>,
}

enum StorageCommand {
    UpsertJob {
        job: JobRecord,
        response: oneshot::Sender<Result<JobRecord, StorageError>>,
    },
    GetJob {
        job_id: String,
        response: oneshot::Sender<Result<Option<JobRecord>, StorageError>>,
    },
    ListJobs {
        response: oneshot::Sender<Result<Vec<JobRecord>, StorageError>>,
    },
    UpsertJobVersion {
        record: JobVersionRecord,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    ListJobVersions {
        job_id: String,
        response: oneshot::Sender<Result<Vec<JobVersionRecord>, StorageError>>,
    },
    UpdateJob {
        job_id: String,
        desired_state: Option<String>,
        observed_state: Option<String>,
        convergence: Option<String>,
        generation: Option<u64>,
        checkpoint_id: Option<String>,
        last_error: Option<String>,
        response: oneshot::Sender<Result<Option<JobRecord>, StorageError>>,
    },
    UpdateJobDesiredState {
        job_id: String,
        desired_state: String,
        expected_generation: u64,
        response: oneshot::Sender<Result<Option<JobRecord>, StorageError>>,
    },
    UpsertJobCheckpoint {
        record: JobCheckpointRecord,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    ListJobCheckpoints {
        job_id: String,
        response: oneshot::Sender<Result<Vec<JobCheckpointRecord>, StorageError>>,
    },
    DeleteJobCheckpoint {
        job_id: String,
        checkpoint_id: String,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    UpsertNode {
        mutation: NodeMutation,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    SetDesired {
        mutation: DesiredMutation,
        response: oneshot::Sender<Result<IntentRecord, StorageError>>,
    },
    GetDesired {
        node_id: String,
        stream_id: String,
        response: oneshot::Sender<Result<Option<DesiredRecord>, StorageError>>,
    },
    GetIntent {
        intent_id: String,
        response: oneshot::Sender<Result<Option<IntentRecord>, StorageError>>,
    },
    ListIntents {
        node_id: Option<String>,
        response: oneshot::Sender<Result<Vec<IntentRecord>, StorageError>>,
    },
    RecoverReconciliation {
        now_ms: u64,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    WakeNode {
        node_id: String,
        now_ms: u64,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    ListEvents {
        node_id: Option<String>,
        response: oneshot::Sender<Result<Vec<StoredEvent>, StorageError>>,
    },
    PruneEvents {
        retain: usize,
        response: oneshot::Sender<Result<usize, StorageError>>,
    },
    ClaimAttempt {
        intent_id: String,
        response: oneshot::Sender<Result<Option<AttemptRecord>, StorageError>>,
    },
    MarkAttemptDispatched {
        attempt_id: String,
        expires_at_ms: u64,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    ExpireAttempts {
        now_ms: u64,
        response: oneshot::Sender<Result<usize, StorageError>>,
    },
    CompleteAttempt {
        attempt_id: String,
        state: String,
        failure_class: Option<String>,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    RecordObserved {
        mutation: ObservedMutation,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    ClaimOutbox {
        worker_id: String,
        now_ms: u64,
        response: oneshot::Sender<Result<Option<OutboxRecord>, StorageError>>,
    },
    MarkOutboxProcessed {
        outbox_id: i64,
        now_ms: u64,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    SetNodeMaintenance {
        mutation: NodeMaintenanceMutation,
        now_ms: u64,
        response: oneshot::Sender<Result<bool, StorageError>>,
    },
    GetNodeMaintenance {
        node_id: String,
        response: oneshot::Sender<Result<Option<String>, StorageError>>,
    },
    OperationalAggregates {
        now_ms: u64,
        response: oneshot::Sender<Result<OperationalAggregates, StorageError>>,
    },
    RecordAudit {
        record: AuditRecord,
        response: oneshot::Sender<Result<i64, StorageError>>,
    },
    ListAudit {
        resource_id: Option<String>,
        response: oneshot::Sender<Result<Vec<AuditRecord>, StorageError>>,
    },
    CreateRollout {
        rollout: RolloutRecord,
        targets: Vec<RolloutTargetRecord>,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    CreateRolloutWithContent {
        rollout: RolloutRecord,
        targets: Vec<RolloutTargetRecord>,
        content: String,
        created_by: Option<String>,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    GetRollout {
        rollout_id: String,
        response: oneshot::Sender<Result<Option<RolloutRecord>, StorageError>>,
    },
    ListRolloutTargets {
        rollout_id: String,
        response: oneshot::Sender<Result<Vec<RolloutTargetRecord>, StorageError>>,
    },
    UpdateRollout {
        rollout_id: String,
        state: String,
        current_batch: u32,
        updated_at_ms: u64,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    UpdateRolloutTarget {
        update: RolloutTargetUpdate,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    GetConfigVersionContent {
        config_version_id: String,
        response: oneshot::Sender<Result<Option<String>, StorageError>>,
    },
    RecoverRollouts {
        response: oneshot::Sender<Result<Vec<RolloutRecord>, StorageError>>,
    },
    ListRollouts {
        response: oneshot::Sender<Result<Vec<RolloutRecord>, StorageError>>,
    },
    UpsertOperation {
        operation: PersistedOperation,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    GetOperation {
        operation_id: String,
        response: oneshot::Sender<Result<Option<PersistedOperation>, StorageError>>,
    },
    ListOperations {
        node_id: Option<String>,
        response: oneshot::Sender<Result<Vec<PersistedOperation>, StorageError>>,
    },
}

#[derive(Clone)]
pub struct StorageActor {
    sender: mpsc::Sender<StorageCommand>,
}

impl StorageActor {
    pub fn start(store: ControlPlaneStore, capacity: usize) -> Self {
        let (sender, mut receiver) = mpsc::channel(capacity.max(1));
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                match command {
                    StorageCommand::UpsertJob { job, response } => {
                        let _ = response.send(store.upsert_job(job));
                    }
                    StorageCommand::GetJob { job_id, response } => {
                        let _ = response.send(store.get_job(&job_id));
                    }
                    StorageCommand::ListJobs { response } => {
                        let _ = response.send(store.list_jobs());
                    }
                    StorageCommand::UpsertJobVersion { record, response } => {
                        let _ = response.send(store.upsert_job_version(record));
                    }
                    StorageCommand::ListJobVersions { job_id, response } => {
                        let _ = response.send(store.list_job_versions(&job_id));
                    }
                    StorageCommand::UpdateJob {
                        job_id,
                        desired_state,
                        observed_state,
                        convergence,
                        generation,
                        checkpoint_id,
                        last_error,
                        response,
                    } => {
                        let _ = response.send(store.update_job(
                            &job_id,
                            desired_state.as_deref(),
                            observed_state.as_deref(),
                            convergence.as_deref(),
                            generation,
                            checkpoint_id.as_deref(),
                            last_error.as_deref(),
                        ));
                    }
                    StorageCommand::UpdateJobDesiredState {
                        job_id,
                        desired_state,
                        expected_generation,
                        response,
                    } => {
                        let _ = response.send(store.update_job_desired_state(
                            &job_id,
                            &desired_state,
                            expected_generation,
                        ));
                    }
                    StorageCommand::UpsertJobCheckpoint { record, response } => {
                        let _ = response.send(store.upsert_job_checkpoint(record));
                    }
                    StorageCommand::ListJobCheckpoints { job_id, response } => {
                        let _ = response.send(store.list_job_checkpoints(&job_id));
                    }
                    StorageCommand::DeleteJobCheckpoint {
                        job_id,
                        checkpoint_id,
                        response,
                    } => {
                        let _ = response.send(store.delete_job_checkpoint(&job_id, &checkpoint_id));
                    }
                    StorageCommand::UpsertNode { mutation, response } => {
                        let _ = response.send(store.upsert_node(mutation));
                    }
                    StorageCommand::SetDesired { mutation, response } => {
                        let _ = response.send(store.set_desired(mutation));
                    }
                    StorageCommand::GetDesired {
                        node_id,
                        stream_id,
                        response,
                    } => {
                        let _ = response.send(store.get_desired(&node_id, &stream_id));
                    }
                    StorageCommand::GetIntent {
                        intent_id,
                        response,
                    } => {
                        let _ = response.send(store.get_intent(&intent_id));
                    }
                    StorageCommand::ListIntents { node_id, response } => {
                        let _ = response.send(store.list_intents(node_id.as_deref()));
                    }
                    StorageCommand::RecoverReconciliation { now_ms, response } => {
                        let _ = response.send(store.recover_reconciliation(now_ms));
                    }
                    StorageCommand::WakeNode {
                        node_id,
                        now_ms,
                        response,
                    } => {
                        let _ = response.send(store.wake_node(&node_id, now_ms));
                    }
                    StorageCommand::ListEvents { node_id, response } => {
                        let _ = response.send(store.list_events(node_id.as_deref()));
                    }
                    StorageCommand::PruneEvents { retain, response } => {
                        let _ = response.send(store.prune_events(retain));
                    }
                    StorageCommand::ClaimAttempt {
                        intent_id,
                        response,
                    } => {
                        let _ = response.send(store.claim_attempt(&intent_id));
                    }
                    StorageCommand::MarkAttemptDispatched {
                        attempt_id,
                        expires_at_ms,
                        response,
                    } => {
                        let _ = response
                            .send(store.mark_attempt_dispatched(&attempt_id, expires_at_ms));
                    }
                    StorageCommand::ExpireAttempts { now_ms, response } => {
                        let _ = response.send(store.expire_attempts(now_ms));
                    }
                    StorageCommand::CompleteAttempt {
                        attempt_id,
                        state,
                        failure_class,
                        response,
                    } => {
                        let _ = response.send(store.complete_attempt(
                            &attempt_id,
                            &state,
                            failure_class.as_deref(),
                        ));
                    }
                    StorageCommand::RecordObserved { mutation, response } => {
                        let _ = response.send(store.record_observed(mutation));
                    }
                    StorageCommand::ClaimOutbox {
                        worker_id,
                        now_ms,
                        response,
                    } => {
                        let _ = response.send(store.claim_outbox(&worker_id, now_ms));
                    }
                    StorageCommand::MarkOutboxProcessed {
                        outbox_id,
                        now_ms,
                        response,
                    } => {
                        let _ = response.send(store.mark_outbox_processed(outbox_id, now_ms));
                    }
                    StorageCommand::SetNodeMaintenance {
                        mutation,
                        now_ms,
                        response,
                    } => {
                        let _ = response.send(store.set_node_maintenance(mutation, now_ms));
                    }
                    StorageCommand::GetNodeMaintenance { node_id, response } => {
                        let _ = response.send(store.get_node_maintenance(&node_id));
                    }
                    StorageCommand::OperationalAggregates { now_ms, response } => {
                        let _ = response.send(store.operational_aggregates(now_ms));
                    }
                    StorageCommand::RecordAudit { record, response } => {
                        let _ = response.send(store.record_audit(record));
                    }
                    StorageCommand::ListAudit {
                        resource_id,
                        response,
                    } => {
                        let _ = response.send(store.list_audit(resource_id.as_deref()));
                    }
                    StorageCommand::CreateRollout {
                        rollout,
                        targets,
                        response,
                    } => {
                        let _ = response.send(store.create_rollout(rollout, targets));
                    }
                    StorageCommand::CreateRolloutWithContent {
                        rollout,
                        targets,
                        content,
                        created_by,
                        response,
                    } => {
                        let _ = response.send(store.create_rollout_with_content(
                            rollout,
                            targets,
                            &content,
                            created_by.as_deref(),
                        ));
                    }
                    StorageCommand::GetRollout {
                        rollout_id,
                        response,
                    } => {
                        let _ = response.send(store.get_rollout(&rollout_id));
                    }
                    StorageCommand::ListRolloutTargets {
                        rollout_id,
                        response,
                    } => {
                        let _ = response.send(store.list_rollout_targets(&rollout_id));
                    }
                    StorageCommand::UpdateRollout {
                        rollout_id,
                        state,
                        current_batch,
                        updated_at_ms,
                        response,
                    } => {
                        let _ = response.send(store.update_rollout(
                            &rollout_id,
                            &state,
                            current_batch,
                            updated_at_ms,
                        ));
                    }
                    StorageCommand::UpdateRolloutTarget { update, response } => {
                        let _ = response.send(store.update_rollout_target(update));
                    }
                    StorageCommand::GetConfigVersionContent {
                        config_version_id,
                        response,
                    } => {
                        let _ = response.send(store.get_config_version_content(&config_version_id));
                    }
                    StorageCommand::RecoverRollouts { response } => {
                        let _ = response.send(store.recover_rollouts());
                    }
                    StorageCommand::ListRollouts { response } => {
                        let _ = response.send(store.list_rollouts());
                    }
                    StorageCommand::UpsertOperation {
                        operation,
                        response,
                    } => {
                        let _ = response.send(store.upsert_operation(operation));
                    }
                    StorageCommand::GetOperation {
                        operation_id,
                        response,
                    } => {
                        let _ = response.send(store.get_operation(&operation_id));
                    }
                    StorageCommand::ListOperations { node_id, response } => {
                        let _ = response.send(store.list_operations(node_id.as_deref()));
                    }
                }
            }
        });
        Self { sender }
    }

    pub async fn upsert_job(&self, job: JobRecord) -> Result<JobRecord, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::UpsertJob { job, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn get_job(
        &self,
        job_id: impl Into<String>,
    ) -> Result<Option<JobRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::GetJob {
                job_id: job_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn list_jobs(&self) -> Result<Vec<JobRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ListJobs { response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn upsert_job_version(&self, record: JobVersionRecord) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::UpsertJobVersion { record, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn list_job_versions(
        &self,
        job_id: impl Into<String>,
    ) -> Result<Vec<JobVersionRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ListJobVersions {
                job_id: job_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_job(
        &self,
        job_id: impl Into<String>,
        desired_state: Option<String>,
        observed_state: Option<String>,
        convergence: Option<String>,
        generation: Option<u64>,
        checkpoint_id: Option<String>,
        last_error: Option<String>,
    ) -> Result<Option<JobRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::UpdateJob {
                job_id: job_id.into(),
                desired_state,
                observed_state,
                convergence,
                generation,
                checkpoint_id,
                last_error,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn update_job_desired_state(
        &self,
        job_id: impl Into<String>,
        desired_state: impl Into<String>,
        expected_generation: u64,
    ) -> Result<Option<JobRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::UpdateJobDesiredState {
                job_id: job_id.into(),
                desired_state: desired_state.into(),
                expected_generation,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn upsert_job_checkpoint(
        &self,
        record: JobCheckpointRecord,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::UpsertJobCheckpoint { record, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn list_job_checkpoints(
        &self,
        job_id: impl Into<String>,
    ) -> Result<Vec<JobCheckpointRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ListJobCheckpoints {
                job_id: job_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn delete_job_checkpoint(
        &self,
        job_id: impl Into<String>,
        checkpoint_id: impl Into<String>,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::DeleteJobCheckpoint {
                job_id: job_id.into(),
                checkpoint_id: checkpoint_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn set_desired(
        &self,
        mutation: DesiredMutation,
    ) -> Result<IntentRecord, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::SetDesired { mutation, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn upsert_node(&self, mutation: NodeMutation) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::UpsertNode { mutation, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn claim_outbox(
        &self,
        worker_id: impl Into<String>,
        now_ms: u64,
    ) -> Result<Option<OutboxRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ClaimOutbox {
                worker_id: worker_id.into(),
                now_ms,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn get_desired(
        &self,
        node_id: impl Into<String>,
        stream_id: impl Into<String>,
    ) -> Result<Option<DesiredRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::GetDesired {
                node_id: node_id.into(),
                stream_id: stream_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn get_intent(
        &self,
        intent_id: impl Into<String>,
    ) -> Result<Option<IntentRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::GetIntent {
                intent_id: intent_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn list_intents(
        &self,
        node_id: Option<impl Into<String>>,
    ) -> Result<Vec<IntentRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ListIntents {
                node_id: node_id.map(Into::into),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn recover_reconciliation(&self, now_ms: u64) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::RecoverReconciliation { now_ms, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn wake_node(
        &self,
        node_id: impl Into<String>,
        now_ms: u64,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::WakeNode {
                node_id: node_id.into(),
                now_ms,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn list_events(
        &self,
        node_id: Option<impl Into<String>>,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ListEvents {
                node_id: node_id.map(Into::into),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn prune_events(&self, retain: usize) -> Result<usize, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::PruneEvents { retain, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn claim_attempt(
        &self,
        intent_id: impl Into<String>,
    ) -> Result<Option<AttemptRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ClaimAttempt {
                intent_id: intent_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn record_observed(&self, mutation: ObservedMutation) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::RecordObserved { mutation, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn mark_attempt_dispatched(
        &self,
        attempt_id: impl Into<String>,
        expires_at_ms: u64,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::MarkAttemptDispatched {
                attempt_id: attempt_id.into(),
                expires_at_ms,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn expire_attempts(&self, now_ms: u64) -> Result<usize, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ExpireAttempts { now_ms, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn complete_attempt(
        &self,
        attempt_id: impl Into<String>,
        state: impl Into<String>,
        failure_class: Option<String>,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::CompleteAttempt {
                attempt_id: attempt_id.into(),
                state: state.into(),
                failure_class,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn set_node_maintenance(
        &self,
        mutation: NodeMaintenanceMutation,
        now_ms: u64,
    ) -> Result<bool, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::SetNodeMaintenance {
                mutation,
                now_ms,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn get_node_maintenance(
        &self,
        node_id: impl Into<String>,
    ) -> Result<Option<String>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::GetNodeMaintenance {
                node_id: node_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn operational_aggregates(
        &self,
        now_ms: u64,
    ) -> Result<OperationalAggregates, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::OperationalAggregates { now_ms, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn mark_outbox_processed(
        &self,
        outbox_id: i64,
        now_ms: u64,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::MarkOutboxProcessed {
                outbox_id,
                now_ms,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn record_audit(&self, record: AuditRecord) -> Result<i64, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::RecordAudit { record, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn list_audit(
        &self,
        resource_id: Option<impl Into<String>>,
    ) -> Result<Vec<AuditRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ListAudit {
                resource_id: resource_id.map(Into::into),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn create_rollout(
        &self,
        rollout: RolloutRecord,
        targets: Vec<RolloutTargetRecord>,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::CreateRollout {
                rollout,
                targets,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn create_rollout_with_content(
        &self,
        rollout: RolloutRecord,
        targets: Vec<RolloutTargetRecord>,
        content: impl Into<String>,
        created_by: Option<String>,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::CreateRolloutWithContent {
                rollout,
                targets,
                content: content.into(),
                created_by,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn get_rollout(
        &self,
        rollout_id: impl Into<String>,
    ) -> Result<Option<RolloutRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::GetRollout {
                rollout_id: rollout_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn list_rollout_targets(
        &self,
        rollout_id: impl Into<String>,
    ) -> Result<Vec<RolloutTargetRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ListRolloutTargets {
                rollout_id: rollout_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn update_rollout(
        &self,
        rollout_id: impl Into<String>,
        state: impl Into<String>,
        current_batch: u32,
        updated_at_ms: u64,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::UpdateRollout {
                rollout_id: rollout_id.into(),
                state: state.into(),
                current_batch,
                updated_at_ms,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn update_rollout_target(
        &self,
        update: RolloutTargetUpdate,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::UpdateRolloutTarget { update, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn get_config_version_content(
        &self,
        config_version_id: impl Into<String>,
    ) -> Result<Option<String>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::GetConfigVersionContent {
                config_version_id: config_version_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn recover_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::RecoverRollouts { response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn list_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ListRollouts { response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn upsert_operation(
        &self,
        operation: PersistedOperation,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::UpsertOperation {
                operation,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn get_operation(
        &self,
        operation_id: impl Into<String>,
    ) -> Result<Option<PersistedOperation>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::GetOperation {
                operation_id: operation_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn list_operations(
        &self,
        node_id: Option<impl Into<String>>,
    ) -> Result<Vec<PersistedOperation>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ListOperations {
                node_id: node_id.map(Into::into),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }
}

impl ControlPlaneStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        let store = Self {
            connection: Arc::new(Mutex::new(connection)),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn in_memory() -> Result<Self, StorageError> {
        Self::open(":memory:")
    }

    pub fn with_connection<T>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T, rusqlite::Error>,
    ) -> Result<T, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(operation(&connection)?)
    }

    /// Execute one short state transition under SQLite's single-writer lock.
    /// Callers must not perform network or other awaitable work in `operation`.
    pub fn immediate_transaction<T>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let mut connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = operation(&transaction)?;
        transaction.commit()?;
        Ok(result)
    }

    /// Persist one desired-state mutation and its reconciliation wake-up as a
    /// single transaction. Dispatch is intentionally left to the reconciler.
    pub fn set_desired(&self, mutation: DesiredMutation) -> Result<IntentRecord, StorageError> {
        let now = now_ms();
        self.immediate_transaction(|transaction| {
            let intent_type = mutation.intent_type.as_deref().unwrap_or_else(|| {
                if mutation.action_id.is_some() {
                    "restart"
                } else {
                    "set_state"
                }
            });
            if let Some(idempotency_key) = mutation.idempotency_key.as_deref() {
                let existing: Option<(IntentRecord, Option<String>, Option<String>)> = transaction
                    .query_row(
                        "SELECT i.intent_id, i.node_id, i.stream_id, i.generation, i.state, i.desired_state, i.config_version_id, i.action_id, i.convergence_state, i.intent_type, i.payload_json, i.retry_count, i.next_retry_at_ms, i.last_failure_class, i.superseded_by_intent_id, i.created_at_ms, i.updated_at_ms, o.observed_generation, o.observed_state, (SELECT generation FROM cp_intents s WHERE s.intent_id = i.superseded_by_intent_id) FROM cp_intents i LEFT JOIN cp_stream_observed o ON o.node_id = i.node_id AND o.stream_id = i.stream_id WHERE i.node_id = ?1 AND i.stream_id = ?2 AND i.idempotency_key = ?3",
                        (&mutation.node_id, &mutation.stream_id, idempotency_key),
                        |row| {
                            Ok((
                                IntentRecord {
                                    intent_id: row.get(0)?,
                                    node_id: row.get(1)?,
                                    stream_id: row.get(2)?,
                                    generation: row.get(3)?,
                                    state: row.get(4)?,
                                    desired_state: row.get(5)?,
                                    config_version_id: row.get(6)?,
                                    action_id: row.get(7)?,
                                    convergence_state: row.get(8)?,
                                    retry_count: row.get(11)?,
                                    next_retry_at_ms: row.get(12)?,
                                    failure_class: row.get(13)?,
                                    superseded_by_intent_id: row.get(14)?,
                                    superseded_generation: row.get(19)?,
                                    created_at_ms: row.get(15)?,
                                    updated_at_ms: row.get(16)?,
                                    observed_generation: row.get(17)?,
                                    observed_state: row.get(18)?,
                                },
                                row.get(9)?,
                                row.get(10)?,
                            ))
                        },
                    )
                    .optional()?;
                if let Some((existing, stored_intent_type, payload_json)) = existing {
                    let requested_intent_type = Some(intent_type.to_owned());
                    if existing.desired_state != mutation.desired_state
                        || existing.config_version_id != mutation.config_version_id
                        || existing.action_id != mutation.action_id
                        || stored_intent_type != requested_intent_type
                        || payload_json != mutation.payload_json
                    {
                        return Err(StorageError::IdempotencyKeyReused);
                    }
                    return Ok(existing);
                }
            }
            let current: u64 = transaction
                .query_row(
                    "SELECT generation FROM cp_stream_desired WHERE node_id = ?1 AND stream_id = ?2",
                    (&mutation.node_id, &mutation.stream_id),
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or(0);
            if let Some(expected) = mutation.expected_generation {
                if expected != current {
                    return Err(StorageError::GenerationConflict { expected, current });
                }
            }
            let generation = current + 1;
            let intent_id = format!("intent-{generation}-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed));
            transaction.execute(
                "INSERT INTO cp_stream_desired (node_id, stream_id, generation, desired_state, config_version_id, desired_action_id, updated_at_ms, updated_by, correlation_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) ON CONFLICT(node_id, stream_id) DO UPDATE SET generation = excluded.generation, desired_state = excluded.desired_state, config_version_id = excluded.config_version_id, desired_action_id = excluded.desired_action_id, updated_at_ms = excluded.updated_at_ms, updated_by = excluded.updated_by, correlation_id = excluded.correlation_id",
                rusqlite::params![
                    mutation.node_id,
                    mutation.stream_id,
                    generation,
                    mutation.desired_state,
                    mutation.config_version_id,
                    mutation.action_id,
                    now,
                    mutation.actor,
                    mutation.correlation_id,
                ],
            )?;
            if let (Some(config_version_id), Some(payload_json)) = (
                mutation.config_version_id.as_deref(),
                mutation.payload_json.as_deref(),
            ) {
                transaction.execute(
                    "INSERT OR IGNORE INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms, created_by, correlation_id) VALUES (?1, 'inline-json', ?2, 'json', ?3, ?4, ?5)",
                    rusqlite::params![
                        config_version_id,
                        payload_json,
                        now,
                        mutation.actor,
                        mutation.correlation_id,
                    ],
                )?;
            }
            transaction.execute(
                "INSERT INTO cp_intents (intent_id, node_id, stream_id, generation, intent_type, desired_state, config_version_id, action_id, payload_json, state, convergence_state, created_at_ms, updated_at_ms, actor, correlation_id, idempotency_key) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'accepted', 'pending', ?10, ?10, ?11, ?12, ?13)",
                rusqlite::params![
                    intent_id,
                    mutation.node_id,
                    mutation.stream_id,
                    generation,
                    intent_type,
                    mutation.desired_state,
                    mutation.config_version_id,
                    mutation.action_id,
                    mutation.payload_json,
                    now,
                    mutation.actor,
                    mutation.correlation_id,
                    mutation.idempotency_key,
                ],
            )?;
            transaction.execute(
                "UPDATE cp_intents SET state = 'superseded', convergence_state = 'pending', superseded_by_intent_id = ?1, updated_at_ms = ?2 WHERE node_id = ?3 AND stream_id = ?4 AND state IN ('accepted', 'converging', 'retrying') AND generation < ?5",
                (&intent_id, now, &mutation.node_id, &mutation.stream_id, generation),
            )?;
            let event_key = format!("reconcile:{intent_id}:{generation}");
            transaction.execute(
                "INSERT INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) VALUES (?1, 'reconcile_intent', ?2, ?3, ?4, ?5, ?5)",
                (&event_key, &mutation.node_id, &mutation.stream_id, &intent_id, now),
            )?;
            transaction.execute(
                "INSERT INTO cp_events (node_id, stream_id, intent_id, event_type, outcome, generation, correlation_id, occurred_at_ms) VALUES (?1, ?2, ?3, 'intent_created', 'accepted', ?4, ?5, ?6)",
                (&mutation.node_id, &mutation.stream_id, &intent_id, generation, &mutation.correlation_id, now),
            )?;
            transaction.execute(
                "INSERT INTO cp_audit_events (actor, action, resource_type, resource_id, node_id, stream_id, correlation_id, outcome, occurred_at_ms) VALUES (?1, ?2, 'stream', ?3, ?4, ?5, ?6, 'accepted', ?7)",
                rusqlite::params![
                    mutation.actor,
                    intent_type,
                    format!("{}:{}", mutation.node_id, mutation.stream_id),
                    mutation.node_id,
                    mutation.stream_id,
                    mutation.correlation_id,
                    now,
                ],
            )?;
            Ok(IntentRecord {
                intent_id,
                node_id: mutation.node_id,
                stream_id: mutation.stream_id,
                generation,
                state: "accepted".into(),
                desired_state: mutation.desired_state,
                config_version_id: mutation.config_version_id,
                action_id: mutation.action_id,
                convergence_state: "pending".into(),
                retry_count: 0,
                next_retry_at_ms: None,
                failure_class: None,
                superseded_by_intent_id: None,
                superseded_generation: None,
                created_at_ms: now,
                updated_at_ms: now,
                observed_generation: None,
                observed_state: None,
            })
        })
    }

    pub fn upsert_node(&self, mutation: NodeMutation) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO cp_nodes (node_id, role, protocol_version, node_version, state, capabilities_json, boot_id, last_report_seq, last_seen_at_ms, lease_expires_at_ms, maintenance_state, maintenance_updated_at_ms, created_at_ms, updated_at_ms) VALUES (?1, 'compute', 'v1', ?2, ?3, ?4, ?5, ?6, ?7, ?8, COALESCE(?9, 'active'), ?10, ?7, ?7) ON CONFLICT(node_id) DO UPDATE SET node_version = excluded.node_version, state = excluded.state, capabilities_json = excluded.capabilities_json, boot_id = excluded.boot_id, last_report_seq = excluded.last_report_seq, last_seen_at_ms = excluded.last_seen_at_ms, lease_expires_at_ms = excluded.lease_expires_at_ms, updated_at_ms = excluded.updated_at_ms",
                rusqlite::params![
                    mutation.node_id,
                    mutation.version,
                    mutation.state,
                    mutation.capabilities_json,
                    mutation.boot_id,
                    mutation.report_seq,
                    mutation.last_seen_at_ms,
                    mutation.lease_expires_at_ms,
                    mutation.maintenance_state,
                    mutation.maintenance_updated_at_ms,
                ],
            )?;
            Ok(())
        })
    }

    pub fn set_node_maintenance(
        &self,
        mutation: NodeMaintenanceMutation,
        now_ms: u64,
    ) -> Result<bool, StorageError> {
        let state = mutation.state.as_str();
        if !matches!(state, "active" | "draining" | "maintenance") {
            return Ok(false);
        }
        self.immediate_transaction(|transaction| {
            let previous: Option<String> = transaction
                .query_row(
                    "SELECT COALESCE(maintenance_state, 'active') FROM cp_nodes WHERE node_id = ?1",
                    [&mutation.node_id],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(previous) = previous else { return Ok(false) };
            if previous != state {
                transaction.execute(
                    "UPDATE cp_nodes SET maintenance_state = ?1, maintenance_updated_at_ms = ?2, updated_at_ms = ?2 WHERE node_id = ?3",
                    rusqlite::params![state, now_ms, mutation.node_id],
                )?;
                transaction.execute(
                    "INSERT INTO cp_events (node_id, event_type, outcome, message, correlation_id, actor, occurred_at_ms) VALUES (?1, 'node_maintenance_changed', 'succeeded', ?2, ?3, ?4, ?5)",
                    rusqlite::params![mutation.node_id, format!("{previous}->{state}"), mutation.correlation_id, mutation.actor, now_ms],
                )?;
                transaction.execute(
                    "INSERT INTO cp_audit_events (actor, action, resource_type, resource_id, node_id, correlation_id, outcome, message, occurred_at_ms) VALUES (?1, 'node.maintenance', 'node', ?2, ?2, ?3, 'accepted', ?4, ?5)",
                    rusqlite::params![mutation.actor, mutation.node_id, mutation.correlation_id, format!("{previous}->{state}"), now_ms],
                )?;
            }
            Ok(true)
        })
    }

    pub fn get_node_maintenance(&self, node_id: &str) -> Result<Option<String>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT COALESCE(maintenance_state, 'active') FROM cp_nodes WHERE node_id = ?1",
                    [node_id],
                    |row| row.get(0),
                )
                .optional()
        })
    }

    pub fn operational_aggregates(
        &self,
        now_ms: u64,
    ) -> Result<OperationalAggregates, StorageError> {
        self.with_connection(|connection| {
            let grouped = |sql: &str| -> Result<Vec<(String, u64)>, rusqlite::Error> {
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64)))?;
                rows.collect()
            };
            let scalar = |sql: &str| -> Result<u64, rusqlite::Error> {
                connection.query_row(sql, [], |row| row.get::<_, i64>(0)).map(|v| v as u64)
            };
            let oldest: Option<i64> = connection.query_row(
                "SELECT MIN(created_at_ms) FROM cp_outbox WHERE processed_at_ms IS NULL", [], |row| row.get(0)
            ).optional()?.flatten();
            Ok(OperationalAggregates {
                node_states: grouped("SELECT state, COUNT(*) FROM cp_nodes GROUP BY state")?,
                maintenance_states: grouped("SELECT COALESCE(maintenance_state, 'active'), COUNT(*) FROM cp_nodes GROUP BY COALESCE(maintenance_state, 'active')")?,
                intent_states: grouped("SELECT state, COUNT(*) FROM cp_intents GROUP BY state")?,
                convergence_states: grouped("SELECT convergence_state, COUNT(*) FROM cp_intents GROUP BY convergence_state")?,
                attempt_states: grouped("SELECT state, COUNT(*) FROM cp_attempts GROUP BY state")?,
                failure_classes: grouped("SELECT COALESCE(last_failure_class, 'none'), COUNT(*) FROM cp_intents GROUP BY COALESCE(last_failure_class, 'none')")?,
                outbox_pending: scalar("SELECT COUNT(*) FROM cp_outbox WHERE processed_at_ms IS NULL")?,
                outbox_claimed: scalar("SELECT COUNT(*) FROM cp_outbox WHERE processed_at_ms IS NULL AND claimed_at_ms IS NOT NULL")?,
                stale_nodes: connection.query_row("SELECT COUNT(*) FROM cp_nodes WHERE state = 'stale' OR lease_expires_at_ms <= ?1", [now_ms], |row| row.get::<_, i64>(0)).map(|v| v as u64)?,
                active_attempts: scalar("SELECT COUNT(*) FROM cp_attempts WHERE state IN ('queued','dispatched','acknowledged','running')")?,
                non_terminal_intents: scalar("SELECT COUNT(*) FROM cp_intents WHERE state IN ('accepted','converging','retrying')")?,
                oldest_pending_age_seconds: oldest.map(|created| now_ms.saturating_sub(created as u64) / 1000),
            })
        })
    }

    #[allow(clippy::type_complexity)]
    pub fn claim_outbox(
        &self,
        worker_id: &str,
        now_ms: u64,
    ) -> Result<Option<OutboxRecord>, StorageError> {
        const CLAIM_LEASE_MS: u64 = 30_000;
        self.immediate_transaction(|transaction| {
            let candidate: Option<(i64, String, String, String, Option<String>, Option<String>)> =
                transaction
                    .query_row(
                        "SELECT outbox_id, event_key, event_type, node_id, stream_id, intent_id FROM cp_outbox WHERE processed_at_ms IS NULL AND available_at_ms <= ?1 AND (claimed_at_ms IS NULL OR claimed_at_ms < ?2) ORDER BY outbox_id LIMIT 1",
                        rusqlite::params![now_ms, now_ms.saturating_sub(CLAIM_LEASE_MS)],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                                row.get(5)?,
                            ))
                        },
                    )
                    .optional()?;
            let Some((outbox_id, event_key, event_type, node_id, stream_id, intent_id)) = candidate
            else {
                return Ok(None);
            };
            let updated = transaction.execute(
                "UPDATE cp_outbox SET claimed_at_ms = ?1, worker_id = ?2 WHERE outbox_id = ?3 AND processed_at_ms IS NULL AND (claimed_at_ms IS NULL OR claimed_at_ms < ?4)",
                rusqlite::params![now_ms, worker_id, outbox_id, now_ms.saturating_sub(CLAIM_LEASE_MS)],
            )?;
            if updated != 1 {
                return Ok(None);
            }
            Ok(Some(OutboxRecord {
                outbox_id,
                event_key,
                event_type,
                node_id,
                stream_id,
                intent_id,
            }))
        })
    }

    pub fn get_desired(
        &self,
        node_id: &str,
        stream_id: &str,
    ) -> Result<Option<DesiredRecord>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT node_id, stream_id, generation, desired_state, config_version_id, desired_action_id, correlation_id FROM cp_stream_desired WHERE node_id = ?1 AND stream_id = ?2",
                    (node_id, stream_id),
                    |row| {
                        Ok(DesiredRecord {
                            node_id: row.get(0)?,
                            stream_id: row.get(1)?,
                            generation: row.get(2)?,
                            desired_state: row.get(3)?,
                            config_version_id: row.get(4)?,
                            action_id: row.get(5)?,
                            correlation_id: row.get(6)?,
                        })
                    },
                )
                .optional()
        })
    }

    pub fn get_intent(&self, intent_id: &str) -> Result<Option<IntentRecord>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT i.intent_id, i.node_id, i.stream_id, i.generation, i.state, i.desired_state, i.config_version_id, i.action_id, i.convergence_state, i.retry_count, i.next_retry_at_ms, i.last_failure_class, i.superseded_by_intent_id, i.created_at_ms, i.updated_at_ms, o.observed_generation, o.observed_state, (SELECT generation FROM cp_intents s WHERE s.intent_id = i.superseded_by_intent_id) FROM cp_intents i LEFT JOIN cp_stream_observed o ON o.node_id = i.node_id AND o.stream_id = i.stream_id WHERE i.intent_id = ?1",
                    [intent_id],
                    |row| {
                        Ok(IntentRecord {
                            intent_id: row.get(0)?,
                            node_id: row.get(1)?,
                            stream_id: row.get(2)?,
                            generation: row.get(3)?,
                            state: row.get(4)?,
                            desired_state: row.get(5)?,
                            config_version_id: row.get(6)?,
                            action_id: row.get(7)?,
                            convergence_state: row.get(8)?,
                            retry_count: row.get(9)?,
                            next_retry_at_ms: row.get(10)?,
                            failure_class: row.get(11)?,
                            superseded_by_intent_id: row.get(12)?,
                            superseded_generation: row.get(17)?,
                            created_at_ms: row.get(13)?,
                            updated_at_ms: row.get(14)?,
                            observed_generation: row.get(15)?,
                            observed_state: row.get(16)?,
                        })
                    },
                )
                .optional()
        })
    }

    pub fn list_intents(&self, node_id: Option<&str>) -> Result<Vec<IntentRecord>, StorageError> {
        let ids = self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT intent_id FROM cp_intents WHERE (?1 IS NULL OR node_id = ?1) ORDER BY created_at_ms DESC, intent_id DESC LIMIT 4096",
            )?;
            let rows = statement.query_map([node_id], |row| row.get::<_, String>(0))?;
            rows.collect::<Result<Vec<_>, _>>()
        })?;
        let mut intents = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(intent) = self.get_intent(&id)? {
                intents.push(intent);
            }
        }
        Ok(intents)
    }

    pub fn recover_reconciliation(&self, now_ms: u64) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) SELECT 'reconcile:recovery:' || i.intent_id || ':' || ?1, 'reconcile_intent', i.node_id, i.stream_id, i.intent_id, ?1, ?1 FROM cp_intents i WHERE i.state IN ('accepted', 'converging', 'retrying') AND (i.last_failure_class IS NULL OR i.last_failure_class <> 'ambiguous') AND NOT EXISTS (SELECT 1 FROM cp_outbox o WHERE o.intent_id = i.intent_id AND o.processed_at_ms IS NULL)",
                [now_ms],
            )?;
            Ok(())
        })
    }

    pub fn wake_node(&self, node_id: &str, now_ms: u64) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT OR IGNORE INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) SELECT 'reconcile:register:' || i.intent_id, 'reconcile_intent', i.node_id, i.stream_id, i.intent_id, ?1, ?1 FROM cp_intents i WHERE i.node_id = ?2 AND i.state IN ('accepted', 'converging', 'retrying') AND (i.last_failure_class IS NULL OR i.last_failure_class <> 'ambiguous') AND NOT EXISTS (SELECT 1 FROM cp_outbox o WHERE o.intent_id = i.intent_id AND o.processed_at_ms IS NULL)",
                rusqlite::params![now_ms, node_id],
            )?;
            Ok(())
        })
    }

    pub fn list_events(&self, node_id: Option<&str>) -> Result<Vec<StoredEvent>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT event_id, node_id, stream_id, intent_id, attempt_id, event_type, outcome, failure_class, message, generation, correlation_id, occurred_at_ms, actor FROM cp_events WHERE (?1 IS NULL OR node_id = ?1) ORDER BY event_id DESC LIMIT 2048",
            )?;
            let rows = statement.query_map([node_id], |row| {
                Ok(StoredEvent {
                    event_id: row.get(0)?,
                    node_id: row.get(1)?,
                    stream_id: row.get(2)?,
                    intent_id: row.get(3)?,
                    attempt_id: row.get(4)?,
                    event_type: row.get(5)?,
                    outcome: row.get(6)?,
                    failure_class: row.get(7)?,
                    message: row.get(8)?,
                    generation: row.get(9)?,
                    correlation_id: row.get(10)?,
                    occurred_at_ms: row.get(11)?,
                    actor: row.get(12)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn prune_events(&self, retain: usize) -> Result<usize, StorageError> {
        self.immediate_transaction(|transaction| {
            let deleted = transaction.execute(
                "DELETE FROM cp_events WHERE event_id NOT IN (SELECT event_id FROM cp_events ORDER BY event_id DESC LIMIT ?1)",
                [retain as i64],
            )?;
            Ok(deleted)
        })
    }

    #[allow(clippy::type_complexity)]
    pub fn claim_attempt(&self, intent_id: &str) -> Result<Option<AttemptRecord>, StorageError> {
        self.immediate_transaction(|transaction| {
            if let Some(attempt) = transaction
                .query_row(
                    "SELECT a.attempt_id, a.intent_id, a.command_id, a.state, a.failure_class, a.node_id, a.stream_id, a.generation, a.operation, i.action_id, i.config_version_id, i.intent_type, COALESCE(i.payload_json, cv.content_ref) FROM cp_attempts a JOIN cp_intents i ON i.intent_id = a.intent_id LEFT JOIN cp_config_versions cv ON cv.config_version_id = i.config_version_id WHERE a.intent_id = ?1 AND a.state IN ('queued', 'dispatched', 'acknowledged', 'running') ORDER BY a.created_at_ms DESC LIMIT 1",
                    [intent_id],
                    |row| {
                        Ok(AttemptRecord {
                            attempt_id: row.get(0)?,
                            intent_id: row.get(1)?,
                            command_id: row.get(2)?,
                            state: row.get(3)?,
                            failure_class: row.get(4)?,
                            node_id: row.get(5)?,
                            stream_id: row.get(6)?,
                            generation: row.get(7)?,
                            operation: row.get(8)?,
                            action_id: row.get(9)?,
                            config_version_id: row.get(10)?,
                            payload_json: row.get(12)?,
                        })
                    },
                )
                .optional()?
            {
                return Ok(Some(attempt));
            }
            let target: Option<(
                String,
                String,
                u64,
                String,
                Option<String>,
                Option<String>,
                String,
                Option<String>,
            )> = transaction
                .query_row(
                    "SELECT i.node_id, i.stream_id, i.generation, COALESCE(i.desired_state, ''), i.action_id, i.config_version_id, i.intent_type, COALESCE(i.payload_json, cv.content_ref) FROM cp_intents i LEFT JOIN cp_config_versions cv ON cv.config_version_id = i.config_version_id WHERE i.intent_id = ?1 AND i.state IN ('accepted', 'converging', 'retrying')",
                    [intent_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                            row.get(7)?,
                        ))
                    },
                )
                .optional()?;
            let Some((
                node_id,
                stream_id,
                generation,
                desired_state,
                action_id,
                config_version_id,
                intent_type,
                payload_json,
            )) = target
            else {
                return Ok(None);
            };
            let operation = if intent_type == "apply_configuration" {
                "apply_configuration"
            } else if action_id.is_some() {
                "restart"
            } else if desired_state == "running" {
                "start"
            } else {
                "stop"
            };
            let suffix = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let attempt_id = format!("attempt-{suffix}");
            let command_id = format!("cmd-{suffix}");
            let now = now_ms();
            transaction.execute(
                "INSERT INTO cp_attempts (attempt_id, intent_id, command_id, node_id, stream_id, generation, operation, state, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'queued', ?8)",
                rusqlite::params![
                    attempt_id,
                    intent_id,
                    command_id,
                    node_id,
                    stream_id,
                    generation,
                    operation,
                    now,
                ],
            )?;
            Ok(Some(AttemptRecord {
                attempt_id,
                intent_id: intent_id.into(),
                command_id,
                state: "queued".into(),
                failure_class: None,
                node_id,
                stream_id,
                generation,
                operation: operation.into(),
                action_id,
                config_version_id,
                payload_json,
            }))
        })
    }

    pub fn complete_attempt(
        &self,
        attempt_id: &str,
        state: &str,
        failure_class: Option<&str>,
    ) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            let attempt: Option<(String, String, String, u64)> = transaction
                .query_row(
                    "SELECT intent_id, node_id, stream_id, generation FROM cp_attempts WHERE attempt_id = ?1",
                    [attempt_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let Some((intent_id, node_id, stream_id, _generation)) = attempt else {
                return Ok(());
            };
            let ambiguous = state == "ambiguous" || failure_class == Some("ambiguous");
            let terminal = ambiguous || matches!(
                state,
                "succeeded"
                    | "failed"
                    | "timed_out"
                    | "node_unavailable"
                    | "cancelled"
                    | "superseded"
            );
            transaction.execute(
                "UPDATE cp_attempts SET state = ?1, failure_class = ?2, finished_at_ms = CASE WHEN ?3 THEN ?4 ELSE finished_at_ms END WHERE attempt_id = ?5",
                rusqlite::params![state, failure_class, terminal, now_ms(), attempt_id],
            )?;
            if terminal {
                match failure_class {
                    Some("temporary_execution") | Some("transport") | Some("node_unavailable") => {
                        let retry_at = now_ms() + 1_000;
                        transaction.execute(
                            "UPDATE cp_intents SET state = 'retrying', convergence_state = 'degraded', retry_count = retry_count + 1, next_retry_at_ms = ?1, last_failure_class = ?2, updated_at_ms = ?1 WHERE intent_id = ?3 AND state IN ('accepted', 'converging', 'retrying')",
                            rusqlite::params![retry_at, failure_class, intent_id],
                        )?;
                        let event_key = format!("reconcile:retry:{attempt_id}");
                        transaction.execute(
                            "INSERT OR IGNORE INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) VALUES (?1, 'retry_intent', ?2, ?3, ?4, ?5, ?5)",
                            rusqlite::params![event_key, node_id, stream_id, intent_id, retry_at],
                        )?;
                    }
                    Some("stale_generation") => {
                        transaction.execute(
                            "UPDATE cp_intents SET state = 'superseded', convergence_state = 'degraded', last_failure_class = ?1, updated_at_ms = ?2 WHERE intent_id = ?3 AND state IN ('accepted', 'converging', 'retrying')",
                            rusqlite::params![failure_class, now_ms(), intent_id],
                        )?;
                    }
                    Some("ambiguous") => {
                        transaction.execute(
                            "UPDATE cp_intents SET state = 'converging', convergence_state = 'degraded', next_retry_at_ms = NULL, last_failure_class = ?1, updated_at_ms = ?2 WHERE intent_id = ?3 AND state IN ('accepted', 'converging', 'retrying')",
                            rusqlite::params![failure_class, now_ms(), intent_id],
                        )?;
                    }
                    Some(_) if state != "succeeded" => {
                        transaction.execute(
                            "UPDATE cp_intents SET state = 'blocked', convergence_state = 'blocked', last_failure_class = ?1, updated_at_ms = ?2 WHERE intent_id = ?3 AND state IN ('accepted', 'converging', 'retrying')",
                            rusqlite::params![failure_class, now_ms(), intent_id],
                        )?;
                    }
                    None if state != "succeeded" => {
                        transaction.execute(
                            "UPDATE cp_intents SET state = 'blocked', convergence_state = 'blocked', updated_at_ms = ?1 WHERE intent_id = ?2 AND state IN ('accepted', 'converging', 'retrying')",
                            rusqlite::params![now_ms(), intent_id],
                        )?;
                    }
                    _ => {}
                }
            }
            transaction.execute(
                "INSERT INTO cp_events (node_id, stream_id, intent_id, attempt_id, event_type, outcome, failure_class, generation, occurred_at_ms) VALUES (?1, ?2, ?3, ?4, 'attempt_completed', ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    node_id,
                    stream_id,
                    intent_id,
                    attempt_id,
                    state,
                    failure_class,
                    _generation,
                    now_ms(),
                ],
            )?;
            Ok(())
        })
    }

    pub fn mark_attempt_dispatched(
        &self,
        attempt_id: &str,
        expires_at_ms: u64,
    ) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "UPDATE cp_attempts SET state = 'dispatched', dispatched_at_ms = ?1, expires_at_ms = ?2 WHERE attempt_id = ?3 AND state = 'queued'",
                rusqlite::params![now_ms(), expires_at_ms, attempt_id],
            )?;
            Ok(())
        })
    }

    pub fn expire_attempts(&self, now_ms: u64) -> Result<usize, StorageError> {
        self.immediate_transaction(|transaction| {
            let expired: Vec<(String, String, String, String)> = transaction
                .prepare(
                    "SELECT attempt_id, intent_id, node_id, stream_id FROM cp_attempts WHERE state IN ('queued', 'dispatched', 'acknowledged', 'running') AND expires_at_ms IS NOT NULL AND expires_at_ms <= ?1",
                )?
                .query_map([now_ms], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            for (attempt_id, intent_id, node_id, stream_id) in &expired {
                transaction.execute(
                    "UPDATE cp_attempts SET state = 'ambiguous', failure_class = 'ambiguous', finished_at_ms = ?1 WHERE attempt_id = ?2 AND state IN ('queued', 'dispatched', 'acknowledged', 'running')",
                    rusqlite::params![now_ms, attempt_id],
                )?;
                transaction.execute(
                    "UPDATE cp_intents SET state = 'converging', convergence_state = 'degraded', next_retry_at_ms = NULL, last_failure_class = 'ambiguous', updated_at_ms = ?1 WHERE intent_id = ?2 AND state IN ('accepted', 'converging', 'retrying')",
                    rusqlite::params![now_ms, intent_id],
                )?;
                transaction.execute(
                    "INSERT INTO cp_events (node_id, stream_id, intent_id, event_type, outcome, failure_class, message, occurred_at_ms) VALUES (?1, ?2, ?3, 'attempt_expired', 'ambiguous', 'ambiguous', 'Attempt lease expired; waiting for a fresh observed report', ?4)",
                    rusqlite::params![node_id, stream_id, intent_id, now_ms],
                )?;
            }
            Ok(expired.len())
        })
    }

    pub fn record_observed(&self, mutation: ObservedMutation) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            let current: Option<(Option<String>, u64)> = transaction
                .query_row(
                    "SELECT boot_id, COALESCE(report_seq, 0) FROM cp_stream_observed WHERE node_id = ?1 AND stream_id = ?2",
                    (&mutation.node_id, &mutation.stream_id),
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if let Some((boot_id, report_seq)) = current {
                if boot_id.as_deref() == mutation.boot_id.as_deref()
                    && mutation.report_seq <= report_seq
                {
                    return Ok(());
                }
            }
            let now = now_ms();
            transaction.execute(
                "INSERT INTO cp_stream_observed (node_id, stream_id, boot_id, report_seq, observed_generation, observed_state, applied_config_version, last_action_id, last_error_code, last_error_message, snapshot_json, observed_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12) ON CONFLICT(node_id, stream_id) DO UPDATE SET boot_id = excluded.boot_id, report_seq = excluded.report_seq, observed_generation = excluded.observed_generation, observed_state = excluded.observed_state, applied_config_version = excluded.applied_config_version, last_action_id = excluded.last_action_id, last_error_code = excluded.last_error_code, last_error_message = excluded.last_error_message, snapshot_json = excluded.snapshot_json, observed_at_ms = excluded.observed_at_ms",
                rusqlite::params![
                    mutation.node_id,
                    mutation.stream_id,
                    mutation.boot_id,
                    mutation.report_seq,
                    mutation.observed_generation,
                    mutation.observed_state,
                    mutation.config_version_id,
                    mutation.action_id,
                    mutation.last_error_code,
                    mutation.last_error_message,
                    mutation.snapshot_json,
                    now,
                ],
            )?;
            transaction.execute(
                "INSERT INTO cp_events (node_id, stream_id, event_type, outcome, message, generation, occurred_at_ms) VALUES (?1, ?2, 'observed_report', ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    mutation.node_id,
                    mutation.stream_id,
                    mutation.observed_state,
                    mutation.last_error_message,
                    mutation.observed_generation,
                    now,
                ],
            )?;
            let desired: Option<(u64, String, Option<String>, Option<String>)> = transaction
                .query_row(
                    "SELECT generation, desired_state, config_version_id, desired_action_id FROM cp_stream_desired WHERE node_id = ?1 AND stream_id = ?2",
                    (&mutation.node_id, &mutation.stream_id),
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            if let Some((generation, desired_state, desired_config, desired_action_id)) = desired {
                let config_matches = desired_config
                    .as_deref()
                    .is_none_or(|version| Some(version) == mutation.config_version_id.as_deref());
                let action_matches = desired_action_id
                    .as_deref()
                    .is_none_or(|action_id| Some(action_id) == mutation.action_id.as_deref());
                let affected_streams_converged = if mutation.stream_id == "__configuration__" {
                    let blockers: i64 = transaction.query_row(
                        "SELECT COUNT(*) FROM cp_stream_desired d LEFT JOIN cp_stream_observed o ON o.node_id = d.node_id AND o.stream_id = d.stream_id WHERE d.node_id = ?1 AND d.stream_id <> '__configuration__' AND (o.stream_id IS NULL OR o.observed_generation <> d.generation OR o.observed_state <> d.desired_state OR o.applied_config_version IS NULL OR o.applied_config_version <> ?2)",
                        rusqlite::params![mutation.node_id, mutation.config_version_id],
                        |row| row.get(0),
                    )?;
                    blockers == 0
                } else {
                    true
                };
                if mutation.observed_generation == Some(generation)
                    && desired_state == mutation.observed_state
                    && config_matches
                    && action_matches
                    && affected_streams_converged
                {
                    transaction.execute(
                        "UPDATE cp_intents SET state = 'converged', convergence_state = 'in_sync', converged_at_ms = ?1, updated_at_ms = ?1 WHERE node_id = ?2 AND stream_id = ?3 AND generation = ?4 AND state IN ('accepted', 'converging', 'retrying')",
                        rusqlite::params![now, mutation.node_id, mutation.stream_id, generation],
                    )?;
                    transaction.execute(
                        "UPDATE cp_attempts SET state = 'succeeded', finished_at_ms = ?1 WHERE node_id = ?2 AND stream_id = ?3 AND generation = ?4 AND state IN ('queued', 'dispatched', 'acknowledged', 'running')",
                        rusqlite::params![now, mutation.node_id, mutation.stream_id, generation],
                    )?;
                    let intent_id: Option<String> = transaction
                        .query_row(
                            "SELECT intent_id FROM cp_intents WHERE node_id = ?1 AND stream_id = ?2 AND generation = ?3 ORDER BY created_at_ms DESC LIMIT 1",
                            rusqlite::params![mutation.node_id, mutation.stream_id, generation],
                            |row| row.get(0),
                        )
                        .optional()?;
                    transaction.execute(
                        "INSERT INTO cp_events (node_id, stream_id, intent_id, event_type, outcome, generation, occurred_at_ms) VALUES (?1, ?2, ?3, 'intent_converged', 'converged', ?4, ?5)",
                        rusqlite::params![mutation.node_id, mutation.stream_id, intent_id, generation, now],
                    )?;
                } else if mutation.stream_id == "__configuration__"
                    && mutation.observed_generation == Some(generation)
                    && desired_state == mutation.observed_state
                    && config_matches
                    && action_matches
                {
                    transaction.execute(
                        "UPDATE cp_intents SET state = 'converging', convergence_state = 'applying', updated_at_ms = ?1 WHERE node_id = ?2 AND stream_id = ?3 AND generation = ?4 AND state IN ('accepted', 'converging', 'retrying')",
                        rusqlite::params![now, mutation.node_id, mutation.stream_id, generation],
                    )?;
                }
            }
            let wake_key = format!(
                "reconcile:observed:{}:{}:{}:{}",
                mutation.node_id,
                mutation.stream_id,
                mutation.boot_id.as_deref().unwrap_or("unknown"),
                mutation.report_seq
            );
            transaction.execute(
                "INSERT OR IGNORE INTO cp_outbox (event_key, event_type, node_id, stream_id, intent_id, available_at_ms, created_at_ms) SELECT ?1, 'reconcile_intent', d.node_id, d.stream_id, i.intent_id, ?2, ?2 FROM cp_stream_desired d JOIN cp_intents i ON i.node_id = d.node_id AND i.stream_id = d.stream_id AND i.generation = d.generation WHERE d.node_id = ?3 AND d.stream_id = ?4 AND i.state IN ('accepted', 'converging', 'retrying') AND NOT EXISTS (SELECT 1 FROM cp_outbox o WHERE o.intent_id = i.intent_id AND o.processed_at_ms IS NULL)",
                rusqlite::params![wake_key, now, mutation.node_id, mutation.stream_id],
            )?;
            Ok(())
        })
    }

    pub fn mark_outbox_processed(&self, outbox_id: i64, now_ms: u64) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "UPDATE cp_outbox SET processed_at_ms = ?1 WHERE outbox_id = ?2 AND processed_at_ms IS NULL",
                rusqlite::params![now_ms, outbox_id],
            )?;
            Ok(())
        })
    }

    pub fn record_audit(&self, record: AuditRecord) -> Result<i64, StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO cp_audit_events (actor, action, resource_type, resource_id, node_id, stream_id, correlation_id, outcome, failure_code, message, occurred_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![
                    record.actor,
                    record.action,
                    record.resource_type,
                    record.resource_id,
                    record.node_id,
                    record.stream_id,
                    record.correlation_id,
                    record.outcome,
                    record.failure_code,
                    record.message,
                    record.occurred_at_ms,
                ],
            )?;
            Ok(transaction.last_insert_rowid())
        })
    }

    pub fn list_audit(&self, resource_id: Option<&str>) -> Result<Vec<AuditRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT event_id, actor, action, resource_type, resource_id, node_id, stream_id, correlation_id, outcome, failure_code, message, occurred_at_ms FROM cp_audit_events WHERE (?1 IS NULL OR resource_id = ?1) ORDER BY event_id DESC LIMIT 1024",
            )?;
            let rows = statement.query_map([resource_id], |row| {
                Ok(AuditRecord {
                    event_id: row.get(0)?,
                    actor: row.get(1)?,
                    action: row.get(2)?,
                    resource_type: row.get(3)?,
                    resource_id: row.get(4)?,
                    node_id: row.get(5)?,
                    stream_id: row.get(6)?,
                    correlation_id: row.get(7)?,
                    outcome: row.get(8)?,
                    failure_code: row.get(9)?,
                    message: row.get(10)?,
                    occurred_at_ms: row.get(11)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn create_rollout(
        &self,
        rollout: RolloutRecord,
        targets: Vec<RolloutTargetRecord>,
    ) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO cp_rollouts (rollout_id, config_version_id, state, batch_size, current_batch, total_targets, actor, correlation_id, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                rusqlite::params![
                    rollout.rollout_id,
                    rollout.config_version_id,
                    rollout.state,
                    rollout.batch_size,
                    rollout.current_batch,
                    rollout.total_targets,
                    rollout.actor,
                    rollout.correlation_id,
                    rollout.created_at_ms,
                    rollout.updated_at_ms,
                ],
            )?;
            for target in targets {
                transaction.execute(
                    "INSERT INTO cp_rollout_targets (rollout_id, node_id, ordinal, state, attempt_id, error, observed_config_version, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        target.rollout_id,
                        target.node_id,
                        target.ordinal,
                        target.state,
                        target.attempt_id,
                        target.error,
                        target.observed_config_version,
                        target.updated_at_ms,
                    ],
                )?;
            }
            Ok(())
        })
    }

    pub fn create_rollout_with_content(
        &self,
        rollout: RolloutRecord,
        targets: Vec<RolloutTargetRecord>,
        content: &str,
        created_by: Option<&str>,
    ) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT OR IGNORE INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms, created_by) VALUES (?1, 'inline-json', ?2, 'json', ?3, ?4)",
                rusqlite::params![
                    rollout.config_version_id,
                    content,
                    rollout.created_at_ms,
                    created_by,
                ],
            )?;
            transaction.execute(
                "INSERT INTO cp_rollouts (rollout_id, config_version_id, state, batch_size, current_batch, total_targets, actor, correlation_id, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                rusqlite::params![
                    rollout.rollout_id,
                    rollout.config_version_id,
                    rollout.state,
                    rollout.batch_size,
                    rollout.current_batch,
                    rollout.total_targets,
                    rollout.actor,
                    rollout.correlation_id,
                    rollout.created_at_ms,
                    rollout.updated_at_ms,
                ],
            )?;
            for target in targets {
                transaction.execute(
                    "INSERT INTO cp_rollout_targets (rollout_id, node_id, ordinal, state, attempt_id, error, observed_config_version, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        target.rollout_id,
                        target.node_id,
                        target.ordinal,
                        target.state,
                        target.attempt_id,
                        target.error,
                        target.observed_config_version,
                        target.updated_at_ms,
                    ],
                )?;
            }
            Ok(())
        })
    }

    pub fn get_rollout(&self, rollout_id: &str) -> Result<Option<RolloutRecord>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT rollout_id, config_version_id, state, batch_size, current_batch, total_targets, actor, correlation_id, created_at_ms, updated_at_ms FROM cp_rollouts WHERE rollout_id = ?1",
                    [rollout_id],
                    |row| {
                        Ok(RolloutRecord {
                            rollout_id: row.get(0)?,
                            config_version_id: row.get(1)?,
                            state: row.get(2)?,
                            batch_size: row.get(3)?,
                            current_batch: row.get(4)?,
                            total_targets: row.get(5)?,
                            actor: row.get(6)?,
                            correlation_id: row.get(7)?,
                            created_at_ms: row.get(8)?,
                            updated_at_ms: row.get(9)?,
                        })
                    },
                )
                .optional()
        })
    }

    pub fn list_rollout_targets(
        &self,
        rollout_id: &str,
    ) -> Result<Vec<RolloutTargetRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT rollout_id, node_id, ordinal, state, attempt_id, error, observed_config_version, updated_at_ms FROM cp_rollout_targets WHERE rollout_id = ?1 ORDER BY ordinal, node_id",
            )?;
            let rows = statement.query_map([rollout_id], |row| {
                Ok(RolloutTargetRecord {
                    rollout_id: row.get(0)?,
                    node_id: row.get(1)?,
                    ordinal: row.get(2)?,
                    state: row.get(3)?,
                    attempt_id: row.get(4)?,
                    error: row.get(5)?,
                    observed_config_version: row.get(6)?,
                    updated_at_ms: row.get(7)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn update_rollout(
        &self,
        rollout_id: &str,
        state: &str,
        current_batch: u32,
        updated_at_ms: u64,
    ) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "UPDATE cp_rollouts SET state = ?1, current_batch = ?2, updated_at_ms = ?3 WHERE rollout_id = ?4",
                rusqlite::params![state, current_batch, updated_at_ms, rollout_id],
            )?;
            Ok(())
        })
    }

    pub fn update_rollout_target(&self, update: RolloutTargetUpdate) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "UPDATE cp_rollout_targets SET state = ?1, attempt_id = ?2, error = ?3, observed_config_version = ?4, updated_at_ms = ?5 WHERE rollout_id = ?6 AND node_id = ?7",
                rusqlite::params![
                    update.state,
                    update.attempt_id,
                    update.error,
                    update.observed_config_version,
                    update.updated_at_ms,
                    update.rollout_id,
                    update.node_id,
                ],
            )?;
            Ok(())
        })
    }

    pub fn get_config_version_content(
        &self,
        config_version_id: &str,
    ) -> Result<Option<String>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT content_ref FROM cp_config_versions WHERE config_version_id = ?1",
                    [config_version_id],
                    |row| row.get(0),
                )
                .optional()
        })
    }

    pub fn recover_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT rollout_id, config_version_id, state, batch_size, current_batch, total_targets, actor, correlation_id, created_at_ms, updated_at_ms FROM cp_rollouts WHERE state NOT IN ('converged', 'cancelled', 'rolled_back') ORDER BY created_at_ms",
            )?;
            let rows = statement.query_map([], |row| {
                Ok(RolloutRecord {
                    rollout_id: row.get(0)?,
                    config_version_id: row.get(1)?,
                    state: row.get(2)?,
                    batch_size: row.get(3)?,
                    current_batch: row.get(4)?,
                    total_targets: row.get(5)?,
                    actor: row.get(6)?,
                    correlation_id: row.get(7)?,
                    created_at_ms: row.get(8)?,
                    updated_at_ms: row.get(9)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn list_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT rollout_id, config_version_id, state, batch_size, current_batch, total_targets, actor, correlation_id, created_at_ms, updated_at_ms FROM cp_rollouts ORDER BY created_at_ms DESC, rollout_id DESC LIMIT 1024",
            )?;
            let rows = statement.query_map([], |row| {
                Ok(RolloutRecord {
                    rollout_id: row.get(0)?,
                    config_version_id: row.get(1)?,
                    state: row.get(2)?,
                    batch_size: row.get(3)?,
                    current_batch: row.get(4)?,
                    total_targets: row.get(5)?,
                    actor: row.get(6)?,
                    correlation_id: row.get(7)?,
                    created_at_ms: row.get(8)?,
                    updated_at_ms: row.get(9)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn upsert_operation(&self, operation: PersistedOperation) -> Result<(), StorageError> {
        self.immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO cp_operations (operation_id, node_id, resource_id, operation, state, created_at_ms, updated_at_ms, operation_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT(operation_id) DO UPDATE SET state = excluded.state, updated_at_ms = excluded.updated_at_ms, operation_json = excluded.operation_json",
                rusqlite::params![
                    operation.operation_id,
                    operation.node_id,
                    operation.resource_id,
                    operation.operation,
                    operation.state,
                    operation.created_at_ms,
                    operation.updated_at_ms,
                    operation.operation_json,
                ],
            )?;
            Ok(())
        })
    }

    pub fn get_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<PersistedOperation>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT operation_id, node_id, resource_id, operation, state, created_at_ms, updated_at_ms, operation_json FROM cp_operations WHERE operation_id = ?1",
                    [operation_id],
                    |row| {
                        Ok(PersistedOperation {
                            operation_id: row.get(0)?,
                            node_id: row.get(1)?,
                            resource_id: row.get(2)?,
                            operation: row.get(3)?,
                            state: row.get(4)?,
                            created_at_ms: row.get(5)?,
                            updated_at_ms: row.get(6)?,
                            operation_json: row.get(7)?,
                        })
                    },
                )
                .optional()
        })
    }

    pub fn list_operations(
        &self,
        node_id: Option<&str>,
    ) -> Result<Vec<PersistedOperation>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT operation_id, node_id, resource_id, operation, state, created_at_ms, updated_at_ms, operation_json FROM cp_operations WHERE (?1 IS NULL OR node_id = ?1) ORDER BY created_at_ms DESC, operation_id DESC LIMIT 1024",
            )?;
            let rows = statement.query_map([node_id], |row| {
                Ok(PersistedOperation {
                    operation_id: row.get(0)?,
                    node_id: row.get(1)?,
                    resource_id: row.get(2)?,
                    operation: row.get(3)?,
                    state: row.get(4)?,
                    created_at_ms: row.get(5)?,
                    updated_at_ms: row.get(6)?,
                    operation_json: row.get(7)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn upsert_job(&self, mut job: JobRecord) -> Result<JobRecord, StorageError> {
        self.immediate_transaction(|connection| {
            let current_generation = connection
                .query_row(
                    "SELECT generation FROM cp_jobs WHERE job_id = ?1",
                    [&job.job_id],
                    |row| row.get::<_, u64>(0),
                )
                .optional()?;
            job.generation = current_generation
                .map(|generation| generation.saturating_add(1))
                .unwrap_or_else(|| job.generation.max(1));
            let node_ids = serde_json::to_string(&job.node_ids).map_err(|error| {
                StorageError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            })?;
            connection.execute(
                "INSERT INTO cp_jobs (job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) ON CONFLICT(job_id) DO UPDATE SET version=excluded.version, spec_json=excluded.spec_json, desired_state=excluded.desired_state, observed_state=excluded.observed_state, convergence=excluded.convergence, generation=excluded.generation, node_ids_json=excluded.node_ids_json, checkpoint_id=excluded.checkpoint_id, last_error=excluded.last_error, updated_at_ms=excluded.updated_at_ms",
                rusqlite::params![
                    job.job_id,
                    job.version,
                    job.spec_json,
                    job.desired_state,
                    job.observed_state,
                    job.convergence,
                    job.generation,
                    node_ids,
                    job.checkpoint_id,
                    job.last_error,
                    job.updated_at_ms,
                ],
            )?;
            Ok(job)
        })
    }

    pub fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms FROM cp_jobs WHERE job_id = ?1",
                    [job_id],
                    row_to_job,
                )
                .optional()
        })
    }

    pub fn upsert_job_version(&self, record: JobVersionRecord) -> Result<(), StorageError> {
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO cp_job_versions (job_id, version, spec_json, plan_json, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(job_id, version) DO UPDATE SET spec_json=excluded.spec_json, plan_json=excluded.plan_json",
                rusqlite::params![
                    record.job_id,
                    record.version,
                    record.spec_json,
                    record.plan_json,
                    record.created_at_ms,
                ],
            )?;
            Ok(())
        })
    }

    pub fn list_job_versions(&self, job_id: &str) -> Result<Vec<JobVersionRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT job_id, version, spec_json, plan_json, created_at_ms FROM cp_job_versions WHERE job_id = ?1 ORDER BY version DESC",
            )?;
            let rows = statement.query_map([job_id], |row| {
                Ok(JobVersionRecord {
                    job_id: row.get(0)?,
                    version: row.get(1)?,
                    spec_json: row.get(2)?,
                    plan_json: row.get(3)?,
                    created_at_ms: row.get(4)?,
                })
            })?;
            rows.collect()
        })
    }

    pub fn list_jobs(&self) -> Result<Vec<JobRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms FROM cp_jobs ORDER BY updated_at_ms DESC, job_id LIMIT 4096",
            )?;
            let rows = statement.query_map([], row_to_job)?;
            rows.collect()
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_job(
        &self,
        job_id: &str,
        desired_state: Option<&str>,
        observed_state: Option<&str>,
        convergence: Option<&str>,
        generation: Option<u64>,
        checkpoint_id: Option<&str>,
        last_error: Option<&str>,
    ) -> Result<Option<JobRecord>, StorageError> {
        self.with_connection(|connection| {
            connection.execute(
                "UPDATE cp_jobs SET desired_state=COALESCE(?2, desired_state), observed_state=COALESCE(?3, observed_state), convergence=COALESCE(?4, convergence), generation=COALESCE(?5, generation), checkpoint_id=COALESCE(?6, checkpoint_id), last_error=?7, updated_at_ms=?8 WHERE job_id=?1",
                rusqlite::params![job_id, desired_state, observed_state, convergence, generation, checkpoint_id, last_error, now_ms()],
            )?;
            connection
                .query_row(
                    "SELECT job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms FROM cp_jobs WHERE job_id = ?1",
                    [job_id],
                    row_to_job,
                )
                .optional()
        })
    }

    pub fn update_job_desired_state(
        &self,
        job_id: &str,
        desired_state: &str,
        expected_generation: u64,
    ) -> Result<Option<JobRecord>, StorageError> {
        self.immediate_transaction(|connection| {
            let changed = connection.execute(
                "UPDATE cp_jobs SET desired_state=?2, convergence='reconciling', generation=?3, updated_at_ms=?4 WHERE job_id=?1 AND generation=?5",
                rusqlite::params![
                    job_id,
                    desired_state,
                    expected_generation.saturating_add(1),
                    now_ms(),
                    expected_generation,
                ],
            )?;
            if changed == 0 {
                let current = connection
                    .query_row(
                        "SELECT generation FROM cp_jobs WHERE job_id = ?1",
                        [job_id],
                        |row| row.get::<_, u64>(0),
                    )
                    .optional()?;
                return match current {
                    Some(current) => Err(StorageError::GenerationConflict {
                        expected: expected_generation,
                        current,
                    }),
                    None => Ok(None),
                };
            }
            Ok(connection
                .query_row(
                    "SELECT job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms FROM cp_jobs WHERE job_id = ?1",
                    [job_id],
                    row_to_job,
                )
                .optional()?)
        })
    }

    pub fn upsert_job_checkpoint(&self, record: JobCheckpointRecord) -> Result<(), StorageError> {
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO cp_job_checkpoints (job_id, job_version, checkpoint_id, kind, status, manifest_uri, format_version, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) ON CONFLICT(job_id, checkpoint_id) DO UPDATE SET job_version=excluded.job_version, status=excluded.status, manifest_uri=excluded.manifest_uri, format_version=excluded.format_version, updated_at_ms=excluded.updated_at_ms",
                rusqlite::params![
                    record.job_id,
                    record.job_version,
                    record.checkpoint_id,
                    record.kind,
                    record.status,
                    record.manifest_uri,
                    record.format_version,
                    record.created_at_ms,
                    record.updated_at_ms,
                ],
            )?;
            Ok(())
        })
    }

    pub fn list_job_checkpoints(
        &self,
        job_id: &str,
    ) -> Result<Vec<JobCheckpointRecord>, StorageError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT job_id, job_version, checkpoint_id, kind, status, manifest_uri, format_version, created_at_ms, updated_at_ms FROM cp_job_checkpoints WHERE job_id = ?1 ORDER BY created_at_ms DESC, checkpoint_id DESC",
            )?;
            let rows = statement.query_map([job_id], |row| {
                Ok(JobCheckpointRecord {
                    job_id: row.get(0)?,
                    job_version: row.get(1)?,
                    checkpoint_id: row.get(2)?,
                    kind: row.get(3)?,
                    status: row.get(4)?,
                    manifest_uri: row.get(5)?,
                    format_version: row.get(6)?,
                    created_at_ms: row.get(7)?,
                    updated_at_ms: row.get(8)?,
                })
            })?;
            rows.collect::<Result<Vec<_>, _>>()
        })
    }

    pub fn delete_job_checkpoint(
        &self,
        job_id: &str,
        checkpoint_id: &str,
    ) -> Result<(), StorageError> {
        self.with_connection(|connection| {
            connection.execute(
                "DELETE FROM cp_job_checkpoints WHERE job_id = ?1 AND checkpoint_id = ?2",
                rusqlite::params![job_id, checkpoint_id],
            )?;
            Ok(())
        })
    }

    fn migrate(&self) -> Result<(), StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS cp_nodes (
                node_id TEXT PRIMARY KEY,
                role TEXT NOT NULL DEFAULT 'compute',
                protocol_version TEXT NOT NULL DEFAULT 'v1',
                node_version TEXT NOT NULL DEFAULT 'unknown',
                state TEXT NOT NULL DEFAULT 'offline',
                capabilities_json TEXT NOT NULL DEFAULT '[]',
                boot_id TEXT,
                last_report_seq INTEGER,
                last_seen_at_ms INTEGER NOT NULL DEFAULT 0,
                lease_expires_at_ms INTEGER NOT NULL DEFAULT 0,
                maintenance_state TEXT NOT NULL DEFAULT 'active',
                maintenance_updated_at_ms INTEGER,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_jobs (
                job_id TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                spec_json TEXT NOT NULL,
                desired_state TEXT NOT NULL DEFAULT 'stopped',
                observed_state TEXT NOT NULL DEFAULT 'draft',
                convergence TEXT NOT NULL DEFAULT 'unknown',
                generation INTEGER NOT NULL DEFAULT 0,
                node_ids_json TEXT NOT NULL DEFAULT '[]',
                checkpoint_id TEXT,
                last_error TEXT,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS cp_jobs_updated
                ON cp_jobs(updated_at_ms DESC, job_id);

            CREATE TABLE IF NOT EXISTS cp_job_versions (
                job_id TEXT NOT NULL, version INTEGER NOT NULL, spec_json TEXT NOT NULL,
                plan_json TEXT NOT NULL, created_at_ms INTEGER NOT NULL,
                PRIMARY KEY (job_id, version)
            );
            CREATE TABLE IF NOT EXISTS cp_job_tasks (
                job_id TEXT NOT NULL, generation INTEGER NOT NULL, task_id TEXT NOT NULL,
                node_id TEXT NOT NULL, attempt_id TEXT NOT NULL, state TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL, PRIMARY KEY (job_id, generation, task_id)
            );
            CREATE TABLE IF NOT EXISTS cp_job_checkpoints (
                job_id TEXT NOT NULL, job_version INTEGER NOT NULL DEFAULT 0,
                checkpoint_id TEXT NOT NULL, kind TEXT NOT NULL,
                status TEXT NOT NULL, manifest_uri TEXT, format_version INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (job_id, checkpoint_id)
            );
            CREATE TABLE IF NOT EXISTS cp_job_observations (
                job_id TEXT NOT NULL, node_id TEXT NOT NULL, boot_id TEXT,
                report_seq INTEGER NOT NULL, generation INTEGER NOT NULL, state TEXT NOT NULL,
                convergence TEXT NOT NULL, checkpoint_id TEXT, snapshot_json TEXT NOT NULL,
                observed_at_ms INTEGER NOT NULL, PRIMARY KEY (job_id, node_id)
            );

            CREATE TABLE IF NOT EXISTS cp_stream_desired (
                node_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                desired_state TEXT NOT NULL,
                config_version_id TEXT,
                desired_action_id TEXT,
                paused INTEGER NOT NULL DEFAULT 0,
                updated_at_ms INTEGER NOT NULL,
                updated_by TEXT,
                correlation_id TEXT,
                PRIMARY KEY (node_id, stream_id)
            );

            CREATE TABLE IF NOT EXISTS cp_stream_observed (
                node_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                boot_id TEXT,
                report_seq INTEGER,
                observed_generation INTEGER,
                observed_state TEXT NOT NULL,
                applied_config_version TEXT,
                last_action_id TEXT,
                active_operation_id TEXT,
                last_error_code TEXT,
                last_error_message TEXT,
                snapshot_json TEXT NOT NULL DEFAULT '{}',
                observed_at_ms INTEGER NOT NULL,
                PRIMARY KEY (node_id, stream_id)
            );

            CREATE TABLE IF NOT EXISTS cp_config_versions (
                config_version_id TEXT PRIMARY KEY,
                parent_version_id TEXT,
                content_digest TEXT NOT NULL,
                content_ref TEXT NOT NULL,
                format TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                created_by TEXT,
                correlation_id TEXT,
                FOREIGN KEY (parent_version_id)
                    REFERENCES cp_config_versions(config_version_id)
            );

            CREATE TABLE IF NOT EXISTS cp_intents (
                intent_id TEXT PRIMARY KEY,
                node_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                intent_type TEXT NOT NULL,
                desired_state TEXT,
                config_version_id TEXT,
                action_id TEXT,
                payload_json TEXT,
                state TEXT NOT NULL,
                convergence_state TEXT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                next_retry_at_ms INTEGER,
                last_failure_class TEXT,
                last_failure_code TEXT,
                last_failure_message TEXT,
                superseded_by_intent_id TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                converged_at_ms INTEGER,
                actor TEXT,
                correlation_id TEXT,
                idempotency_key TEXT,
                UNIQUE (node_id, stream_id, generation),
                FOREIGN KEY (superseded_by_intent_id)
                    REFERENCES cp_intents(intent_id)
            );

            CREATE TABLE IF NOT EXISTS cp_attempts (
                attempt_id TEXT PRIMARY KEY,
                intent_id TEXT NOT NULL,
                command_id TEXT NOT NULL UNIQUE,
                node_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                operation TEXT NOT NULL,
                state TEXT NOT NULL,
                failure_class TEXT,
                dispatched_at_ms INTEGER,
                acknowledged_at_ms INTEGER,
                started_at_ms INTEGER,
                finished_at_ms INTEGER,
                expires_at_ms INTEGER,
                error_code TEXT,
                error_message TEXT,
                created_at_ms INTEGER NOT NULL,
                FOREIGN KEY (intent_id) REFERENCES cp_intents(intent_id)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS cp_one_active_attempt
                ON cp_attempts(node_id, stream_id, generation)
                WHERE state IN ('queued', 'dispatched', 'acknowledged', 'running');

            CREATE TABLE IF NOT EXISTS cp_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT,
                stream_id TEXT,
                intent_id TEXT,
                attempt_id TEXT,
                event_type TEXT NOT NULL,
                outcome TEXT NOT NULL,
                failure_class TEXT,
                message TEXT,
                generation INTEGER,
                correlation_id TEXT,
                actor TEXT,
                occurred_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_audit_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor TEXT,
                action TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                resource_id TEXT,
                node_id TEXT,
                stream_id TEXT,
                correlation_id TEXT,
                outcome TEXT NOT NULL,
                failure_code TEXT,
                message TEXT,
                occurred_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_rollouts (
                rollout_id TEXT PRIMARY KEY,
                config_version_id TEXT NOT NULL,
                state TEXT NOT NULL,
                batch_size INTEGER NOT NULL,
                current_batch INTEGER NOT NULL DEFAULT 0,
                total_targets INTEGER NOT NULL,
                actor TEXT,
                correlation_id TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                FOREIGN KEY (config_version_id)
                    REFERENCES cp_config_versions(config_version_id)
            );

            CREATE TABLE IF NOT EXISTS cp_rollout_targets (
                rollout_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                state TEXT NOT NULL,
                attempt_id TEXT,
                error TEXT,
                observed_config_version TEXT,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (rollout_id, node_id),
                FOREIGN KEY (rollout_id)
                    REFERENCES cp_rollouts(rollout_id)
            );

            CREATE TABLE IF NOT EXISTS cp_operations (
                operation_id TEXT PRIMARY KEY,
                node_id TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                operation TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                operation_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cp_outbox (
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                node_id TEXT NOT NULL,
                stream_id TEXT,
                intent_id TEXT,
                available_at_ms INTEGER NOT NULL,
                claimed_at_ms INTEGER,
                worker_id TEXT,
                processed_at_ms INTEGER,
                created_at_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS cp_intents_due
                ON cp_intents(state, next_retry_at_ms);
            CREATE INDEX IF NOT EXISTS cp_attempts_pending
                ON cp_attempts(state, expires_at_ms);
            CREATE INDEX IF NOT EXISTS cp_events_resource
                ON cp_events(node_id, stream_id, occurred_at_ms);
            CREATE INDEX IF NOT EXISTS cp_audit_resource
                ON cp_audit_events(resource_id, occurred_at_ms);
            CREATE INDEX IF NOT EXISTS cp_rollout_targets_state
                ON cp_rollout_targets(rollout_id, state, ordinal);
            CREATE INDEX IF NOT EXISTS cp_operations_node_created
                ON cp_operations(node_id, created_at_ms DESC);
            CREATE INDEX IF NOT EXISTS cp_outbox_ready
                ON cp_outbox(processed_at_ms, available_at_ms);
            "#,
        )?;
        let has_idempotency_key: bool = connection
            .prepare("PRAGMA table_info(cp_intents)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|name| name == "idempotency_key");
        if !has_idempotency_key {
            connection.execute("ALTER TABLE cp_intents ADD COLUMN idempotency_key TEXT", [])?;
        }
        let has_payload_json: bool = connection
            .prepare("PRAGMA table_info(cp_intents)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|name| name == "payload_json");
        if !has_payload_json {
            connection.execute("ALTER TABLE cp_intents ADD COLUMN payload_json TEXT", [])?;
        }
        let has_node_version: bool = connection
            .prepare("PRAGMA table_info(cp_nodes)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|name| name == "node_version");
        if !has_node_version {
            connection.execute(
                "ALTER TABLE cp_nodes ADD COLUMN node_version TEXT NOT NULL DEFAULT 'unknown'",
                [],
            )?;
        }
        let node_columns: Vec<String> = connection
            .prepare("PRAGMA table_info(cp_nodes)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        if !node_columns.iter().any(|name| name == "maintenance_state") {
            connection.execute(
                "ALTER TABLE cp_nodes ADD COLUMN maintenance_state TEXT NOT NULL DEFAULT 'active'",
                [],
            )?;
        }
        if !node_columns
            .iter()
            .any(|name| name == "maintenance_updated_at_ms")
        {
            connection.execute(
                "ALTER TABLE cp_nodes ADD COLUMN maintenance_updated_at_ms INTEGER",
                [],
            )?;
        }
        let event_columns: Vec<String> = connection
            .prepare("PRAGMA table_info(cp_events)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        if !event_columns.iter().any(|name| name == "actor") {
            connection.execute("ALTER TABLE cp_events ADD COLUMN actor TEXT", [])?;
        }
        let checkpoint_columns: Vec<String> = connection
            .prepare("PRAGMA table_info(cp_job_checkpoints)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        if !checkpoint_columns.iter().any(|name| name == "job_version") {
            // Existing checkpoints have no durable producer version. Mark them
            // incompatible (0) so they are never auto-selected for recovery.
            connection.execute(
                "ALTER TABLE cp_job_checkpoints ADD COLUMN job_version INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS cp_intents_idempotency ON cp_intents(node_id, stream_id, idempotency_key) WHERE idempotency_key IS NOT NULL",
            [],
        )?;
        Ok(())
    }

    pub fn table_exists(&self, name: &str) -> Result<bool, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [name],
                    |_| Ok(()),
                )
                .optional()
                .map(|value| value.is_some())
        })
    }

    pub fn index_exists(&self, name: &str) -> Result<bool, StorageError> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1",
                    [name],
                    |_| Ok(()),
                )
                .optional()
                .map(|value| value.is_some())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_reconciliation_schema_with_active_attempt_guard() {
        let store = ControlPlaneStore::in_memory().unwrap();
        assert!(store.table_exists("cp_intents").unwrap());
        assert!(store.index_exists("cp_one_active_attempt").unwrap());
        assert!(store.table_exists("cp_audit_events").unwrap());
        assert!(store.table_exists("cp_rollouts").unwrap());
        assert!(store.table_exists("cp_rollout_targets").unwrap());
    }

    #[test]
    fn audit_and_rollout_records_survive_store_reopen() {
        let path = std::env::temp_dir().join(format!(
            "arkflow-control-plane-{}-{}.sqlite",
            std::process::id(),
            now_ms()
        ));
        let store = ControlPlaneStore::open(&path).unwrap();
        let audit_id = store
            .record_audit(AuditRecord {
                event_id: 0,
                actor: Some("operator".into()),
                action: "rollout.create".into(),
                resource_type: "rollout".into(),
                resource_id: Some("rollout-1".into()),
                node_id: None,
                stream_id: None,
                correlation_id: Some("corr-1".into()),
                outcome: "accepted".into(),
                failure_code: None,
                message: None,
                occurred_at_ms: 10,
            })
            .unwrap();
        assert!(audit_id > 0);
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-1', 'digest', '{}', 'json', 10)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        store
            .create_rollout(
                RolloutRecord {
                    rollout_id: "rollout-1".into(),
                    config_version_id: "cfg-1".into(),
                    state: "applying".into(),
                    batch_size: 1,
                    current_batch: 0,
                    total_targets: 1,
                    actor: Some("operator".into()),
                    correlation_id: Some("corr-1".into()),
                    created_at_ms: 10,
                    updated_at_ms: 10,
                },
                vec![RolloutTargetRecord {
                    rollout_id: "rollout-1".into(),
                    node_id: "node-a".into(),
                    ordinal: 0,
                    state: "pending".into(),
                    attempt_id: None,
                    error: None,
                    observed_config_version: None,
                    updated_at_ms: 10,
                }],
            )
            .unwrap();
        drop(store);

        let reopened = ControlPlaneStore::open(&path).unwrap();
        assert_eq!(reopened.list_audit(Some("rollout-1")).unwrap().len(), 1);
        assert_eq!(
            reopened.recover_rollouts().unwrap()[0].rollout_id,
            "rollout-1"
        );
        assert_eq!(reopened.list_rollout_targets("rollout-1").unwrap().len(), 1);
        reopened
            .upsert_operation(PersistedOperation {
                operation_id: "op-1".into(),
                node_id: "node-a".into(),
                resource_id: "orders".into(),
                operation: "restart".into(),
                state: "queued".into(),
                created_at_ms: 10,
                updated_at_ms: 10,
                operation_json: r#"{"id":"op-1"}"#.into(),
            })
            .unwrap();
        assert_eq!(
            reopened
                .get_operation("op-1")
                .unwrap()
                .unwrap()
                .operation_json,
            r#"{"id":"op-1"}"#
        );
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rollout_creation_is_atomic_when_a_target_conflicts() {
        let store = ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_config_versions (config_version_id, content_digest, content_ref, format, created_at_ms) VALUES ('cfg-1', 'digest', '{}', 'json', 10)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let result = store.create_rollout(
            RolloutRecord {
                rollout_id: "rollout-1".into(),
                config_version_id: "cfg-1".into(),
                state: "applying".into(),
                batch_size: 1,
                current_batch: 0,
                total_targets: 2,
                actor: None,
                correlation_id: None,
                created_at_ms: 10,
                updated_at_ms: 10,
            },
            vec![
                RolloutTargetRecord {
                    rollout_id: "rollout-1".into(),
                    node_id: "node-a".into(),
                    ordinal: 0,
                    state: "pending".into(),
                    attempt_id: None,
                    error: None,
                    observed_config_version: None,
                    updated_at_ms: 10,
                },
                RolloutTargetRecord {
                    rollout_id: "rollout-1".into(),
                    node_id: "node-a".into(),
                    ordinal: 1,
                    state: "pending".into(),
                    attempt_id: None,
                    error: None,
                    observed_config_version: None,
                    updated_at_ms: 10,
                },
            ],
        );
        assert!(result.is_err());
        assert!(store.get_rollout("rollout-1").unwrap().is_none());
    }

    #[test]
    fn maintenance_transitions_are_durable_and_audited() {
        let store = ControlPlaneStore::in_memory().unwrap();
        store
            .upsert_node(NodeMutation {
                node_id: "node-a".into(),
                version: "v1".into(),
                state: "online".into(),
                capabilities_json: "[]".into(),
                boot_id: None,
                report_seq: None,
                last_seen_at_ms: 10,
                lease_expires_at_ms: 1000,
                maintenance_state: None,
                maintenance_updated_at_ms: None,
            })
            .unwrap();
        assert!(store
            .set_node_maintenance(
                NodeMaintenanceMutation {
                    node_id: "node-a".into(),
                    state: "draining".into(),
                    actor: Some("operator".into()),
                    correlation_id: Some("corr-1".into())
                },
                20
            )
            .unwrap());
        assert_eq!(
            store.get_node_maintenance("node-a").unwrap().as_deref(),
            Some("draining")
        );
        let events = store.list_events(Some("node-a")).unwrap();
        assert_eq!(events[0].event_type, "node_maintenance_changed");
        assert_eq!(events[0].actor.as_deref(), Some("operator"));
        assert_eq!(events[0].correlation_id.as_deref(), Some("corr-1"));
    }

    #[test]
    fn event_retention_prunes_oldest_durable_ids() {
        let store = ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| {
                for timestamp in 1..=3 {
                    connection.execute(
                        "INSERT INTO cp_events (event_type, outcome, occurred_at_ms) VALUES ('test', 'accepted', ?1)",
                        [timestamp],
                    )?;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(store.prune_events(2).unwrap(), 1);
        let events = store.list_events(None).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_id, 3);
        assert_eq!(events[1].event_id, 2);
    }

    #[test]
    fn operational_aggregates_are_bounded_and_include_pending_age() {
        let store = ControlPlaneStore::in_memory().unwrap();
        store
            .upsert_node(NodeMutation {
                node_id: "node-a".into(),
                version: "v1".into(),
                state: "stale".into(),
                capabilities_json: "[]".into(),
                boot_id: None,
                report_seq: None,
                last_seen_at_ms: 10,
                lease_expires_at_ms: 10,
                maintenance_state: None,
                maintenance_updated_at_ms: None,
            })
            .unwrap();
        let status = store.operational_aggregates(10_010).unwrap();
        assert_eq!(status.stale_nodes, 1);
        assert_eq!(status.node_states, vec![("stale".into(), 1)]);
    }

    #[test]
    fn legacy_node_observation_initialization_does_not_create_operator_intent() {
        let store = ControlPlaneStore::in_memory().unwrap();
        store
            .upsert_node(NodeMutation {
                node_id: "node-a".into(),
                version: "legacy".into(),
                state: "online".into(),
                capabilities_json: "[]".into(),
                boot_id: Some("boot-1".into()),
                report_seq: Some(1),
                last_seen_at_ms: 1,
                lease_expires_at_ms: 100,
                maintenance_state: None,
                maintenance_updated_at_ms: None,
            })
            .unwrap();
        store
            .record_observed(ObservedMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                boot_id: Some("boot-1".into()),
                report_seq: 1,
                observed_generation: None,
                observed_state: "running".into(),
                config_version_id: None,
                action_id: None,
                snapshot_json: "{}".into(),
                last_error_code: None,
                last_error_message: None,
            })
            .unwrap();
        let version: String = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT node_version FROM cp_nodes WHERE node_id = 'node-a'",
                    [],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(version, "legacy");
        let desired_count: i64 = store
            .with_connection(|connection| {
                connection.query_row("SELECT COUNT(*) FROM cp_stream_desired", [], |row| {
                    row.get(0)
                })
            })
            .unwrap();
        assert_eq!(desired_count, 0);
    }

    #[test]
    fn immediate_transaction_commits_atomically_and_rolls_back_on_error() {
        let store = ControlPlaneStore::in_memory().unwrap();
        store
            .immediate_transaction(|transaction| -> Result<(), StorageError> {
                transaction.execute(
                    "INSERT INTO cp_nodes (node_id, created_at_ms, updated_at_ms) VALUES (?1, 1, 1)",
                    ["node-a"],
                )?;
                Ok(())
            })
            .unwrap();
        assert!(store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT 1 FROM cp_nodes WHERE node_id = 'node-a'",
                    [],
                    |_| Ok(()),
                )
            })
            .is_ok());

        let result: Result<(), StorageError> =
            store.immediate_transaction(|transaction| -> Result<(), StorageError> {
                transaction.execute(
                "INSERT INTO cp_nodes (node_id, created_at_ms, updated_at_ms) VALUES (?1, 2, 2)",
                ["node-b"],
            )?;
                Err(StorageError::Sqlite(rusqlite::Error::InvalidQuery))
            });
        assert!(result.is_err());
        assert!(store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT 1 FROM cp_nodes WHERE node_id = 'node-b'",
                    [],
                    |_| Ok(()),
                )
            })
            .is_err());
    }

    #[test]
    fn desired_mutation_commits_intent_and_outbox_atomically() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                config_version_id: None,
                action_id: None,
                expected_generation: Some(0),
                actor: Some("operator".into()),
                correlation_id: Some("corr-1".into()),
                idempotency_key: None,
                intent_type: None,
                payload_json: None,
            })
            .unwrap();
        assert_eq!(intent.generation, 1);
        let events = store.list_events(Some("node-a")).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "intent_created");
        assert_eq!(
            events[0].intent_id.as_deref(),
            Some(intent.intent_id.as_str())
        );
        assert!(
            store
                .with_connection(|connection| connection.query_row(
                    "SELECT COUNT(*) FROM cp_outbox WHERE intent_id = ?1",
                    [&intent.intent_id],
                    |row| row.get::<_, i64>(0),
                ))
                .unwrap()
                == 1
        );
        assert!(matches!(
            store.set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "stopped".into(),
                config_version_id: None,
                action_id: None,
                expected_generation: Some(0),
                actor: None,
                correlation_id: None,
                idempotency_key: None,
                ..Default::default()
            }),
            Err(StorageError::GenerationConflict { .. })
        ));
    }

    #[test]
    fn outbox_claim_is_idempotent_and_reclaimable_after_lease() {
        let store = ControlPlaneStore::in_memory().unwrap();
        store
            .immediate_transaction(|transaction| -> Result<(), StorageError> {
                transaction.execute(
                    "INSERT INTO cp_outbox (event_key, event_type, node_id, available_at_ms, created_at_ms) VALUES ('event-1', 'reconcile_intent', 'node-a', 10, 10)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let first = store.claim_outbox("worker-a", 10).unwrap().unwrap();
        assert_eq!(first.event_key, "event-1");
        assert!(store.claim_outbox("worker-b", 11).unwrap().is_none());
        assert_eq!(
            store
                .claim_outbox("worker-b", 30_011)
                .unwrap()
                .unwrap()
                .event_key,
            "event-1"
        );
        store
            .mark_outbox_processed(first.outbox_id, 30_012)
            .unwrap();
        assert!(store.claim_outbox("worker-c", 30_013).unwrap().is_none());
    }

    #[tokio::test]
    async fn storage_actor_serializes_desired_mutations() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let actor = StorageActor::start(store, 8);
        let first = actor
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                config_version_id: None,
                action_id: None,
                expected_generation: None,
                actor: None,
                correlation_id: None,
                idempotency_key: None,
                ..Default::default()
            })
            .await
            .unwrap();
        let second = actor
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "stopped".into(),
                config_version_id: None,
                action_id: None,
                expected_generation: None,
                actor: None,
                correlation_id: None,
                idempotency_key: None,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(first.generation + 1, second.generation);
    }

    #[test]
    fn observed_generation_converges_intent_and_attempt() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                config_version_id: None,
                action_id: None,
                expected_generation: Some(0),
                actor: None,
                correlation_id: None,
                idempotency_key: None,
                ..Default::default()
            })
            .unwrap();
        let attempt = store.claim_attempt(&intent.intent_id).unwrap().unwrap();
        assert_eq!(attempt.generation, 1);
        store
            .record_observed(ObservedMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                boot_id: Some("boot-1".into()),
                report_seq: 2,
                observed_generation: Some(1),
                observed_state: "running".into(),
                config_version_id: None,
                action_id: None,
                snapshot_json: "{}".into(),
                last_error_code: None,
                last_error_message: None,
            })
            .unwrap();
        let states: (String, String) = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT i.state, a.state FROM cp_intents i JOIN cp_attempts a ON a.intent_id = i.intent_id WHERE i.intent_id = ?1",
                    [&intent.intent_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
        })
        .unwrap();
        assert_eq!(states, ("converged".into(), "succeeded".into()));
        let events = store.list_events(Some("node-a")).unwrap();
        assert!(events.iter().any(|event| {
            event.event_type == "intent_converged"
                && event.intent_id.as_deref() == Some(intent.intent_id.as_str())
        }));
        store
            .record_observed(ObservedMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                boot_id: Some("boot-1".into()),
                report_seq: 1,
                observed_generation: Some(0),
                observed_state: "stopped".into(),
                config_version_id: None,
                action_id: None,
                snapshot_json: "{}".into(),
                last_error_code: None,
                last_error_message: None,
            })
            .unwrap();
        let observed: String = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT observed_state FROM cp_stream_observed WHERE node_id = 'node-a' AND stream_id = 'orders'",
                    [],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(observed, "running");
    }

    #[test]
    fn restart_intent_requires_matching_completed_action() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                config_version_id: None,
                action_id: Some("restart-1".into()),
                expected_generation: Some(0),
                actor: None,
                correlation_id: None,
                idempotency_key: None,
                ..Default::default()
            })
            .unwrap();
        let attempt = store.claim_attempt(&intent.intent_id).unwrap().unwrap();
        assert_eq!(attempt.operation, "restart");
        assert_eq!(attempt.action_id.as_deref(), Some("restart-1"));
        store
            .record_observed(ObservedMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                boot_id: Some("boot-1".into()),
                report_seq: 1,
                observed_generation: Some(1),
                observed_state: "running".into(),
                config_version_id: None,
                action_id: Some("restart-old".into()),
                snapshot_json: "{}".into(),
                last_error_code: None,
                last_error_message: None,
            })
            .unwrap();
        let pending: String = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT state FROM cp_intents WHERE intent_id = ?1",
                    [&intent.intent_id],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(pending, "accepted");
        store
            .record_observed(ObservedMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                boot_id: Some("boot-1".into()),
                report_seq: 2,
                observed_generation: Some(1),
                observed_state: "running".into(),
                config_version_id: None,
                action_id: Some("restart-1".into()),
                snapshot_json: "{}".into(),
                last_error_code: None,
                last_error_message: None,
            })
            .unwrap();
        let converged: String = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT state FROM cp_intents WHERE intent_id = ?1",
                    [&intent.intent_id],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(converged, "converged");
    }

    #[test]
    fn recovery_requeues_pending_intents_after_processed_outbox() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                config_version_id: None,
                action_id: None,
                expected_generation: Some(0),
                actor: None,
                correlation_id: None,
                idempotency_key: None,
                ..Default::default()
            })
            .unwrap();
        let base = now_ms();
        store.recover_reconciliation(base).unwrap();
        let count: i64 = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM cp_outbox WHERE intent_id = ?1",
                    [&intent.intent_id],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(count, 1);
        let outbox = store.claim_outbox("worker", base).unwrap().unwrap();
        store
            .mark_outbox_processed(outbox.outbox_id, base + 1)
            .unwrap();
        store.recover_reconciliation(base + 2).unwrap();
        let count: i64 = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM cp_outbox WHERE intent_id = ?1",
                    [&intent.intent_id],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn attempt_ack_is_not_terminal_and_temporary_failure_retries() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                config_version_id: None,
                action_id: None,
                expected_generation: Some(0),
                actor: None,
                correlation_id: None,
                idempotency_key: None,
                ..Default::default()
            })
            .unwrap();
        let attempt = store.claim_attempt(&intent.intent_id).unwrap().unwrap();
        store
            .complete_attempt(&attempt.attempt_id, "acknowledged", None)
            .unwrap();
        let finished: Option<u64> = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT finished_at_ms FROM cp_attempts WHERE attempt_id = ?1",
                    [&attempt.attempt_id],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert!(finished.is_none());
        store
            .complete_attempt(
                &attempt.attempt_id,
                "timed_out",
                Some("temporary_execution"),
            )
            .unwrap();
        let state: String = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT state FROM cp_intents WHERE intent_id = ?1",
                    [&intent.intent_id],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(state, "retrying");
        let retries: i64 = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM cp_outbox WHERE intent_id = ?1 AND event_type = 'retry_intent'",
                    [&intent.intent_id],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(retries, 1);
    }

    #[test]
    fn node_registration_wakes_unprocessed_intents_after_prior_outbox_work() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .unwrap();
        let timestamp = now_ms();
        let outbox = store.claim_outbox("worker", timestamp).unwrap().unwrap();
        store
            .mark_outbox_processed(outbox.outbox_id, timestamp + 1)
            .unwrap();
        store.wake_node("node-a", timestamp + 2).unwrap();
        let pending: i64 = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM cp_outbox WHERE intent_id = ?1 AND processed_at_ms IS NULL",
                    [&intent.intent_id],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(pending, 1);
    }

    #[test]
    fn configuration_intent_requires_matching_observed_version() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "__configuration__".into(),
                desired_state: "configured".into(),
                config_version_id: Some("cfg-1".into()),
                expected_generation: Some(0),
                intent_type: Some("apply_configuration".into()),
                payload_json: Some(r#"{"format":"json","content":"{}"}"#.into()),
                ..Default::default()
            })
            .unwrap();
        let attempt = store.claim_attempt(&intent.intent_id).unwrap().unwrap();
        store
            .mark_attempt_dispatched(&attempt.attempt_id, 10)
            .unwrap();
        assert_eq!(store.expire_attempts(10).unwrap(), 1);
        let ambiguous = store.get_intent(&intent.intent_id).unwrap().unwrap();
        assert_eq!(ambiguous.convergence_state, "degraded");
        assert_eq!(ambiguous.failure_class.as_deref(), Some("ambiguous"));
        let observed = |version: &str, seq: u64| ObservedMutation {
            node_id: "node-a".into(),
            stream_id: "__configuration__".into(),
            boot_id: Some("boot-1".into()),
            report_seq: seq,
            observed_generation: Some(intent.generation),
            observed_state: "configured".into(),
            config_version_id: Some(version.into()),
            action_id: None,
            snapshot_json: "{}".into(),
            last_error_code: None,
            last_error_message: None,
        };
        store.record_observed(observed("cfg-old", 1)).unwrap();
        let pending = store.get_intent(&intent.intent_id).unwrap().unwrap();
        assert_eq!(pending.state, "converging");
        store.record_observed(observed("cfg-1", 2)).unwrap();
        let converged = store.get_intent(&intent.intent_id).unwrap().unwrap();
        assert_eq!(converged.state, "converged");
    }

    #[test]
    fn permanent_configuration_failure_blocks_until_a_new_generation() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let first = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "__configuration__".into(),
                desired_state: "configured".into(),
                config_version_id: Some("cfg-bad".into()),
                intent_type: Some("apply_configuration".into()),
                payload_json: Some(r#"{"format":"json","content":"bad"}"#.into()),
                expected_generation: Some(0),
                ..Default::default()
            })
            .unwrap();
        let attempt = store.claim_attempt(&first.intent_id).unwrap().unwrap();
        store
            .complete_attempt(&attempt.attempt_id, "failed", Some("permanent_execution"))
            .unwrap();
        let blocked = store.get_intent(&first.intent_id).unwrap().unwrap();
        assert_eq!(blocked.state, "blocked");
        assert_eq!(blocked.convergence_state, "blocked");
        assert_eq!(
            blocked.failure_class.as_deref(),
            Some("permanent_execution")
        );
        let retry_count: i64 = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM cp_outbox WHERE intent_id = ?1 AND event_type = 'retry_intent'",
                    [&first.intent_id],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(retry_count, 0);

        let rollback = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "__configuration__".into(),
                desired_state: "configured".into(),
                config_version_id: Some("cfg-good".into()),
                intent_type: Some("rollback_configuration".into()),
                payload_json: Some(r#"{"id":"cfg-good"}"#.into()),
                expected_generation: Some(first.generation),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rollback.generation, first.generation + 1);
        assert_eq!(rollback.state, "accepted");
    }

    #[test]
    fn configuration_convergence_waits_for_affected_streams() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let stream = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .unwrap();
        let config = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "__configuration__".into(),
                desired_state: "configured".into(),
                config_version_id: Some("cfg-1".into()),
                intent_type: Some("apply_configuration".into()),
                payload_json: Some(r#"{"format":"json","content":"{}"}"#.into()),
                expected_generation: Some(0),
                ..Default::default()
            })
            .unwrap();
        store
            .record_observed(ObservedMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                boot_id: Some("boot-1".into()),
                report_seq: 1,
                observed_generation: Some(stream.generation),
                observed_state: "running".into(),
                config_version_id: Some("cfg-1".into()),
                action_id: None,
                snapshot_json: "{}".into(),
                last_error_code: None,
                last_error_message: None,
            })
            .unwrap();
        store
            .record_observed(ObservedMutation {
                node_id: "node-a".into(),
                stream_id: "__configuration__".into(),
                boot_id: Some("boot-1".into()),
                report_seq: 1,
                observed_generation: Some(config.generation),
                observed_state: "configured".into(),
                config_version_id: Some("cfg-1".into()),
                action_id: None,
                snapshot_json: "{}".into(),
                last_error_code: None,
                last_error_message: None,
            })
            .unwrap();
        assert_eq!(
            store.get_intent(&config.intent_id).unwrap().unwrap().state,
            "converged"
        );

        let next = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "__configuration__".into(),
                desired_state: "configured".into(),
                config_version_id: Some("cfg-2".into()),
                intent_type: Some("apply_configuration".into()),
                payload_json: Some(r#"{"format":"json","content":"v2"}"#.into()),
                expected_generation: Some(config.generation),
                ..Default::default()
            })
            .unwrap();
        store
            .record_observed(ObservedMutation {
                node_id: "node-a".into(),
                stream_id: "__configuration__".into(),
                boot_id: Some("boot-1".into()),
                report_seq: 2,
                observed_generation: Some(next.generation),
                observed_state: "configured".into(),
                config_version_id: Some("cfg-2".into()),
                action_id: None,
                snapshot_json: "{}".into(),
                last_error_code: None,
                last_error_message: None,
            })
            .unwrap();
        let next_state = store.get_intent(&next.intent_id).unwrap().unwrap();
        assert_eq!(next_state.state, "converging");
        assert_eq!(next_state.convergence_state, "applying");
    }

    #[test]
    fn expired_attempt_becomes_ambiguous_until_fresh_report() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .unwrap();
        let wake = store.claim_outbox("worker", now_ms()).unwrap().unwrap();
        store
            .mark_outbox_processed(wake.outbox_id, now_ms())
            .unwrap();
        let attempt = store.claim_attempt(&intent.intent_id).unwrap().unwrap();
        store
            .mark_attempt_dispatched(&attempt.attempt_id, 10)
            .unwrap();
        assert_eq!(store.expire_attempts(10).unwrap(), 1);
        let state: (String, String) = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT a.state, i.convergence_state FROM cp_attempts a JOIN cp_intents i ON i.intent_id = a.intent_id WHERE a.attempt_id = ?1",
                    [&attempt.attempt_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
            })
            .unwrap();
        assert_eq!(state, ("ambiguous".into(), "degraded".into()));
        store
            .complete_attempt(&attempt.attempt_id, "ambiguous", Some("ambiguous"))
            .unwrap();
        let state: (String, String) = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT i.state, i.convergence_state FROM cp_intents i WHERE i.intent_id = ?1",
                    [&intent.intent_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
            })
            .unwrap();
        assert_eq!(state, ("converging".into(), "degraded".into()));
        let pending_outbox: i64 = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM cp_outbox WHERE intent_id = ?1 AND processed_at_ms IS NULL",
                    [&intent.intent_id],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(pending_outbox, 0);
        store.wake_node("node-a", 11).unwrap();
        let pending_outbox: i64 = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM cp_outbox WHERE intent_id = ?1 AND processed_at_ms IS NULL",
                    [&intent.intent_id],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(pending_outbox, 0);
        store
            .record_observed(ObservedMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                boot_id: Some("boot-2".into()),
                report_seq: 1,
                observed_generation: Some(1),
                observed_state: "stopped".into(),
                config_version_id: None,
                action_id: None,
                snapshot_json: "{}".into(),
                last_error_code: None,
                last_error_message: None,
            })
            .unwrap();
        let pending_outbox: i64 = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM cp_outbox WHERE intent_id = ?1 AND processed_at_ms IS NULL",
                    [&intent.intent_id],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(pending_outbox, 1);
    }
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod job_storage_tests {
    use super::*;

    #[test]
    fn job_records_survive_store_reopen() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let job = JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: "{}".into(),
            desired_state: "stopped".into(),
            observed_state: "draft".into(),
            convergence: "unknown".into(),
            generation: 0,
            node_ids: vec!["node-a".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 1,
        };
        let stored = store.upsert_job(job.clone()).unwrap();
        assert_eq!(stored.generation, 1);
        assert_eq!(store.get_job("orders").unwrap(), Some(stored));
        let updated = store
            .update_job(
                "orders",
                Some("running"),
                Some("running"),
                Some("in_sync"),
                Some(1),
                Some("cp-1"),
                None,
            )
            .unwrap()
            .unwrap();
        assert_eq!(updated.desired_state, "running");
        assert_eq!(updated.checkpoint_id.as_deref(), Some("cp-1"));
        assert_eq!(store.list_jobs().unwrap().len(), 1);
    }

    #[test]
    fn replacing_a_job_advances_its_generation() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let original = JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: "{\"version\":1}".into(),
            desired_state: "stopped".into(),
            observed_state: "stopped".into(),
            convergence: "converged".into(),
            generation: 4,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 1,
        };
        assert_eq!(store.upsert_job(original).unwrap().generation, 4);

        let replacement = store
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 2,
                spec_json: "{\"version\":2}".into(),
                desired_state: "running".into(),
                observed_state: "validated".into(),
                convergence: "pending".into(),
                generation: 1,
                node_ids: Vec::new(),
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 2,
            })
            .unwrap();

        assert_eq!(replacement.generation, 5);
        assert_eq!(replacement.version, 2);
        assert_eq!(store.get_job("orders").unwrap(), Some(replacement));
    }

    #[test]
    fn desired_state_update_marks_job_as_reconciling() {
        let store = ControlPlaneStore::in_memory().unwrap();
        store
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: "{}".into(),
                desired_state: "stopped".into(),
                observed_state: "stopped".into(),
                convergence: "converged".into(),
                generation: 3,
                node_ids: Vec::new(),
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 1,
            })
            .unwrap();

        let updated = store
            .update_job_desired_state("orders", "running", 3)
            .unwrap()
            .unwrap();
        assert_eq!(updated.generation, 4);
        assert_eq!(updated.convergence, "reconciling");
    }
}

//! Hub control-plane storage: records, the FIFO storage actor, and the
//! backend contract dispatching between SQLite (default) and PostgreSQL.
pub mod migrate_tool;
pub mod postgres;
pub mod sqlite;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

pub use sqlite::SqliteBackend;

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
    #[error("unsupported backend operation: {0}")]
    Unsupported(&'static str),
    #[error("postgres pool error: {0}")]
    Pool(#[from] sqlx::Error),
}

enum StorageCommand {
    UpsertJob {
        job: JobRecord,
        response: oneshot::Sender<Result<JobRecord, StorageError>>,
    },
    UpdateJobWithExpectedGeneration {
        job: JobRecord,
        expected_generation: u64,
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
    UpdateJobObservation {
        job_id: String,
        observed_state: String,
        convergence: String,
        generation: u64,
        expected_generation: u64,
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
    ResetObservedCursors {
        node_id: String,
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
    PruneOperationHistory {
        older_than_ms: i64,
        max_retained: i64,
        response: oneshot::Sender<Result<usize, StorageError>>,
    },
    PruneJobCheckpointRecords {
        older_than_ms: i64,
        response: oneshot::Sender<Result<usize, StorageError>>,
    },
    PruneAuditEvents {
        older_than_ms: i64,
        max_retained: i64,
        response: oneshot::Sender<Result<usize, StorageError>>,
    },
    PruneProcessedOutbox {
        older_than_ms: i64,
        max_retained: i64,
        response: oneshot::Sender<Result<usize, StorageError>>,
    },
    PruneTerminalAttempts {
        older_than_ms: i64,
        max_retained: i64,
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
    ListJobStartOperations {
        resource_id: String,
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
                        let _ = response.send(store.upsert_job(job).await);
                    }
                    StorageCommand::UpdateJobWithExpectedGeneration {
                        job,
                        expected_generation,
                        response,
                    } => {
                        let _ = response.send(
                            store.update_job_with_expected_generation(job, expected_generation).await,
                        );
                    }
                    StorageCommand::GetJob { job_id, response } => {
                        let _ = response.send(store.get_job(&job_id).await);
                    }
                    StorageCommand::ListJobs { response } => {
                        let _ = response.send(store.list_jobs().await);
                    }
                    StorageCommand::UpsertJobVersion { record, response } => {
                        let _ = response.send(store.upsert_job_version(record).await);
                    }
                    StorageCommand::ListJobVersions { job_id, response } => {
                        let _ = response.send(store.list_job_versions(&job_id).await);
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
                        ).await);
                    }
                    StorageCommand::UpdateJobObservation {
                        job_id,
                        observed_state,
                        convergence,
                        generation,
                        expected_generation,
                        checkpoint_id,
                        last_error,
                        response,
                    } => {
                        let _ = response.send(store.update_job_observation(
                            &job_id,
                            &observed_state,
                            &convergence,
                            generation,
                            expected_generation,
                            checkpoint_id.as_deref(),
                            last_error.as_deref(),
                        ).await);
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
                        ).await);
                    }
                    StorageCommand::UpsertJobCheckpoint { record, response } => {
                        let _ = response.send(store.upsert_job_checkpoint(record).await);
                    }
                    StorageCommand::ListJobCheckpoints { job_id, response } => {
                        let _ = response.send(store.list_job_checkpoints(&job_id).await);
                    }
                    StorageCommand::DeleteJobCheckpoint {
                        job_id,
                        checkpoint_id,
                        response,
                    } => {
                        let _ = response.send(store.delete_job_checkpoint(&job_id, &checkpoint_id).await);
                    }
                    StorageCommand::UpsertNode { mutation, response } => {
                        let _ = response.send(store.upsert_node(mutation).await);
                    }
                    StorageCommand::ResetObservedCursors { node_id, response } => {
                        let _ = response.send(store.reset_observed_cursors(&node_id).await);
                    }
                    StorageCommand::SetDesired { mutation, response } => {
                        let _ = response.send(store.set_desired(mutation).await);
                    }
                    StorageCommand::GetDesired {
                        node_id,
                        stream_id,
                        response,
                    } => {
                        let _ = response.send(store.get_desired(&node_id, &stream_id).await);
                    }
                    StorageCommand::GetIntent {
                        intent_id,
                        response,
                    } => {
                        let _ = response.send(store.get_intent(&intent_id).await);
                    }
                    StorageCommand::ListIntents { node_id, response } => {
                        let _ = response.send(store.list_intents(node_id.as_deref()).await);
                    }
                    StorageCommand::RecoverReconciliation { now_ms, response } => {
                        let _ = response.send(store.recover_reconciliation(now_ms).await);
                    }
                    StorageCommand::WakeNode {
                        node_id,
                        now_ms,
                        response,
                    } => {
                        let _ = response.send(store.wake_node(&node_id, now_ms).await);
                    }
                    StorageCommand::ListEvents { node_id, response } => {
                        let _ = response.send(store.list_events(node_id.as_deref()).await);
                    }
                    StorageCommand::PruneEvents { retain, response } => {
                        let _ = response.send(store.prune_events(retain).await);
                    }
                    StorageCommand::PruneOperationHistory {
                        older_than_ms,
                        max_retained,
                        response,
                    } => {
                        let _ = response
                            .send(store.prune_operation_history(older_than_ms, max_retained).await);
                    }
                    StorageCommand::PruneJobCheckpointRecords {
                        older_than_ms,
                        response,
                    } => {
                        let _ = response.send(store.prune_job_checkpoint_records(older_than_ms).await);
                    }
                    StorageCommand::PruneAuditEvents {
                        older_than_ms,
                        max_retained,
                        response,
                    } => {
                        let _ =
                            response.send(store.prune_audit_events(older_than_ms, max_retained).await);
                    }
                    StorageCommand::PruneProcessedOutbox {
                        older_than_ms,
                        max_retained,
                        response,
                    } => {
                        let _ = response
                            .send(store.prune_processed_outbox(older_than_ms, max_retained).await);
                    }
                    StorageCommand::PruneTerminalAttempts {
                        older_than_ms,
                        max_retained,
                        response,
                    } => {
                        let _ = response
                            .send(store.prune_terminal_attempts(older_than_ms, max_retained).await);
                    }
                    StorageCommand::ClaimAttempt {
                        intent_id,
                        response,
                    } => {
                        let _ = response.send(store.claim_attempt(&intent_id).await);
                    }
                    StorageCommand::MarkAttemptDispatched {
                        attempt_id,
                        expires_at_ms,
                        response,
                    } => {
                        let _ = response
                            .send(store.mark_attempt_dispatched(&attempt_id, expires_at_ms).await);
                    }
                    StorageCommand::ExpireAttempts { now_ms, response } => {
                        let _ = response.send(store.expire_attempts(now_ms).await);
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
                        ).await);
                    }
                    StorageCommand::RecordObserved { mutation, response } => {
                        let _ = response.send(store.record_observed(mutation).await);
                    }
                    StorageCommand::ClaimOutbox {
                        worker_id,
                        now_ms,
                        response,
                    } => {
                        let _ = response.send(store.claim_outbox(&worker_id, now_ms).await);
                    }
                    StorageCommand::MarkOutboxProcessed {
                        outbox_id,
                        now_ms,
                        response,
                    } => {
                        let _ = response.send(store.mark_outbox_processed(outbox_id, now_ms).await);
                    }
                    StorageCommand::SetNodeMaintenance {
                        mutation,
                        now_ms,
                        response,
                    } => {
                        let _ = response.send(store.set_node_maintenance(mutation, now_ms).await);
                    }
                    StorageCommand::GetNodeMaintenance { node_id, response } => {
                        let _ = response.send(store.get_node_maintenance(&node_id).await);
                    }
                    StorageCommand::OperationalAggregates { now_ms, response } => {
                        let _ = response.send(store.operational_aggregates(now_ms).await);
                    }
                    StorageCommand::RecordAudit { record, response } => {
                        let _ = response.send(store.record_audit(record).await);
                    }
                    StorageCommand::ListAudit {
                        resource_id,
                        response,
                    } => {
                        let _ = response.send(store.list_audit(resource_id.as_deref()).await);
                    }
                    StorageCommand::CreateRollout {
                        rollout,
                        targets,
                        response,
                    } => {
                        let _ = response.send(store.create_rollout(rollout, targets).await);
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
                        ).await);
                    }
                    StorageCommand::GetRollout {
                        rollout_id,
                        response,
                    } => {
                        let _ = response.send(store.get_rollout(&rollout_id).await);
                    }
                    StorageCommand::ListRolloutTargets {
                        rollout_id,
                        response,
                    } => {
                        let _ = response.send(store.list_rollout_targets(&rollout_id).await);
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
                        ).await);
                    }
                    StorageCommand::UpdateRolloutTarget { update, response } => {
                        let _ = response.send(store.update_rollout_target(update).await);
                    }
                    StorageCommand::GetConfigVersionContent {
                        config_version_id,
                        response,
                    } => {
                        let _ = response.send(store.get_config_version_content(&config_version_id).await);
                    }
                    StorageCommand::RecoverRollouts { response } => {
                        let _ = response.send(store.recover_rollouts().await);
                    }
                    StorageCommand::ListRollouts { response } => {
                        let _ = response.send(store.list_rollouts().await);
                    }
                    StorageCommand::UpsertOperation {
                        operation,
                        response,
                    } => {
                        let _ = response.send(store.upsert_operation(operation).await);
                    }
                    StorageCommand::GetOperation {
                        operation_id,
                        response,
                    } => {
                        let _ = response.send(store.get_operation(&operation_id).await);
                    }
                    StorageCommand::ListOperations { node_id, response } => {
                        let _ = response.send(store.list_operations(node_id.as_deref()).await);
                    }
                    StorageCommand::ListJobStartOperations {
                        resource_id,
                        response,
                    } => {
                        let _ = response.send(store.list_job_start_operations(&resource_id).await);
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

    pub async fn update_job_with_expected_generation(
        &self,
        job: JobRecord,
        expected_generation: u64,
    ) -> Result<JobRecord, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::UpdateJobWithExpectedGeneration {
                job,
                expected_generation,
                response,
            })
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

    pub async fn update_job_observation(
        &self,
        job_id: impl Into<String>,
        observed_state: impl Into<String>,
        convergence: impl Into<String>,
        generation: u64,
        expected_generation: u64,
        checkpoint_id: Option<String>,
        last_error: Option<String>,
    ) -> Result<Option<JobRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::UpdateJobObservation {
                job_id: job_id.into(),
                observed_state: observed_state.into(),
                convergence: convergence.into(),
                generation,
                expected_generation,
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

    pub async fn reset_observed_cursors(
        &self,
        node_id: impl Into<String>,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ResetObservedCursors {
                node_id: node_id.into(),
                response,
            })
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

    /// Bounded retention for the durable operation history: terminal
    /// operation rows older than `older_than_ms` are deleted, and beyond
    /// `max_retained` the oldest terminal rows are dropped. Active rows are
    /// never touched.
    pub async fn prune_operation_history(
        &self,
        older_than_ms: i64,
        max_retained: i64,
    ) -> Result<usize, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::PruneOperationHistory {
                older_than_ms,
                max_retained,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    /// Reclaim pending/failed checkpoint attempt records older than
    /// `older_than_ms`; completed records are managed by the checkpoint
    /// retention policy instead.
    pub async fn prune_job_checkpoint_records(
        &self,
        older_than_ms: i64,
    ) -> Result<usize, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::PruneJobCheckpointRecords {
                older_than_ms,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn prune_audit_events(
        &self,
        older_than_ms: i64,
        max_retained: i64,
    ) -> Result<usize, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::PruneAuditEvents {
                older_than_ms,
                max_retained,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    /// Reclaim processed reconciliation outbox rows by age and count bound.
    /// Unprocessed rows — pending or claimed — are the outstanding work queue
    /// and are never touched.
    pub async fn prune_processed_outbox(
        &self,
        older_than_ms: i64,
        max_retained: i64,
    ) -> Result<usize, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::PruneProcessedOutbox {
                older_than_ms,
                max_retained,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    /// Reclaim terminal Attempt records by age and count bound. Active
    /// attempts (queued/dispatched/acknowledged/running) are never touched;
    /// the `cp_one_active_attempt` unique index relies on their presence.
    pub async fn prune_terminal_attempts(
        &self,
        older_than_ms: i64,
        max_retained: i64,
    ) -> Result<usize, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::PruneTerminalAttempts {
                older_than_ms,
                max_retained,
                response,
            })
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

    pub async fn list_job_start_operations(
        &self,
        resource_id: impl Into<String>,
    ) -> Result<Vec<PersistedOperation>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ListJobStartOperations {
                resource_id: resource_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }
}

/// Storage contract shared by every backend. One method per storage command;
/// signatures match the former synchronous `ControlPlaneStore` API verbatim.
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    async fn set_desired(&self, mutation: DesiredMutation) -> Result<IntentRecord, StorageError>;
    async fn upsert_node(&self, mutation: NodeMutation) -> Result<(), StorageError>;
    async fn reset_observed_cursors(&self, node_id: &str) -> Result<(), StorageError>;
    async fn set_node_maintenance(
&self,
mutation: NodeMaintenanceMutation,
now_ms: u64,
) -> Result<bool, StorageError>;
    async fn get_node_maintenance(&self, node_id: &str) -> Result<Option<String>, StorageError>;
    async fn operational_aggregates(
&self,
now_ms: u64,
) -> Result<OperationalAggregates, StorageError>;
    async fn claim_outbox(
&self,
worker_id: &str,
now_ms: u64,
) -> Result<Option<OutboxRecord>, StorageError>;
    async fn get_desired(
&self,
node_id: &str,
stream_id: &str,
) -> Result<Option<DesiredRecord>, StorageError>;
    async fn get_intent(&self, intent_id: &str) -> Result<Option<IntentRecord>, StorageError>;
    async fn list_intents(&self, node_id: Option<&str>) -> Result<Vec<IntentRecord>, StorageError>;
    async fn recover_reconciliation(&self, now_ms: u64) -> Result<(), StorageError>;
    async fn wake_node(&self, node_id: &str, now_ms: u64) -> Result<(), StorageError>;
    async fn list_events(&self, node_id: Option<&str>) -> Result<Vec<StoredEvent>, StorageError>;
    async fn prune_events(&self, retain: usize) -> Result<usize, StorageError>;
    async fn prune_operation_history(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError>;
    async fn prune_job_checkpoint_records(&self, older_than_ms: i64) -> Result<usize, StorageError>;
    async fn prune_audit_events(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError>;
    async fn prune_processed_outbox(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError>;
    async fn prune_terminal_attempts(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError>;
    async fn claim_attempt(&self, intent_id: &str) -> Result<Option<AttemptRecord>, StorageError>;
    async fn complete_attempt(
&self,
attempt_id: &str,
state: &str,
failure_class: Option<&str>,
) -> Result<(), StorageError>;
    async fn mark_attempt_dispatched(
&self,
attempt_id: &str,
expires_at_ms: u64,
) -> Result<(), StorageError>;
    async fn expire_attempts(&self, now_ms: u64) -> Result<usize, StorageError>;
    async fn record_observed(&self, mutation: ObservedMutation) -> Result<(), StorageError>;
    async fn mark_outbox_processed(&self, outbox_id: i64, now_ms: u64) -> Result<(), StorageError>;
    async fn record_audit(&self, record: AuditRecord) -> Result<i64, StorageError>;
    async fn list_audit(&self, resource_id: Option<&str>) -> Result<Vec<AuditRecord>, StorageError>;
    async fn create_rollout(
&self,
rollout: RolloutRecord,
targets: Vec<RolloutTargetRecord>,
) -> Result<(), StorageError>;
    async fn create_rollout_with_content(
&self,
rollout: RolloutRecord,
targets: Vec<RolloutTargetRecord>,
content: &str,
created_by: Option<&str>,
) -> Result<(), StorageError>;
    async fn get_rollout(&self, rollout_id: &str) -> Result<Option<RolloutRecord>, StorageError>;
    async fn list_rollout_targets(
&self,
rollout_id: &str,
) -> Result<Vec<RolloutTargetRecord>, StorageError>;
    async fn update_rollout(
&self,
rollout_id: &str,
state: &str,
current_batch: u32,
updated_at_ms: u64,
) -> Result<(), StorageError>;
    async fn update_rollout_target(&self, update: RolloutTargetUpdate) -> Result<(), StorageError>;
    async fn get_config_version_content(
&self,
config_version_id: &str,
) -> Result<Option<String>, StorageError>;
    async fn recover_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError>;
    async fn list_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError>;
    async fn upsert_operation(&self, operation: PersistedOperation) -> Result<(), StorageError>;
    async fn get_operation(
&self,
operation_id: &str,
) -> Result<Option<PersistedOperation>, StorageError>;
    async fn list_operations(
&self,
node_id: Option<&str>,
) -> Result<Vec<PersistedOperation>, StorageError>;
    async fn list_job_start_operations(
&self,
resource_id: &str,
) -> Result<Vec<PersistedOperation>, StorageError>;
    async fn upsert_job(&self, mut job: JobRecord) -> Result<JobRecord, StorageError>;
    async fn update_job_with_expected_generation(
&self,
mut job: JobRecord,
expected_generation: u64,
) -> Result<JobRecord, StorageError>;
    async fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>, StorageError>;
    async fn upsert_job_version(&self, record: JobVersionRecord) -> Result<(), StorageError>;
    async fn list_job_versions(&self, job_id: &str) -> Result<Vec<JobVersionRecord>, StorageError>;
    async fn list_jobs(&self) -> Result<Vec<JobRecord>, StorageError>;
    async fn update_job(
&self,
job_id: &str,
desired_state: Option<&str>,
observed_state: Option<&str>,
convergence: Option<&str>,
generation: Option<u64>,
checkpoint_id: Option<&str>,
last_error: Option<&str>,
) -> Result<Option<JobRecord>, StorageError>;
    async fn update_job_observation(
&self,
job_id: &str,
observed_state: &str,
convergence: &str,
generation: u64,
expected_generation: u64,
checkpoint_id: Option<&str>,
last_error: Option<&str>,
) -> Result<Option<JobRecord>, StorageError>;
    async fn update_job_desired_state(
&self,
job_id: &str,
desired_state: &str,
expected_generation: u64,
) -> Result<Option<JobRecord>, StorageError>;
    async fn upsert_job_checkpoint(&self, record: JobCheckpointRecord) -> Result<(), StorageError>;
    async fn list_job_checkpoints(
&self,
job_id: &str,
) -> Result<Vec<JobCheckpointRecord>, StorageError>;
    async fn delete_job_checkpoint(
&self,
job_id: &str,
checkpoint_id: &str,
) -> Result<(), StorageError>;
}

#[derive(Clone)]
pub enum ControlPlaneStore {
    Sqlite(SqliteBackend),
    Postgres(postgres::PostgresBackend),
}

impl ControlPlaneStore {
    pub fn in_memory() -> Result<Self, StorageError> {
        Ok(Self::Sqlite(SqliteBackend::in_memory()?))
    }

    /// Open a backend by storage value: `postgres://` / `postgresql://` URLs
    /// select PostgreSQL (with a startup connectivity probe); anything else
    /// is treated as a SQLite file path exactly as before.
    pub async fn open(value: impl AsRef<str>) -> Result<Self, StorageError> {
        let value = value.as_ref();
        if let Some(rest) = value
            .strip_prefix("postgres://")
            .or_else(|| value.strip_prefix("postgresql://"))
        {
            let url = format!("postgres://{rest}");
            Ok(Self::Postgres(postgres::PostgresBackend::open(&url).await?))
        } else {
            Ok(Self::Sqlite(SqliteBackend::open(value)?))
        }
    }

    /// SQLite-only introspection helper used by tests to seed and inspect
    /// schema state directly.
    pub fn with_connection<T>(
        &self,
        operation: impl FnOnce(&rusqlite::Connection) -> Result<T, rusqlite::Error>,
    ) -> Result<T, StorageError> {
        match self {
            Self::Sqlite(backend) => backend.with_connection(operation),
            Self::Postgres(_) => Err(StorageError::Unsupported(
                "with_connection is SQLite-only; use the storage contract",
            )),
        }
    }

    /// SQLite-only schema introspection used by tests.
    pub fn table_exists(&self, name: &str) -> Result<bool, StorageError> {
        match self {
            Self::Sqlite(backend) => backend.table_exists(name),
            Self::Postgres(_) => Ok(true),
        }
    }

    /// SQLite-only schema introspection used by tests.
    pub fn index_exists(&self, name: &str) -> Result<bool, StorageError> {
        match self {
            Self::Sqlite(backend) => backend.index_exists(name),
            Self::Postgres(_) => Ok(true),
        }
    }

    /// SQLite-only transaction helper used by tests to stage multi-statement
    /// fixture writes atomically.
    pub fn immediate_transaction<T>(
        &self,
        operation: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        match self {
            Self::Sqlite(backend) => backend.immediate_transaction(operation),
            Self::Postgres(_) => Err(StorageError::Unsupported(
                "immediate_transaction is SQLite-only; use the storage contract",
            )),
        }
    }
}

#[async_trait]
impl StorageBackend for ControlPlaneStore {
    async fn set_desired(&self, mutation: DesiredMutation) -> Result<IntentRecord, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::set_desired(backend, mutation).await,
            Self::Postgres(backend) => StorageBackend::set_desired(backend, mutation).await,
        }
    }
    async fn upsert_node(&self, mutation: NodeMutation) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::upsert_node(backend, mutation).await,
            Self::Postgres(backend) => StorageBackend::upsert_node(backend, mutation).await,
        }
    }
    async fn reset_observed_cursors(&self, node_id: &str) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::reset_observed_cursors(backend, node_id).await,
            Self::Postgres(backend) => StorageBackend::reset_observed_cursors(backend, node_id).await,
        }
    }
    async fn set_node_maintenance(
&self,
mutation: NodeMaintenanceMutation,
now_ms: u64,
) -> Result<bool, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::set_node_maintenance(backend, mutation, now_ms).await,
            Self::Postgres(backend) => StorageBackend::set_node_maintenance(backend, mutation, now_ms).await,
        }
    }
    async fn get_node_maintenance(&self, node_id: &str) -> Result<Option<String>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::get_node_maintenance(backend, node_id).await,
            Self::Postgres(backend) => StorageBackend::get_node_maintenance(backend, node_id).await,
        }
    }
    async fn operational_aggregates(
&self,
now_ms: u64,
) -> Result<OperationalAggregates, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::operational_aggregates(backend, now_ms).await,
            Self::Postgres(backend) => StorageBackend::operational_aggregates(backend, now_ms).await,
        }
    }
    async fn claim_outbox(
&self,
worker_id: &str,
now_ms: u64,
) -> Result<Option<OutboxRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::claim_outbox(backend, worker_id, now_ms).await,
            Self::Postgres(backend) => StorageBackend::claim_outbox(backend, worker_id, now_ms).await,
        }
    }
    async fn get_desired(
&self,
node_id: &str,
stream_id: &str,
) -> Result<Option<DesiredRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::get_desired(backend, node_id, stream_id).await,
            Self::Postgres(backend) => StorageBackend::get_desired(backend, node_id, stream_id).await,
        }
    }
    async fn get_intent(&self, intent_id: &str) -> Result<Option<IntentRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::get_intent(backend, intent_id).await,
            Self::Postgres(backend) => StorageBackend::get_intent(backend, intent_id).await,
        }
    }
    async fn list_intents(&self, node_id: Option<&str>) -> Result<Vec<IntentRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::list_intents(backend, node_id).await,
            Self::Postgres(backend) => StorageBackend::list_intents(backend, node_id).await,
        }
    }
    async fn recover_reconciliation(&self, now_ms: u64) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::recover_reconciliation(backend, now_ms).await,
            Self::Postgres(backend) => StorageBackend::recover_reconciliation(backend, now_ms).await,
        }
    }
    async fn wake_node(&self, node_id: &str, now_ms: u64) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::wake_node(backend, node_id, now_ms).await,
            Self::Postgres(backend) => StorageBackend::wake_node(backend, node_id, now_ms).await,
        }
    }
    async fn list_events(&self, node_id: Option<&str>) -> Result<Vec<StoredEvent>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::list_events(backend, node_id).await,
            Self::Postgres(backend) => StorageBackend::list_events(backend, node_id).await,
        }
    }
    async fn prune_events(&self, retain: usize) -> Result<usize, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::prune_events(backend, retain).await,
            Self::Postgres(backend) => StorageBackend::prune_events(backend, retain).await,
        }
    }
    async fn prune_operation_history(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::prune_operation_history(backend, older_than_ms, max_retained).await,
            Self::Postgres(backend) => StorageBackend::prune_operation_history(backend, older_than_ms, max_retained).await,
        }
    }
    async fn prune_job_checkpoint_records(&self, older_than_ms: i64) -> Result<usize, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::prune_job_checkpoint_records(backend, older_than_ms).await,
            Self::Postgres(backend) => StorageBackend::prune_job_checkpoint_records(backend, older_than_ms).await,
        }
    }
    async fn prune_audit_events(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::prune_audit_events(backend, older_than_ms, max_retained).await,
            Self::Postgres(backend) => StorageBackend::prune_audit_events(backend, older_than_ms, max_retained).await,
        }
    }
    async fn prune_processed_outbox(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::prune_processed_outbox(backend, older_than_ms, max_retained).await,
            Self::Postgres(backend) => StorageBackend::prune_processed_outbox(backend, older_than_ms, max_retained).await,
        }
    }
    async fn prune_terminal_attempts(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::prune_terminal_attempts(backend, older_than_ms, max_retained).await,
            Self::Postgres(backend) => StorageBackend::prune_terminal_attempts(backend, older_than_ms, max_retained).await,
        }
    }
    async fn claim_attempt(&self, intent_id: &str) -> Result<Option<AttemptRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::claim_attempt(backend, intent_id).await,
            Self::Postgres(backend) => StorageBackend::claim_attempt(backend, intent_id).await,
        }
    }
    async fn complete_attempt(
&self,
attempt_id: &str,
state: &str,
failure_class: Option<&str>,
) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::complete_attempt(backend, attempt_id, state, failure_class).await,
            Self::Postgres(backend) => StorageBackend::complete_attempt(backend, attempt_id, state, failure_class).await,
        }
    }
    async fn mark_attempt_dispatched(
&self,
attempt_id: &str,
expires_at_ms: u64,
) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::mark_attempt_dispatched(backend, attempt_id, expires_at_ms).await,
            Self::Postgres(backend) => StorageBackend::mark_attempt_dispatched(backend, attempt_id, expires_at_ms).await,
        }
    }
    async fn expire_attempts(&self, now_ms: u64) -> Result<usize, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::expire_attempts(backend, now_ms).await,
            Self::Postgres(backend) => StorageBackend::expire_attempts(backend, now_ms).await,
        }
    }
    async fn record_observed(&self, mutation: ObservedMutation) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::record_observed(backend, mutation).await,
            Self::Postgres(backend) => StorageBackend::record_observed(backend, mutation).await,
        }
    }
    async fn mark_outbox_processed(&self, outbox_id: i64, now_ms: u64) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::mark_outbox_processed(backend, outbox_id, now_ms).await,
            Self::Postgres(backend) => StorageBackend::mark_outbox_processed(backend, outbox_id, now_ms).await,
        }
    }
    async fn record_audit(&self, record: AuditRecord) -> Result<i64, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::record_audit(backend, record).await,
            Self::Postgres(backend) => StorageBackend::record_audit(backend, record).await,
        }
    }
    async fn list_audit(&self, resource_id: Option<&str>) -> Result<Vec<AuditRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::list_audit(backend, resource_id).await,
            Self::Postgres(backend) => StorageBackend::list_audit(backend, resource_id).await,
        }
    }
    async fn create_rollout(
&self,
rollout: RolloutRecord,
targets: Vec<RolloutTargetRecord>,
) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::create_rollout(backend, rollout, targets).await,
            Self::Postgres(backend) => StorageBackend::create_rollout(backend, rollout, targets).await,
        }
    }
    async fn create_rollout_with_content(
&self,
rollout: RolloutRecord,
targets: Vec<RolloutTargetRecord>,
content: &str,
created_by: Option<&str>,
) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::create_rollout_with_content(backend, rollout, targets, content, created_by).await,
            Self::Postgres(backend) => StorageBackend::create_rollout_with_content(backend, rollout, targets, content, created_by).await,
        }
    }
    async fn get_rollout(&self, rollout_id: &str) -> Result<Option<RolloutRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::get_rollout(backend, rollout_id).await,
            Self::Postgres(backend) => StorageBackend::get_rollout(backend, rollout_id).await,
        }
    }
    async fn list_rollout_targets(
&self,
rollout_id: &str,
) -> Result<Vec<RolloutTargetRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::list_rollout_targets(backend, rollout_id).await,
            Self::Postgres(backend) => StorageBackend::list_rollout_targets(backend, rollout_id).await,
        }
    }
    async fn update_rollout(
&self,
rollout_id: &str,
state: &str,
current_batch: u32,
updated_at_ms: u64,
) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::update_rollout(backend, rollout_id, state, current_batch, updated_at_ms).await,
            Self::Postgres(backend) => StorageBackend::update_rollout(backend, rollout_id, state, current_batch, updated_at_ms).await,
        }
    }
    async fn update_rollout_target(&self, update: RolloutTargetUpdate) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::update_rollout_target(backend, update).await,
            Self::Postgres(backend) => StorageBackend::update_rollout_target(backend, update).await,
        }
    }
    async fn get_config_version_content(
&self,
config_version_id: &str,
) -> Result<Option<String>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::get_config_version_content(backend, config_version_id).await,
            Self::Postgres(backend) => StorageBackend::get_config_version_content(backend, config_version_id).await,
        }
    }
    async fn recover_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::recover_rollouts(backend, ).await,
            Self::Postgres(backend) => StorageBackend::recover_rollouts(backend, ).await,
        }
    }
    async fn list_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::list_rollouts(backend, ).await,
            Self::Postgres(backend) => StorageBackend::list_rollouts(backend, ).await,
        }
    }
    async fn upsert_operation(&self, operation: PersistedOperation) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::upsert_operation(backend, operation).await,
            Self::Postgres(backend) => StorageBackend::upsert_operation(backend, operation).await,
        }
    }
    async fn get_operation(
&self,
operation_id: &str,
) -> Result<Option<PersistedOperation>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::get_operation(backend, operation_id).await,
            Self::Postgres(backend) => StorageBackend::get_operation(backend, operation_id).await,
        }
    }
    async fn list_operations(
&self,
node_id: Option<&str>,
) -> Result<Vec<PersistedOperation>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::list_operations(backend, node_id).await,
            Self::Postgres(backend) => StorageBackend::list_operations(backend, node_id).await,
        }
    }
    async fn list_job_start_operations(
&self,
resource_id: &str,
) -> Result<Vec<PersistedOperation>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::list_job_start_operations(backend, resource_id).await,
            Self::Postgres(backend) => StorageBackend::list_job_start_operations(backend, resource_id).await,
        }
    }
    async fn upsert_job(&self, mut job: JobRecord) -> Result<JobRecord, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::upsert_job(backend, job).await,
            Self::Postgres(backend) => StorageBackend::upsert_job(backend, job).await,
        }
    }
    async fn update_job_with_expected_generation(
&self,
mut job: JobRecord,
expected_generation: u64,
) -> Result<JobRecord, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::update_job_with_expected_generation(backend, job, expected_generation).await,
            Self::Postgres(backend) => StorageBackend::update_job_with_expected_generation(backend, job, expected_generation).await,
        }
    }
    async fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::get_job(backend, job_id).await,
            Self::Postgres(backend) => StorageBackend::get_job(backend, job_id).await,
        }
    }
    async fn upsert_job_version(&self, record: JobVersionRecord) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::upsert_job_version(backend, record).await,
            Self::Postgres(backend) => StorageBackend::upsert_job_version(backend, record).await,
        }
    }
    async fn list_job_versions(&self, job_id: &str) -> Result<Vec<JobVersionRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::list_job_versions(backend, job_id).await,
            Self::Postgres(backend) => StorageBackend::list_job_versions(backend, job_id).await,
        }
    }
    async fn list_jobs(&self) -> Result<Vec<JobRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::list_jobs(backend, ).await,
            Self::Postgres(backend) => StorageBackend::list_jobs(backend, ).await,
        }
    }
    async fn update_job(
&self,
job_id: &str,
desired_state: Option<&str>,
observed_state: Option<&str>,
convergence: Option<&str>,
generation: Option<u64>,
checkpoint_id: Option<&str>,
last_error: Option<&str>,
) -> Result<Option<JobRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::update_job(backend, job_id, desired_state, observed_state, convergence, generation, checkpoint_id, last_error).await,
            Self::Postgres(backend) => StorageBackend::update_job(backend, job_id, desired_state, observed_state, convergence, generation, checkpoint_id, last_error).await,
        }
    }
    async fn update_job_observation(
&self,
job_id: &str,
observed_state: &str,
convergence: &str,
generation: u64,
expected_generation: u64,
checkpoint_id: Option<&str>,
last_error: Option<&str>,
) -> Result<Option<JobRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::update_job_observation(backend, job_id, observed_state, convergence, generation, expected_generation, checkpoint_id, last_error).await,
            Self::Postgres(backend) => StorageBackend::update_job_observation(backend, job_id, observed_state, convergence, generation, expected_generation, checkpoint_id, last_error).await,
        }
    }
    async fn update_job_desired_state(
&self,
job_id: &str,
desired_state: &str,
expected_generation: u64,
) -> Result<Option<JobRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::update_job_desired_state(backend, job_id, desired_state, expected_generation).await,
            Self::Postgres(backend) => StorageBackend::update_job_desired_state(backend, job_id, desired_state, expected_generation).await,
        }
    }
    async fn upsert_job_checkpoint(&self, record: JobCheckpointRecord) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::upsert_job_checkpoint(backend, record).await,
            Self::Postgres(backend) => StorageBackend::upsert_job_checkpoint(backend, record).await,
        }
    }
    async fn list_job_checkpoints(
&self,
job_id: &str,
) -> Result<Vec<JobCheckpointRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::list_job_checkpoints(backend, job_id).await,
            Self::Postgres(backend) => StorageBackend::list_job_checkpoints(backend, job_id).await,
        }
    }
    async fn delete_job_checkpoint(
&self,
job_id: &str,
checkpoint_id: &str,
) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::delete_job_checkpoint(backend, job_id, checkpoint_id).await,
            Self::Postgres(backend) => StorageBackend::delete_job_checkpoint(backend, job_id, checkpoint_id).await,
        }
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
mod tests {
    use super::*;

    #[tokio::test]
    async fn creates_reconciliation_schema_with_active_attempt_guard() {
        let store = ControlPlaneStore::in_memory().unwrap();
        assert!(store.table_exists("cp_intents").unwrap());
        assert!(store.index_exists("cp_one_active_attempt").unwrap());
        assert!(store.table_exists("cp_audit_events").unwrap());
        assert!(store.table_exists("cp_rollouts").unwrap());
        assert!(store.table_exists("cp_rollout_targets").unwrap());
    }

    #[tokio::test]
    async fn audit_and_rollout_records_survive_store_reopen() {
        let path = std::env::temp_dir().join(format!(
            "arkflow-control-plane-{}-{}.sqlite",
            std::process::id(),
            now_ms()
        ));
        let store = ControlPlaneStore::open(path.to_str().unwrap()).await.unwrap();
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
            .await.unwrap();
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
            .await.unwrap();
        drop(store);

        let reopened = ControlPlaneStore::open(path.to_str().unwrap()).await.unwrap();
        assert_eq!(reopened.list_audit(Some("rollout-1")).await.unwrap().len(), 1);
        assert_eq!(
            reopened.recover_rollouts().await.unwrap()[0].rollout_id,
            "rollout-1"
        );
        assert_eq!(reopened.list_rollout_targets("rollout-1").await.unwrap().len(), 1);
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
            .await.unwrap();
        assert_eq!(
            reopened
                .get_operation("op-1")
                .await.unwrap()
                .unwrap()
                .operation_json,
            r#"{"id":"op-1"}"#
        );
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn rollout_creation_is_atomic_when_a_target_conflicts() {
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
        ).await;
        assert!(result.is_err());
        assert!(store.get_rollout("rollout-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn maintenance_transitions_are_durable_and_audited() {
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
            .await.unwrap();
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
            .await.unwrap());
        assert_eq!(
            store.get_node_maintenance("node-a").await.unwrap().as_deref(),
            Some("draining")
        );
        let events = store.list_events(Some("node-a")).await.unwrap();
        assert_eq!(events[0].event_type, "node_maintenance_changed");
        assert_eq!(events[0].actor.as_deref(), Some("operator"));
        assert_eq!(events[0].correlation_id.as_deref(), Some("corr-1"));
    }

    #[tokio::test]
    async fn event_retention_prunes_oldest_durable_ids() {
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
        assert_eq!(store.prune_events(2).await.unwrap(), 1);
        let events = store.list_events(None).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_id, 3);
        assert_eq!(events[1].event_id, 2);
    }

    #[tokio::test]
    async fn operational_aggregates_are_bounded_and_include_pending_age() {
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
            .await.unwrap();
        let status = store.operational_aggregates(10_010).await.unwrap();
        assert_eq!(status.stale_nodes, 1);
        assert_eq!(status.node_states, vec![("stale".into(), 1)]);
    }

    #[tokio::test]
    async fn legacy_node_observation_initialization_does_not_create_operator_intent() {
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
            .await.unwrap();
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
            .await.unwrap();
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

    #[tokio::test]
    async fn immediate_transaction_commits_atomically_and_rolls_back_on_error() {
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

    #[tokio::test]
    async fn desired_mutation_commits_intent_and_outbox_atomically() {
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
            .await.unwrap();
        assert_eq!(intent.generation, 1);
        let events = store.list_events(Some("node-a")).await.unwrap();
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
            }).await,
            Err(StorageError::GenerationConflict { .. })
        ));
    }

    /// Regression: a checkpoint that lands between a rollback handler's read
    /// and its conditional write updates `checkpoint_id` WITHOUT bumping the
    /// generation (the checkpoint path deliberately leaves the generation
    /// alone). The write must therefore preserve the stored pointer instead of
    /// writing the caller's older one back — a regressed pointer can reference
    /// a checkpoint retention has already deleted, degrading the next start to
    /// a stateless one.
    #[tokio::test]
    async fn conditional_job_write_preserves_a_newer_recovery_pointer() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let job = |checkpoint_id: Option<&str>| JobRecord {
            job_id: "orders".into(),
            version: 2,
            spec_json: "{}".into(),
            desired_state: "stopped".into(),
            observed_state: "stopped".into(),
            convergence: "pending".into(),
            generation: 1,
            node_ids: vec![],
            checkpoint_id: checkpoint_id.map(str::to_owned),
            last_error: None,
            updated_at_ms: 0,
        };
        store.upsert_job(job(Some("ckpt-old"))).await.unwrap();

        // A concurrent checkpoint observation moves the pointer without
        // touching the generation.
        let concurrent = store
            .update_job(
                "orders",
                None,
                None,
                None,
                None,
                Some("ckpt-new".into()),
                None,
            )
            .await.unwrap();
        assert_eq!(
            concurrent
                .as_ref()
                .and_then(|job| job.checkpoint_id.clone()),
            Some("ckpt-new".to_string())
        );

        // The rollback handler writes the record it read (the older pointer).
        let written = store
            .update_job_with_expected_generation(job(Some("ckpt-old")), 1)
            .await.unwrap();
        assert_eq!(
            written.checkpoint_id.as_deref(),
            Some("ckpt-new"),
            "the returned record reports the pointer the row actually holds"
        );
        let stored = store.get_job("orders").await.unwrap().unwrap();
        assert_eq!(
            stored.checkpoint_id.as_deref(),
            Some("ckpt-new"),
            "a concurrent checkpoint must not be regressed by the rollback write"
        );

        // The conditional write never moves the pointer in either direction,
        // so a NULL pointer stays NULL and the version/spec change lands.
        store
            .immediate_transaction(|connection| -> Result<(), StorageError> {
                connection.execute(
                    "UPDATE cp_jobs SET checkpoint_id = NULL WHERE job_id = 'orders'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let written = store
            .update_job_with_expected_generation(job(Some("ckpt-fresh")), 2)
            .await.unwrap();
        assert_eq!(
            written.checkpoint_id, None,
            "the rollback write must not invent a recovery pointer"
        );
        assert_eq!(written.version, 2);

        // A stale generation still conflicts.
        assert!(matches!(
            store.update_job_with_expected_generation(job(None), 1).await,
            Err(StorageError::GenerationConflict { .. })
        ));
    }

    #[tokio::test]
    async fn outbox_claim_is_idempotent_and_reclaimable_after_lease() {
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
        let first = store.claim_outbox("worker-a", 10).await.unwrap().unwrap();
        assert_eq!(first.event_key, "event-1");
        assert!(store.claim_outbox("worker-b", 11).await.unwrap().is_none());
        assert_eq!(
            store
                .claim_outbox("worker-b", 30_011)
                .await.unwrap()
                .unwrap()
                .event_key,
            "event-1"
        );
        store
            .mark_outbox_processed(first.outbox_id, 30_012)
            .await.unwrap();
        assert!(store.claim_outbox("worker-c", 30_013).await.unwrap().is_none());
    }

    /// The outbox retention reclaims only processed rows: the unprocessed
    /// work queue (pending or claimed) survives every sweep, and the status
    /// counters — which only look at unprocessed rows — are unaffected.
    #[tokio::test]
    async fn prune_processed_outbox_reclaims_only_processed_rows() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let insert = |event_key: &str, processed_at_ms: Option<i64>, claimed: bool| {
            store
                .immediate_transaction(|transaction| -> Result<(), StorageError> {
                    transaction.execute(
                        "INSERT INTO cp_outbox (event_key, event_type, node_id, available_at_ms, created_at_ms, claimed_at_ms, processed_at_ms) VALUES (?1, 'reconcile_intent', 'node-a', 1, 1, ?2, ?3)",
                        rusqlite::params![
                            event_key,
                            if claimed { Some(5) } else { None },
                            processed_at_ms
                        ],
                    )?;
                    Ok(())
                })
                .unwrap();
        };
        insert("old-processed", Some(100), false);
        insert("recent-processed", Some(9_000), false);
        insert("claimed-pending", None, true);

        let aggregates_before = store.operational_aggregates(10_000).await.unwrap();
        // The age window reclaims only the processed row past the cutoff.
        assert_eq!(store.prune_processed_outbox(1_000, 4096).await.unwrap(), 1);
        // The count bound keeps the newest processed rows when history
        // accumulates faster than the age window reclaims it.
        for index in 0..6 {
            insert(&format!("bulk-{index}"), Some(2_000 + index), false);
        }
        assert_eq!(store.prune_processed_outbox(1_000, 2).await.unwrap(), 5);
        let remaining = store
            .immediate_transaction(|transaction| {
                let mut statement =
                    transaction.prepare("SELECT event_key FROM cp_outbox ORDER BY outbox_id")?;
                let keys = statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(keys)
            })
            .unwrap();
        assert_eq!(
            remaining,
            vec!["recent-processed", "claimed-pending", "bulk-5"]
        );
        let aggregates_after = store.operational_aggregates(10_000).await.unwrap();
        assert_eq!(
            aggregates_before.outbox_pending,
            aggregates_after.outbox_pending
        );
        assert_eq!(
            aggregates_before.outbox_claimed,
            aggregates_after.outbox_claimed
        );
    }

    /// Attempt retention reclaims terminal rows only; the active attempt —
    /// guarded by both the state predicate and the `cp_one_active_attempt`
    /// unique index — is preserved unchanged.
    #[tokio::test]
    async fn prune_terminal_attempts_reclaims_only_terminal_rows() {
        let store = ControlPlaneStore::in_memory().unwrap();
        store
            .immediate_transaction(|transaction| -> Result<(), StorageError> {
                transaction.execute(
                    "INSERT INTO cp_intents (intent_id, node_id, stream_id, generation, intent_type, state, convergence_state, created_at_ms, updated_at_ms) VALUES ('intent-1', 'node-a', 'orders', 1, 'stream_lifecycle', 'converged', 'converged', 1, 1)",
                    [],
                )?;
                let insert_attempt = |attempt_id: &str,
                                      state: &str,
                                      finished_at_ms: Option<i64>|
                 -> Result<(), StorageError> {
                    transaction.execute(
                        "INSERT INTO cp_attempts (attempt_id, intent_id, command_id, node_id, stream_id, generation, operation, state, finished_at_ms, created_at_ms) VALUES (?1, 'intent-1', ?1, 'node-a', 'orders', 1, 'apply_configuration', ?2, ?3, 1)",
                        rusqlite::params![attempt_id, state, finished_at_ms],
                    )?;
                    Ok(())
                };
                insert_attempt("old-terminal", "succeeded", Some(100))?;
                insert_attempt("recent-terminal", "failed", Some(9_000))?;
                insert_attempt("active-attempt", "running", None)?;
                Ok(())
            })
            .unwrap();
        // The age window reclaims only the terminal row past the cutoff.
        assert_eq!(store.prune_terminal_attempts(1_000, 4096).await.unwrap(), 1);
        // The count bound trims terminal history down to the newest rows.
        assert_eq!(store.prune_terminal_attempts(1_000, 0).await.unwrap(), 1);
        let remaining = store
            .immediate_transaction(|transaction| {
                let mut statement =
                    transaction.prepare("SELECT attempt_id, state FROM cp_attempts")?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .unwrap();
        assert_eq!(
            remaining,
            vec![("active-attempt".to_string(), "running".to_string())]
        );
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

    #[tokio::test]
    async fn observed_generation_converges_intent_and_attempt() {
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
            .await.unwrap();
        let attempt = store.claim_attempt(&intent.intent_id).await.unwrap().unwrap();
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
            .await.unwrap();
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
        let events = store.list_events(Some("node-a")).await.unwrap();
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
            .await.unwrap();
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

    /// A session rebuild (re-register with the same stable boot identity)
    /// restarts the Agent's report_seq at 1. The Hub resets the per-stream
    /// cursors at register; without that reset every new observation is
    /// silently dropped until the node re-reaches the previous session's
    /// high-water mark, blinding convergence for the whole rebuild gap.
    #[tokio::test]
    async fn stable_boot_session_rebuild_resets_the_observation_cursor() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let observed = |seq: u64, state: &str| ObservedMutation {
            node_id: "node-a".into(),
            stream_id: "orders".into(),
            boot_id: Some("boot-stable".into()),
            report_seq: seq,
            observed_generation: Some(1),
            observed_state: state.into(),
            config_version_id: None,
            action_id: None,
            snapshot_json: "{}".into(),
            last_error_code: None,
            last_error_message: None,
        };
        store.record_observed(observed(7, "running")).await.unwrap();
        // Re-register resets the stored cursors for this node.
        store.reset_observed_cursors("node-a").await.unwrap();
        // Sequence 1 of the rebuilt session must be accepted, not dropped
        // as stale under the previous session's cursor of 7.
        store.record_observed(observed(1, "failed")).await.unwrap();
        let (state, seq): (String, u64) = store
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT observed_state, report_seq FROM cp_stream_observed WHERE node_id = 'node-a' AND stream_id = 'orders'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
            })
            .unwrap();
        assert_eq!((state.as_str(), seq), ("failed", 1));
    }

    #[tokio::test]
    async fn restart_intent_requires_matching_completed_action() {
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
            .await.unwrap();
        let attempt = store.claim_attempt(&intent.intent_id).await.unwrap().unwrap();
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
            .await.unwrap();
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
            .await.unwrap();
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

    #[tokio::test]
    async fn recovery_requeues_pending_intents_after_processed_outbox() {
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
            .await.unwrap();
        let base = now_ms();
        store.recover_reconciliation(base).await.unwrap();
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
        let outbox = store.claim_outbox("worker", base).await.unwrap().unwrap();
        store
            .mark_outbox_processed(outbox.outbox_id, base + 1)
            .await.unwrap();
        store.recover_reconciliation(base + 2).await.unwrap();
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

    #[tokio::test]
    async fn attempt_ack_is_not_terminal_and_temporary_failure_retries() {
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
            .await.unwrap();
        let attempt = store.claim_attempt(&intent.intent_id).await.unwrap().unwrap();
        store
            .complete_attempt(&attempt.attempt_id, "acknowledged", None)
            .await.unwrap();
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
            .await.unwrap();
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

    #[tokio::test]
    async fn node_registration_wakes_unprocessed_intents_after_prior_outbox_work() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await.unwrap();
        let timestamp = now_ms();
        let outbox = store.claim_outbox("worker", timestamp).await.unwrap().unwrap();
        store
            .mark_outbox_processed(outbox.outbox_id, timestamp + 1)
            .await.unwrap();
        store.wake_node("node-a", timestamp + 2).await.unwrap();
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

    #[tokio::test]
    async fn configuration_intent_requires_matching_observed_version() {
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
            .await.unwrap();
        let attempt = store.claim_attempt(&intent.intent_id).await.unwrap().unwrap();
        store
            .mark_attempt_dispatched(&attempt.attempt_id, 10)
            .await.unwrap();
        assert_eq!(store.expire_attempts(10).await.unwrap(), 1);
        let ambiguous = store.get_intent(&intent.intent_id).await.unwrap().unwrap();
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
        store.record_observed(observed("cfg-old", 1)).await.unwrap();
        let pending = store.get_intent(&intent.intent_id).await.unwrap().unwrap();
        assert_eq!(pending.state, "converging");
        store.record_observed(observed("cfg-1", 2)).await.unwrap();
        let converged = store.get_intent(&intent.intent_id).await.unwrap().unwrap();
        assert_eq!(converged.state, "converged");
    }

    #[tokio::test]
    async fn permanent_configuration_failure_blocks_until_a_new_generation() {
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
            .await.unwrap();
        let attempt = store.claim_attempt(&first.intent_id).await.unwrap().unwrap();
        store
            .complete_attempt(&attempt.attempt_id, "failed", Some("permanent_execution"))
            .await.unwrap();
        let blocked = store.get_intent(&first.intent_id).await.unwrap().unwrap();
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
            .await.unwrap();
        assert_eq!(rollback.generation, first.generation + 1);
        assert_eq!(rollback.state, "accepted");
    }

    #[tokio::test]
    async fn configuration_convergence_waits_for_affected_streams() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let stream = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await.unwrap();
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
            .await.unwrap();
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
            .await.unwrap();
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
            .await.unwrap();
        assert_eq!(
            store.get_intent(&config.intent_id).await.unwrap().unwrap().state,
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
            .await.unwrap();
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
            .await.unwrap();
        let next_state = store.get_intent(&next.intent_id).await.unwrap().unwrap();
        assert_eq!(next_state.state, "converging");
        assert_eq!(next_state.convergence_state, "applying");
    }

    #[tokio::test]
    async fn expired_attempt_becomes_ambiguous_until_fresh_report() {
        let store = ControlPlaneStore::in_memory().unwrap();
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await.unwrap();
        let wake = store.claim_outbox("worker", now_ms()).await.unwrap().unwrap();
        store
            .mark_outbox_processed(wake.outbox_id, now_ms())
            .await.unwrap();
        let attempt = store.claim_attempt(&intent.intent_id).await.unwrap().unwrap();
        store
            .mark_attempt_dispatched(&attempt.attempt_id, 10)
            .await.unwrap();
        assert_eq!(store.expire_attempts(10).await.unwrap(), 1);
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
            .await.unwrap();
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
        store.wake_node("node-a", 11).await.unwrap();
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
            .await.unwrap();
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

#[cfg(test)]
mod job_storage_tests {
    use super::*;

    #[tokio::test]
    async fn job_records_survive_store_reopen() {
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
        let stored = store.upsert_job(job.clone()).await.unwrap();
        assert_eq!(stored.generation, 1);
        assert_eq!(store.get_job("orders").await.unwrap(), Some(stored));
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
            .await.unwrap()
            .unwrap();
        assert_eq!(updated.desired_state, "running");
        assert_eq!(updated.checkpoint_id.as_deref(), Some("cp-1"));
        assert_eq!(store.list_jobs().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn replacing_a_job_advances_its_generation() {
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
        assert_eq!(store.upsert_job(original).await.unwrap().generation, 4);

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
            .await.unwrap();

        assert_eq!(replacement.generation, 5);
        assert_eq!(replacement.version, 2);
        assert_eq!(store.get_job("orders").await.unwrap(), Some(replacement));
    }

    #[tokio::test]
    async fn desired_state_update_marks_job_as_reconciling() {
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
            .await.unwrap();

        let updated = store
            .update_job_desired_state("orders", "running", 3)
            .await.unwrap()
            .unwrap();
        assert_eq!(updated.generation, 4);
        assert_eq!(updated.convergence, "reconciling");
    }

    #[tokio::test]
    async fn operation_pruning_preserves_the_latest_job_start_recovery_fact() {
        let store = ControlPlaneStore::in_memory().unwrap();
        store
            .upsert_operation(PersistedOperation {
                operation_id: "job-start-1".into(),
                node_id: "node-a".into(),
                resource_id: "orders".into(),
                operation: "job_start".into(),
                state: "succeeded".into(),
                created_at_ms: 1,
                updated_at_ms: 1,
                operation_json: r#"{"operation":"job_start","resource_id":"orders","generation":1,"state":"succeeded"}"#.into(),
            })
            .await.unwrap();
        store
            .upsert_operation(PersistedOperation {
                operation_id: "old-stop".into(),
                node_id: "node-a".into(),
                resource_id: "orders".into(),
                operation: "job_stop".into(),
                state: "succeeded".into(),
                created_at_ms: 1,
                updated_at_ms: 1,
                operation_json: "{}".into(),
            })
            .await.unwrap();

        store.prune_operation_history(100, 0).await.unwrap();
        assert!(store.get_operation("job-start-1").await.unwrap().is_some());
        assert!(store.get_operation("old-stop").await.unwrap().is_none());
        assert_eq!(store.list_job_start_operations("orders").await.unwrap().len(), 1);
    }
}

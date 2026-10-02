//! Hub control-plane storage: records, the FIFO storage actor, and the
//! backend contract dispatching between SQLite (default) and PostgreSQL.
pub mod migrate_tool;
pub mod postgres;
pub mod sqlite;

use async_trait::async_trait;
use std::sync::Arc;
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

/// Durable record of one atomic job-upgrade orchestration (see
/// `hub/job_orchestration.rs`). `phase` is a plain string validated at use
/// sites, matching the rollout state convention. `target_spec_json` is the
/// persisted new spec (with `recovery = LatestSavepoint` forced) so a Hub
/// restart can resume the commit without re-deriving it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobUpgradeRecord {
    pub upgrade_id: String,
    pub job_id: String,
    pub from_version: u64,
    pub to_version: u64,
    pub phase: String,
    /// The savepoint this orchestration dispatched; `None` while the next
    /// round has not been dispatched yet (retry or fresh start).
    pub savepoint_id: Option<String>,
    pub target_spec_json: String,
    /// Wall-clock deadline of the current phase; re-armed on transitions.
    pub phase_deadline_at_ms: u64,
    pub savepoint_retries: u32,
    /// Request-supplied verification timeout override (0 = default).
    pub verify_timeout_ms: u64,
    pub actor: Option<String>,
    pub correlation_id: Option<String>,
    pub last_error: Option<String>,
    /// The phase an operator pause interrupted; resume re-enters it.
    pub paused_from: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl JobUpgradeRecord {
    /// Terminal phases release the reconciler fence and the retention pin.
    pub fn phase_is_terminal(&self) -> bool {
        matches!(
            self.phase.as_str(),
            "succeeded" | "aborted" | "failed" | "rolled_back" | "cancelled"
        )
    }
}

/// The same terminal-phase set as [`JobUpgradeRecord::phase_is_terminal`], in
/// SQL list form. The recovery/prune queries in both backends interpolate
/// this constant so the set exists in exactly one place per language.
pub(crate) const TERMINAL_JOB_UPGRADE_PHASES_SQL: &str =
    "('succeeded', 'aborted', 'failed', 'rolled_back', 'cancelled')";

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
    #[error("stale leader: claimed epoch {claimed_epoch}, current lease epoch {current_epoch}")]
    StaleLeader {
        claimed_epoch: u64,
        current_epoch: u64,
    },
    #[error("postgres pool error: {0}")]
    Pool(#[from] sqlx::Error),
}

/// Outcome of `begin_write_fence`: a lock held until `end_write_fence`
/// (`Held`) or a verified passthrough with nothing to release.
#[derive(Debug, PartialEq, Eq)]
pub enum WriteFence {
    Held,
    Passthrough,
}

/// Leadership-claim value meaning "HA disabled: do not fence". Distinct
/// from `0`, which means "HA enabled, standby claim" (fenced while a lease
/// row exists).
pub const UNFENCED: u64 = u64::MAX;

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
    UpsertJobUpgrade {
        record: JobUpgradeRecord,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    TransitionJobUpgrade {
        record: JobUpgradeRecord,
        expected_phase: String,
        response: oneshot::Sender<Result<bool, StorageError>>,
    },
    GetJobUpgrade {
        upgrade_id: String,
        response: oneshot::Sender<Result<Option<JobUpgradeRecord>, StorageError>>,
    },
    RecoverJobUpgrades {
        response: oneshot::Sender<Result<Vec<JobUpgradeRecord>, StorageError>>,
    },
    ListJobUpgrades {
        job_id: String,
        response: oneshot::Sender<Result<Vec<JobUpgradeRecord>, StorageError>>,
    },
    PruneJobUpgrades {
        older_than_ms: i64,
        max_retained: i64,
        response: oneshot::Sender<Result<usize, StorageError>>,
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
    TryAcquireHubLease {
        holder: String,
        advertise_url: Option<String>,
        ttl_ms: u64,
        now_ms: u64,
        response: oneshot::Sender<Result<HubLeaseAcquire, StorageError>>,
    },
    RenewHubLease {
        holder: String,
        advertise_url: Option<String>,
        ttl_ms: u64,
        now_ms: u64,
        response: oneshot::Sender<Result<HubLeaseRenew, StorageError>>,
    },
    ReadHubLeaseSnapshot {
        response: oneshot::Sender<Result<Option<HubLeaseSnapshot>, StorageError>>,
    },
    ReleaseHubLease {
        holder: String,
        now_ms: u64,
        response: oneshot::Sender<Result<bool, StorageError>>,
    },    /// Write-fencing envelope: execute the inner command only when the
    /// caller's claimed lease epoch (captured at send time) still matches
    /// the lease row's current epoch at execution time.
    Fenced {
        claimed_epoch: u64,
        command: Box<StorageCommand>,
    },

}

impl StorageCommand {
    /// Deliver a terminal error to the command's response channel WITHOUT
    /// executing it (stale-leader fencing). Every command variant carries a
    /// `response` oneshot; a missing arm is a compile error, which forces
    /// new variants to decide their fencing classification.
    fn nack(self, error: StorageError) {
        match self {
            Self::UpsertJob { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::UpdateJobWithExpectedGeneration { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::GetJob { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ListJobs { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::UpsertJobVersion { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ListJobVersions { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::UpdateJob { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::UpdateJobObservation { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::UpdateJobDesiredState { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::UpsertJobCheckpoint { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ListJobCheckpoints { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::DeleteJobCheckpoint { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::UpsertNode { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ResetObservedCursors { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::SetDesired { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::GetDesired { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::GetIntent { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ListIntents { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::RecoverReconciliation { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::WakeNode { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ListEvents { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::PruneEvents { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::PruneOperationHistory { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::PruneJobCheckpointRecords { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::PruneAuditEvents { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::PruneProcessedOutbox { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::PruneTerminalAttempts { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ClaimAttempt { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::MarkAttemptDispatched { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ExpireAttempts { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::CompleteAttempt { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::RecordObserved { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ClaimOutbox { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::MarkOutboxProcessed { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::SetNodeMaintenance { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::GetNodeMaintenance { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::OperationalAggregates { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::RecordAudit { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ListAudit { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::CreateRollout { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::CreateRolloutWithContent { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::GetRollout { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ListRolloutTargets { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::UpdateRollout { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::UpdateRolloutTarget { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::GetConfigVersionContent { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::RecoverRollouts { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ListRollouts { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::UpsertJobUpgrade { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::TransitionJobUpgrade { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::GetJobUpgrade { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::RecoverJobUpgrades { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ListJobUpgrades { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::PruneJobUpgrades { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::UpsertOperation { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::GetOperation { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ListOperations { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ListJobStartOperations { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::TryAcquireHubLease { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::RenewHubLease { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ReleaseHubLease { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::ReadHubLeaseSnapshot { response } => {
                let _ = response.send(Err(error));
            }
            Self::Fenced { command, .. } => command.nack(error),
        }
    }
}

/// Snapshot of the singleton control-plane lease row (`cp_hub_lease`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubLeaseSnapshot {
    pub holder: String,
    pub epoch: u64,
    pub expires_at_ms: u64,
    /// Leader's advertised API base URL (hub-ha stage 3): written by
    /// acquire/renew, mirrored by the standby 503 as the `leader_url` hint.
    pub advertise_url: Option<String>,
}

/// Outcome of `try_acquire_hub_lease`: either the caller now holds the lease
/// (a takeover bumped the fencing epoch; a self-acquire keeps it) or another
/// live holder owns it and nothing was modified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubLeaseAcquire {
    Acquired { epoch: u64 },
    HeldByOther(HubLeaseSnapshot),
}

/// Outcome of `renew_hub_lease`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubLeaseRenew {
    Renewed { epoch: u64 },
    Lost,
}

#[derive(Clone)]
pub struct StorageActor {
    sender: mpsc::Sender<StorageCommand>,
    /// The process's current leadership claim (lease epoch while leader,
    /// 0 otherwise). Fenced senders capture this at send time; the actor
    /// verifies it against the lease row at execution time.
    leadership_epoch: Arc<AtomicU64>,
}

impl StorageActor {
    pub fn start(store: ControlPlaneStore, capacity: usize) -> Self {
        let (sender, mut receiver) = mpsc::channel(capacity.max(1));
        let leadership_epoch = Arc::new(AtomicU64::new(UNFENCED));
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                dispatch(&store, command).await;
            }
        });
        Self {
            sender,
            leadership_epoch,
        }
    }
}

/// Execute one storage command. The `Fenced` envelope verifies the caller's
/// claimed lease epoch against the lease row BEFORE the inner command runs:
/// a superseded leader's mutations are rejected with `StaleLeader` and
/// produce no durable side effect. No lease row (HA disabled) passes
/// through unchanged.
async fn dispatch(store: &ControlPlaneStore, command: StorageCommand) {
    match command {
        // Fenced mutations run inside a takeover-serialized fence: the
        // claim is verified AND held locked across the write, so a
        // competing Hub's takeover cannot commit between the check and
        // the mutation. A stale claim is nack'd with no side effects.
        StorageCommand::Fenced {
            claimed_epoch,
            command: inner,
        } => {
            match store.begin_write_fence(claimed_epoch).await {
                Ok(WriteFence::Held) => {
                    Box::pin(dispatch(store, *inner)).await;
                    if let Err(error) = store.end_write_fence().await {
                        // The mutation already reported success; a fence
                        // commit failure is a durability incident, logged
                        // loudly rather than dropped.
                        tracing::error!(
                            %error,
                            "write-fence commit failed after a fenced mutation"
                        );
                    }
                }
                // Nothing to fence against (no lease row): run bare.
                Ok(WriteFence::Passthrough) => {
                    Box::pin(dispatch(store, *inner)).await;
                }
                Err(error) => inner.nack(error),
            }
        }
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
                    StorageCommand::UpsertJobUpgrade { record, response } => {
                        let _ = response.send(store.upsert_job_upgrade(record).await);
                    }
                    StorageCommand::TransitionJobUpgrade {
                        record,
                        expected_phase,
                        response,
                    } => {
                        let _ = response
                            .send(store.transition_job_upgrade(record, &expected_phase).await);
                    }
                    StorageCommand::GetJobUpgrade {
                        upgrade_id,
                        response,
                    } => {
                        let _ = response.send(store.get_job_upgrade(&upgrade_id).await);
                    }
                    StorageCommand::RecoverJobUpgrades { response } => {
                        let _ = response.send(store.recover_job_upgrades().await);
                    }
                    StorageCommand::ListJobUpgrades { job_id, response } => {
                        let _ = response.send(store.list_job_upgrades(&job_id).await);
                    }
                    StorageCommand::PruneJobUpgrades {
                        older_than_ms,
                        max_retained,
                        response,
                    } => {
                        let _ = response
                            .send(store.prune_job_upgrades(older_than_ms, max_retained).await);
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
                    StorageCommand::TryAcquireHubLease {
                        holder,
                        advertise_url,
                        ttl_ms,
                        now_ms,
                        response,
                    } => {
                        let _ = response.send(
                            store
                                .try_acquire_hub_lease(&holder, advertise_url.as_deref(), ttl_ms, now_ms)
                                .await,
                        );
                    }
                    StorageCommand::RenewHubLease {
                        holder,
                        advertise_url,
                        ttl_ms,
                        now_ms,
                        response,
                    } => {
                        let _ = response.send(
                            store
                                .renew_hub_lease(&holder, advertise_url.as_deref(), ttl_ms, now_ms)
                                .await,
                        );
                    }
                    StorageCommand::ReleaseHubLease {
                        holder,
                        now_ms,
                        response,
                    } => {
                        let _ = response.send(store.release_hub_lease(&holder, now_ms).await);
                    }
                    StorageCommand::ReadHubLeaseSnapshot { response } => {
                        let _ = response.send(store.hub_lease_snapshot().await);
                    }
                }
}

impl StorageActor {

    /// The current leadership claim, for the election loop to keep in sync
    /// (lease epoch while leader, 0 on losing/never-holding the lease).
    pub fn leadership_epoch(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.leadership_epoch)
    }

    /// Send one mutating command behind the write-fencing envelope: the
    /// claimed epoch is captured NOW, the actor checks it against the
    /// lease row when the command executes.
    async fn send_fenced(
        &self,
        command: StorageCommand,
    ) -> Result<(), mpsc::error::SendError<StorageCommand>> {
        let claimed_epoch = self.leadership_epoch.load(std::sync::atomic::Ordering::Acquire);
        if claimed_epoch == UNFENCED {
            // HA disabled: fencing must not depend on the absence of a
            // lease row — a leftover row from an earlier HA deployment
            // would otherwise reject every write.
            return self.sender.send(command).await;
        }
        self.sender
            .send(StorageCommand::Fenced {
                claimed_epoch,
                command: Box::new(command),
            })
            .await
    }

    pub async fn upsert_job(&self, job: JobRecord) -> Result<JobRecord, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.send_fenced(StorageCommand::UpsertJob { job, response })
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
        self.send_fenced(StorageCommand::UpdateJobWithExpectedGeneration {
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
        self.send_fenced(StorageCommand::UpsertJobVersion { record, response })
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
        self.send_fenced(StorageCommand::UpdateJob {
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

    #[allow(clippy::too_many_arguments)]
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
        self.send_fenced(StorageCommand::UpdateJobObservation {
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
        self.send_fenced(StorageCommand::UpdateJobDesiredState {
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
        self.send_fenced(StorageCommand::UpsertJobCheckpoint { record, response })
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
        self.send_fenced(StorageCommand::DeleteJobCheckpoint {
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
        self.send_fenced(StorageCommand::SetDesired { mutation, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn upsert_node(&self, mutation: NodeMutation) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.send_fenced(StorageCommand::UpsertNode { mutation, response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn reset_observed_cursors(
        &self,
        node_id: impl Into<String>,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.send_fenced(StorageCommand::ResetObservedCursors {
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
        self.send_fenced(StorageCommand::ClaimOutbox {
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
        self.send_fenced(StorageCommand::RecoverReconciliation { now_ms, response })
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
        self.send_fenced(StorageCommand::WakeNode {
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
        self.send_fenced(StorageCommand::PruneEvents { retain, response })
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
        self.send_fenced(StorageCommand::PruneOperationHistory {
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
        self.send_fenced(StorageCommand::PruneJobCheckpointRecords {
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
        self.send_fenced(StorageCommand::PruneAuditEvents {
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
        self.send_fenced(StorageCommand::PruneProcessedOutbox {
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
        self.send_fenced(StorageCommand::PruneTerminalAttempts {
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
        self.send_fenced(StorageCommand::ClaimAttempt {
                intent_id: intent_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn record_observed(&self, mutation: ObservedMutation) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.send_fenced(StorageCommand::RecordObserved { mutation, response })
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
        self.send_fenced(StorageCommand::MarkAttemptDispatched {
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
        self.send_fenced(StorageCommand::ExpireAttempts { now_ms, response })
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
        self.send_fenced(StorageCommand::CompleteAttempt {
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
        self.send_fenced(StorageCommand::SetNodeMaintenance {
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
        self.send_fenced(StorageCommand::MarkOutboxProcessed {
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
        self.send_fenced(StorageCommand::RecordAudit { record, response })
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
        self.send_fenced(StorageCommand::CreateRollout {
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
        self.send_fenced(StorageCommand::CreateRolloutWithContent {
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
        self.send_fenced(StorageCommand::UpdateRollout {
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
        self.send_fenced(StorageCommand::UpdateRolloutTarget { update, response })
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
        self.send_fenced(StorageCommand::RecoverRollouts { response })
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

    pub async fn upsert_job_upgrade(&self, record: JobUpgradeRecord) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.send_fenced(StorageCommand::UpsertJobUpgrade {
                record,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn transition_job_upgrade(
        &self,
        record: JobUpgradeRecord,
        expected_phase: impl Into<String>,
    ) -> Result<bool, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.send_fenced(StorageCommand::TransitionJobUpgrade {
                record,
                expected_phase: expected_phase.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn get_job_upgrade(
        &self,
        upgrade_id: impl Into<String>,
    ) -> Result<Option<JobUpgradeRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::GetJobUpgrade {
                upgrade_id: upgrade_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn recover_job_upgrades(&self) -> Result<Vec<JobUpgradeRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.send_fenced(StorageCommand::RecoverJobUpgrades { response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn list_job_upgrades(
        &self,
        job_id: impl Into<String>,
    ) -> Result<Vec<JobUpgradeRecord>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ListJobUpgrades {
                job_id: job_id.into(),
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn prune_job_upgrades(
        &self,
        older_than_ms: i64,
        max_retained: i64,
    ) -> Result<usize, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.send_fenced(StorageCommand::PruneJobUpgrades {
                older_than_ms,
                max_retained,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn upsert_operation(
        &self,
        operation: PersistedOperation,
    ) -> Result<(), StorageError> {
        let (response, receiver) = oneshot::channel();
        self.send_fenced(StorageCommand::UpsertOperation {
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

    pub async fn try_acquire_hub_lease(
        &self,
        holder: impl Into<String>,
        advertise_url: Option<String>,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<HubLeaseAcquire, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::TryAcquireHubLease {
                holder: holder.into(),
                advertise_url,
                ttl_ms,
                now_ms,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn renew_hub_lease(
        &self,
        holder: impl Into<String>,
        advertise_url: Option<String>,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<HubLeaseRenew, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::RenewHubLease {
                holder: holder.into(),
                advertise_url,
                ttl_ms,
                now_ms,
                response,
            })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    /// Current lease row for the standby `leader_url` hint; `None` when the
    /// table is empty (HA disabled). Read-only, exempt from write fencing.
    pub async fn hub_lease_snapshot(&self) -> Result<Option<HubLeaseSnapshot>, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ReadHubLeaseSnapshot { response })
            .await
            .map_err(|_| StorageError::ActorClosed)?;
        receiver.await.map_err(|_| StorageError::ActorClosed)?
    }

    pub async fn release_hub_lease(
        &self,
        holder: impl Into<String>,
        now_ms: u64,
    ) -> Result<bool, StorageError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::ReleaseHubLease {
                holder: holder.into(),
                now_ms,
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
    async fn upsert_job_upgrade(&self, record: JobUpgradeRecord) -> Result<(), StorageError>;
    /// Optimistically-concurrent mutation of an existing orchestration row:
    /// the mutable columns apply only while the row still holds
    /// `expected_phase`. Returns false (no error) when the phase moved, so
    /// the caller can treat it as a lost race instead of a storage fault.
    async fn transition_job_upgrade(
        &self,
        record: JobUpgradeRecord,
        expected_phase: &str,
    ) -> Result<bool, StorageError>;
    async fn get_job_upgrade(
        &self,
        upgrade_id: &str,
    ) -> Result<Option<JobUpgradeRecord>, StorageError>;
    async fn recover_job_upgrades(&self) -> Result<Vec<JobUpgradeRecord>, StorageError>;
    async fn list_job_upgrades(&self, job_id: &str) -> Result<Vec<JobUpgradeRecord>, StorageError>;
    async fn prune_job_upgrades(
        &self,
        older_than_ms: i64,
        max_retained: i64,
    ) -> Result<usize, StorageError>;
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
    #[allow(clippy::too_many_arguments)]
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
    #[allow(clippy::too_many_arguments)]
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
    async fn try_acquire_hub_lease(
        &self,
        holder: &str,
        advertise_url: Option<&str>,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<HubLeaseAcquire, StorageError>;
    async fn renew_hub_lease(
        &self,
        holder: &str,
        advertise_url: Option<&str>,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<HubLeaseRenew, StorageError>;
    async fn release_hub_lease(&self, holder: &str, now_ms: u64) -> Result<bool, StorageError>;
    /// Full lease row; `None` when no row exists (HA disabled).
    async fn hub_lease_snapshot(&self) -> Result<Option<HubLeaseSnapshot>, StorageError>;
    /// Current lease epoch for write fencing. `None` = no lease row (HA
    /// disabled): fenced commands pass through unchanged.
    async fn current_lease_epoch(&self) -> Result<Option<u64>, StorageError>;
    /// Begin a write fence for `claimed_epoch`, serialized against lease
    /// takeovers for the duration of the fenced mutation: PostgreSQL holds
    /// a `FOR SHARE` row lock on the lease until `end_write_fence`;
    /// SQLite holds an open `BEGIN IMMEDIATE` transaction. A stale claim
    /// errors without side effects; no lease row passes without holding a
    /// lock. This closes the check-then-write window a competing Hub
    /// process could otherwise interleave a takeover through.
    async fn begin_write_fence(&self, claimed_epoch: u64) -> Result<WriteFence, StorageError>;
    /// Commit/release the fence begun by `begin_write_fence`.
    async fn end_write_fence(&self) -> Result<(), StorageError>;
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

    /// Contract-test backend selector: SQLite in-memory normally; when
    /// `ARKFLOW_TEST_POSTGRES_URL` points at a live server, the label gets a
    /// dedicated database (dropped and recreated) so parallel contract tests
    /// stay isolated while exercising the Postgres code paths.
    #[cfg(test)]
    pub async fn contract(label: &str) -> ControlPlaneStore {
        match std::env::var("ARKFLOW_TEST_POSTGRES_URL") {
            Ok(_) => ControlPlaneStore::open(&contract_database_url(label).await).await.unwrap(),
            Err(_) => ControlPlaneStore::in_memory().unwrap(),
        }
    }
}

/// Create (or recreate) a dedicated Postgres database for a contract test
/// and return the full URL to it. Panics without a live server: callers are
/// gated on `ARKFLOW_TEST_POSTGRES_URL` first.
#[cfg(test)]
#[allow(clippy::await_holding_lock)] // the admin lock intentionally spans the awaits (test helper)
pub(crate) async fn contract_database_url(label: &str) -> String {
    use sqlx::Connection as _;
    // CREATE/DROP DATABASE cannot run while other sessions touch template1;
    // serialize the admin phase process-wide so parallel tests queue here.
    // The guard intentionally spans the awaits: holding the lock across the
    // admin session's lifetime is exactly the serialization we need (test
    // helper only; the pool size is one admin connection at a time).
    static ADMIN_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = ADMIN_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    // A label can be requested more than once per test run (a fixture
    // helper plus the test body); each request gets its own database so a
    // recreate never drops a database another holder still uses.
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let base = std::env::var("ARKFLOW_TEST_POSTGRES_URL").unwrap();
    let suffix: String = label
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .take(50)
        .collect();
    let db = format!("ct_{suffix}_{sequence}");
    let mut admin = sqlx::PgConnection::connect(&base).await.unwrap();
    // FORCE drops concurrent connections from a previously aborted run.
    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS {db} WITH (FORCE)"
    )))
    .execute(&mut admin)
    .await;
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {db}")))
        .execute(&mut admin)
        .await
        .unwrap();
    drop(admin);
    // Swap the database name in "scheme://authority/dbname".
    let pos = base.find("://").map(|p| p + 3).unwrap_or(0);
    let after = &base[pos..];
    match after.find('/') {
        Some(i) => format!("{}{}/{db}", &base[..pos], &after[..i]),
        None => format!("{}/{db}", base.trim_end_matches('/')),
    }
}

impl ControlPlaneStore {
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
        operation: impl FnOnce(&rusqlite::Connection) -> Result<T, StorageError>,
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
    async fn upsert_job_upgrade(&self, record: JobUpgradeRecord) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::upsert_job_upgrade(backend, record).await,
            Self::Postgres(backend) => StorageBackend::upsert_job_upgrade(backend, record).await,
        }
    }
    async fn transition_job_upgrade(
        &self,
        record: JobUpgradeRecord,
        expected_phase: &str,
    ) -> Result<bool, StorageError> {
        match self {
            Self::Sqlite(backend) => {
                StorageBackend::transition_job_upgrade(backend, record, expected_phase).await
            }
            Self::Postgres(backend) => {
                StorageBackend::transition_job_upgrade(backend, record, expected_phase).await
            }
        }
    }
    async fn get_job_upgrade(
        &self,
        upgrade_id: &str,
    ) -> Result<Option<JobUpgradeRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::get_job_upgrade(backend, upgrade_id).await,
            Self::Postgres(backend) => StorageBackend::get_job_upgrade(backend, upgrade_id).await,
        }
    }
    async fn recover_job_upgrades(&self) -> Result<Vec<JobUpgradeRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::recover_job_upgrades(backend).await,
            Self::Postgres(backend) => StorageBackend::recover_job_upgrades(backend).await,
        }
    }
    async fn list_job_upgrades(&self, job_id: &str) -> Result<Vec<JobUpgradeRecord>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::list_job_upgrades(backend, job_id).await,
            Self::Postgres(backend) => StorageBackend::list_job_upgrades(backend, job_id).await,
        }
    }
    async fn prune_job_upgrades(
        &self,
        older_than_ms: i64,
        max_retained: i64,
    ) -> Result<usize, StorageError> {
        match self {
            Self::Sqlite(backend) => {
                StorageBackend::prune_job_upgrades(backend, older_than_ms, max_retained).await
            }
            Self::Postgres(backend) => {
                StorageBackend::prune_job_upgrades(backend, older_than_ms, max_retained).await
            }
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
    async fn upsert_job(&self, job: JobRecord) -> Result<JobRecord, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::upsert_job(backend, job).await,
            Self::Postgres(backend) => StorageBackend::upsert_job(backend, job).await,
        }
    }
    async fn update_job_with_expected_generation(
&self,
job: JobRecord,
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
    async fn try_acquire_hub_lease(
        &self,
        holder: &str,
        advertise_url: Option<&str>,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<HubLeaseAcquire, StorageError> {
        match self {
            Self::Sqlite(backend) => {
                StorageBackend::try_acquire_hub_lease(backend, holder, advertise_url, ttl_ms, now_ms)
                    .await
            }
            Self::Postgres(backend) => {
                StorageBackend::try_acquire_hub_lease(backend, holder, advertise_url, ttl_ms, now_ms)
                    .await
            }
        }
    }
    async fn renew_hub_lease(
        &self,
        holder: &str,
        advertise_url: Option<&str>,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<HubLeaseRenew, StorageError> {
        match self {
            Self::Sqlite(backend) => {
                StorageBackend::renew_hub_lease(backend, holder, advertise_url, ttl_ms, now_ms).await
            }
            Self::Postgres(backend) => {
                StorageBackend::renew_hub_lease(backend, holder, advertise_url, ttl_ms, now_ms).await
            }
        }
    }
    async fn hub_lease_snapshot(&self) -> Result<Option<HubLeaseSnapshot>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::hub_lease_snapshot(backend).await,
            Self::Postgres(backend) => StorageBackend::hub_lease_snapshot(backend).await,
        }
    }
    async fn release_hub_lease(&self, holder: &str, now_ms: u64) -> Result<bool, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::release_hub_lease(backend, holder, now_ms).await,
            Self::Postgres(backend) => StorageBackend::release_hub_lease(backend, holder, now_ms).await,
        }
    }
    async fn current_lease_epoch(&self) -> Result<Option<u64>, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::current_lease_epoch(backend).await,
            Self::Postgres(backend) => StorageBackend::current_lease_epoch(backend).await,
        }
    }
    async fn begin_write_fence(&self, claimed_epoch: u64) -> Result<WriteFence, StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::begin_write_fence(backend, claimed_epoch).await,
            Self::Postgres(backend) => StorageBackend::begin_write_fence(backend, claimed_epoch).await,
        }
    }
    async fn end_write_fence(&self) -> Result<(), StorageError> {
        match self {
            Self::Sqlite(backend) => StorageBackend::end_write_fence(backend).await,
            Self::Postgres(backend) => StorageBackend::end_write_fence(backend).await,
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
        assert!(store.table_exists("cp_job_upgrades").unwrap());
    }

    #[tokio::test]
    async fn job_upgrade_record_round_trips_and_prunes() {
        let store = ControlPlaneStore::contract("job_upgrade_record_round_trips_and_prunes").await;
        let mut record = JobUpgradeRecord {
            upgrade_id: "job-upgrade-1".into(),
            job_id: "job-a".into(),
            from_version: 1,
            to_version: 2,
            phase: "saving_savepoint".into(),
            savepoint_id: None,
            target_spec_json: "{\"version\":[2]}".into(),
            phase_deadline_at_ms: 100,
            savepoint_retries: 0,
            verify_timeout_ms: 0,
            actor: Some("operator".into()),
            correlation_id: None,
            last_error: None,
            paused_from: None,
            created_at_ms: 50,
            updated_at_ms: 50,
        };
        store.upsert_job_upgrade(record.clone()).await.unwrap();

        // Upsert mutates the mutable columns only; identity columns stay.
        record.phase = "verifying".into();
        record.savepoint_id = Some("savepoint-job-a-2-90".into());
        record.savepoint_retries = 1;
        store.upsert_job_upgrade(record.clone()).await.unwrap();

        let loaded = store.get_job_upgrade("job-upgrade-1").await.unwrap();
        assert_eq!(loaded.as_ref(), Some(&record));
        assert!(!record.phase_is_terminal());

        // Phase guard: a transition applies only against the phase the
        // caller read; a moved phase is a lost race, not a storage fault.
        // The stored phase here is "verifying" (set above).
        let mut guarded = record.clone();
        guarded.phase = "committing_version".into();
        assert!(
            store
                .transition_job_upgrade(guarded, "verifying")
                .await
                .unwrap()
        );
        let mut moved = record.clone();
        moved.phase = "rolling_back".into();
        assert!(
            !store
                .transition_job_upgrade(moved, "verifying")
                .await
                .unwrap(),
            "the phase moved; the stale writer must lose"
        );
        let stored = store.get_job_upgrade("job-upgrade-1").await.unwrap().unwrap();
        assert_eq!(stored.phase, "committing_version");
        record.phase = "committing_version".into();

        // Recover returns only non-terminal rows.
        let active = store.recover_job_upgrades().await.unwrap();
        assert_eq!(active, vec![record.clone()]);
        let listed = store.list_job_upgrades("job-a").await.unwrap();
        assert_eq!(listed.len(), 1);

        // Terminal rows are excluded from recovery and pruned past bounds.
        record.phase = "succeeded".into();
        record.updated_at_ms = 10_000;
        store.upsert_job_upgrade(record.clone()).await.unwrap();
        assert!(record.phase_is_terminal());
        assert!(store.recover_job_upgrades().await.unwrap().is_empty());
        store
            .prune_job_upgrades(15_000, 0)
            .await
            .unwrap();
        assert!(store.get_job_upgrade("job-upgrade-1").await.unwrap().is_none());
    }

    fn audit_row(event: u64) -> AuditRecord {
        AuditRecord {
            event_id: 0,
            actor: Some("fencing-test".into()),
            action: format!("probe.{event}"),
            resource_type: "probe".into(),
            resource_id: Some(format!("probe-{event}")),
            node_id: None,
            stream_id: None,
            correlation_id: None,
            outcome: "ok".into(),
            failure_code: None,
            message: None,
            occurred_at_ms: event,
        }
    }

    /// Write fencing: fenced commands carry the sender's claimed lease
    /// epoch; the actor verifies it against the lease row at execution time.
    /// No lease row (HA disabled) passes everything; a live lease row only
    /// accepts the matching claim; lease operations themselves are exempt.
    #[tokio::test]
    async fn fenced_writes_reject_stale_leaders() {
        let store = ControlPlaneStore::contract("fenced_writes_reject_stale_leaders").await;
        let actor = StorageActor::start(store.clone(), 16);

        // No lease row yet: any claim passes, behaviour identical to a
        // deployment without HA.
        actor.leadership_epoch().store(7, Ordering::Release);
        actor.record_audit(audit_row(1)).await.unwrap();
        assert_eq!(store.current_lease_epoch().await.unwrap(), None);

        // hub-a acquires (epoch 1) through the UNfenced lease operation.
        assert_eq!(
            actor.try_acquire_hub_lease("hub-a", None, 1_000, 100).await.unwrap(),
            HubLeaseAcquire::Acquired { epoch: 1 }
        );
        assert_eq!(store.current_lease_epoch().await.unwrap(), Some(1));

        // The leader with the matching claim writes fine.
        actor.leadership_epoch().store(1, Ordering::Release);
        actor.record_audit(audit_row(2)).await.unwrap();

        // A standby (claim 0) is fenced: no durable side effect.
        actor.leadership_epoch().store(0, Ordering::Release);
        assert!(matches!(
            actor.record_audit(audit_row(3)).await,
            Err(StorageError::StaleLeader { claimed_epoch: 0, current_epoch: 1 })
        ));

        // Takeover by hub-b past expiry bumps the epoch to 2; hub-a's old
        // claim (still 1) is now stale — the exact zombie-leader window.
        assert_eq!(
            actor.try_acquire_hub_lease("hub-b", None, 1_000, 2_000).await.unwrap(),
            HubLeaseAcquire::Acquired { epoch: 2 }
        );
        // Restore hub-a's stale leader claim (it has not noticed yet).
        actor.leadership_epoch().store(1, Ordering::Release);
        let before = actor.list_audit(None::<String>).await.unwrap().len();
        assert!(matches!(
            actor.record_audit(audit_row(4)).await,
            Err(StorageError::StaleLeader { claimed_epoch: 1, current_epoch: 2 })
        ));
        assert_eq!(
            actor.list_audit(None::<String>).await.unwrap().len(),
            before,
            "fenced writes must leave no durable side effect"
        );

        // The new leader (claim 2) writes again; reads stayed unfenced
        // throughout.
        actor.leadership_epoch().store(2, Ordering::Release);
        actor.record_audit(audit_row(5)).await.unwrap();
        assert_eq!(actor.list_audit(None::<String>).await.unwrap().len(), before + 1);
    }

    /// CodeRabbit finding: with HA disabled, a LEFTOVER lease row (an
    /// earlier HA deployment) must not fence writes — the unfenced state
    /// is the explicit UNFENCED sentinel, not the absence of the row.
    #[tokio::test]
    async fn unfenced_claim_bypasses_a_leftover_lease_row() {
        let store = ControlPlaneStore::contract("unfenced_claim_bypasses_a_leftover_lease_row").await;
        let actor = StorageActor::start(store.clone(), 16);
        // Simulate a prior HA deployment: a lease row exists (epoch 1 —
        // a live holder's self-acquire stays idempotent).
        assert_eq!(
            store.try_acquire_hub_lease("old-hub", None, 1_000, 100).await.unwrap(),
            HubLeaseAcquire::Acquired { epoch: 1 }
        );
        assert_eq!(store.current_lease_epoch().await.unwrap(), Some(1));

        // This process never enters the election: claim stays UNFENCED and
        // every mutation passes despite the row.
        assert_eq!(
            actor.leadership_epoch().load(Ordering::Acquire),
            crate::storage::UNFENCED
        );
        actor.record_audit(audit_row(11)).await.unwrap();
        actor.record_audit(audit_row(12)).await.unwrap();
        assert_eq!(
            actor.list_audit(None::<String>).await.unwrap().len(),
            2
        );
    }

    /// The hub-lease contract every backend must satisfy: expiry takeover
    /// bumps the fencing epoch, live holders refuse takeover, renewal only
    /// works while held and unexpired, self-acquire is idempotent, and
    /// release expires the caller's lease immediately.
    #[tokio::test]
    async fn hub_lease_acquire_renew_release_contract() {
        let store = ControlPlaneStore::contract("hub_lease_acquire_renew_release_contract").await;
        assert_eq!(store.hub_lease_snapshot().await.unwrap(), None);
        // Fresh row: the first acquire is a takeover of the expired default,
        // and it persists the leader's advertised address.
        assert_eq!(
            store
                .try_acquire_hub_lease("hub-a", Some("http://leader-a:8080"), 1_000, 100)
                .await
                .unwrap(),
            HubLeaseAcquire::Acquired { epoch: 1 }
        );
        // Another live holder is refused and observes the current lease
        // including the advertisement.
        assert_eq!(
            store.try_acquire_hub_lease("hub-b", None, 1_000, 200).await.unwrap(),
            HubLeaseAcquire::HeldByOther(HubLeaseSnapshot {
                holder: "hub-a".into(),
                epoch: 1,
                expires_at_ms: 1_100,
                advertise_url: Some("http://leader-a:8080".into()),
            })
        );
        // Holder renews; epoch is stable and the row mirrors the renewal's
        // (changed) advertisement.
        assert_eq!(
            store
                .renew_hub_lease("hub-a", Some("http://leader-a:8081"), 1_000, 500)
                .await
                .unwrap(),
            HubLeaseRenew::Renewed { epoch: 1 }
        );
        assert_eq!(
            store.hub_lease_snapshot().await.unwrap(),
            Some(HubLeaseSnapshot {
                holder: "hub-a".into(),
                epoch: 1,
                expires_at_ms: 1_500,
                advertise_url: Some("http://leader-a:8081".into()),
            })
        );
        // Non-holder renewal is Lost without touching the row.
        assert_eq!(
            store.renew_hub_lease("hub-b", None, 1_000, 500).await.unwrap(),
            HubLeaseRenew::Lost
        );
        // Past expiry the old holder can no longer renew.
        assert_eq!(
            store.renew_hub_lease("hub-a", None, 1_000, 2_000).await.unwrap(),
            HubLeaseRenew::Lost
        );
        // Takeover after expiry bumps the epoch and rewrites the row to the
        // taker's advertisement (here: none, which clears the column).
        assert_eq!(
            store.try_acquire_hub_lease("hub-b", None, 1_000, 2_000).await.unwrap(),
            HubLeaseAcquire::Acquired { epoch: 2 }
        );
        assert_eq!(
            store.hub_lease_snapshot().await.unwrap(),
            Some(HubLeaseSnapshot {
                holder: "hub-b".into(),
                epoch: 2,
                expires_at_ms: 3_000,
                advertise_url: None,
            })
        );
        // Self-acquire keeps the epoch and extends the TTL.
        assert_eq!(
            store.try_acquire_hub_lease("hub-b", None, 2_000, 2_500).await.unwrap(),
            HubLeaseAcquire::Acquired { epoch: 2 }
        );
        assert_eq!(
            store.try_acquire_hub_lease("hub-a", None, 1_000, 2_600).await.unwrap(),
            HubLeaseAcquire::HeldByOther(HubLeaseSnapshot {
                holder: "hub-b".into(),
                epoch: 2,
                expires_at_ms: 4_500,
                advertise_url: None,
            })
        );
        // Release expires immediately (and only for the holder); a second
        // release of an already-expired lease is a no-op.
        assert!(!store.release_hub_lease("hub-a", 2_700).await.unwrap());
        assert!(store.release_hub_lease("hub-b", 2_900).await.unwrap());
        assert!(!store.release_hub_lease("hub-b", 2_950).await.unwrap());
        assert_eq!(
            store.try_acquire_hub_lease("hub-a", None, 1_000, 3_000).await.unwrap(),
            HubLeaseAcquire::Acquired { epoch: 3 }
        );
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
        let store = ControlPlaneStore::contract("rollout_creation_is_atomic_when_a_target_conflicts").await;
        // Seed the config version through the public surface: a desired
        // mutation carrying config + payload inserts the inline version row
        // (PostgreSQL enforces the rollout foreign key).
        store
            .set_desired(DesiredMutation {
                node_id: "node-seed".into(),
                stream_id: "__configuration__".into(),
                desired_state: "configured".into(),
                config_version_id: Some("cfg-1".into()),
                intent_type: Some("apply_configuration".into()),
                payload_json: Some("{\"seed\":true}".into()),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await
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
        let store = ControlPlaneStore::contract("maintenance_transitions_are_durable_and_audited").await;
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
        let store = ControlPlaneStore::contract("event_retention_prunes_oldest_durable_ids").await;
        // Each desired mutation durably appends one intent_created event.
        for stream in ["orders", "etl"] {
            store
                .set_desired(DesiredMutation {
                    node_id: "node-a".into(),
                    stream_id: stream.into(),
                    desired_state: "running".into(),
                    expected_generation: Some(0),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        let events = store.list_events(None).await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(events[0].event_id > events[1].event_id, "newest first");
        assert_eq!(store.prune_events(1).await.unwrap(), 1);
        let kept = store.list_events(None).await.unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].event_id, events[0].event_id);
    }

    /// Audit retention reclaims by age and count bound while the resource
    /// filter keeps addressing single resources.
    #[tokio::test]
    async fn audit_retention_reclaims_old_rows_and_keeps_the_filter_addressable() {
        let store =
            ControlPlaneStore::contract("audit_retention_reclaims_old_rows_and_keeps_the_filter_addressable")
                .await;
        let row = |resource: &str, occurred_at_ms: u64| AuditRecord {
            event_id: 0,
            actor: Some("retention-test".into()),
            action: "probe.audit".into(),
            resource_type: "probe".into(),
            resource_id: Some(resource.into()),
            node_id: None,
            stream_id: None,
            correlation_id: None,
            outcome: "ok".into(),
            failure_code: None,
            message: None,
            occurred_at_ms,
        };
        store.record_audit(row("probe-old", 100)).await.unwrap();
        store.record_audit(row("probe-a", 9_000)).await.unwrap();
        store.record_audit(row("probe-b", 9_001)).await.unwrap();
        assert_eq!(store.list_audit(Some("probe-a")).await.unwrap().len(), 1);
        assert_eq!(store.list_audit(Some("probe-old")).await.unwrap().len(), 1);
        assert_eq!(store.list_audit(None::<&str>).await.unwrap().len(), 3);
        // The age window reclaims only the row past the cutoff.
        assert_eq!(store.prune_audit_events(1_000, 4_096).await.unwrap(), 1);
        // The count bound keeps the newest rows when history accumulates
        // faster than the age window reclaims it.
        for index in 0..4 {
            store
                .record_audit(row("probe-bulk", 9_002 + index))
                .await
                .unwrap();
        }
        assert_eq!(store.prune_audit_events(1_000, 2).await.unwrap(), 4);
        let audit = store.list_audit(None::<&str>).await.unwrap();
        assert_eq!(audit.len(), 2);
        assert!(audit.iter().all(|record| record.occurred_at_ms >= 9_004));
    }

    #[tokio::test]
    async fn operational_aggregates_are_bounded_and_include_pending_age() {
        let store = ControlPlaneStore::contract("operational_aggregates_are_bounded_and_include_pending_age").await;
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
        let store =
            ControlPlaneStore::contract("desired_mutation_commits_intent_and_outbox_atomically").await;
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
        // The outbox row is committed in the same atomic unit as the intent.
        assert_eq!(
            store
                .operational_aggregates(now_ms())
                .await
                .unwrap()
                .outbox_pending,
            1
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
        let store = ControlPlaneStore::contract("conditional_job_write_preserves_a_newer_recovery_pointer").await;
        let job = |job_id: &str, checkpoint_id: Option<&str>| JobRecord {
            job_id: job_id.into(),
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
        store.upsert_job(job("orders", Some("ckpt-old"))).await.unwrap();

        // A concurrent checkpoint observation moves the pointer without
        // touching the generation.
        let concurrent = store
            .update_job(
                "orders",
                None,
                None,
                None,
                None,
                Some("ckpt-new"),
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
            .update_job_with_expected_generation(job("orders", Some("ckpt-old")), 1)
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
        // so a NULL pointer stays NULL and the version/spec change lands. A
        // freshly created job stores a NULL pointer without SQL surgery.
        store.upsert_job(job("etl", None)).await.unwrap();
        let written = store
            .update_job_with_expected_generation(job("etl", Some("ckpt-fresh")), 1)
            .await.unwrap();
        assert_eq!(
            written.checkpoint_id, None,
            "the rollback write must not invent a recovery pointer"
        );
        assert_eq!(written.version, 2);

        // A stale generation still conflicts.
        assert!(matches!(
            store.update_job_with_expected_generation(job("orders", None), 1).await,
            Err(StorageError::GenerationConflict { .. })
        ));
    }

    #[tokio::test]
    async fn outbox_claim_is_idempotent_and_reclaimable_after_lease() {
        let store =
            ControlPlaneStore::contract("outbox_claim_is_idempotent_and_reclaimable_after_lease").await;
        // A desired mutation enqueues exactly one reconcile outbox row.
        store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await
            .unwrap();
        let base = now_ms();
        let first = store.claim_outbox("worker-a", base).await.unwrap().unwrap();
        assert_eq!(first.event_type, "reconcile_intent");
        assert_eq!(first.node_id, "node-a");
        assert_eq!(first.stream_id.as_deref(), Some("orders"));
        assert!(store.claim_outbox("worker-b", base + 1).await.unwrap().is_none());
        assert_eq!(
            store
                .claim_outbox("worker-b", base + 30_011)
                .await.unwrap()
                .unwrap()
                .outbox_id,
            first.outbox_id
        );
        store
            .mark_outbox_processed(first.outbox_id, base + 30_012)
            .await.unwrap();
        assert!(store.claim_outbox("worker-c", base + 30_013).await.unwrap().is_none());
    }

    /// The outbox retention reclaims only processed rows: the unprocessed
    /// work queue (pending or claimed) survives every sweep, and the status
    /// counters — which only look at unprocessed rows — are unaffected.
    #[tokio::test]
    async fn prune_processed_outbox_reclaims_only_processed_rows() {
        let store =
            ControlPlaneStore::contract("prune_processed_outbox_reclaims_only_processed_rows").await;
        // Enqueue three rows: one to process early, one to leave claimed, and
        // one to process recently.
        let seed = |node: &str, stream: &str| {
            let store = store.clone();
            let node = node.to_owned();
            let stream = stream.to_owned();
            async move {
                store
                    .set_desired(DesiredMutation {
                        node_id: node,
                        stream_id: stream,
                        desired_state: "running".into(),
                        expected_generation: Some(0),
                        ..Default::default()
                    })
                    .await
                    .unwrap()
            }
        };
        seed("node-a", "orders").await;
        seed("node-a", "etl").await;
        seed("node-b", "etl").await;
        // Capture the clock after the seeds: their availability timestamps
        // were assigned at insert time (a remote backend may sit a few
        // round trips after any earlier reading).
        let base = now_ms();
        let old_processed = store.claim_outbox("worker", base).await.unwrap().unwrap();
        store
            .mark_outbox_processed(old_processed.outbox_id, 100)
            .await
            .unwrap();
        let claimed_pending = store.claim_outbox("worker", base + 1).await.unwrap().unwrap();
        let recent_processed = store.claim_outbox("worker", base + 2).await.unwrap().unwrap();
        store
            .mark_outbox_processed(recent_processed.outbox_id, 9_000)
            .await
            .unwrap();
        let aggregates_before = store.operational_aggregates(base + 10).await.unwrap();
        assert_eq!(aggregates_before.outbox_pending, 1);
        assert_eq!(aggregates_before.outbox_claimed, 1);

        // The age window reclaims only the processed row past the cutoff.
        assert_eq!(store.prune_processed_outbox(1_000, 4_096).await.unwrap(), 1);

        // The count bound keeps the newest processed rows when history
        // accumulates faster than the age window reclaims it.
        for index in 0u64..6 {
            seed(&format!("node-c{index}"), "etl").await;
            let bulk = store
                .claim_outbox("worker", now_ms() + 10)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(bulk.outbox_id, recent_processed.outbox_id + 1 + index as i64);
            store
                .mark_outbox_processed(bulk.outbox_id, 2_000 + index)
                .await
                .unwrap();
        }
        assert_eq!(store.prune_processed_outbox(1_000, 2).await.unwrap(), 5);
        let aggregates_after = store.operational_aggregates(base + 100).await.unwrap();
        assert_eq!(
            aggregates_before.outbox_pending,
            aggregates_after.outbox_pending,
            "the claimed-but-unprocessed work queue survives every sweep"
        );
        assert_eq!(aggregates_before.outbox_claimed, aggregates_after.outbox_claimed);
        // The claimed row is still reclaimable by its original bookkeeping.
        assert_eq!(
            store
                .claim_outbox("worker-z", base + 30_011)
                .await.unwrap()
                .unwrap()
                .outbox_id,
            claimed_pending.outbox_id
        );
    }

    /// Attempt retention reclaims terminal rows only; the active attempt —
    /// guarded by both the state predicate and the `cp_one_active_attempt`
    /// unique index — is preserved unchanged.
    #[tokio::test]
    async fn prune_terminal_attempts_reclaims_only_terminal_rows() {
        let store =
            ControlPlaneStore::contract("prune_terminal_attempts_reclaims_only_terminal_rows").await;
        // Past 2100-01-01: every attempt finished at wall-clock now is older.
        const FAR_FUTURE_MS: i64 = 4_102_444_800_000;
        let terminal_intent = |node: &str, stream: &str, state: &str| {
            let store = store.clone();
            let node = node.to_owned();
            let stream = stream.to_owned();
            let state = state.to_owned();
            async move {
                let intent = store
                    .set_desired(DesiredMutation {
                        node_id: node,
                        stream_id: stream,
                        desired_state: "running".into(),
                        expected_generation: Some(0),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                let attempt = store.claim_attempt(&intent.intent_id).await.unwrap().unwrap();
                store
                    .complete_attempt(&attempt.attempt_id, &state, None)
                    .await
                    .unwrap();
            }
        };
        terminal_intent("node-a", "orders", "succeeded").await;
        terminal_intent("node-a", "etl", "failed").await;
        // A still-active attempt: claimed but never completed.
        let active = store
            .set_desired(DesiredMutation {
                node_id: "node-b".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await
            .unwrap();
        let active_attempt = store.claim_attempt(&active.intent_id).await.unwrap().unwrap();
        assert_eq!(active_attempt.state, "queued");

        // The age window reclaims only the terminal rows past the cutoff.
        assert_eq!(
            store.prune_terminal_attempts(FAR_FUTURE_MS, 4_096).await.unwrap(),
            2
        );
        // The count bound trims terminal history down to the newest rows.
        terminal_intent("node-b", "etl", "succeeded").await;
        assert_eq!(store.prune_terminal_attempts(FAR_FUTURE_MS, 0).await.unwrap(), 1);
        let aggregates = store.operational_aggregates(now_ms()).await.unwrap();
        assert_eq!(aggregates.attempt_states, vec![("queued".into(), 1)]);
        assert_eq!(aggregates.active_attempts, 1);
        // The surviving active attempt is preserved unchanged.
        assert_eq!(
            store
                .claim_attempt(&active.intent_id)
                .await
                .unwrap()
                .unwrap()
                .attempt_id,
            active_attempt.attempt_id
        );
    }

    #[tokio::test]
    async fn storage_actor_serializes_desired_mutations() {
        let store = ControlPlaneStore::contract("storage_actor_serializes_desired_mutations").await;
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
        let store = ControlPlaneStore::contract("observed_generation_converges_intent_and_attempt").await;
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
        let converged = store.get_intent(&intent.intent_id).await.unwrap().unwrap();
        assert_eq!(converged.state, "converged");
        assert_eq!(converged.convergence_state, "in_sync");
        let aggregates = store.operational_aggregates(now_ms()).await.unwrap();
        assert_eq!(aggregates.attempt_states, vec![("succeeded".into(), 1)]);
        let events = store.list_events(Some("node-a")).await.unwrap();
        assert!(events.iter().any(|event| {
            event.event_type == "intent_converged"
                && event.intent_id.as_deref() == Some(intent.intent_id.as_str())
        }));
        // A replayed report from the same session (seq below the cursor) is
        // rejected: the stored observation keeps the newer state.
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
        let reports = store
            .list_events(Some("node-a"))
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.event_type == "observed_report")
            .collect::<Vec<_>>();
        assert_eq!(reports.len(), 1, "the replayed report leaves no durable trace");
        assert_eq!(reports[0].outcome, "running");
    }

    /// A session rebuild (re-register with the same stable boot identity)
    /// restarts the Agent's report_seq at 1. The Hub resets the per-stream
    /// cursors at register; without that reset every new observation is
    /// silently dropped until the node re-reaches the previous session's
    /// high-water mark, blinding convergence for the whole rebuild gap.
    #[tokio::test]
    async fn stable_boot_session_rebuild_resets_the_observation_cursor() {
        let store =
            ControlPlaneStore::contract("stable_boot_session_rebuild_resets_the_observation_cursor").await;
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
        // The rebuilt report is durable: its observed_report event exists.
        let reports = store
            .list_events(Some("node-a"))
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.event_type == "observed_report")
            .collect::<Vec<_>>();
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].outcome, "failed");
        assert_eq!(reports[1].outcome, "running");
    }

    #[tokio::test]
    async fn restart_intent_requires_matching_completed_action() {
        let store =
            ControlPlaneStore::contract("restart_intent_requires_matching_completed_action").await;
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
        // A different completed action does not satisfy the restart intent.
        assert_eq!(
            store
                .get_intent(&intent.intent_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            "accepted"
        );
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
        // The matching action id completes the restart.
        assert_eq!(
            store
                .get_intent(&intent.intent_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            "converged"
        );
    }

    #[tokio::test]
    async fn recovery_requeues_pending_intents_after_processed_outbox() {
        let store =
            ControlPlaneStore::contract("recovery_requeues_pending_intents_after_processed_outbox").await;
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
        let pending = || async {
            store
                .operational_aggregates(now_ms())
                .await
                .unwrap()
                .outbox_pending
        };
        // Recovery is a no-op while unprocessed work already exists.
        store.recover_reconciliation(base).await.unwrap();
        assert_eq!(pending().await, 1);
        let outbox = store.claim_outbox("worker", base).await.unwrap().unwrap();
        assert_eq!(outbox.intent_id.as_deref(), Some(intent.intent_id.as_str()));
        store
            .mark_outbox_processed(outbox.outbox_id, base + 1)
            .await.unwrap();
        assert_eq!(pending().await, 0);
        // Once the outbox work is processed, recovery requeues the intent.
        store.recover_reconciliation(base + 2).await.unwrap();
        assert_eq!(pending().await, 1);
    }

    #[tokio::test]
    async fn attempt_ack_is_not_terminal_and_temporary_failure_retries() {
        let store =
            ControlPlaneStore::contract("attempt_ack_is_not_terminal_and_temporary_failure_retries").await;
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
        // "acknowledged" is not terminal: the same attempt stays claimable
        // and the intent keeps converging instead of being blocked.
        let reclaimed = store.claim_attempt(&intent.intent_id).await.unwrap().unwrap();
        assert_eq!(reclaimed.attempt_id, attempt.attempt_id);
        assert_eq!(reclaimed.state, "acknowledged");
        store
            .complete_attempt(
                &attempt.attempt_id,
                "timed_out",
                Some("temporary_execution"),
            )
            .await.unwrap();
        let requeued = store.get_intent(&intent.intent_id).await.unwrap().unwrap();
        assert_eq!(requeued.state, "retrying");
        assert_eq!(requeued.convergence_state, "degraded");
        assert_eq!(requeued.retry_count, 1);
        assert_eq!(requeued.failure_class.as_deref(), Some("temporary_execution"));
        // The retry is durable outbox work: claim the original reconcile row
        // first, then the retry row once its backoff makes it available.
        let base = now_ms();
        let first = store.claim_outbox("worker", base).await.unwrap().unwrap();
        assert_eq!(first.event_type, "reconcile_intent");
        store
            .mark_outbox_processed(first.outbox_id, base + 1)
            .await.unwrap();
        let retry = store
            .claim_outbox("worker", now_ms() + 1_500)
            .await.unwrap().unwrap();
        assert_eq!(retry.event_type, "retry_intent");
        assert_eq!(retry.intent_id.as_deref(), Some(intent.intent_id.as_str()));
    }

    #[tokio::test]
    async fn node_registration_wakes_unprocessed_intents_after_prior_outbox_work() {
        let store = ControlPlaneStore::contract(
            "node_registration_wakes_unprocessed_intents_after_prior_outbox_work",
        )
        .await;
        store
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
        let pending = || async {
            store
                .operational_aggregates(now_ms())
                .await
                .unwrap()
                .outbox_pending
        };
        assert_eq!(pending().await, 0);
        // Re-registering the node requeues its still-unconverged intents.
        store.wake_node("node-a", timestamp + 2).await.unwrap();
        assert_eq!(pending().await, 1);
        // Waking again is idempotent while unprocessed work exists.
        store.wake_node("node-a", timestamp + 3).await.unwrap();
        assert_eq!(pending().await, 1);
        // Waking an unknown node leaves the queue untouched.
        store.wake_node("node-zzz", timestamp + 4).await.unwrap();
        assert_eq!(pending().await, 1);
    }

    #[tokio::test]
    async fn configuration_intent_requires_matching_observed_version() {
        let store = ControlPlaneStore::contract("configuration_intent_requires_matching_observed_version").await;
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
        let store = ControlPlaneStore::contract(
            "permanent_configuration_failure_blocks_until_a_new_generation",
        )
        .await;
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
        // A permanent failure never schedules retry work.
        let timestamp = now_ms();
        let outbox = store.claim_outbox("worker", timestamp).await.unwrap().unwrap();
        store
            .mark_outbox_processed(outbox.outbox_id, timestamp + 1)
            .await.unwrap();
        assert_eq!(
            store
                .operational_aggregates(now_ms())
                .await.unwrap()
                .outbox_pending,
            0
        );

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
        let store = ControlPlaneStore::contract("configuration_convergence_waits_for_affected_streams").await;
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
        let store =
            ControlPlaneStore::contract("expired_attempt_becomes_ambiguous_until_fresh_report").await;
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
        // Both the attempt and the intent degrade to ambiguous.
        let ambiguous = store.get_intent(&intent.intent_id).await.unwrap().unwrap();
        assert_eq!(ambiguous.state, "converging");
        assert_eq!(ambiguous.convergence_state, "degraded");
        assert_eq!(ambiguous.failure_class.as_deref(), Some("ambiguous"));
        let aggregates = store.operational_aggregates(now_ms()).await.unwrap();
        assert!(aggregates.attempt_states.contains(&("ambiguous".into(), 1)));
        store
            .complete_attempt(&attempt.attempt_id, "ambiguous", Some("ambiguous"))
            .await.unwrap();
        let still_degraded = store.get_intent(&intent.intent_id).await.unwrap().unwrap();
        assert_eq!(still_degraded.state, "converging");
        assert_eq!(still_degraded.convergence_state, "degraded");
        assert_eq!(
            store
                .operational_aggregates(now_ms())
                .await.unwrap()
                .outbox_pending,
            0,
            "an ambiguous intent is never auto-retried"
        );
        store.wake_node("node-a", 11).await.unwrap();
        assert_eq!(
            store
                .operational_aggregates(now_ms())
                .await.unwrap()
                .outbox_pending,
            0,
            "waking the node must not bypass the ambiguity fence"
        );
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
        // A fresh report from the new session resolves the ambiguity and
        // requeues reconciliation work.
        assert_eq!(
            store
                .operational_aggregates(now_ms())
                .await.unwrap()
                .outbox_pending,
            1
        );
    }

    /// Desired and intent reads address what the mutation paths wrote:
    /// point lookups miss unknown streams, and the intent list filters by
    /// node while preserving the full record shape.
    #[tokio::test]
    async fn desired_and_intent_reads_round_trip() {
        let store = ControlPlaneStore::contract("desired_and_intent_reads_round_trip").await;
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                config_version_id: Some("cfg-1".into()),
                intent_type: Some("apply_configuration".into()),
                payload_json: Some(r#"{"format":"json"}"#.into()),
                actor: Some("operator".into()),
                correlation_id: Some("corr-1".into()),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await.unwrap();
        let desired = store.get_desired("node-a", "orders").await.unwrap().unwrap();
        assert_eq!(desired.node_id, "node-a");
        assert_eq!(desired.stream_id, "orders");
        assert_eq!(desired.generation, 1);
        assert_eq!(desired.desired_state, "running");
        assert_eq!(desired.config_version_id.as_deref(), Some("cfg-1"));
        assert_eq!(desired.action_id, None);
        assert_eq!(desired.correlation_id.as_deref(), Some("corr-1"));
        assert!(store.get_desired("node-a", "missing").await.unwrap().is_none());

        store
            .set_desired(DesiredMutation {
                node_id: "node-b".into(),
                stream_id: "orders".into(),
                desired_state: "stopped".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await.unwrap();
        let all = store.list_intents(None).await.unwrap();
        assert_eq!(all.len(), 2);
        let node_a = store.list_intents(Some("node-a")).await.unwrap();
        assert_eq!(node_a.len(), 1);
        assert_eq!(node_a[0].intent_id, intent.intent_id);
        assert_eq!(node_a[0].node_id, "node-a");
        assert_eq!(node_a[0].generation, 1);
        assert_eq!(node_a[0].state, "accepted");
        assert_eq!(node_a[0].convergence_state, "pending");
        assert_eq!(node_a[0].retry_count, 0);
        assert_eq!(node_a[0].next_retry_at_ms, None);
        assert_eq!(node_a[0].failure_class, None);
        assert_eq!(node_a[0].superseded_by_intent_id, None);
        assert_eq!(node_a[0].superseded_generation, None);
        assert_eq!(node_a[0].observed_generation, None);
        assert_eq!(node_a[0].observed_state, None);
        assert!(store.list_intents(Some("node-zzz")).await.unwrap().is_empty());
    }

    /// The rollout orchestration surface: content-bearing creation seeds the
    /// config version, updates move the rollout and its targets, recovery
    /// returns only live rollouts, and listing is bounded but complete.
    #[tokio::test]
    async fn rollout_lifecycle_round_trips_and_recovers() {
        let store = ControlPlaneStore::contract("rollout_lifecycle_round_trips_and_recovers").await;
        let target = |rollout_id: &str, node: &str, ordinal: u32| RolloutTargetRecord {
            rollout_id: rollout_id.into(),
            node_id: node.into(),
            ordinal,
            state: "pending".into(),
            attempt_id: None,
            error: None,
            observed_config_version: None,
            updated_at_ms: 10,
        };
        store
            .create_rollout_with_content(
                RolloutRecord {
                    rollout_id: "rollout-1".into(),
                    config_version_id: "cfg-1".into(),
                    state: "applying".into(),
                    batch_size: 1,
                    current_batch: 0,
                    total_targets: 2,
                    actor: Some("operator".into()),
                    correlation_id: Some("corr-1".into()),
                    created_at_ms: 10,
                    updated_at_ms: 10,
                },
                vec![target("rollout-1", "node-a", 0), target("rollout-1", "node-b", 1)],
                "{\"version\":1}",
                Some("operator"),
            )
            .await.unwrap();
        // The inline content is addressable as a config version.
        assert_eq!(
            store.get_config_version_content("cfg-1").await.unwrap().as_deref(),
            Some("{\"version\":1}")
        );
        assert!(store
            .get_config_version_content("cfg-missing")
            .await.unwrap()
            .is_none());
        assert_eq!(
            store.get_rollout("rollout-1").await.unwrap().unwrap(),
            RolloutRecord {
                rollout_id: "rollout-1".into(),
                config_version_id: "cfg-1".into(),
                state: "applying".into(),
                batch_size: 1,
                current_batch: 0,
                total_targets: 2,
                actor: Some("operator".into()),
                correlation_id: Some("corr-1".into()),
                created_at_ms: 10,
                updated_at_ms: 10,
            }
        );
        assert!(store.get_rollout("rollout-missing").await.unwrap().is_none());
        let targets = store.list_rollout_targets("rollout-1").await.unwrap();
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].node_id, "node-a");
        assert_eq!(targets[1].node_id, "node-b");
        assert_eq!(targets[0].state, "pending");

        // Batch and per-target progress land durably.
        store.update_rollout("rollout-1", "applying", 1, 50).await.unwrap();
        store
            .update_rollout_target(RolloutTargetUpdate {
                rollout_id: "rollout-1".into(),
                node_id: "node-a".into(),
                state: "applied".into(),
                attempt_id: Some("attempt-1".into()),
                error: None,
                observed_config_version: Some("cfg-1".into()),
                updated_at_ms: 55,
            })
            .await.unwrap();
        let updated = store.get_rollout("rollout-1").await.unwrap().unwrap();
        assert_eq!(updated.current_batch, 1);
        assert_eq!(updated.updated_at_ms, 50);
        let targets = store.list_rollout_targets("rollout-1").await.unwrap();
        assert_eq!(targets[0].state, "applied");
        assert_eq!(targets[0].attempt_id.as_deref(), Some("attempt-1"));
        assert_eq!(targets[0].observed_config_version.as_deref(), Some("cfg-1"));
        assert_eq!(targets[1].state, "pending");

        // A terminal rollout is excluded from recovery; a live one is not.
        store
            .create_rollout_with_content(
                RolloutRecord {
                    rollout_id: "rollout-2".into(),
                    config_version_id: "cfg-2".into(),
                    state: "converged".into(),
                    batch_size: 1,
                    current_batch: 0,
                    total_targets: 0,
                    actor: None,
                    correlation_id: None,
                    created_at_ms: 20,
                    updated_at_ms: 20,
                },
                Vec::new(),
                "{}",
                None,
            )
            .await.unwrap();
        let recoverable = store.recover_rollouts().await.unwrap();
        assert_eq!(recoverable.len(), 1);
        assert_eq!(recoverable[0].rollout_id, "rollout-1");
        let listed = store.list_rollouts().await.unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].rollout_id, "rollout-2", "newest first");

        // Plain creation reuses an already-seeded config version (the
        // PostgreSQL rollout foreign key must be satisfiable).
        store
            .create_rollout(
                RolloutRecord {
                    rollout_id: "rollout-3".into(),
                    config_version_id: "cfg-1".into(),
                    state: "applying".into(),
                    batch_size: 1,
                    current_batch: 0,
                    total_targets: 1,
                    actor: None,
                    correlation_id: None,
                    created_at_ms: 30,
                    updated_at_ms: 30,
                },
                vec![target("rollout-3", "node-c", 0)],
            )
            .await.unwrap();
        assert!(store.get_rollout("rollout-3").await.unwrap().is_some());
        assert_eq!(store.list_rollout_targets("rollout-3").await.unwrap().len(), 1);
    }

    /// Job version history and checkpoint artifacts round-trip, and
    /// checkpoint retention reclaims only pending/failed artifacts.
    #[tokio::test]
    async fn job_versions_and_checkpoints_round_trip() {
        let store = ControlPlaneStore::contract("job_versions_and_checkpoints_round_trip").await;
        let version = |version: u64, plan: &str| JobVersionRecord {
            job_id: "orders".into(),
            version,
            spec_json: format!("{{\"version\":{version}}}"),
            plan_json: plan.into(),
            created_at_ms: version,
        };
        store.upsert_job_version(version(1, "plan-1")).await.unwrap();
        store.upsert_job_version(version(2, "plan-2")).await.unwrap();
        // Re-upserting a version rewrites its plan.
        store.upsert_job_version(version(2, "plan-2b")).await.unwrap();
        let versions = store.list_job_versions("orders").await.unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, 2, "newest version first");
        assert_eq!(versions[0].plan_json, "plan-2b");
        assert_eq!(versions[0].spec_json, "{\"version\":2}");
        assert_eq!(versions[1].version, 1);
        assert!(store.list_job_versions("missing").await.unwrap().is_empty());

        let checkpoint = |checkpoint_id: &str,
                          kind: &str,
                          status: &str,
                          created_at_ms: u64,
                          updated_at_ms: u64| JobCheckpointRecord {
            job_id: "orders".into(),
            job_version: 2,
            checkpoint_id: checkpoint_id.into(),
            kind: kind.into(),
            status: status.into(),
            manifest_uri: Some(format!("file://{checkpoint_id}")),
            format_version: 1,
            created_at_ms,
            updated_at_ms,
        };
        store
            .upsert_job_checkpoint(checkpoint("cp-1", "savepoint", "pending", 10, 10))
            .await.unwrap();
        store
            .upsert_job_checkpoint(checkpoint("cp-2", "snapshot", "completed", 20, 20))
            .await.unwrap();
        store
            .upsert_job_checkpoint(checkpoint("cp-3", "savepoint", "failed", 5, 5))
            .await.unwrap();
        let checkpoints = store.list_job_checkpoints("orders").await.unwrap();
        assert_eq!(
            checkpoints
                .iter()
                .map(|record| record.checkpoint_id.as_str())
                .collect::<Vec<_>>(),
            vec!["cp-2", "cp-1", "cp-3"],
            "newest created first"
        );
        assert_eq!(checkpoints[0].kind, "snapshot");
        assert_eq!(checkpoints[0].manifest_uri.as_deref(), Some("file://cp-2"));
        // A completed status re-arms the retention pin on an existing row.
        store
            .upsert_job_checkpoint(checkpoint("cp-1", "savepoint", "completed", 10, 30))
            .await.unwrap();
        // Retention reclaims only pending/failed artifacts past the cutoff.
        assert_eq!(store.prune_job_checkpoint_records(15).await.unwrap(), 1);
        let surviving = store.list_job_checkpoints("orders").await.unwrap();
        assert_eq!(
            surviving
                .iter()
                .map(|record| record.checkpoint_id.as_str())
                .collect::<Vec<_>>(),
            vec!["cp-2", "cp-1"]
        );
        assert_eq!(surviving[1].status, "completed", "cp-1 was re-armed above");
        // Explicit delete removes a specific artifact; missing ids are no-ops.
        store.delete_job_checkpoint("orders", "cp-2").await.unwrap();
        store.delete_job_checkpoint("orders", "cp-missing").await.unwrap();
        let remaining = store.list_job_checkpoints("orders").await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].checkpoint_id, "cp-1");
        assert!(store.list_job_checkpoints("missing").await.unwrap().is_empty());
    }

    /// Job observations apply as a compare-and-swap: a report conditioned on
    /// a moved generation is rejected instead of rolling the Job back.
    #[tokio::test]
    async fn job_observation_update_is_conditioned_on_generation() {
        let store =
            ControlPlaneStore::contract("job_observation_update_is_conditioned_on_generation").await;
        let job = JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: "{}".into(),
            desired_state: "running".into(),
            observed_state: "draft".into(),
            convergence: "unknown".into(),
            generation: 0,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 1,
        };
        let stored = store.upsert_job(job.clone()).await.unwrap();
        assert_eq!(stored.generation, 1);
        let observed = store
            .update_job_observation("orders", "running", "in_sync", 2, 1, Some("cp-obs"), None)
            .await.unwrap().unwrap();
        assert_eq!(observed.observed_state, "running");
        assert_eq!(observed.convergence, "in_sync");
        assert_eq!(observed.generation, 2);
        assert_eq!(observed.checkpoint_id.as_deref(), Some("cp-obs"));
        // A report conditioned on the superseded generation conflicts.
        assert!(matches!(
            store
                .update_job_observation("orders", "failed", "degraded", 3, 1, None, Some("boom"))
                .await,
            Err(StorageError::GenerationConflict { .. })
        ));
        let untouched = store.get_job("orders").await.unwrap().unwrap();
        assert_eq!(untouched.observed_state, "running");
        assert_eq!(untouched.checkpoint_id.as_deref(), Some("cp-obs"));
        // The report conditioned on the current generation applies and the
        // checkpoint pointer survives a NULL carry.
        let next = store
            .update_job_observation("orders", "failed", "degraded", 3, 2, None, Some("boom"))
            .await.unwrap().unwrap();
        assert_eq!(next.observed_state, "failed");
        assert_eq!(next.last_error.as_deref(), Some("boom"));
        assert_eq!(next.checkpoint_id.as_deref(), Some("cp-obs"));
        // Observing an unknown Job is a miss, not a conflict.
        assert!(store
            .update_job_observation("missing", "running", "in_sync", 1, 0, None, None)
            .await
            .unwrap()
            .is_none());
    }

    /// Persisted operations round-trip and the node-filtered listing stays
    /// bounded and ordered, while job-start recovery facts are addressable
    /// per resource.
    #[tokio::test]
    async fn operations_round_trip_and_filter_by_node() {
        let store = ControlPlaneStore::contract("operations_round_trip_and_filter_by_node").await;
        let operation = |operation_id: &str, node_id: &str, updated_at_ms: u64| PersistedOperation {
            operation_id: operation_id.into(),
            node_id: node_id.into(),
            resource_id: "orders".into(),
            operation: "restart".into(),
            state: "queued".into(),
            created_at_ms: 1,
            updated_at_ms,
            operation_json: format!("{{\"id\":\"{operation_id}\"}}"),
        };
        store.upsert_operation(operation("op-1", "node-a", 1)).await.unwrap();
        store.upsert_operation(operation("op-2", "node-b", 2)).await.unwrap();
        store.upsert_operation(operation("op-3", "node-a", 3)).await.unwrap();
        let loaded = store.get_operation("op-1").await.unwrap().unwrap();
        assert_eq!(loaded.node_id, "node-a");
        assert_eq!(loaded.operation, "restart");
        assert_eq!(loaded.operation_json, r#"{"id":"op-1"}"#);
        assert!(store.get_operation("op-missing").await.unwrap().is_none());
        let all = store.list_operations(None).await.unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].operation_id, "op-3", "newest first");
        let node_a = store.list_operations(Some("node-a")).await.unwrap();
        assert_eq!(
            node_a
                .iter()
                .map(|record| record.operation_id.as_str())
                .collect::<Vec<_>>(),
            vec!["op-3", "op-1"]
        );
        assert!(store.list_operations(Some("node-zzz")).await.unwrap().is_empty());
        // Re-upserting updates the mutable columns only.
        store
            .upsert_operation(PersistedOperation {
                operation_id: "op-1".into(),
                node_id: "node-a".into(),
                resource_id: "orders".into(),
                operation: "restart".into(),
                state: "succeeded".into(),
                created_at_ms: 1,
                updated_at_ms: 4,
                operation_json: r#"{"id":"op-1","state":"succeeded"}"#.into(),
            })
            .await.unwrap();
        let updated = store.get_operation("op-1").await.unwrap().unwrap();
        assert_eq!(updated.state, "succeeded");
        assert_eq!(updated.updated_at_ms, 4);
        // Job-start recovery facts are addressable per resource.
        store
            .upsert_operation(PersistedOperation {
                operation_id: "start-1".into(),
                node_id: "node-a".into(),
                resource_id: "orders".into(),
                operation: "job_start".into(),
                state: "succeeded".into(),
                created_at_ms: 5,
                updated_at_ms: 5,
                operation_json: r#"{"operation":"job_start"}"#.into(),
            })
            .await.unwrap();
        store
            .upsert_operation(operation("start-2", "node-a", 6))
            .await.unwrap();
        let starts = store.list_job_start_operations("orders").await.unwrap();
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].operation_id, "start-1");
    }

    /// The status surface reports every counter family: node and maintenance
    /// states, intent/attempt/convergence groupings, failure classes, the
    /// outbox queue, and the pending age.
    #[tokio::test]
    async fn operational_aggregates_expose_every_counter_family() {
        let store =
            ControlPlaneStore::contract("operational_aggregates_expose_every_counter_family").await;
        let node = |node_id: &str, state: &str, maintenance: Option<&str>| NodeMutation {
            node_id: node_id.into(),
            version: "v1".into(),
            state: state.into(),
            capabilities_json: "[]".into(),
            boot_id: Some("boot-1".into()),
            report_seq: Some(1),
            last_seen_at_ms: 10,
            lease_expires_at_ms: 4_102_444_800_000,
            maintenance_state: maintenance.map(str::to_owned),
            maintenance_updated_at_ms: None,
        };
        store.upsert_node(node("node-a", "online", None)).await.unwrap();
        store.upsert_node(node("node-b", "online", Some("draining"))).await.unwrap();
        // Re-registering an existing node updates rather than duplicates.
        store.upsert_node(node("node-a", "offline", None)).await.unwrap();
        store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await.unwrap();
        let intent = store
            .list_intents(Some("node-a"))
            .await.unwrap()
            .pop()
            .unwrap();
        store.claim_attempt(&intent.intent_id).await.unwrap().unwrap();
        let aggregates = store.operational_aggregates(now_ms()).await.unwrap();
        let grouped = |pairs: &[(String, u64)]| {
            let mut sorted = pairs.to_vec();
            sorted.sort();
            sorted
        };
        assert_eq!(
            grouped(&aggregates.node_states),
            vec![("offline".into(), 1), ("online".into(), 1)]
        );
        assert_eq!(
            grouped(&aggregates.maintenance_states),
            vec![("active".into(), 1), ("draining".into(), 1)]
        );
        assert_eq!(
            grouped(&aggregates.intent_states),
            vec![("accepted".into(), 1)]
        );
        assert_eq!(
            grouped(&aggregates.convergence_states),
            vec![("pending".into(), 1)]
        );
        assert_eq!(
            grouped(&aggregates.attempt_states),
            vec![("queued".into(), 1)]
        );
        assert_eq!(
            grouped(&aggregates.failure_classes),
            vec![("none".into(), 1)]
        );
        assert_eq!(aggregates.active_attempts, 1);
        assert_eq!(aggregates.non_terminal_intents, 1);
        assert_eq!(aggregates.outbox_pending, 1);
        assert_eq!(aggregates.outbox_claimed, 0);
        assert_eq!(aggregates.stale_nodes, 0);
        assert!(
            matches!(aggregates.oldest_pending_age_seconds, Some(age) if age <= 60),
            "the pending age must be reported for queued work"
        );
        // Claiming the only row moves it into the claimed counter and gives
        // the pending queue an age.
        let outbox = store.claim_outbox("worker", now_ms()).await.unwrap().unwrap();
        let claimed = store.operational_aggregates(now_ms()).await.unwrap();
        assert_eq!(claimed.outbox_pending, 1);
        assert_eq!(claimed.outbox_claimed, 1);
        drop(outbox);
    }

    /// Maintenance transitions validate the requested state, require the
    /// node to exist, and only audit actual transitions.
    #[tokio::test]
    async fn maintenance_transitions_reject_unknown_states_and_nodes() {
        let store =
            ControlPlaneStore::contract("maintenance_transitions_reject_unknown_states_and_nodes").await;
        let mutation = |node_id: &str, state: &str| NodeMaintenanceMutation {
            node_id: node_id.into(),
            state: state.into(),
            actor: Some("operator".into()),
            correlation_id: None,
        };
        // An unsupported state is refused before touching the store.
        assert!(!store.set_node_maintenance(mutation("node-a", "bogus"), 10).await.unwrap());
        // An unknown node is a miss, not an error.
        assert!(!store.set_node_maintenance(mutation("node-zzz", "draining"), 11).await.unwrap());
        assert_eq!(store.get_node_maintenance("node-zzz").await.unwrap(), None);
        store
            .upsert_node(NodeMutation {
                node_id: "node-a".into(),
                version: "v1".into(),
                state: "online".into(),
                capabilities_json: "[]".into(),
                boot_id: None,
                report_seq: None,
                last_seen_at_ms: 10,
                lease_expires_at_ms: 1_000,
                maintenance_state: None,
                maintenance_updated_at_ms: None,
            })
            .await.unwrap();
        assert!(store.set_node_maintenance(mutation("node-a", "draining"), 20).await.unwrap());
        // Re-asserting the same state succeeds without a second audit event.
        assert!(store.set_node_maintenance(mutation("node-a", "draining"), 21).await.unwrap());
        assert_eq!(
            store.get_node_maintenance("node-a").await.unwrap().as_deref(),
            Some("draining")
        );
        let events = store.list_events(Some("node-a")).await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == "node_maintenance_changed")
                .count(),
            1
        );
        assert!(store.set_node_maintenance(mutation("node-a", "maintenance"), 22).await.unwrap());
        assert_eq!(
            store.get_node_maintenance("node-a").await.unwrap().as_deref(),
            Some("maintenance")
        );
    }

    /// Every mutating command family travels behind the write-fencing
    /// envelope, so a stale leader is nack'd uniformly across the whole
    /// surface: no mutation family may silently bypass the fence, and each
    /// rejection leaves no durable side effect.
    #[tokio::test]
    async fn every_fenced_mutation_family_rejects_a_stale_leader() {
        let store =
            ControlPlaneStore::contract("every_fenced_mutation_family_rejects_a_stale_leader")
                .await;
        let actor = StorageActor::start(store.clone(), 64);
        // A live lease row exists at epoch 1; this process carries a standby
        // claim (0) that never entered the election.
        assert_eq!(
            store.try_acquire_hub_lease("hub-live", None, 3_600_000, 100).await.unwrap(),
            HubLeaseAcquire::Acquired { epoch: 1 }
        );
        actor.leadership_epoch().store(0, Ordering::Release);

        fn assert_stale<T: std::fmt::Debug>(result: Result<T, StorageError>) {
            assert!(
                matches!(
                    result,
                    Err(StorageError::StaleLeader {
                        claimed_epoch: 0,
                        current_epoch: 1
                    })
                ),
                "expected every fenced mutation family to reject the stale claim, got {result:?}"
            );
        }

        let job = JobRecord {
            job_id: "orders".into(),
            version: 1,
            spec_json: "{}".into(),
            desired_state: "running".into(),
            observed_state: "draft".into(),
            convergence: "unknown".into(),
            generation: 1,
            node_ids: Vec::new(),
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 1,
        };
        let node = NodeMutation {
            node_id: "node-a".into(),
            version: "v1".into(),
            state: "online".into(),
            capabilities_json: "[]".into(),
            boot_id: None,
            report_seq: None,
            last_seen_at_ms: 1,
            lease_expires_at_ms: 2,
            maintenance_state: None,
            maintenance_updated_at_ms: None,
        };
        let upgrade = JobUpgradeRecord {
            upgrade_id: "upgrade-1".into(),
            job_id: "orders".into(),
            from_version: 1,
            to_version: 2,
            phase: "saving_savepoint".into(),
            savepoint_id: None,
            target_spec_json: "{}".into(),
            phase_deadline_at_ms: 100,
            savepoint_retries: 0,
            verify_timeout_ms: 0,
            actor: None,
            correlation_id: None,
            last_error: None,
            paused_from: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        };
        let rollout = RolloutRecord {
            rollout_id: "rollout-1".into(),
            config_version_id: "cfg-1".into(),
            state: "applying".into(),
            batch_size: 1,
            current_batch: 0,
            total_targets: 0,
            actor: None,
            correlation_id: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        };

        assert_stale(actor.upsert_job(job.clone()).await);
        assert_stale(actor.update_job_with_expected_generation(job, 1).await);
        assert_stale(
            actor.upsert_job_version(JobVersionRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: "{}".into(),
                plan_json: "plan".into(),
                created_at_ms: 1,
            })
            .await,
        );
        assert_stale(
            actor
                .update_job("orders", None, None, None, None, None, None)
                .await,
        );
        assert_stale(
            actor
                .update_job_observation("orders", "running", "in_sync", 2, 1, None, None)
                .await,
        );
        assert_stale(actor.update_job_desired_state("orders", "stopped", 1).await);
        assert_stale(
            actor
                .upsert_job_checkpoint(JobCheckpointRecord {
                    job_id: "orders".into(),
                    job_version: 1,
                    checkpoint_id: "cp-1".into(),
                    kind: "savepoint".into(),
                    status: "pending".into(),
                    manifest_uri: None,
                    format_version: 1,
                    created_at_ms: 1,
                    updated_at_ms: 1,
                })
                .await,
        );
        assert_stale(actor.delete_job_checkpoint("orders", "cp-1").await);
        assert_stale(actor.upsert_node(node).await);
        assert_stale(actor.reset_observed_cursors("node-a").await);
        assert_stale(
            actor
                .set_desired(DesiredMutation {
                    node_id: "node-a".into(),
                    stream_id: "orders".into(),
                    desired_state: "running".into(),
                    ..Default::default()
                })
                .await,
        );
        assert_stale(actor.recover_reconciliation(10).await);
        assert_stale(actor.wake_node("node-a", 10).await);
        assert_stale(actor.prune_events(10).await);
        assert_stale(actor.prune_operation_history(10, 10).await);
        assert_stale(actor.prune_job_checkpoint_records(10).await);
        assert_stale(actor.prune_audit_events(10, 10).await);
        assert_stale(actor.prune_processed_outbox(10, 10).await);
        assert_stale(actor.prune_terminal_attempts(10, 10).await);
        assert_stale(actor.claim_attempt("intent-1").await);
        assert_stale(actor.mark_attempt_dispatched("attempt-1", 10).await);
        assert_stale(actor.expire_attempts(10).await);
        assert_stale(actor.complete_attempt("attempt-1", "failed", None).await);
        assert_stale(
            actor
                .record_observed(ObservedMutation {
                    node_id: "node-a".into(),
                    stream_id: "orders".into(),
                    boot_id: None,
                    report_seq: 1,
                    observed_generation: None,
                    observed_state: "running".into(),
                    config_version_id: None,
                    action_id: None,
                    snapshot_json: "{}".into(),
                    last_error_code: None,
                    last_error_message: None,
                })
                .await,
        );
        assert_stale(actor.claim_outbox("worker", 10).await);
        assert_stale(actor.mark_outbox_processed(1, 10).await);
        assert_stale(
            actor
                .set_node_maintenance(
                    NodeMaintenanceMutation {
                        node_id: "node-a".into(),
                        state: "draining".into(),
                        actor: None,
                        correlation_id: None,
                    },
                    10,
                )
                .await,
        );
        assert_stale(actor.record_audit(audit_row(99)).await);
        assert_stale(actor.create_rollout(rollout.clone(), Vec::new()).await);
        assert_stale(
            actor
                .create_rollout_with_content(rollout, Vec::new(), "{}", None)
                .await,
        );
        assert_stale(actor.update_rollout("rollout-1", "converged", 1, 10).await);
        assert_stale(
            actor
                .update_rollout_target(RolloutTargetUpdate {
                    rollout_id: "rollout-1".into(),
                    node_id: "node-a".into(),
                    state: "applied".into(),
                    attempt_id: None,
                    error: None,
                    observed_config_version: None,
                    updated_at_ms: 10,
                })
                .await,
        );
        assert_stale(actor.recover_rollouts().await);
        assert_stale(actor.upsert_job_upgrade(upgrade.clone()).await);
        assert_stale(actor.transition_job_upgrade(upgrade, "saving_savepoint").await);
        assert_stale(actor.recover_job_upgrades().await);
        assert_stale(actor.prune_job_upgrades(10, 10).await);
        assert_stale(
            actor
                .upsert_operation(PersistedOperation {
                    operation_id: "op-1".into(),
                    node_id: "node-a".into(),
                    resource_id: "orders".into(),
                    operation: "restart".into(),
                    state: "queued".into(),
                    created_at_ms: 1,
                    updated_at_ms: 1,
                    operation_json: "{}".into(),
                })
                .await,
        );

        // None of the rejected families produced a durable side effect.
        assert!(store.list_jobs().await.unwrap().is_empty());
        assert!(store.list_intents(None::<&str>).await.unwrap().is_empty());
        assert!(store.list_rollouts().await.unwrap().is_empty());
        assert!(store.list_operations(None::<&str>).await.unwrap().is_empty());
    }

    /// The actor carries the (unfenced) lease and retention surfaces through
    /// the same FIFO: acquire, renew, lose, release, and event retention.
    #[tokio::test]
    async fn storage_actor_exposes_the_lease_and_retention_surfaces() {
        let store = ControlPlaneStore::contract("storage_actor_exposes_the_lease_and_retention_surfaces").await;
        let actor = StorageActor::start(store, 16);
        assert_eq!(
            actor.try_acquire_hub_lease("hub-a", None, 1_000, 100).await.unwrap(),
            HubLeaseAcquire::Acquired { epoch: 1 }
        );
        assert_eq!(
            actor.renew_hub_lease("hub-a", None, 1_000, 200).await.unwrap(),
            HubLeaseRenew::Renewed { epoch: 1 }
        );
        assert_eq!(
            actor.renew_hub_lease("hub-b", None, 1_000, 200).await.unwrap(),
            HubLeaseRenew::Lost
        );
        assert!(!actor.release_hub_lease("hub-b", 300).await.unwrap());
        assert!(actor.release_hub_lease("hub-a", 300).await.unwrap());
        // Retention rides the same queue: one durable event in, pruned out.
        actor
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(actor.list_events(Some("node-a")).await.unwrap().len(), 1);
        assert_eq!(actor.prune_events(0).await.unwrap(), 1);
        assert!(actor.list_events(None::<String>).await.unwrap().is_empty());
    }

    /// Dropping the last actor handle closes the command channel: the
    /// spawned task observes it and runs to completion instead of leaking.
    #[tokio::test]
    async fn storage_actor_task_completes_after_the_last_handle_drops() {
        let store =
            ControlPlaneStore::contract("storage_actor_task_completes_after_the_last_handle_drops")
                .await;
        let actor = StorageActor::start(store, 8);
        assert!(actor.list_jobs().await.unwrap().is_empty());
        drop(actor);
        // Yield a few scheduler turns so the actor task observes the closed
        // channel and exits its receive loop.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    /// `postgres://` / `postgresql://` values dispatch to the PostgreSQL
    /// backend (whose startup probe fails fast against an unreachable
    /// endpoint) instead of being treated as SQLite file paths.
    #[tokio::test]
    async fn open_dispatches_postgres_urls_to_the_postgres_backend() {
        assert!(
            ControlPlaneStore::open("postgres://127.0.0.1:1/arkflow_test")
                .await
                .is_err()
        );
    }

    /// A legacy on-disk schema (created before the newest columns existed)
    /// is upgraded in place by `open`: every historical ALTER applies, and
    /// the upgraded store serves the full contract.
    #[tokio::test]
    async fn sqlite_migrate_upgrades_a_legacy_schema_in_place() {
        let path = std::env::temp_dir().join(format!(
            "arkflow-legacy-schema-{}-{}.sqlite",
            std::process::id(),
            now_ms()
        ));
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    r#"
                    CREATE TABLE cp_intents (
                        intent_id TEXT PRIMARY KEY, node_id TEXT NOT NULL,
                        stream_id TEXT NOT NULL, generation INTEGER NOT NULL,
                        intent_type TEXT NOT NULL, desired_state TEXT,
                        config_version_id TEXT, action_id TEXT, state TEXT NOT NULL,
                        convergence_state TEXT NOT NULL,
                        retry_count INTEGER NOT NULL DEFAULT 0,
                        next_retry_at_ms INTEGER, last_failure_class TEXT,
                        last_failure_code TEXT, last_failure_message TEXT,
                        superseded_by_intent_id TEXT, created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL, converged_at_ms INTEGER,
                        actor TEXT, correlation_id TEXT
                    );
                    CREATE TABLE cp_nodes (
                        node_id TEXT PRIMARY KEY, role TEXT NOT NULL DEFAULT 'compute',
                        protocol_version TEXT NOT NULL DEFAULT 'v1',
                        state TEXT NOT NULL DEFAULT 'offline',
                        capabilities_json TEXT NOT NULL DEFAULT '[]', boot_id TEXT,
                        last_report_seq INTEGER, last_seen_at_ms INTEGER NOT NULL DEFAULT 0,
                        lease_expires_at_ms INTEGER NOT NULL DEFAULT 0,
                        created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL
                    );
                    CREATE TABLE cp_events (
                        event_id INTEGER PRIMARY KEY AUTOINCREMENT, node_id TEXT,
                        stream_id TEXT, intent_id TEXT, attempt_id TEXT,
                        event_type TEXT NOT NULL, outcome TEXT NOT NULL,
                        failure_class TEXT, message TEXT, generation INTEGER,
                        correlation_id TEXT, occurred_at_ms INTEGER NOT NULL
                    );
                    CREATE TABLE cp_job_checkpoints (
                        job_id TEXT NOT NULL, checkpoint_id TEXT NOT NULL,
                        kind TEXT NOT NULL, status TEXT NOT NULL, manifest_uri TEXT,
                        format_version INTEGER NOT NULL, created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL, PRIMARY KEY (job_id, checkpoint_id)
                    );
                    "#,
                )
                .unwrap();
        }
        let store = ControlPlaneStore::open(path.to_str().unwrap()).await.unwrap();
        fn has_column(store: &ControlPlaneStore, table: &str, column: &str) -> bool {
            store
                .with_connection(|connection| {
                    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
                    let columns = statement
                        .query_map([], |row| row.get::<_, String>(1))?
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(columns.iter().any(|name| name == column))
                })
                .unwrap()
        }
        assert!(has_column(&store, "cp_intents", "idempotency_key"));
        assert!(has_column(&store, "cp_intents", "payload_json"));
        assert!(has_column(&store, "cp_nodes", "node_version"));
        assert!(has_column(&store, "cp_nodes", "maintenance_state"));
        assert!(has_column(&store, "cp_nodes", "maintenance_updated_at_ms"));
        assert!(has_column(&store, "cp_events", "actor"));
        assert!(has_column(&store, "cp_job_checkpoints", "job_version"));
        // The upgraded schema serves the contract end to end.
        store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                idempotency_key: Some("idem-1".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .upsert_job_checkpoint(JobCheckpointRecord {
                job_id: "orders".into(),
                job_version: 1,
                checkpoint_id: "cp-1".into(),
                kind: "savepoint".into(),
                status: "completed".into(),
                manifest_uri: None,
                format_version: 1,
                created_at_ms: 1,
                updated_at_ms: 1,
            })
            .await
            .unwrap();
        assert_eq!(store.list_job_checkpoints("orders").await.unwrap().len(), 1);
        drop(store);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    /// The write fence surfaces lease-catalog errors instead of guessing a
    /// passthrough, and tolerates an unpaired end (only the outermost fence
    /// commits).
    #[tokio::test]
    async fn sqlite_write_fence_reports_catalog_errors_and_ignores_unpaired_ends() {
        let store = ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| connection.execute_batch("DROP TABLE cp_hub_lease"))
            .unwrap();
        // A vanished lease catalog is an error: the write lock is rolled
        // back and the failure reported.
        assert!(store.begin_write_fence(0).await.is_err());

        // An unpaired end with no fence held is a no-op, not a fault.
        let bare = ControlPlaneStore::in_memory().unwrap();
        bare.end_write_fence().await.unwrap();
    }

    /// A fence whose ambient transaction died between begin and commit must
    /// fail the commit loudly instead of reporting success.
    #[tokio::test]
    async fn sqlite_write_fence_commit_failure_is_reported_loudly() {
        let store = ControlPlaneStore::in_memory().unwrap();
        assert_eq!(
            store.try_acquire_hub_lease("hub-a", None, 1_000, 100).await.unwrap(),
            HubLeaseAcquire::Acquired { epoch: 1 }
        );
        assert_eq!(
            store.begin_write_fence(1).await.unwrap(),
            WriteFence::Held
        );
        // Kill the ambient transaction behind the fence's back (the
        // I/O-level rollback it cannot observe).
        store
            .with_connection(|connection| connection.execute_batch("ROLLBACK"))
            .unwrap();
        assert!(store.end_write_fence().await.is_err());
    }

    /// A Job row with a corrupt `node_ids_json` fails the read instead of
    /// fabricating a record.
    #[tokio::test]
    async fn sqlite_job_row_with_corrupt_node_ids_fails_the_read() {
        let store = ControlPlaneStore::in_memory().unwrap();
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO cp_jobs (job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, updated_at_ms) VALUES ('bad', 1, '{}', 'stopped', 'draft', 'unknown', 1, 'not-json', 1)",
                    [],
                )
            })
            .unwrap();
        assert!(store.get_job("bad").await.is_err());
        assert!(store.list_jobs().await.is_err());
    }

    /// A `stale_generation` completion supersedes the intent (a lost race,
    /// not a retryable fault); unknown attempts and intents are no-op misses.
    #[tokio::test]
    async fn stale_generation_attempt_supersedes_the_intent() {
        let store =
            ControlPlaneStore::contract("stale_generation_attempt_supersedes_the_intent").await;
        let intent = store
            .set_desired(DesiredMutation {
                node_id: "node-a".into(),
                stream_id: "orders".into(),
                desired_state: "running".into(),
                expected_generation: Some(0),
                ..Default::default()
            })
            .await
            .unwrap();
        let attempt = store.claim_attempt(&intent.intent_id).await.unwrap().unwrap();
        store
            .complete_attempt(&attempt.attempt_id, "superseded", Some("stale_generation"))
            .await
            .unwrap();
        let superseded = store.get_intent(&intent.intent_id).await.unwrap().unwrap();
        assert_eq!(superseded.state, "superseded");
        assert_eq!(superseded.convergence_state, "degraded");
        assert_eq!(
            superseded.failure_class.as_deref(),
            Some("stale_generation")
        );
        // Completing an unknown attempt changes nothing; claiming an
        // unknown intent has nothing to claim.
        store
            .complete_attempt("attempt-missing", "failed", None)
            .await
            .unwrap();
        assert!(store.claim_attempt("intent-missing").await.unwrap().is_none());
    }

    /// The desired-state CAS distinguishes a moved generation (conflict)
    /// from an unknown Job (miss).
    #[tokio::test]
    async fn job_desired_state_update_conflicts_and_misses_are_distinct() {
        let store =
            ControlPlaneStore::contract("job_desired_state_update_conflicts_and_misses_are_distinct")
                .await;
        store
            .upsert_job(JobRecord {
                job_id: "orders".into(),
                version: 1,
                spec_json: "{}".into(),
                desired_state: "stopped".into(),
                observed_state: "stopped".into(),
                convergence: "converged".into(),
                generation: 0,
                node_ids: Vec::new(),
                checkpoint_id: None,
                last_error: None,
                updated_at_ms: 1,
            })
            .await
            .unwrap();
        // The upsert bumped the stored generation to 1: a write conditioned
        // on the superseded generation 0 conflicts.
        assert!(matches!(
            store.update_job_desired_state("orders", "running", 0).await,
            Err(StorageError::GenerationConflict { expected: 0, current: 1 })
        ));
        // An unknown Job is a miss, not a conflict.
        assert!(store
            .update_job_desired_state("missing", "running", 0)
            .await
            .unwrap()
            .is_none());
    }

    /// Boot-less agents report seq 0 forever: their reports are never gated
    /// by the sequence cursor (only a stable boot identity is fencible).
    #[tokio::test]
    async fn bootless_reports_are_never_gated_by_the_sequence_cursor() {
        let store =
            ControlPlaneStore::contract("bootless_reports_are_never_gated_by_the_sequence_cursor")
                .await;
        let report = |seq: u64, state: &str| ObservedMutation {
            node_id: "node-a".into(),
            stream_id: "orders".into(),
            boot_id: None,
            report_seq: seq,
            observed_generation: None,
            observed_state: state.into(),
            config_version_id: None,
            action_id: None,
            snapshot_json: "{}".into(),
            last_error_code: None,
            last_error_message: None,
        };
        store.record_observed(report(0, "stopped")).await.unwrap();
        // A second boot-less report at the same seq 0 must still land.
        store.record_observed(report(0, "running")).await.unwrap();
        let reports = store
            .list_events(Some("node-a"))
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.event_type == "observed_report")
            .collect::<Vec<_>>();
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].outcome, "running", "newest first");
    }

    /// Rollout creation is keyed by identity: a duplicate rollout id (or a
    /// duplicate target within one rollout) errors atomically instead of
    /// silently merging histories.
    #[tokio::test]
    async fn rollout_identity_conflicts_are_atomic_errors() {
        let store = ControlPlaneStore::contract("rollout_identity_conflicts_are_atomic_errors").await;
        let target = |rollout_id: &str, node: &str| RolloutTargetRecord {
            rollout_id: rollout_id.into(),
            node_id: node.into(),
            ordinal: 0,
            state: "pending".into(),
            attempt_id: None,
            error: None,
            observed_config_version: None,
            updated_at_ms: 10,
        };
        store
            .create_rollout_with_content(
                RolloutRecord {
                    rollout_id: "rollout-1".into(),
                    config_version_id: "cfg-1".into(),
                    state: "applying".into(),
                    batch_size: 1,
                    current_batch: 0,
                    total_targets: 1,
                    actor: None,
                    correlation_id: None,
                    created_at_ms: 10,
                    updated_at_ms: 10,
                },
                vec![target("rollout-1", "node-a")],
                "{}",
                None,
            )
            .await
            .unwrap();
        // Re-creating the same rollout id through either entry point is an
        // error; the config version already seeded survives.
        assert!(
            store
                .create_rollout_with_content(
                    RolloutRecord {
                        rollout_id: "rollout-1".into(),
                        config_version_id: "cfg-1".into(),
                        state: "applying".into(),
                        batch_size: 1,
                        current_batch: 0,
                        total_targets: 0,
                        actor: None,
                        correlation_id: None,
                        created_at_ms: 20,
                        updated_at_ms: 20,
                    },
                    Vec::new(),
                    "{}",
                    None,
                )
                .await
                .is_err()
        );
        assert!(
            store
                .create_rollout(
                    RolloutRecord {
                        rollout_id: "rollout-1".into(),
                        config_version_id: "cfg-1".into(),
                        state: "applying".into(),
                        batch_size: 1,
                        current_batch: 0,
                        total_targets: 0,
                        actor: None,
                        correlation_id: None,
                        created_at_ms: 30,
                        updated_at_ms: 30,
                    },
                    Vec::new(),
                )
                .await
                .is_err()
        );
        assert_eq!(
            store.list_rollout_targets("rollout-1").await.unwrap().len(),
            1,
            "the original rollout is untouched by the rejected recreations"
        );
        // A duplicate target node inside one content-bearing rollout is an
        // error too (the primary key is (rollout_id, node_id)).
        assert!(
            store
                .create_rollout_with_content(
                    RolloutRecord {
                        rollout_id: "rollout-2".into(),
                        config_version_id: "cfg-1".into(),
                        state: "applying".into(),
                        batch_size: 1,
                        current_batch: 0,
                        total_targets: 2,
                        actor: None,
                        correlation_id: None,
                        created_at_ms: 40,
                        updated_at_ms: 40,
                    },
                    vec![target("rollout-2", "node-a"), target("rollout-2", "node-a")],
                    "{}",
                    None,
                )
                .await
                .is_err()
        );
        assert!(store.get_rollout("rollout-2").await.unwrap().is_none());
    }
}

#[cfg(test)]
mod job_storage_tests {
    use super::*;

    #[tokio::test]
    async fn job_records_survive_store_reopen() {
        let store = ControlPlaneStore::contract("job_records_survive_store_reopen").await;
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
        let store = ControlPlaneStore::contract("replacing_a_job_advances_its_generation").await;
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
        let store = ControlPlaneStore::contract("desired_state_update_marks_job_as_reconciling").await;
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
        let store = ControlPlaneStore::contract("operation_pruning_preserves_the_latest_job_start_recovery_fact").await;
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

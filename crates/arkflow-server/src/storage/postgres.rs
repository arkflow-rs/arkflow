//! PostgreSQL backend for the Hub control-plane store (sqlx).
//!
//! Placeholder generated for the SQLite refactor step of
//! `add-hub-postgres-storage`; real implementation replaces these bodies.
use super::*;

#[derive(Clone)]
pub struct PostgresBackend;

impl PostgresBackend {
    pub(crate) async fn open(_url: &str) -> Result<Self, StorageError> {
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
}

#[async_trait::async_trait]
impl StorageBackend for PostgresBackend {
    async fn set_desired(&self, mutation: DesiredMutation) -> Result<IntentRecord, StorageError> {
        let _ = mutation;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn upsert_node(&self, mutation: NodeMutation) -> Result<(), StorageError> {
        let _ = mutation;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn reset_observed_cursors(&self, node_id: &str) -> Result<(), StorageError> {
        let _ = node_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn set_node_maintenance(
&self,
mutation: NodeMaintenanceMutation,
now_ms: u64,
) -> Result<bool, StorageError> {
        let _ = mutation; let _ = now_ms;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn get_node_maintenance(&self, node_id: &str) -> Result<Option<String>, StorageError> {
        let _ = node_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn operational_aggregates(
&self,
now_ms: u64,
) -> Result<OperationalAggregates, StorageError> {
        let _ = now_ms;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn claim_outbox(
&self,
worker_id: &str,
now_ms: u64,
) -> Result<Option<OutboxRecord>, StorageError> {
        let _ = worker_id; let _ = now_ms;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn get_desired(
&self,
node_id: &str,
stream_id: &str,
) -> Result<Option<DesiredRecord>, StorageError> {
        let _ = node_id; let _ = stream_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn get_intent(&self, intent_id: &str) -> Result<Option<IntentRecord>, StorageError> {
        let _ = intent_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn list_intents(&self, node_id: Option<&str>) -> Result<Vec<IntentRecord>, StorageError> {
        let _ = node_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn recover_reconciliation(&self, now_ms: u64) -> Result<(), StorageError> {
        let _ = now_ms;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn wake_node(&self, node_id: &str, now_ms: u64) -> Result<(), StorageError> {
        let _ = node_id; let _ = now_ms;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn list_events(&self, node_id: Option<&str>) -> Result<Vec<StoredEvent>, StorageError> {
        let _ = node_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn prune_events(&self, retain: usize) -> Result<usize, StorageError> {
        let _ = retain;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn prune_operation_history(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError> {
        let _ = older_than_ms; let _ = max_retained;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn prune_job_checkpoint_records(&self, older_than_ms: i64) -> Result<usize, StorageError> {
        let _ = older_than_ms;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn prune_audit_events(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError> {
        let _ = older_than_ms; let _ = max_retained;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn prune_processed_outbox(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError> {
        let _ = older_than_ms; let _ = max_retained;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn prune_terminal_attempts(
&self,
older_than_ms: i64,
max_retained: i64,
) -> Result<usize, StorageError> {
        let _ = older_than_ms; let _ = max_retained;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn claim_attempt(&self, intent_id: &str) -> Result<Option<AttemptRecord>, StorageError> {
        let _ = intent_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn complete_attempt(
&self,
attempt_id: &str,
state: &str,
failure_class: Option<&str>,
) -> Result<(), StorageError> {
        let _ = attempt_id; let _ = state; let _ = failure_class;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn mark_attempt_dispatched(
&self,
attempt_id: &str,
expires_at_ms: u64,
) -> Result<(), StorageError> {
        let _ = attempt_id; let _ = expires_at_ms;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn expire_attempts(&self, now_ms: u64) -> Result<usize, StorageError> {
        let _ = now_ms;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn record_observed(&self, mutation: ObservedMutation) -> Result<(), StorageError> {
        let _ = mutation;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn mark_outbox_processed(&self, outbox_id: i64, now_ms: u64) -> Result<(), StorageError> {
        let _ = outbox_id; let _ = now_ms;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn record_audit(&self, record: AuditRecord) -> Result<i64, StorageError> {
        let _ = record;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn list_audit(&self, resource_id: Option<&str>) -> Result<Vec<AuditRecord>, StorageError> {
        let _ = resource_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn create_rollout(
&self,
rollout: RolloutRecord,
targets: Vec<RolloutTargetRecord>,
) -> Result<(), StorageError> {
        let _ = rollout; let _ = targets;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn create_rollout_with_content(
&self,
rollout: RolloutRecord,
targets: Vec<RolloutTargetRecord>,
content: &str,
created_by: Option<&str>,
) -> Result<(), StorageError> {
        let _ = rollout; let _ = targets; let _ = content; let _ = created_by;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn get_rollout(&self, rollout_id: &str) -> Result<Option<RolloutRecord>, StorageError> {
        let _ = rollout_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn list_rollout_targets(
&self,
rollout_id: &str,
) -> Result<Vec<RolloutTargetRecord>, StorageError> {
        let _ = rollout_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn update_rollout(
&self,
rollout_id: &str,
state: &str,
current_batch: u32,
updated_at_ms: u64,
) -> Result<(), StorageError> {
        let _ = rollout_id; let _ = state; let _ = current_batch; let _ = updated_at_ms;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn update_rollout_target(&self, update: RolloutTargetUpdate) -> Result<(), StorageError> {
        let _ = update;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn get_config_version_content(
&self,
config_version_id: &str,
) -> Result<Option<String>, StorageError> {
        let _ = config_version_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn recover_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError> {
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn list_rollouts(&self) -> Result<Vec<RolloutRecord>, StorageError> {
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn upsert_operation(&self, operation: PersistedOperation) -> Result<(), StorageError> {
        let _ = operation;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn get_operation(
&self,
operation_id: &str,
) -> Result<Option<PersistedOperation>, StorageError> {
        let _ = operation_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn list_operations(
&self,
node_id: Option<&str>,
) -> Result<Vec<PersistedOperation>, StorageError> {
        let _ = node_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn list_job_start_operations(
&self,
resource_id: &str,
) -> Result<Vec<PersistedOperation>, StorageError> {
        let _ = resource_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn upsert_job(&self, mut job: JobRecord) -> Result<JobRecord, StorageError> {
        let _ = job;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn update_job_with_expected_generation(
&self,
mut job: JobRecord,
expected_generation: u64,
) -> Result<JobRecord, StorageError> {
        let _ = job; let _ = expected_generation;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>, StorageError> {
        let _ = job_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn upsert_job_version(&self, record: JobVersionRecord) -> Result<(), StorageError> {
        let _ = record;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn list_job_versions(&self, job_id: &str) -> Result<Vec<JobVersionRecord>, StorageError> {
        let _ = job_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn list_jobs(&self) -> Result<Vec<JobRecord>, StorageError> {
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
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
        let _ = job_id; let _ = desired_state; let _ = observed_state; let _ = convergence; let _ = generation; let _ = checkpoint_id; let _ = last_error;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
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
        let _ = job_id; let _ = observed_state; let _ = convergence; let _ = generation; let _ = expected_generation; let _ = checkpoint_id; let _ = last_error;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn update_job_desired_state(
&self,
job_id: &str,
desired_state: &str,
expected_generation: u64,
) -> Result<Option<JobRecord>, StorageError> {
        let _ = job_id; let _ = desired_state; let _ = expected_generation;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn upsert_job_checkpoint(&self, record: JobCheckpointRecord) -> Result<(), StorageError> {
        let _ = record;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn list_job_checkpoints(
&self,
job_id: &str,
) -> Result<Vec<JobCheckpointRecord>, StorageError> {
        let _ = job_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
    async fn delete_job_checkpoint(
&self,
job_id: &str,
checkpoint_id: &str,
) -> Result<(), StorageError> {
        let _ = job_id; let _ = checkpoint_id;
        Err(StorageError::Unsupported("postgres backend not yet implemented"))
    }
}

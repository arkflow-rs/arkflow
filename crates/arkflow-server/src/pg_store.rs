//! PostgreSQL backend for the control-plane storage (Hub HA phase 1).
//!
//! Mirrors [`crate::storage::ControlPlaneStore`]'s synchronous rusqlite
//! surface over `sqlx`. The schema is created idempotently at startup; the
//! storage actor still serialises commands, so ordering semantics match the
//! SQLite backend exactly.

use std::time::Duration;

use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::Row;

use crate::storage::{
    AuditRecord, AttemptRecord, DesiredMutation, DesiredRecord, IntentRecord, JobCheckpointRecord,
    JobRecord, JobVersionRecord, NodeMaintenanceMutation, NodeMutation, ObservedMutation,
    OperationalAggregates, OutboxRecord, PersistedOperation, RolloutRecord, RolloutTargetRecord,
    RolloutTargetUpdate, StorageError, StoredEvent,
};

/// Idempotent schema, transliterated 1:1 from the SQLite DDL:
/// `INTEGER PRIMARY KEY AUTOINCREMENT` becomes identity columns and the
/// conditional unique index carries over unchanged.
pub const PG_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS cp_nodes (
    node_id TEXT PRIMARY KEY,
    role TEXT NOT NULL DEFAULT 'compute',
    protocol_version TEXT NOT NULL DEFAULT 'v1',
    node_version TEXT NOT NULL DEFAULT 'unknown',
    state TEXT NOT NULL DEFAULT 'offline',
    capabilities_json TEXT NOT NULL DEFAULT '[]',
    boot_id TEXT,
    last_report_seq BIGINT,
    last_seen_at_ms BIGINT NOT NULL DEFAULT 0,
    lease_expires_at_ms BIGINT NOT NULL DEFAULT 0,
    maintenance_state TEXT NOT NULL DEFAULT 'active',
    maintenance_updated_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_jobs (
    job_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    spec_json TEXT NOT NULL,
    desired_state TEXT NOT NULL DEFAULT 'stopped',
    observed_state TEXT NOT NULL DEFAULT 'draft',
    convergence TEXT NOT NULL DEFAULT 'unknown',
    generation BIGINT NOT NULL DEFAULT 0,
    node_ids_json TEXT NOT NULL DEFAULT '[]',
    checkpoint_id TEXT,
    last_error TEXT,
    updated_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS cp_jobs_updated ON cp_jobs(updated_at_ms DESC, job_id);
CREATE TABLE IF NOT EXISTS cp_job_versions (
    job_id TEXT NOT NULL, version BIGINT NOT NULL, spec_json TEXT NOT NULL,
    plan_json TEXT NOT NULL, created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (job_id, version)
);
CREATE TABLE IF NOT EXISTS cp_job_tasks (
    job_id TEXT NOT NULL, generation BIGINT NOT NULL, task_id TEXT NOT NULL,
    node_id TEXT NOT NULL, attempt_id TEXT NOT NULL, state TEXT NOT NULL,
    updated_at_ms BIGINT NOT NULL, PRIMARY KEY (job_id, generation, task_id)
);
CREATE TABLE IF NOT EXISTS cp_job_checkpoints (
    job_id TEXT NOT NULL, job_version BIGINT NOT NULL DEFAULT 0,
    checkpoint_id TEXT NOT NULL, kind TEXT NOT NULL,
    status TEXT NOT NULL, manifest_uri TEXT, format_version BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL, updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (job_id, checkpoint_id)
);
CREATE TABLE IF NOT EXISTS cp_stream_desired (
    node_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    generation BIGINT NOT NULL,
    desired_state TEXT NOT NULL,
    config_version_id TEXT,
    desired_action_id TEXT,
    paused BIGINT NOT NULL DEFAULT 0,
    updated_at_ms BIGINT NOT NULL,
    updated_by TEXT,
    correlation_id TEXT,
    PRIMARY KEY (node_id, stream_id)
);
CREATE TABLE IF NOT EXISTS cp_stream_observed (
    node_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    boot_id TEXT,
    report_seq BIGINT,
    observed_generation BIGINT,
    observed_state TEXT NOT NULL,
    applied_config_version TEXT,
    last_action_id TEXT,
    active_operation_id TEXT,
    last_error_code TEXT,
    last_error_message TEXT,
    snapshot_json TEXT NOT NULL DEFAULT '{}',
    observed_at_ms BIGINT NOT NULL,
    PRIMARY KEY (node_id, stream_id)
);
CREATE TABLE IF NOT EXISTS cp_config_versions (
    config_version_id TEXT PRIMARY KEY,
    parent_version_id TEXT,
    content_digest TEXT NOT NULL,
    content_ref TEXT NOT NULL,
    format TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    created_by TEXT,
    correlation_id TEXT,
    FOREIGN KEY (parent_version_id) REFERENCES cp_config_versions(config_version_id)
);
CREATE TABLE IF NOT EXISTS cp_intents (
    intent_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    generation BIGINT NOT NULL,
    intent_type TEXT NOT NULL,
    desired_state TEXT,
    config_version_id TEXT,
    action_id TEXT,
    payload_json TEXT,
    state TEXT NOT NULL,
    convergence_state TEXT NOT NULL,
    retry_count BIGINT NOT NULL DEFAULT 0,
    next_retry_at_ms BIGINT,
    last_failure_class TEXT,
    last_failure_code TEXT,
    last_failure_message TEXT,
    superseded_by_intent_id TEXT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    converged_at_ms BIGINT,
    actor TEXT,
    correlation_id TEXT,
    idempotency_key TEXT,
    UNIQUE (node_id, stream_id, generation),
    FOREIGN KEY (superseded_by_intent_id) REFERENCES cp_intents(intent_id)
);
CREATE TABLE IF NOT EXISTS cp_attempts (
    attempt_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL,
    command_id TEXT NOT NULL UNIQUE,
    node_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    generation BIGINT NOT NULL,
    operation TEXT NOT NULL,
    state TEXT NOT NULL,
    failure_class TEXT,
    dispatched_at_ms BIGINT,
    acknowledged_at_ms BIGINT,
    started_at_ms BIGINT,
    finished_at_ms BIGINT,
    expires_at_ms BIGINT,
    error_code TEXT,
    error_message TEXT,
    created_at_ms BIGINT NOT NULL,
    FOREIGN KEY (intent_id) REFERENCES cp_intents(intent_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS cp_one_active_attempt
    ON cp_attempts(node_id, stream_id, generation)
    WHERE state IN ('queued', 'dispatched', 'acknowledged', 'running');
CREATE TABLE IF NOT EXISTS cp_events (
    event_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    node_id TEXT,
    stream_id TEXT,
    intent_id TEXT,
    attempt_id TEXT,
    event_type TEXT NOT NULL,
    outcome TEXT NOT NULL,
    failure_class TEXT,
    message TEXT,
    generation BIGINT,
    correlation_id TEXT,
    actor TEXT,
    occurred_at_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_audit_events (
    event_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
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
    occurred_at_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_rollouts (
    rollout_id TEXT PRIMARY KEY,
    config_version_id TEXT NOT NULL,
    state TEXT NOT NULL,
    batch_size BIGINT NOT NULL,
    current_batch BIGINT NOT NULL DEFAULT 0,
    total_targets BIGINT NOT NULL,
    actor TEXT,
    correlation_id TEXT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (config_version_id) REFERENCES cp_config_versions(config_version_id)
);
CREATE TABLE IF NOT EXISTS cp_rollout_targets (
    rollout_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    ordinal BIGINT NOT NULL,
    state TEXT NOT NULL,
    attempt_id TEXT,
    error TEXT,
    observed_config_version TEXT,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (rollout_id, node_id),
    FOREIGN KEY (rollout_id) REFERENCES cp_rollouts(rollout_id)
);
CREATE TABLE IF NOT EXISTS cp_operations (
    operation_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    operation_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_outbox (
    outbox_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    event_key TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    node_id TEXT NOT NULL,
    stream_id TEXT,
    intent_id TEXT,
    available_at_ms BIGINT NOT NULL,
    claimed_at_ms BIGINT,
    worker_id TEXT,
    processed_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS cp_intents_due ON cp_intents(state, next_retry_at_ms);
CREATE INDEX IF NOT EXISTS cp_attempts_pending ON cp_attempts(state, expires_at_ms);
CREATE INDEX IF NOT EXISTS cp_events_resource ON cp_events(node_id, stream_id, occurred_at_ms);
CREATE INDEX IF NOT EXISTS cp_audit_resource ON cp_audit_events(resource_id, occurred_at_ms);
CREATE INDEX IF NOT EXISTS cp_rollout_targets_state ON cp_rollout_targets(rollout_id, state, ordinal);
"#;

/// PostgreSQL control-plane store.
pub struct PgStore {
    pool: sqlx::PgPool,
}


fn pg_row_to_job(row: &PgRow) -> Result<JobRecord, StorageError> {
    let node_ids_json: String = row.try_get(7)?;
    let node_ids = serde_json::from_str(&node_ids_json)
        .map_err(|error| StorageError::Unsupported(format!("node_ids_json decode failed: {error}")))?;
    Ok(JobRecord {
        job_id: row.try_get(0)?,
        version: row.try_get::<i64, usize>(1)? as u64,
        spec_json: row.try_get(2)?,
        desired_state: row.try_get(3)?,
        observed_state: row.try_get(4)?,
        convergence: row.try_get(5)?,
        generation: row.try_get::<i64, usize>(6)? as u64,
        node_ids,
        checkpoint_id: row.try_get(8)?,
        last_error: row.try_get(9)?,
        updated_at_ms: row.try_get::<i64, usize>(10)? as u64,
    })
}

const JOB_COLUMNS: &str = "SELECT job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms FROM cp_jobs";

async fn pg_fetch_job(conn: &mut sqlx::PgConnection, job_id: &str) -> Result<Option<JobRecord>, StorageError> {
    let row = sqlx::query(&format!("{JOB_COLUMNS} WHERE job_id = $1"))
        .bind(job_id)
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|row| pg_row_to_job(&row)).transpose()?)
}

async fn pg_job_generation(conn: &mut sqlx::PgConnection, job_id: &str) -> Result<Option<u64>, StorageError> {
    let row: Option<(i64,)> = sqlx::query_as("SELECT generation FROM cp_jobs WHERE job_id = $1")
        .bind(job_id)
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|(generation,)| generation as u64))
}


impl PgStore {
    /// Connects, probes the server and creates the schema idempotently.
    /// A failure here fails Hub startup fast: a control plane without its
    /// storage is not partially usable.
    pub async fn open(url: &str) -> Result<Self, StorageError> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await?;
        sqlx::raw_sql(PG_DDL)
            .execute(&pool)
            .await?;
        Ok(Self { pool })
    }


    pub async fn set_desired(&self, mutation: DesiredMutation) -> Result<IntentRecord, StorageError> {
        let _ = (&mutation);
        Err(StorageError::Unsupported(format!(
            "postgres backend: set_desired is not implemented yet"
        )))
    }

    pub async fn upsert_node(&self, mutation: NodeMutation) -> Result<(), StorageError> {
        let _ = (&mutation);
        Err(StorageError::Unsupported(format!(
            "postgres backend: upsert_node is not implemented yet"
        )))
    }

    pub async fn reset_observed_cursors(&self, node_id: &str) -> Result<(), StorageError> {
        let _ = (&node_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: reset_observed_cursors is not implemented yet"
        )))
    }

    pub async fn set_node_maintenance(&self, mutation: NodeMaintenanceMutation, now_ms: u64) -> Result<bool, StorageError> {
        let _ = (&mutation, &now_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: set_node_maintenance is not implemented yet"
        )))
    }

    pub async fn get_node_maintenance(&self, node_id: &str) -> Result<Option<String>, StorageError> {
        let _ = (&node_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: get_node_maintenance is not implemented yet"
        )))
    }

    pub async fn operational_aggregates(&self, now_ms: u64) -> Result<OperationalAggregates, StorageError> {
        let _ = (&now_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: operational_aggregates is not implemented yet"
        )))
    }

    pub async fn claim_outbox(&self, worker_id: &str, now_ms: u64) -> Result<Option<OutboxRecord>, StorageError> {
        let _ = (&worker_id, &now_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: claim_outbox is not implemented yet"
        )))
    }

    pub async fn get_desired(&self, node_id: &str, stream_id: &str) -> Result<Option<DesiredRecord>, StorageError> {
        let _ = (&node_id, &stream_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: get_desired is not implemented yet"
        )))
    }

    pub async fn get_intent(&self, intent_id: &str) -> Result<Option<IntentRecord>, StorageError> {
        let _ = (&intent_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: get_intent is not implemented yet"
        )))
    }

    pub async fn list_intents(&self, node_id: Option<&str>) -> Result<Vec<IntentRecord>, StorageError> {
        let _ = (&node_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_intents is not implemented yet"
        )))
    }

    pub async fn recover_reconciliation(&self, now_ms: u64) -> Result<(), StorageError> {
        let _ = (&now_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: recover_reconciliation is not implemented yet"
        )))
    }

    pub async fn wake_node(&self, node_id: &str, now_ms: u64) -> Result<(), StorageError> {
        let _ = (&node_id, &now_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: wake_node is not implemented yet"
        )))
    }

    pub async fn list_events(&self, node_id: Option<&str>) -> Result<Vec<StoredEvent>, StorageError> {
        let _ = (&node_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_events is not implemented yet"
        )))
    }

    pub async fn prune_events(&self, retain: usize) -> Result<usize, StorageError> {
        let _ = (&retain);
        Err(StorageError::Unsupported(format!(
            "postgres backend: prune_events is not implemented yet"
        )))
    }

    pub async fn prune_operation_history(&self, older_than_ms: i64, max_retained: i64) -> Result<usize, StorageError> {
        let _ = (&older_than_ms, &max_retained);
        Err(StorageError::Unsupported(format!(
            "postgres backend: prune_operation_history is not implemented yet"
        )))
    }

    pub async fn prune_job_checkpoint_records(&self, older_than_ms: i64) -> Result<usize, StorageError> {
        let _ = (&older_than_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: prune_job_checkpoint_records is not implemented yet"
        )))
    }

    pub async fn prune_audit_events(&self, older_than_ms: i64, max_retained: i64) -> Result<usize, StorageError> {
        let _ = (&older_than_ms, &max_retained);
        Err(StorageError::Unsupported(format!(
            "postgres backend: prune_audit_events is not implemented yet"
        )))
    }

    pub async fn prune_processed_outbox(&self, older_than_ms: i64, max_retained: i64) -> Result<usize, StorageError> {
        let _ = (&older_than_ms, &max_retained);
        Err(StorageError::Unsupported(format!(
            "postgres backend: prune_processed_outbox is not implemented yet"
        )))
    }

    pub async fn prune_terminal_attempts(&self, older_than_ms: i64, max_retained: i64) -> Result<usize, StorageError> {
        let _ = (&older_than_ms, &max_retained);
        Err(StorageError::Unsupported(format!(
            "postgres backend: prune_terminal_attempts is not implemented yet"
        )))
    }

    pub async fn claim_attempt(&self, intent_id: &str) -> Result<Option<AttemptRecord>, StorageError> {
        let _ = (&intent_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: claim_attempt is not implemented yet"
        )))
    }

    pub async fn complete_attempt(&self, attempt_id: &str, state: &str, failure_class: Option<&str>) -> Result<(), StorageError> {
        let _ = (&attempt_id, &state, &failure_class);
        Err(StorageError::Unsupported(format!(
            "postgres backend: complete_attempt is not implemented yet"
        )))
    }

    pub async fn mark_attempt_dispatched(&self, attempt_id: &str, expires_at_ms: u64) -> Result<(), StorageError> {
        let _ = (&attempt_id, &expires_at_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: mark_attempt_dispatched is not implemented yet"
        )))
    }

    pub async fn expire_attempts(&self, now_ms: u64) -> Result<usize, StorageError> {
        let _ = (&now_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: expire_attempts is not implemented yet"
        )))
    }

    pub async fn record_observed(&self, mutation: ObservedMutation) -> Result<(), StorageError> {
        let _ = (&mutation);
        Err(StorageError::Unsupported(format!(
            "postgres backend: record_observed is not implemented yet"
        )))
    }

    pub async fn mark_outbox_processed(&self, outbox_id: i64, now_ms: u64) -> Result<(), StorageError> {
        let _ = (&outbox_id, &now_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: mark_outbox_processed is not implemented yet"
        )))
    }

    pub async fn record_audit(&self, record: AuditRecord) -> Result<i64, StorageError> {
        let _ = (&record);
        Err(StorageError::Unsupported(format!(
            "postgres backend: record_audit is not implemented yet"
        )))
    }

    pub async fn list_audit(&self, resource_id: Option<&str>) -> Result<Vec<AuditRecord>, StorageError> {
        let _ = (&resource_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_audit is not implemented yet"
        )))
    }

    pub async fn create_rollout(&self, rollout: RolloutRecord, targets: Vec<RolloutTargetRecord>) -> Result<(), StorageError> {
        let _ = (&rollout, &targets);
        Err(StorageError::Unsupported(format!(
            "postgres backend: create_rollout is not implemented yet"
        )))
    }

    pub async fn create_rollout_with_content(&self, rollout: RolloutRecord, targets: Vec<RolloutTargetRecord>, content: &str, created_by: Option<&str>) -> Result<(), StorageError> {
        let _ = (&rollout, &targets, &content, &created_by);
        Err(StorageError::Unsupported(format!(
            "postgres backend: create_rollout_with_content is not implemented yet"
        )))
    }

    pub async fn get_rollout(&self, rollout_id: &str) -> Result<Option<RolloutRecord>, StorageError> {
        let _ = (&rollout_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: get_rollout is not implemented yet"
        )))
    }

    pub async fn list_rollout_targets(&self, rollout_id: &str) -> Result<Vec<RolloutTargetRecord>, StorageError> {
        let _ = (&rollout_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_rollout_targets is not implemented yet"
        )))
    }

    pub async fn update_rollout(&self, rollout_id: &str, state: &str, current_batch: u32, updated_at_ms: u64) -> Result<(), StorageError> {
        let _ = (&rollout_id, &state, &current_batch, &updated_at_ms);
        Err(StorageError::Unsupported(format!(
            "postgres backend: update_rollout is not implemented yet"
        )))
    }

    pub async fn update_rollout_target(&self, update: RolloutTargetUpdate) -> Result<(), StorageError> {
        let _ = (&update);
        Err(StorageError::Unsupported(format!(
            "postgres backend: update_rollout_target is not implemented yet"
        )))
    }

    pub async fn get_config_version_content(&self, config_version_id: &str) -> Result<Option<String>, StorageError> {
        let _ = (&config_version_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: get_config_version_content is not implemented yet"
        )))
    }

    pub async fn recover_rollouts(&self, ) -> Result<Vec<RolloutRecord>, StorageError> {
        Err(StorageError::Unsupported(format!(
            "postgres backend: recover_rollouts is not implemented yet"
        )))
    }

    pub async fn list_rollouts(&self, ) -> Result<Vec<RolloutRecord>, StorageError> {
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_rollouts is not implemented yet"
        )))
    }

    pub async fn upsert_operation(&self, operation: PersistedOperation) -> Result<(), StorageError> {
        let _ = (&operation);
        Err(StorageError::Unsupported(format!(
            "postgres backend: upsert_operation is not implemented yet"
        )))
    }

    pub async fn get_operation(&self, operation_id: &str) -> Result<Option<PersistedOperation>, StorageError> {
        let _ = (&operation_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: get_operation is not implemented yet"
        )))
    }

    pub async fn list_operations(&self, node_id: Option<&str>) -> Result<Vec<PersistedOperation>, StorageError> {
        let _ = (&node_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_operations is not implemented yet"
        )))
    }

    pub async fn list_job_start_operations(&self, resource_id: &str) -> Result<Vec<PersistedOperation>, StorageError> {
        let _ = (&resource_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_job_start_operations is not implemented yet"
        )))
    }

    pub async fn upsert_job(&self, mut job: JobRecord) -> Result<JobRecord, StorageError> {
        let mut tx = self.pool.begin().await?;
        let current_generation = pg_job_generation(&mut tx, &job.job_id).await?;
        job.generation = current_generation
            .map(|generation| generation.saturating_add(1))
            .unwrap_or_else(|| job.generation.max(1));
        let node_ids = serde_json::to_string(&job.node_ids)
            .map_err(|error| StorageError::Unsupported(format!("node_ids encode failed: {error}")))?;
        sqlx::query(
            "INSERT INTO cp_jobs (job_id, version, spec_json, desired_state, observed_state, convergence, generation, node_ids_json, checkpoint_id, last_error, updated_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT(job_id) DO UPDATE SET version=excluded.version, spec_json=excluded.spec_json, desired_state=excluded.desired_state, observed_state=excluded.observed_state, convergence=excluded.convergence, generation=excluded.generation, node_ids_json=excluded.node_ids_json, checkpoint_id=excluded.checkpoint_id, last_error=excluded.last_error, updated_at_ms=excluded.updated_at_ms",
        )
        .bind(&job.job_id)
        .bind(job.version as i64)
        .bind(&job.spec_json)
        .bind(&job.desired_state)
        .bind(&job.observed_state)
        .bind(&job.convergence)
        .bind(job.generation as i64)
        .bind(&node_ids)
        .bind(&job.checkpoint_id)
        .bind(&job.last_error)
        .bind(job.updated_at_ms as i64)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(job)
    }

    pub async fn update_job_with_expected_generation(&self, mut job: JobRecord, expected_generation: u64) -> Result<JobRecord, StorageError> {
        let mut tx = self.pool.begin().await?;
        job.generation = expected_generation.saturating_add(1);
        let node_ids = serde_json::to_string(&job.node_ids)
            .map_err(|error| StorageError::Unsupported(format!("node_ids encode failed: {error}")))?;
        // The recovery pointer (checkpoint_id) is deliberately NOT in the SET
        // list: it is owned by the checkpoint path and preserving it cannot
        // regress recovery (same rationale as the SQLite implementation).
        let changed = sqlx::query(
            "UPDATE cp_jobs SET version=$1, spec_json=$2, desired_state=$3, observed_state=$4, convergence=$5, generation=$6, node_ids_json=$7, last_error=$8, updated_at_ms=$9 WHERE job_id=$10 AND generation=$11",
        )
        .bind(job.version as i64)
        .bind(&job.spec_json)
        .bind(&job.desired_state)
        .bind(&job.observed_state)
        .bind(&job.convergence)
        .bind(job.generation as i64)
        .bind(&node_ids)
        .bind(&job.last_error)
        .bind(job.updated_at_ms as i64)
        .bind(&job.job_id)
        .bind(expected_generation as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if changed == 0 {
            let current = pg_job_generation(&mut tx, &job.job_id).await?;
            return Err(StorageError::GenerationConflict {
                expected: expected_generation,
                current: current.unwrap_or(0),
            });
        }
        let stored = pg_fetch_job(&mut tx, &job.job_id).await?;
        tx.commit().await?;
        Ok(stored.unwrap_or(job))
    }

    pub async fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>, StorageError> {
        let mut tx = self.pool.begin().await?;
        let job = pg_fetch_job(&mut tx, job_id).await?;
        tx.commit().await?;
        Ok(job)
    }

    pub async fn upsert_job_version(&self, record: JobVersionRecord) -> Result<(), StorageError> {
        let _ = (&record);
        Err(StorageError::Unsupported(format!(
            "postgres backend: upsert_job_version is not implemented yet"
        )))
    }

    pub async fn list_job_versions(&self, job_id: &str) -> Result<Vec<JobVersionRecord>, StorageError> {
        let _ = (&job_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_job_versions is not implemented yet"
        )))
    }

    pub async fn list_jobs(&self, ) -> Result<Vec<JobRecord>, StorageError> {
        let rows = sqlx::query(&format!("{JOB_COLUMNS} ORDER BY updated_at_ms DESC, job_id LIMIT 4096"))
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| pg_row_to_job(&row))
            .collect::<Result<Vec<_>, StorageError>>()?)
    }

    pub async fn update_job(&self, job_id: &str, desired_state: Option<&str>, observed_state: Option<&str>, convergence: Option<&str>, generation: Option<u64>, checkpoint_id: Option<&str>, last_error: Option<&str>) -> Result<Option<JobRecord>, StorageError> {
        let _ = (&job_id, &desired_state, &observed_state, &convergence, &generation, &checkpoint_id, &last_error);
        Err(StorageError::Unsupported(format!(
            "postgres backend: update_job is not implemented yet"
        )))
    }

    pub async fn update_job_observation(&self, job_id: &str, observed_state: &str, convergence: &str, generation: u64, expected_generation: u64, checkpoint_id: Option<&str>, last_error: Option<&str>) -> Result<Option<JobRecord>, StorageError> {
        let _ = (&job_id, &observed_state, &convergence, &generation, &expected_generation, &checkpoint_id, &last_error);
        Err(StorageError::Unsupported(format!(
            "postgres backend: update_job_observation is not implemented yet"
        )))
    }

    pub async fn update_job_desired_state(&self, job_id: &str, desired_state: &str, expected_generation: u64) -> Result<Option<JobRecord>, StorageError> {
        let _ = (&job_id, &desired_state, &expected_generation);
        Err(StorageError::Unsupported(format!(
            "postgres backend: update_job_desired_state is not implemented yet"
        )))
    }

    pub async fn upsert_job_checkpoint(&self, record: JobCheckpointRecord) -> Result<(), StorageError> {
        let _ = (&record);
        Err(StorageError::Unsupported(format!(
            "postgres backend: upsert_job_checkpoint is not implemented yet"
        )))
    }

    pub async fn list_job_checkpoints(&self, job_id: &str) -> Result<Vec<JobCheckpointRecord>, StorageError> {
        let _ = (&job_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: list_job_checkpoints is not implemented yet"
        )))
    }

    pub async fn delete_job_checkpoint(&self, job_id: &str, checkpoint_id: &str) -> Result<(), StorageError> {
        let _ = (&job_id, &checkpoint_id);
        Err(StorageError::Unsupported(format!(
            "postgres backend: delete_job_checkpoint is not implemented yet"
        )))
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;

    fn pg_url() -> Option<String> {
        std::env::var("ARKFLOW_TEST_POSTGRES_URL").ok()
    }

    async fn open_or_skip() -> Option<PgStore> {
        let url = pg_url()?;
        Some(PgStore::open(&url).await.expect("pg open"))
    }

    fn sample_job(job_id: &str) -> JobRecord {
        JobRecord {
            job_id: job_id.to_string(),
            version: 3,
            spec_json: "{}".to_string(),
            desired_state: "running".to_string(),
            observed_state: "starting".to_string(),
            convergence: "converging".to_string(),
            generation: 0,
            node_ids: vec!["node-a".to_string()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 1_000,
        }
    }

    #[tokio::test]
    async fn jobs_group_smoke() {
        let Some(store) = open_or_skip().await else {
            eprintln!("skipping: ARKFLOW_TEST_POSTGRES_URL not set");
            return;
        };
        let job = sample_job("smoke-job");
        let stored = store.upsert_job(job).await.expect("upsert");
        assert_eq!(stored.generation, 1, "first write bumps generation to 1");
        assert_eq!(stored.node_ids, vec!["node-a".to_string()]);

        // Second upsert bumps generation again (reads 1, writes 2).
        let stored = store
            .upsert_job(sample_job("smoke-job"))
            .await
            .expect("upsert 2");
        assert_eq!(stored.generation, 2);

        let fetched = store.get_job("smoke-job").await.expect("get").expect("exists");
        assert_eq!(fetched.generation, 2);
        assert_eq!(fetched.desired_state, "running");

        let listed = store.list_jobs().await.expect("list");
        assert!(listed.iter().any(|job| job.job_id == "smoke-job"));

        // CAS update with the correct expected generation succeeds and bumps.
        let mut cas = stored.clone();
        cas.desired_state = "stopped".to_string();
        let updated = store
            .update_job_with_expected_generation(cas, 2)
            .await
            .expect("cas update");
        assert_eq!(updated.generation, 3);
        assert_eq!(updated.desired_state, "stopped");

        // CAS with a stale generation conflicts.
        let mut stale = stored;
        stale.desired_state = "running".to_string();
        let error = store
            .update_job_with_expected_generation(stale, 2)
            .await
            .expect_err("stale generation must conflict");
        assert!(error.to_string().contains("generation conflict"), "{error}");
    }
}
